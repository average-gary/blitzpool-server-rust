// SPDX-License-Identifier: AGPL-3.0-or-later

//! `DistributionBuilder` — production-side wrapper around
//! `bp_pplns::build_coinbase_distribution`.
//!
//! Reads the current window from Redis (per-address aggregate hash),
//! loads the open-balance ledger rows from Postgres, calls the
//! pure-math distribution builder, then persists a snapshot into
//! `pplns:snapshot` so [`crate::ledger::apply_distribution`] can
//! replay the same distribution deterministically when the block is
//! found.
//!
//! Two layers of `bp_inflight_cache::InflightResultCache` (30s TTL by
//! default):
//!
//! - **Built distributions**, keyed by `(block_reward_sats, finder)` —
//!   concurrent callers for the same key share one computation. The
//!   finder half is `None` unless a finder bonus is configured, because
//!   only then does the build depend on who is asking.
//! - **Window+ledger inputs**, keyed by `()` — concurrent callers for
//!   *different* rewards still share the Redis window read and the
//!   Postgres ledger query, since neither depends on the reward.
//!
//! The second layer is what keeps a burst of unrelated callers cheap.
//! The per-reward layer alone never dedups them: SV1/SV2 job builds
//! arrive with whatever template revenue their stream currently holds,
//! so N simultaneous callers at a chain-tip change can mean N distinct
//! keys and, without the inputs layer, N window reads plus N ledger
//! queries in the same few milliseconds.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bp_coinbase_snapshot::{build_and_snapshot, BuildRequest};
use bp_common::{AddressId, Sats};
use bp_db::{find_pplns_balances_with_open_balance, PplnsBalanceRow};
use bp_pplns::{WeightBuildError, WeightDistribution};
use sqlx::PgPool;
use thiserror::Error;
use tracing::error;

use crate::autoscale::LiveBudget;
use crate::window::{WindowError, WindowStore};
use bp_coinbase_snapshot::share_map_from_redis_hash;
use bp_inflight_cache::InflightResultCache;

/// Default cache TTL for `DistributionBuilder::build` (30 s).
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(30);

/// Errors surfaced by [`DistributionBuilder::build`].
///
/// `Default` is required so the in-flight cache can construct a
/// "leader-dropped" placeholder if the leader's compute task panics
/// (rare; surfaces as a CRITICAL operational event the caller logs).
#[derive(Debug, Default, Error)]
pub enum DistributionError {
    /// Placeholder used by the in-flight cache when the leader's
    /// compute task drops without publishing. Reaching this means a
    /// panic happened mid-build; the caller's recovery path is to
    /// retry the call.
    #[default]
    #[error("inflight leader dropped without publishing — retry")]
    LeaderDropped,
    #[error("window read: {0}")]
    Window(#[from] WindowError),
    /// The shared window+ledger load failed. Carries the underlying
    /// error's message rather than the error itself: the inputs cache
    /// hands back an `Arc<DistributionError>` shared across all waiters,
    /// which can't be unwrapped back into an owned error.
    #[error("distribution inputs: {0}")]
    Inputs(String),
    /// The weight model has no distribution without a pool-output
    /// recipient — `pay_P` is structural (SV2 ext 0x0003 §4).
    #[error("no fee address configured — the weight model requires the pool-output recipient")]
    NoFeeAddress,
    #[error("weight build: {0}")]
    WeightBuild(#[from] WeightBuildError),
}

/// The part of a distribution build that does NOT depend on
/// `block_reward_sats`: the current payout window and the open-balance
/// ledger, both already sanitized to parseable payout addresses.
///
/// Every concurrent build shares these — the weights are a property of
/// the window, not of the reward. Only the scaling to a concrete reward
/// (and the dust/trim decisions that follow from it) is per-build, which
/// is why this is cached separately: N concurrent builds for N distinct
/// rewards cost one Redis window read and one Postgres ledger query, not
/// N of each.
#[derive(Clone, Debug, Default)]
pub struct DistributionInputs {
    pub address_shares: HashMap<AddressId, f64>,
    pub balances: HashMap<AddressId, Sats>,
}

/// Result of one distribution build. Cheap to clone-via-Arc because
/// the in-flight cache shares `Arc<DistributionResult>` across waiters.
#[derive(Clone, Debug)]
pub struct DistributionResult {
    /// The weight-native distribution (SV2 ext 0x0003 model): entries
    /// with settlement inputs + published wire weights, `weight_P`,
    /// fee, dust limits, and the weights fingerprint. Every consumer
    /// derives concrete satoshis from it via the §4 formula —
    /// [`WeightDistribution::payout_entries_at`] for the pool's own
    /// templates, the JDP publisher for `SetPayoutDistribution`.
    pub distribution: WeightDistribution,
    /// Did the schema-2 snapshot under `distribution.fingerprint`
    /// actually get written?
    ///
    /// `false` means this build succeeded but its snapshot did not
    /// land, so the fingerprint names a key that does not exist. The
    /// distribution is still correct and still becomes a coinbase —
    /// failing the build over a lost snapshot would leave every miner in
    /// it without a job. But a caller that promises a found block will be
    /// booked automatically MUST NOT make that promise on a `false`.
    pub snapshot_written: bool,
}

impl DistributionResult {
    /// The snapshot key this build landed under (see
    /// [`bp_share::weights_fingerprint_from_parts`]). Threaded onto
    /// every job built from this distribution — a found block carries
    /// it back so settlement can read exactly these inputs.
    pub fn payouts_fingerprint(&self) -> [u8; 32] {
        self.distribution.fingerprint
    }
}

/// Knobs for the distribution path. Built from
/// [`crate::config::PplnsEngineConfig`] at engine startup. Most fields are
/// static; `coinbase_weight_budget` is a live [`LiveBudget`] handle so the
/// autoscaler can change it at runtime — every build reads the current value.
#[derive(Clone, Debug)]
pub struct DistributionConfig {
    pub fee_address: Option<AddressId>,
    pub fee_percent: f64,
    pub min_payout_sats: Sats,
    /// Live, runtime-mutable coinbase weight budget shared with the autoscaler.
    pub coinbase_weight_budget: LiveBudget,
    /// Finder bonus in parts-per-million of the miners' cut; `0` disables.
    /// Boot-validated against [`bp_pplns::MAX_FINDER_BONUS_PPM`].
    pub finder_bonus_ppm: u32,
    pub snapshot_ttl_secs: u32,
}

impl DistributionConfig {
    pub fn from_engine_config(cfg: &crate::config::PplnsEngineConfig) -> Self {
        Self {
            fee_address: cfg.fee_address.clone(),
            fee_percent: cfg.fee_percent,
            min_payout_sats: cfg.min_payout_sats,
            coinbase_weight_budget: LiveBudget::new(cfg.coinbase_weight_budget),
            finder_bonus_ppm: cfg.finder_bonus_ppm,
            snapshot_ttl_secs: cfg.snapshot_ttl_secs,
        }
    }

    /// Does a build have to name the finder?
    ///
    /// The one place this question is answered, because both halves of
    /// the feature turn on it: whether the cache key needs the finder,
    /// and whether JDP can still serve one pool-wide distribution to
    /// every client. Two independent `> 0` checks would let those halves
    /// drift into a per-finder cache serving a finder-blind build, or
    /// worse, a pool-wide publish of a build that names one.
    pub fn finder_bonus_active(&self) -> bool {
        self.finder_bonus_ppm > 0
    }
}

/// Cache key for a built distribution.
///
/// The `Option<String>` is the prospective finder, and it is `None`
/// exactly when `finder_bonus_ppm == 0`. With the bonus off the build
/// does not depend on who is asking, so every connection at one revenue
/// shares a single entry — the pre-bonus behaviour, preserved rather
/// than paid for.
///
/// With the bonus on the finder is load-bearing for isolation, not
/// speed: it guarantees miner X can never be served the distribution
/// built naming miner Y as finder. Drop it and whichever miner's request
/// arrives first mints the entry, every other miner in the window hashes
/// a job that pays *that* miner the bonus, and nothing errors — the
/// booking is internally consistent, so it is a silent misdirection.
///
/// Mirrors `bp_group_solo_engine::distribution::CacheKey`, which carries
/// its finder unconditionally because a Group-Solo build always names
/// one.
type CacheKey = (u64, Option<String>);

/// Orchestrator. Cheap to clone (each field is either an `Arc`-cheap
/// handle or `Clone`-cheap config).
#[derive(Clone)]
pub struct DistributionBuilder {
    pool: PgPool,
    window: WindowStore,
    config: DistributionConfig,
    cache: InflightResultCache<CacheKey, DistributionResult, DistributionError>,
    /// Reward-independent window+ledger inputs, shared across every
    /// concurrent build. Keyed by `()` — there is exactly one payout
    /// window — so the cache degenerates to "one load per invalidation
    /// epoch, deduped across all in-flight builds".
    inputs_cache: InflightResultCache<(), DistributionInputs, DistributionError>,
    /// How often the window+ledger load actually ran. Observability, and
    /// the assertion hook for the dedup tests.
    inputs_loads: Arc<AtomicU64>,
}

impl DistributionBuilder {
    pub fn new(pool: PgPool, window: WindowStore, config: DistributionConfig) -> Self {
        Self::with_cache_ttl(pool, window, config, DEFAULT_CACHE_TTL)
    }

    pub fn with_cache_ttl(
        pool: PgPool,
        window: WindowStore,
        config: DistributionConfig,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            pool,
            window,
            config,
            cache: InflightResultCache::new(cache_ttl),
            inputs_cache: InflightResultCache::new(cache_ttl),
            inputs_loads: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Number of window+ledger loads performed so far. Under a burst of
    /// concurrent builds this stays far below the build count — that is
    /// the whole point of the inputs cache.
    pub fn inputs_loads(&self) -> u64 {
        self.inputs_loads.load(Ordering::Relaxed)
    }

    /// Cached-plus-in-flight distribution count. One entry per distinct
    /// cache key since the last invalidation — so with the bonus off this
    /// is one per template revenue, and with it on, one per connection
    /// served that revenue. That difference IS the footprint the finder
    /// bonus adds, so it is exposed for the scale sweep to measure rather
    /// than left to argument.
    pub fn cache_entries(&self) -> usize {
        self.cache.len()
    }

    /// Build the current PPLNS weight distribution against
    /// `reference_revenue_sats` (the pool's current template value —
    /// the projection base for balance boosts).
    ///
    /// `finder` is the miner this build is *for* — the prospective finder
    /// of a block mined on a job carrying it. It is ignored unless
    /// `finder_bonus_ppm > 0`, so a pool that never configured a bonus
    /// keeps the pre-bonus behaviour exactly: one shared build per
    /// revenue, and the caller may pass `None`.
    ///
    /// With the bonus on there is no such thing as a finder-independent
    /// PPLNS distribution: the finder's score weight differs, so the
    /// fingerprint differs, so each connection mints its own snapshot.
    /// That is the cost the bonus buys, and it is the reason `finder` is
    /// a required parameter rather than an `Option` the caller may
    /// forget — passing `None` while the bonus is on is a build that
    /// silently pays nobody a bonus.
    ///
    /// Concurrent callers for the same key share one compute; callers for
    /// *different* keys still share the window+ledger read.
    pub async fn build(
        &self,
        reference_revenue_sats: u64,
        finder: Option<&AddressId>,
    ) -> Result<Arc<DistributionResult>, Arc<DistributionError>> {
        // The bonus being off makes the finder irrelevant to the RESULT,
        // so it must also be irrelevant to the KEY — otherwise every
        // connection mints its own identical entry and the O(N²)
        // snapshot cost lands on pools that never asked for a bonus.
        let finder = self
            .config
            .finder_bonus_active()
            .then_some(finder)
            .flatten();
        let key: CacheKey = (
            reference_revenue_sats,
            finder.map(|f| f.as_str().to_string()),
        );
        let finder = finder.cloned();
        let pool = self.pool.clone();
        let window = self.window.clone();
        let window_for_inputs = self.window.clone();
        let config = self.config.clone();
        let inputs_cache = self.inputs_cache.clone();
        let inputs_loads = self.inputs_loads.clone();
        self.cache
            .get_or_compute(key, || async move {
                let inputs = inputs_cache
                    .get_or_compute((), || async move {
                        inputs_loads.fetch_add(1, Ordering::Relaxed);
                        load_inputs(&pool, &window_for_inputs).await
                    })
                    .await
                    .map_err(|e| DistributionError::Inputs(e.to_string()))?;
                // No bootstrap claimant even when the finder is known: a
                // claimant takes the WHOLE block on an empty window,
                // which is a far bigger promise than a bonus, and it is
                // gated on `NoScoredMiners` rather than offered
                // speculatively. See [`Self::build_bootstrap`].
                build_from_inputs(
                    &inputs,
                    &window,
                    &config,
                    reference_revenue_sats,
                    None,
                    finder.as_ref(),
                )
                .await
            })
            .await
    }

    /// The empty-window answer for ONE asking miner.
    ///
    /// [`Self::build`] cannot give it: its result is shared across every
    /// PPLNS connection (keyed by revenue only), and a distribution that
    /// names one miner as the sole claimant must never be handed to
    /// another. So the bootstrap build is per-miner and deliberately
    /// UNCACHED at the distribution layer — it only runs while the window
    /// holds no scored miner at all, which lasts until that miner's first
    /// accepted share.
    ///
    /// The window+ledger `inputs_cache` IS still shared, because the read
    /// does not depend on the claimant.
    ///
    /// Call this only after [`Self::build`] answered
    /// [`bp_pplns::WeightBuildError::NoScoredMiners`]. Calling it
    /// unconditionally would hand a miner the whole block on a window
    /// that has other claimants in it.
    pub async fn build_bootstrap(
        &self,
        reference_revenue_sats: u64,
        claimant: &AddressId,
    ) -> Result<Arc<DistributionResult>, Arc<DistributionError>> {
        let pool = self.pool.clone();
        let window_for_inputs = self.window.clone();
        let inputs_loads = self.inputs_loads.clone();
        let inputs = self
            .inputs_cache
            .get_or_compute((), || async move {
                inputs_loads.fetch_add(1, Ordering::Relaxed);
                load_inputs(&pool, &window_for_inputs).await
            })
            .await
            .map_err(|e| Arc::new(DistributionError::Inputs(e.to_string())))?;
        // The claimant IS the prospective finder — same miner, same
        // reason they were named. Passing them as finder too costs
        // nothing and keeps the bonus from being the one thing that
        // silently does not apply on a fresh window: with an empty
        // window the claimant holds the whole score space, so the bonus
        // resolves to a boost on a total they already own and the payout
        // is unchanged. It matters for the FINGERPRINT — the snapshot
        // then records the same bonus inputs every other build on this
        // pool records, so settlement reads one shape, not two.
        let finder = self.config.finder_bonus_active().then_some(claimant);
        build_from_inputs(
            &inputs,
            &self.window,
            &self.config,
            reference_revenue_sats,
            Some(claimant),
            finder,
        )
        .await
        .map(Arc::new)
        .map_err(Arc::new)
    }

    /// Drops the built distributions AND the shared window+ledger
    /// inputs. Both must go: the callers are state-change events (a
    /// share landed, the budget moved), and keeping stale inputs would
    /// just rebuild the same stale distribution.
    ///
    /// The only invalidation there is. A per-reward variant used to sit
    /// beside this one with no production caller — every real path
    /// (share-record, settlement, the autoscaler) already reached for
    /// this. Under a per-finder key it would have become actively wrong:
    /// one reward now maps to one entry per connected miner, and
    /// everything that invalidates does so because the WINDOW changed,
    /// which staled all of them equally. Dropping one finder's entry and
    /// leaving the rest is not a cheaper version of that — it is a
    /// silent partial invalidation.
    pub fn invalidate_all(&self) {
        self.cache.clear();
        self.inputs_cache.clear();
    }

    /// The live coinbase-weight-budget handle this builder reads per build.
    /// The autoscaler driver clones it to observe pressure + write new values.
    pub fn live_budget(&self) -> LiveBudget {
        self.config.coinbase_weight_budget.clone()
    }

    /// Is a finder bonus configured? See
    /// [`DistributionConfig::finder_bonus_active`].
    pub fn finder_bonus_active(&self) -> bool {
        self.config.finder_bonus_active()
    }
}

// ── Internals ────────────────────────────────────────────────────────

/// Steps 1-3: the reward-independent half of a build — read the window
/// and the ledger, sanitize both. Shared by every concurrent build via
/// [`DistributionBuilder::inputs_cache`].
///
/// **The two reads fail differently, on purpose.**
///
/// The window IS the shares. Without it there is nothing to distribute,
/// nothing may be invented, and the caller must serve no job at all
/// (`bp_mining_job::ResolvedPayouts::none`) — so a window error
/// propagates.
///
/// The ledger is a set of PROMISES on top of that split, and a promise
/// that cannot be read this second is not a promise that is lost. It
/// still sits in `pplns_balance`, and a build without it is not
/// approximate: every entry carries `balance_sats = 0`, so `X = 0`, no
/// wire weight is boosted, and settlement recomputes the same zeros from
/// the snapshot and books `delta ≈ 0`. The standing balances are not
/// touched and are paid out of the next block instead. A block found
/// during the outage pays correctly by score and is fully bookable.
///
/// That is worth the degradation because the alternative is severe and
/// pool-wide: `record_share` writes only to Redis, so during a Postgres
/// outage the share accounting is intact and every miner keeps earning —
/// failing the build would blank the whole pool's jobs over a fault that
/// costs nothing but a one-block delay in repayments. It is also not a
/// new code path in the math: Group-Solo passes an empty balance map on
/// every single build.
async fn load_inputs(
    pool: &PgPool,
    window: &WindowStore,
) -> Result<DistributionInputs, DistributionError> {
    // 1. Read window aggregate from Redis (HashMap<String, f64>). Hard.
    let window_raw = window.read_window_by_address().await?;

    // 2. Read open-balance ledger rows from PG. Soft — see the docs above.
    let balances = match find_pplns_balances_with_open_balance(pool).await {
        Ok(rows) => open_balance_rows_to_balance_map(&rows),
        Err(err) => {
            error!(
                %err,
                "pplns distribution: ledger unreadable — building this distribution by SCORE \
                 ONLY. Standing balances are untouched and are repaid from a later block; a \
                 block found meanwhile still pays correctly and books. Fix the database."
            );
            HashMap::new()
        }
    };

    // 3. Convert to bp_pplns inputs. Window addresses are raw strings —
    //    ones that fail `AddressId` validation are skipped with a warn
    //    (an upstream bug could have pushed an invalid address into
    //    Redis; better to skip its share than fail the distribution).
    //    Dropping addresses that parse but are not usable payout scripts
    //    happens in the shared build.
    Ok(DistributionInputs {
        address_shares: share_map_from_redis_hash(
            &window_raw,
            "pplns distribution: skipping invalid address in window — likely from a buggy upstream",
        ),
        balances,
    })
}

/// Steps 4-5: project the shared inputs into the weight model against
/// the reference revenue, persist the schema-2 snapshot.
async fn build_from_inputs(
    inputs: &DistributionInputs,
    window: &WindowStore,
    config: &DistributionConfig,
    reference_revenue_sats: u64,
    bootstrap_claimant: Option<&AddressId>,
    finder: Option<&AddressId>,
) -> Result<DistributionResult, DistributionError> {
    // 4-5. Sanitize, project onto weights, persist the snapshot — the
    //      one path both payout engines share. The *live* budget is read
    //      here so a runtime autoscaler change takes effect on the next
    //      build.
    let fee_address = config
        .fee_address
        .as_ref()
        .ok_or(DistributionError::NoFeeAddress)?;
    let mut conn = window.connection_for_snapshot();
    let built = build_and_snapshot(
        BuildRequest {
            address_shares: inputs.address_shares.clone(),
            balances: inputs.balances.clone(),
            fee_address,
            fee_percent: config.fee_percent,
            min_payout_sats: config.min_payout_sats,
            coinbase_weight_budget: config.coinbase_weight_budget.get(),
            // Both or neither. `finder_address: None` with a non-zero
            // ppm builds no bonus at all (the builder's `if let Some`
            // guard), and a finder with a zero ppm is inert — either way
            // the operator's configured bonus silently does not exist.
            // Gating both on the one `finder_bonus_active()` predicate
            // makes that pair unfalsifiable here.
            finder_bonus_ppm: if finder.is_some() {
                config.finder_bonus_ppm
            } else {
                0
            },
            finder_address: finder,
            reference_revenue_sats,
            // PPLNS keeps a withheld miner's share inside the miners' cut
            // and remembers what it owes them in `pplns_balance`. That is
            // the point of the mode — a small miner accumulates across
            // blocks until they clear `min_payout` instead of forfeiting.
            withheld_value: bp_pplns::WithheldValue::ToOtherMiners,
            bootstrap_claimant,
            scope: "pplns",
        },
        &mut conn,
        crate::window::snapshot_key_for,
        config.snapshot_ttl_secs,
    )
    .await?;

    // Feed the autoscaler with this build's blockspace pressure.
    config
        .coinbase_weight_budget
        .record_sample(built.distribution.budget_telemetry);

    Ok(DistributionResult {
        distribution: built.distribution,
        snapshot_written: built.snapshot_written,
    })
}

fn open_balance_rows_to_balance_map(rows: &[PplnsBalanceRow]) -> HashMap<AddressId, Sats> {
    let mut out = HashMap::with_capacity(rows.len());
    for row in rows {
        out.insert(row.address.clone(), row.balance_sats);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bp_pplns::{build_weight_distribution, WeightDistributionInput};

    #[test]
    fn distribution_config_from_engine_config_carries_fields() {
        let engine_cfg = crate::config::PplnsEngineConfig {
            fee_address: Some(AddressId::new("bc1qfee0000000000000000000000000").unwrap()),
            fee_percent: 2.5,
            coinbase_weight_budget: 60_000,
            snapshot_ttl_secs: 1800,
            ..crate::config::PplnsEngineConfig::default()
        };

        let dist_cfg = DistributionConfig::from_engine_config(&engine_cfg);
        assert_eq!(
            dist_cfg.fee_address.as_ref().unwrap().as_str(),
            "bc1qfee0000000000000000000000000"
        );
        assert!((dist_cfg.fee_percent - 2.5).abs() < 1e-9);
        assert_eq!(dist_cfg.coinbase_weight_budget.get(), 60_000);
        assert_eq!(dist_cfg.snapshot_ttl_secs, 1800);
    }

    #[test]
    fn open_balance_rows_to_balance_map_preserves_signed_values() {
        let rows = vec![
            PplnsBalanceRow {
                address: AddressId::new("bc1qcredit").unwrap(),
                balance_sats: Sats(5_000),
                total_paid_sats: Sats(100_000),
                updated_at: 0,
                last_accepted_share_at: None,
            },
            PplnsBalanceRow {
                address: AddressId::new("bc1qdebit").unwrap(),
                balance_sats: Sats(-5_000),
                total_paid_sats: Sats(50_000),
                updated_at: 0,
                last_accepted_share_at: None,
            },
        ];
        let map = open_balance_rows_to_balance_map(&rows);
        assert_eq!(map.len(), 2);
        assert_eq!(map[&AddressId::new("bc1qcredit").unwrap()].0, 5_000);
        assert_eq!(map[&AddressId::new("bc1qdebit").unwrap()].0, -5_000);
    }

    #[test]
    fn distribution_result_is_cloneable() {
        // The InflightResultCache shares Arc<DistributionResult> across
        // waiters; verify the type composes.
        let shares = HashMap::from([(
            AddressId::new("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4").unwrap(),
            1.0,
        )]);
        let balances = HashMap::new();
        let fee = AddressId::new("3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy").unwrap();
        let distribution = build_weight_distribution(WeightDistributionInput {
            address_shares: &shares,
            balances: &balances,
            fee_percent: 1.5,
            fee_address: &fee,
            coinbase_weight_budget: 50_000,
            min_payout_sats: Some(Sats(5_000)),
            finder_bonus_ppm: 0,
            finder_address: None,
            reference_revenue_sats: 312_500_000,
            withheld_value: bp_pplns::WithheldValue::ToOtherMiners,
        })
        .unwrap();
        let result = DistributionResult {
            distribution,
            snapshot_written: true,
        };
        let cloned = result.clone();
        assert_eq!(cloned.distribution.reference_revenue_sats, 312_500_000);
        assert_eq!(cloned.payouts_fingerprint(), result.payouts_fingerprint());
    }
}
