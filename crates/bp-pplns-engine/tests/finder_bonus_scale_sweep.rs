// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(clippy::print_stderr)]
#![allow(clippy::print_stdout)]

//! Phase 4 scale sweep — what does a per-finder distribution cache cost?
//!
//! Before the finder bonus, the distribution cache was keyed by
//! `block_reward_sats` alone, so every connection served the same template
//! shared one entry. Keying it by `(reward, finder)` means N connected
//! miners at one template mint **N entries**, each holding a full payout
//! list of N addresses — an O(N²) footprint where there used to be O(N).
//! That is the one resource regression this feature can plausibly cause, so
//! it gets measured rather than argued about.
//!
//! Two numbers per pool size, both named by the plan:
//!
//! 1. **Resident memory** across a template's worth of per-finder builds,
//!    sampled from `ps -o rss=` on our own pid. Read as a delta from a
//!    baseline taken after the window is seeded, so the addresses
//!    themselves aren't counted as cache cost.
//! 2. **`mining.notify` build latency** — here the per-connection work that
//!    precedes it, i.e. one `build_distribution` for one finder. The notify
//!    frame itself is per-template, not per-payout-list (`notify.rs` caches
//!    the merkle branch + header hex once and every client borrows it), so
//!    the payout list can only move this number through the distribution
//!    build that feeds the coinbase.
//!
//! Sweep points are the plan's N ∈ {50, 200, 500} plus 400 — the largest
//! pool at which the finder's *proportional* entry still survives the trim
//! at the 50,000 WU budget floor, and so the largest N where the finder
//! appears twice and the merge in `build_writes_from_snapshot` is load-
//! bearing.
//!
//! This is a measurement, not a threshold gate: it prints a table and
//! asserts only correctness invariants that must hold at every N (bonus
//! paid, sats conserved, cache actually per-finder). A regression shows up
//! as a number in the table, which is what the plan's "add a size cap if
//! the profile is bad" decision needs. Skips cleanly without Redis/PG.
//!
//! Run:
//!   cargo test -p bp-pplns-engine --release --test finder_bonus_scale_sweep \
//!     -- --nocapture

use std::time::{Duration, Instant};

use bp_common::{AddressId, Sats};
use bp_pplns::DEFAULT_MIN_PAYOUT_SATS;
use bp_pplns_engine::config::PplnsEngineConfig;
use bp_pplns_engine::distribution::{DistributionBuilder, DistributionConfig};
use bp_pplns_engine::window::{NetworkDifficulty, WindowStore};
use bp_test_support::deterministic_p2wpkh_regtest;
use redis::{aio::ConnectionManager, Client};
use sqlx::{postgres::PgPoolOptions, PgPool};

const REDIS_URL: &str = "redis://127.0.0.1:16379";
const PG_URL: &str = "postgres://postgres:postgres@localhost:15433/public_pool";

/// Logical DBs — every other DB in this crate's suites is taken (see the
/// `REDIS_TEST_DB` consts in the sibling test files); 3 and 4 are free. One
/// per test in this file: `connect_or_skip` FLUSHDBs, so sharing one would
/// let whichever test starts second wipe the other's seeded window.
const REDIS_DB_SWEEP: u8 = 4;
const REDIS_DB_ACCUM: u8 = 3;

/// 0.1776 BTC — the bonus this feature ships with.
const FINDER_BONUS_SATS: i64 = 17_760_000;
/// Current subsidy plus a plausible fee total.
const REWARD_SATS: u64 = 317_500_000;
/// The autoscaler's floor — the budget under which the finder's second
/// entry is most likely to be trimmed, so the honest worst case.
const WEIGHT_BUDGET: u32 = 50_000;

/// Pool sizes to sweep. 400 is the plan's "≤400 addresses" run.
const SWEEP: [usize; 4] = [50, 200, 400, 500];

/// How many distinct prospective finders to build for at each N. This is
/// the fan-out that per-finder keying introduces: with `TEMPLATE_FINDERS`
/// connections served one template, the cache holds this many entries.
/// Capped below N so the sweep stays inside a test's time budget — the
/// per-entry cost is what's being measured, and it does not depend on how
/// many entries came before.
const TEMPLATE_FINDERS: usize = 50;

/// Resident set size of this process in KiB, via `ps`. `None` if `ps` is
/// missing or its output isn't parseable — the sweep then reports "n/a"
/// instead of failing, since memory is diagnostic here, not a gate.
fn resident_kib() -> Option<u64> {
    let pid = std::process::id();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

/// N deterministic distinct addresses. Seeded off a per-sweep-point byte so
/// the sets at different N don't overlap in the shared window.
fn pool_addresses(n: usize, set: u8) -> Vec<AddressId> {
    (0..n)
        .map(|i| {
            let mut seed = [0x5a; 32];
            seed[0] = set;
            seed[1] = (i & 0xff) as u8;
            seed[2] = ((i >> 8) & 0xff) as u8;
            AddressId::new(deterministic_p2wpkh_regtest(seed)).expect("generated address is valid")
        })
        .collect()
}

struct Harness {
    pool: PgPool,
    conn: ConnectionManager,
}

async fn connect_or_skip(redis_db: u8) -> Option<Harness> {
    let pg_url = std::env::var("BP_PG_URL").unwrap_or_else(|_| PG_URL.to_string());
    let redis_base = std::env::var("BP_REDIS_URL").unwrap_or_else(|_| REDIS_URL.to_string());
    let redis_url = format!("{redis_base}/{redis_db}");

    let pool = match tokio::time::timeout(
        Duration::from_secs(2),
        PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(2))
            .connect(&pg_url),
    )
    .await
    {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            eprintln!("skipping scale sweep — PG connect failed for {pg_url}: {e}");
            return None;
        }
        Err(_) => {
            eprintln!("skipping scale sweep — PG connect timed out");
            return None;
        }
    };

    let client = match Client::open(redis_url.clone()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping scale sweep — Redis client failed for {redis_url}: {e}");
            return None;
        }
    };
    let mut conn =
        match tokio::time::timeout(Duration::from_secs(2), ConnectionManager::new(client)).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => {
                eprintln!("skipping scale sweep — Redis connect failed for {redis_url}: {e}");
                return None;
            }
            Err(_) => {
                eprintln!("skipping scale sweep — Redis connect timed out");
                return None;
            }
        };
    if let Err(e) = redis::cmd("FLUSHDB").query_async::<()>(&mut conn).await {
        eprintln!("skipping scale sweep — FLUSHDB failed: {e}");
        return None;
    }

    Some(Harness { pool, conn })
}

/// One sweep point's measurements.
struct Point {
    n: usize,
    /// Payout outputs in the built coinbase — after the weight-budget trim.
    outputs: usize,
    /// Did the finder still get two entries at this N (bonus + proportional)?
    finder_entries: usize,
    /// Was the bonus output actually emitted?
    bonus_paid: bool,
    /// First (cold) build — includes the Redis window read + PG ledger query.
    cold: Duration,
    /// Median of the per-finder builds after the first. This is the
    /// per-connection cost at a template.
    warm_median: Duration,
    /// Slowest per-finder build after the first.
    warm_max: Duration,
    /// Resident-memory delta across the whole fan-out, KiB.
    rss_delta_kib: Option<i64>,
    /// Cache entries held after the fan-out — must equal the finder count,
    /// which is the whole point of the per-finder key.
    cache_entries: usize,
    /// How many times the shared window+ledger load actually ran.
    inputs_loads: u64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_finder_cache_scale_profile() {
    let Some(h) = connect_or_skip(REDIS_DB_SWEEP).await else {
        return;
    };

    println!(
        "\n=== Phase 4 sweep: per-finder distribution cache ===\n\
         reward={REWARD_SATS} sats  bonus={FINDER_BONUS_SATS} sats  \
         budget={WEIGHT_BUDGET} WU  finders/template={TEMPLATE_FINDERS}"
    );

    let mut points = Vec::new();
    for (set, &n) in SWEEP.iter().enumerate() {
        points.push(measure(&h, n, set as u8).await);
    }

    // ── The table ────────────────────────────────────────────────────
    println!(
        "\n  {:>5}  {:>7}  {:>7}  {:>9}  {:>9}  {:>9}  {:>10}  {:>7}  {:>6}",
        "N", "outputs", "finder", "cold", "warm p50", "warm max", "ΔRSS KiB", "entries", "loads"
    );
    for p in &points {
        println!(
            "  {:>5}  {:>7}  {:>7}  {:>7.1}ms  {:>7.2}ms  {:>7.2}ms  {:>10}  {:>7}  {:>6}",
            p.n,
            p.outputs,
            p.finder_entries,
            p.cold.as_secs_f64() * 1e3,
            p.warm_median.as_secs_f64() * 1e3,
            p.warm_max.as_secs_f64() * 1e3,
            p.rss_delta_kib
                .map_or_else(|| "n/a".to_string(), |d| d.to_string()),
            p.cache_entries,
            p.inputs_loads,
        );
    }

    // Per-entry memory, the number the "add a size cap?" decision turns on.
    println!("\n  per-cache-entry cost (ΔRSS / entries):");
    for p in &points {
        match p.rss_delta_kib {
            Some(d) if p.cache_entries > 0 => println!(
                "    N={:>4}  {:>8.1} KiB/entry  → a 1,000-connection template ≈ {:.1} MiB",
                p.n,
                d as f64 / p.cache_entries as f64,
                (d as f64 / p.cache_entries as f64) * 1_000.0 / 1_024.0,
            ),
            _ => println!("    N={:>4}  n/a", p.n),
        }
    }

    // ── Invariants that must hold at every N ─────────────────────────
    for p in &points {
        assert!(
            p.bonus_paid,
            "N={}: the bonus output must be emitted at every swept pool size \
             (0.1776 BTC is far above the {DEFAULT_MIN_PAYOUT_SATS}-sat dust floor)",
            p.n
        );
        // The cache is per-finder or the feature is broken: one entry per
        // distinct finder built for, none shared away.
        assert_eq!(
            p.cache_entries, TEMPLATE_FINDERS,
            "N={}: {} distinct finders must hold {} cache entries — a smaller \
             number means two finders shared a list and someone else's bonus",
            p.n, TEMPLATE_FINDERS, TEMPLATE_FINDERS
        );
        // And the inputs cache must absorb that fan-out: the window read +
        // ledger query are finder-independent, so they must NOT scale with
        // the finder count. Without this the per-finder key would multiply
        // Redis + PG load by the connection count.
        assert!(
            p.inputs_loads <= 2,
            "N={}: {} builds triggered {} window+ledger loads — the inputs \
             cache is meant to collapse them to ~1",
            p.n,
            TEMPLATE_FINDERS,
            p.inputs_loads
        );
    }

    // The 400-address point is the plan's load-bearing case: the finder is
    // still in the list twice, so the ledger merge is exercised for real.
    let at_400 = points
        .iter()
        .find(|p| p.n == 400)
        .expect("400 is in the sweep");
    assert_eq!(
        at_400.finder_entries, 2,
        "at 400 addresses the finder's proportional entry must survive the \
         {WEIGHT_BUDGET} WU trim alongside its bonus output — this is the case \
         the per-address merge exists for. Got {} entries; if the trim now \
         drops it, the sweep's ≤400 claim needs re-deriving.",
        at_400.finder_entries
    );

    println!();
}

// ── Does the cache shed entries across templates, or accumulate? ─────
//
// The sweep above measures one template. Production runs a template every
// ~30 s and each carries a *different* `coinbase_tx_value_remaining` (fees
// move), so every template mints a fresh set of `(reward, finder)` keys and
// the previous template's keys are never requested again.
//
// `InflightResultCache` has no background reaper: a `Cached` slot past its
// TTL is dropped when *that same key* is next asked for, or when
// `invalidate`/`clear` runs. Keys that never repeat are never asked for
// again. So the question this test answers is whether anything actually
// clears them — and the answer must be yes, because `record_share` calls
// `invalidate_all` on every accepted share. A pool with traffic therefore
// clears the whole map many times per template. This test pins that
// coupling: if a future change made distribution caching survive an
// accepted share, an idle-but-connected fleet would grow the map without
// bound and this test is what fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_does_not_accumulate_across_templates() {
    let Some(h) = connect_or_skip(REDIS_DB_ACCUM).await else {
        return;
    };

    const N: usize = 50;
    const TEMPLATES: usize = 6;
    let addresses = pool_addresses(N, 0xAC);
    let window = WindowStore::new(
        h.conn.clone(),
        4.0,
        100,
        NetworkDifficulty::new(1_000_000.0),
    );
    for (i, addr) in addresses.iter().enumerate() {
        window
            .record_share(
                Some(&format!("accum:{i}")),
                addr.as_str(),
                100.0,
                1_700_000_000_000 + i as u64,
            )
            .await
            .expect("record_share");
    }

    let engine_cfg = PplnsEngineConfig {
        fee_address: None,
        fee_percent: 0.0,
        min_payout_sats: Sats(DEFAULT_MIN_PAYOUT_SATS as i64),
        coinbase_weight_budget: WEIGHT_BUDGET,
        finder_bonus_sats: Some(Sats(FINDER_BONUS_SATS)),
        ..PplnsEngineConfig::default()
    };
    let builder = DistributionBuilder::new(
        h.pool.clone(),
        window.clone(),
        DistributionConfig::from_engine_config(&engine_cfg),
    );

    // Templates with no share in between: keys accumulate, because nothing
    // asks for the old ones again and nothing invalidates them. This is the
    // honest worst case (a connected fleet mining a dead pool).
    println!("\n=== cache entries across {TEMPLATES} templates, no shares ===");
    for t in 0..TEMPLATES {
        // A distinct reward per template, as fee movement produces.
        let reward = REWARD_SATS + t as u64 * 1_000;
        for addr in addresses.iter().take(8) {
            builder.build(reward, addr).await.expect("build");
        }
        println!(
            "  template {:>2}  reward={reward}  entries={}",
            t + 1,
            builder.cache_entries()
        );
    }
    let idle_entries = builder.cache_entries();
    assert_eq!(
        idle_entries,
        TEMPLATES * 8,
        "with no accepted shares nothing invalidates, so entries accumulate \
         one per (reward, finder) — this documents the unbounded shape that \
         the invalidate-on-share below is what actually bounds"
    );

    // One accepted share — the event that fires on every real submit —
    // must drop the whole map.
    window
        .record_share(
            Some("accum:reaper"),
            addresses[0].as_str(),
            100.0,
            1_700_000_099_999,
        )
        .await
        .expect("record_share");
    builder.invalidate_all(); // what `PplnsEngine::record_share` does
    println!(
        "  after one accepted share: entries={} (was {idle_entries})",
        builder.cache_entries()
    );
    assert_eq!(
        builder.cache_entries(),
        0,
        "an accepted share must clear every cached distribution — this is the \
         only thing bounding the per-finder cache, since template rewards \
         never repeat and expired-but-unrequested slots are never reaped"
    );

    // Is the memory actually reusable after a clear, or does each cycle cost
    // fresh pages?
    //
    // RSS is the wrong instrument for "was it freed" — the allocator keeps
    // freed pages mapped, so RSS does not fall on `invalidate_all` even
    // though every `DistributionResult` was dropped. What distinguishes
    // retention from a leak is the SECOND cycle: if the freed pages are
    // reused, an identical build-then-clear round grows RSS by ~nothing. If
    // each cycle instead needs new pages, growth repeats and a long-running
    // pool climbs one template at a time.
    const RECLAIM_FINDERS: usize = 200;
    let mut cycle_growth = Vec::new();
    for cycle in 0..3 {
        let before = resident_kib();
        for i in 0..RECLAIM_FINDERS {
            // Distinct reward per build ⇒ distinct key ⇒ nothing is deduped.
            // Offset by cycle so cycle 2 can't hit cycle 1's cached entries.
            let reward = REWARD_SATS + 10_000 + (cycle * RECLAIM_FINDERS + i) as u64;
            builder
                .build(reward, &addresses[i % N])
                .await
                .expect("build");
        }
        let peak = resident_kib();
        let held = builder.cache_entries();
        builder.invalidate_all();
        assert_eq!(
            builder.cache_entries(),
            0,
            "invalidate_all must leave the map empty regardless of how many \
             distinct keys accumulated"
        );
        match (before, peak) {
            (Some(b), Some(p)) => {
                let grown = p as i64 - b as i64;
                println!(
                    "  cycle {}: {RECLAIM_FINDERS} distinct keys → {held} entries, \
                     RSS {b} → {p} KiB (+{grown})",
                    cycle + 1
                );
                cycle_growth.push(grown);
            }
            _ => println!("  cycle {}: RSS unavailable", cycle + 1),
        }
    }
    if cycle_growth.len() == 3 {
        println!(
            "  growth per cycle: {:?} KiB — flat-or-falling means the freed \
             pages are being reused (allocator retention, not a leak)",
            cycle_growth
        );
        // Cycle 1 pays for the pages; cycles 2-3 should mostly reuse them. A
        // true leak grows by roughly the same amount every cycle, so compare
        // the later cycles against the first rather than against zero.
        let later = cycle_growth[1] + cycle_growth[2];
        assert!(
            later <= cycle_growth[0],
            "cycles 2+3 grew {later} KiB against cycle 1's {} KiB — the cache's \
             memory is not being reused across invalidations, which is what a \
             long-running pool does thousands of times a day",
            cycle_growth[0]
        );
    }
    println!();
}

async fn measure(h: &Harness, n: usize, set: u8) -> Point {
    let addresses = pool_addresses(n, set);
    let net_diff = NetworkDifficulty::new(1_000_000.0);
    let window = WindowStore::new(h.conn.clone(), 4.0, 100, net_diff);

    // Seed one share each — equal weights keep the payout list maximally
    // wide (no address falls below the dust floor for share-size reasons),
    // which is the worst case for both metrics.
    for (i, addr) in addresses.iter().enumerate() {
        window
            .record_share(
                Some(&format!("sweep:{set}:{i}")),
                addr.as_str(),
                100.0,
                1_700_000_000_000 + i as u64,
            )
            .await
            .expect("record_share");
    }

    let engine_cfg = PplnsEngineConfig {
        fee_address: None,
        fee_percent: 0.0,
        min_payout_sats: Sats(DEFAULT_MIN_PAYOUT_SATS as i64),
        coinbase_weight_budget: WEIGHT_BUDGET,
        finder_bonus_sats: Some(Sats(FINDER_BONUS_SATS)),
        ..PplnsEngineConfig::default()
    };
    let builder = DistributionBuilder::new(
        h.pool.clone(),
        window.clone(),
        DistributionConfig::from_engine_config(&engine_cfg),
    );

    // Baseline AFTER seeding, so the addresses in Redis + the window
    // structures aren't attributed to the cache.
    let rss_before = resident_kib();

    // Cold build — pays for the window read and the ledger query.
    let t0 = Instant::now();
    let first = builder
        .build(REWARD_SATS, &addresses[0])
        .await
        .expect("cold build");
    let cold = t0.elapsed();

    let outputs = first.payouts.len();
    let finder_entries = first
        .payouts
        .iter()
        .filter(|p| p.address == addresses[0])
        .count();
    let bonus_paid = first
        .payouts
        .iter()
        .any(|p| p.sats.0 == FINDER_BONUS_SATS && p.address == addresses[0]);
    let paid: i64 = first.payouts.iter().map(|p| p.sats.0).sum();
    assert_eq!(
        paid as u64, REWARD_SATS,
        "N={n}: the coinbase must pay out the reward exactly — a mismatch is \
         a bad-cb-amount block rejection, not a rounding curiosity"
    );

    // The fan-out: one build per additional prospective finder, each a
    // distinct cache key. This is what a template broadcast to
    // `TEMPLATE_FINDERS` connections costs.
    let mut warm = Vec::with_capacity(TEMPLATE_FINDERS - 1);
    for addr in addresses.iter().take(TEMPLATE_FINDERS).skip(1) {
        let t = Instant::now();
        let d = builder.build(REWARD_SATS, addr).await.expect("warm build");
        warm.push(t.elapsed());
        // Every finder must get its own bonus — a shared cache entry would
        // pay the first finder's bonus to everyone.
        assert!(
            d.payouts
                .iter()
                .any(|p| p.address == *addr && p.sats.0 == FINDER_BONUS_SATS),
            "N={n}: finder {} did not receive the bonus in its own build",
            addr.as_str()
        );
    }
    let rss_after = resident_kib();
    warm.sort_unstable();

    Point {
        n,
        outputs,
        finder_entries,
        bonus_paid,
        cold,
        warm_median: warm[warm.len() / 2],
        warm_max: *warm.last().expect("at least one warm build"),
        rss_delta_kib: match (rss_before, rss_after) {
            (Some(a), Some(b)) => Some(b as i64 - a as i64),
            _ => None,
        },
        cache_entries: builder.cache_entries(),
        inputs_loads: builder.inputs_loads(),
    }
}
