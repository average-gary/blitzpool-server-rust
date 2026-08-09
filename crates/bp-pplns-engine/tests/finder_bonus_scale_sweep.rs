// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(clippy::print_stderr)]
#![allow(clippy::print_stdout)]

//! Scale sweep — what does a per-finder distribution cache cost?
//!
//! With no finder bonus the distribution cache is keyed by revenue alone,
//! so every connection served one template shares a single entry. A
//! non-zero `finder_bonus_ppm` puts the finder in the key, so N connected
//! miners at one template mint **N entries**, each holding a full payout
//! list of N addresses — an O(N²) footprint where there used to be O(N).
//! That is the one resource regression this feature can plausibly cause,
//! so it gets measured rather than argued about.
//!
//! PPLNS has no member cap to bound N. Group-Solo's per-finder cache is
//! bounded by its ~50-member groups; a PPLNS window is however many miners
//! point hashrate at the pool, which is why this sweep exists here and not
//! there.
//!
//! Two numbers per pool size:
//!
//! 1. **Resident memory** across a template's worth of per-finder builds,
//!    from `ps -o rss=` on our own pid. Read as a delta from a baseline
//!    taken after the window is seeded, so the addresses themselves aren't
//!    counted as cache cost.
//! 2. **Per-connection build latency** — one `build` for one finder. The
//!    `mining.notify` frame itself is per-template, not per-payout-list
//!    (the merkle branch and header hex are built once and every client
//!    borrows them), so the payout list can only move a client's job
//!    through the distribution build that feeds the coinbase.
//!
//! Sweep points N ∈ {50, 200, 400, 500}.
//!
//! This is a measurement, not a threshold gate: it prints a table and
//! asserts only invariants that must hold at every N (bonus paid, one
//! output per address, cache actually per-finder, inputs load deduped). A
//! regression shows up as a number in the table, which is what an "add a
//! size cap?" decision needs. Skips cleanly without Redis/PG.
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

/// One DB per test in this file: `connect_or_skip` FLUSHDBs, so sharing one
/// would let whichever test starts second wipe the other's seeded window.
/// Local numbering inside this binary's own range — see
/// `bp_test_support::redis_db`.
const REDIS_DB_SWEEP: u8 = 0;
const REDIS_DB_ACCUM: u8 = 1;

/// 10 % of the miners' cut. A round number rather than the operator's
/// configured value: the footprint being measured depends on the bonus
/// being *on*, not on its size.
const FINDER_BONUS_PPM: u32 = 100_000;
/// Current subsidy plus a plausible fee total.
const REWARD_SATS: u64 = 317_500_000;
/// The autoscaler's floor — the tightest budget, so the widest trim and
/// the honest worst case for how much of the list survives.
const WEIGHT_BUDGET: u32 = 50_000;

/// Pool sizes to sweep.
const SWEEP: [usize; 4] = [50, 200, 400, 500];

/// How many distinct prospective finders to build for at each N. This is
/// the fan-out per-finder keying introduces: with `TEMPLATE_FINDERS`
/// connections served one template, the cache holds this many entries.
/// Capped below N so the sweep stays inside a test's time budget — the
/// per-entry cost is what's being measured, and it does not depend on how
/// many entries came before.
const TEMPLATE_FINDERS: usize = 50;

/// Serializes the two RSS-measuring tests in this binary.
///
/// `resident_kib` reads the RSS of the whole *process*, and `cargo test`
/// runs a binary's tests as concurrent tasks in ONE process — so without
/// this the two tests measure each other. Observed before it existed:
/// `cache_does_not_accumulate_across_templates` failed its reclaim
/// assertion because the sweep's N=400 and N=500 builds allocated ~18 MiB
/// inside its cycle-2 window. That was a real measurement of the wrong
/// thing.
static RSS_MEASUREMENT: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

fn engine_config() -> PplnsEngineConfig {
    PplnsEngineConfig {
        fee_address: Some(
            AddressId::new("bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080")
                .expect("regtest fee address"),
        ),
        fee_percent: 1.5,
        min_payout_sats: Sats(DEFAULT_MIN_PAYOUT_SATS as i64),
        coinbase_weight_budget: WEIGHT_BUDGET,
        finder_bonus_ppm: FINDER_BONUS_PPM,
        ..PplnsEngineConfig::default()
    }
}

struct Harness {
    pool: PgPool,
    conn: ConnectionManager,
}

async fn connect_or_skip(redis_db: u8) -> Option<Harness> {
    let pg_url = std::env::var("BP_PG_URL").unwrap_or_else(|_| PG_URL.to_string());
    let redis_base = std::env::var("BP_REDIS_URL").unwrap_or_else(|_| REDIS_URL.to_string());
    let db = bp_test_support::redis_db_in_range(
        bp_test_support::redis_db::PPLNS_FINDER_BONUS_SWEEP,
        redis_db,
    )
    .await;
    let redis_url = format!("{redis_base}/{db}");

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
    /// Payout outputs in the built coinbase — after the weight-budget trim,
    /// including the pool's own output.
    outputs: usize,
    /// Coinbase outputs paying the finder. Must be 1: the bonus is score
    /// weight, so it merges into the entry the finder already had.
    finder_outputs: usize,
    /// Did the finder actually out-earn an equal-weight peer?
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
    // Held across every `measure` call: its ΔRSS is process-wide.
    let _rss = RSS_MEASUREMENT.lock().await;

    println!(
        "\n=== sweep: per-finder distribution cache ===\n\
         reward={REWARD_SATS} sats  bonus={FINDER_BONUS_PPM} ppm  \
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
            p.finder_outputs,
            p.cold.as_secs_f64() * 1e3,
            p.warm_median.as_secs_f64() * 1e3,
            p.warm_max.as_secs_f64() * 1e3,
            p.rss_delta_kib
                .map_or_else(|| "n/a".to_string(), |d| d.to_string()),
            p.cache_entries,
            p.inputs_loads,
        );
    }

    // Per-entry memory, the number an "add a size cap?" decision turns on.
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
            "N={}: the named finder must out-earn an equal-weight peer at every \
             swept pool size — a bonus that vanishes under the trim is a bonus \
             the operator configured and the chain never paid",
            p.n
        );
        // One output, not two. Under §4 the bonus is extra score weight on
        // an entry the finder already holds, so it merges structurally —
        // there is no second output for a consumer to forget to fold.
        assert_eq!(
            p.finder_outputs, 1,
            "N={}: the finder must hold exactly ONE coinbase output; {} means \
             the bonus stopped merging into their own entry",
            p.n, p.finder_outputs
        );
        // The cache is per-finder or the feature is broken: one entry per
        // distinct finder built for, none shared away.
        assert_eq!(
            p.cache_entries, TEMPLATE_FINDERS,
            "N={}: {TEMPLATE_FINDERS} distinct finders must hold \
             {TEMPLATE_FINDERS} cache entries — a smaller number means two \
             finders shared a list, and one of them was served the other's bonus",
            p.n
        );
        // And the inputs cache must absorb that fan-out: the window read +
        // ledger query are finder-independent, so they must NOT scale with
        // the finder count. Without this the per-finder key would multiply
        // Redis + PG load by the connection count.
        assert!(
            p.inputs_loads <= 2,
            "N={}: {TEMPLATE_FINDERS} builds triggered {} window+ledger loads — \
             the inputs cache is meant to collapse them to ~1",
            p.n,
            p.inputs_loads
        );
    }

    println!();
}

// ── Does the cache shed entries across templates, or accumulate? ─────
//
// The sweep above measures one template. Production runs a template every
// ~30 s and each carries a *different* `coinbase_tx_value_remaining` (fees
// move), so every template mints a fresh set of keys and the previous
// template's keys are never requested again.
//
// `InflightResultCache` has no background reaper: a cached slot past its
// TTL is dropped when *that same key* is next asked for, or when
// `invalidate_all`/`clear` runs. Keys that never repeat are never asked for
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
    const PER_TEMPLATE: usize = 8;
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

    let builder = DistributionBuilder::new(
        h.pool.clone(),
        window.clone(),
        DistributionConfig::from_engine_config(&engine_config()),
    );

    // Templates with no share in between: keys accumulate, because nothing
    // asks for the old ones again and nothing invalidates them. This is the
    // honest worst case (a connected fleet mining a dead pool).
    println!("\n=== cache entries across {TEMPLATES} templates, no shares ===");
    for t in 0..TEMPLATES {
        // A distinct reward per template, as fee movement produces.
        let reward = REWARD_SATS + t as u64 * 1_000;
        for addr in addresses.iter().take(PER_TEMPLATE) {
            builder.build(reward, Some(addr)).await.expect("build");
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
        TEMPLATES * PER_TEMPLATE,
        "with no accepted shares nothing invalidates, so entries accumulate one \
         per (reward, finder) — this documents the unbounded shape that the \
         invalidate-on-share below is what actually bounds"
    );

    // One accepted share — the event that fires on every real submit — must
    // drop the whole map.
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
         only thing bounding the per-finder cache, since template rewards never \
         repeat and expired-but-unrequested slots are never reaped"
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
    // Process-wide RSS — see `RSS_MEASUREMENT`.
    let _rss = RSS_MEASUREMENT.lock().await;
    for cycle in 0..3 {
        let before = resident_kib();
        for i in 0..RECLAIM_FINDERS {
            // Distinct reward per build ⇒ distinct key ⇒ nothing is deduped.
            // Offset by cycle so cycle 2 can't hit cycle 1's cached entries.
            let reward = REWARD_SATS + 10_000 + (cycle * RECLAIM_FINDERS + i) as u64;
            builder
                .build(reward, Some(&addresses[i % N]))
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
        let total: i64 = cycle_growth.iter().sum();
        // What ONE cycle's entries occupy, accounted rather than measured.
        // 200 conservative bytes per payout entry — an `AddressId` is a
        // heap `String` of ~62 bytes plus its header, and the rest of a
        // `WeightEntry` is fixed-width fields.
        const BYTES_PER_ENTRY: usize = 200;
        let live_kib = (RECLAIM_FINDERS * N * BYTES_PER_ENTRY / 1_024) as i64;
        println!(
            "  growth per cycle: {cycle_growth:?} KiB, total {total} — one cycle's \
             {RECLAIM_FINDERS} entries account for ~{live_kib} KiB live, so three \
             unreclaimed cycles would be ~{} KiB",
            live_kib * 3
        );
        // The bound is DERIVED, not tuned: if each cycle's entries survived
        // its `invalidate_all`, three cycles would cost ~3× the live size.
        // Staying under 1× means at least two of the three were reclaimed.
        //
        // Deliberately not a per-cycle comparison. RSS is process-wide and
        // the allocator's arena is already warm by the time this runs, so a
        // single cycle's number is dominated by noise — measured
        // [0, 32, 416] KiB in one full-suite run, where `last <= first`
        // fails on 416 > 0 while the total (448 KiB against a ~1,900 KiB
        // live size) shows reclamation working fine.
        assert!(
            total < live_kib,
            "three build-then-clear rounds grew RSS by {total} KiB, at or above \
             the ~{live_kib} KiB one cycle's entries occupy — the cache's memory \
             is not being reclaimed across invalidations, which is what a \
             long-running pool does thousands of times a day. Per cycle: \
             {cycle_growth:?}"
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
    // which is the worst case for both metrics. Equal weights also make the
    // bonus assertion below a clean comparison: the finder and every peer
    // hold the same score, so any difference in what they are paid IS the
    // bonus.
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

    let builder = DistributionBuilder::new(
        h.pool.clone(),
        window.clone(),
        DistributionConfig::from_engine_config(&engine_config()),
    );

    // Baseline AFTER seeding, so the addresses in Redis + the window
    // structures aren't attributed to the cache.
    let rss_before = resident_kib();

    // Cold build — pays for the window read and the ledger query.
    let t0 = Instant::now();
    let first = builder
        .build(REWARD_SATS, Some(&addresses[0]))
        .await
        .expect("cold build");
    let cold = t0.elapsed();

    let entries = first
        .distribution
        .payout_entries_at(REWARD_SATS)
        .expect("§4 payout vector");
    let outputs = entries.len();
    let finder_outputs = entries.iter().filter(|(a, _)| *a == addresses[0]).count();
    let paid_finder = entries
        .iter()
        .find(|(a, _)| *a == addresses[0])
        .map(|(_, s)| *s);
    // An equal-weight peer that is NOT the finder. Whether a given peer
    // survives the trim depends on N, so take whichever published entry is
    // neither the finder nor the pool output.
    let paid_peer = entries
        .iter()
        .skip(1) // pool output is first
        .find(|(a, _)| *a != addresses[0])
        .map(|(_, s)| *s);
    let bonus_paid = match (paid_finder, paid_peer) {
        (Some(f), Some(peer)) => f > peer,
        // No surviving peer to compare against — the trim left the finder
        // alone, so this point cannot speak to the bonus either way.
        _ => false,
    };

    // Warm fan-out: one build per prospective finder, as a template's worth
    // of connections produces. The first finder is already cached, so start
    // at 1 to time genuinely new keys.
    let mut warm: Vec<Duration> = Vec::with_capacity(TEMPLATE_FINDERS);
    for addr in addresses.iter().take(TEMPLATE_FINDERS).skip(1) {
        let t = Instant::now();
        builder
            .build(REWARD_SATS, Some(addr))
            .await
            .expect("warm build");
        warm.push(t.elapsed());
    }
    warm.sort_unstable();
    let warm_median = warm.get(warm.len() / 2).copied().unwrap_or_default();
    let warm_max = warm.last().copied().unwrap_or_default();

    let rss_after = resident_kib();
    let rss_delta_kib = match (rss_before, rss_after) {
        (Some(b), Some(a)) => Some(a as i64 - b as i64),
        _ => None,
    };

    Point {
        n,
        outputs,
        finder_outputs,
        bonus_paid,
        cold,
        warm_median,
        warm_max,
        rss_delta_kib,
        cache_entries: builder.cache_entries(),
        inputs_loads: builder.inputs_loads(),
    }
}
