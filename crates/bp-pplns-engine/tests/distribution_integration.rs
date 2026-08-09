// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(clippy::print_stderr)]
#![allow(clippy::needless_return)]

//! End-to-end integration tests for `bp-pplns-engine::distribution`.
//!
//! Exercises the full hot path: Redis window read → PG open-balance
//! read → pure-math distribution build → Redis snapshot write. Plus
//! in-flight dedup of concurrent callers.
//!
//! Gated on both docker-Redis (Port 16379) and docker-PG (Port 15433);
//! tests skip cleanly via `eprintln!` if either is missing. PG state
//! is cleaned by per-test prefix DELETE; Redis state isolated by
//! per-test DB number (0..=15).

use std::sync::Arc;

use bp_common::AddressId;
use bp_pplns_engine::config::PplnsEngineConfig;
use bp_pplns_engine::distribution::{
    DistributionBuilder, DistributionConfig, DistributionError, DistributionResult,
};
use bp_pplns_engine::window::{NetworkDifficulty, WindowStore};

/// Pool-output recipient. §4 makes `pay_P` structural, so the weight model
/// has no distribution without one. One constant because three harnesses
/// in this file configured the same literal separately.
const FEE_ADDR: &str = "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4";
use redis::{aio::ConnectionManager, Client};
use sqlx::{postgres::PgPoolOptions, PgPool};

const REDIS_URL: &str = "redis://127.0.0.1:16379";
const PG_URL: &str = "postgres://postgres:postgres@localhost:15433/public_pool";

struct Harness {
    pool: PgPool,
    builder: DistributionBuilder,
    address_prefix: String,
}

async fn connect_or_skip(redis_db: u8, address_prefix: &str) -> Option<Harness> {
    connect_or_skip_with_bonus(redis_db, address_prefix, 0).await
}

/// As [`connect_or_skip`], with `[pplns] finder_bonus_ppm` set.
///
/// A separate entry point rather than a field on `Harness`: the bonus is
/// read at `DistributionConfig` construction, so it cannot be toggled on
/// a built `DistributionBuilder` — which is exactly the property the
/// cache-keying tests below depend on.
async fn connect_or_skip_with_bonus(
    redis_db: u8,
    address_prefix: &str,
    finder_bonus_ppm: u32,
) -> Option<Harness> {
    let pg_url = std::env::var("BP_PG_URL").unwrap_or_else(|_| PG_URL.to_string());
    let redis_base = std::env::var("BP_REDIS_URL").unwrap_or_else(|_| REDIS_URL.to_string());
    // Fold this binary's local number into its own DB range — see
    // `bp_test_support::redis_db`.
    let redis_db =
        bp_test_support::redis_db_in_range(bp_test_support::redis_db::PPLNS_DISTRIBUTION, redis_db)
            .await;
    let redis_url = format!("{redis_base}/{redis_db}");

    let pool = match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(std::time::Duration::from_secs(2))
            .connect(&pg_url),
    )
    .await
    {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => {
            eprintln!("PG connect failed for {pg_url}: {e} — skipping");
            return None;
        }
        Err(_) => {
            eprintln!("PG connect timed out — skipping");
            return None;
        }
    };

    let client = match Client::open(redis_url.clone()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Redis client failed for {redis_url}: {e} — skipping");
            return None;
        }
    };
    let mut conn = match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        ConnectionManager::new(client),
    )
    .await
    {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            eprintln!("Redis connect failed for {redis_url}: {e} — skipping");
            return None;
        }
        Err(_) => {
            eprintln!("redis connect timed out (>2s) — skipping integration test");
            return None;
        }
    };
    if let Err(e) = redis::cmd("FLUSHDB").query_async::<()>(&mut conn).await {
        eprintln!("FLUSHDB failed: {e} — skipping");
        return None;
    }

    // Cleanup any leftover rows from previous test runs.
    let _ = sqlx::query("DELETE FROM pplns_balance WHERE address LIKE $1")
        .bind(format!("{address_prefix}%"))
        .execute(&pool)
        .await;

    let net_diff = NetworkDifficulty::new(1_000_000.0);
    let window = WindowStore::new(
        conn, /*factor=*/ 4.0, /*bucket_shares=*/ 100, net_diff,
    );
    // The weight model requires the pool-output recipient (pay_P is
    // structural) — mirror the production requirement in the harness.
    let cfg = DistributionConfig::from_engine_config(&PplnsEngineConfig {
        fee_address: Some(AddressId::new(FEE_ADDR).unwrap()),
        finder_bonus_ppm,
        ..PplnsEngineConfig::default()
    });
    let builder = DistributionBuilder::new(pool.clone(), window, cfg);

    Some(Harness {
        pool,
        builder,
        address_prefix: address_prefix.to_string(),
    })
}

async fn seed_share(window: &WindowStore, address: &str, diff: f64, ts: u64) {
    window
        .record_share(None, address, diff, ts)
        .await
        .expect("record_share");
}

async fn seed_open_balance(pool: &PgPool, address: &str, balance_sats: i64, total_paid: i64) {
    sqlx::query(
        r#"INSERT INTO pplns_balance (address, "balanceSats", "totalPaidSats", "updatedAt")
           VALUES ($1, $2, $3, 0)"#,
    )
    .bind(address)
    .bind(balance_sats)
    .bind(total_paid)
    .execute(pool)
    .await
    .expect("seed balance");
}

async fn cleanup(pool: &PgPool, prefix: &str) {
    let _ = sqlx::query("DELETE FROM pplns_balance WHERE address LIKE $1")
        .bind(format!("{prefix}%"))
        .execute(pool)
        .await;
}

/// Every test owns its OWN pair of addresses.
///
/// `pplns_balance` is keyed on the address alone and is shared by every
/// test in this target — they only differ by Redis database. While they
/// all used the same two literals, one test's `cleanup_addresses` deleted
/// the balance row another was mid-way through asserting on. Distinct
/// addresses per test is what makes them independent; the Redis database
/// index alone never did.
async fn cleanup_addresses(pool: &PgPool, addresses: &[&str]) {
    for addr in addresses {
        let _ = sqlx::query("DELETE FROM pplns_balance WHERE address = $1")
            .bind(*addr)
            .execute(pool)
            .await;
    }
}

// ── Test 1 — end-to-end build returns payouts + writes snapshot ────

#[tokio::test]
async fn build_with_shares_only_returns_payouts_and_writes_snapshot() {
    let h = match connect_or_skip(8, "test_dist_e2e_").await {
        Some(h) => h,
        None => return,
    };

    // Use valid Bitcoin addresses so they survive the payout-address
    // sanitisation filter applied before distribution math.
    const ADDR_A: &str = "bc1qvzf0p407umrsaxmsnq62yudwf27lmsxd8sshzl";
    const ADDR_B: &str = "bc1q2a2z2q02mf38pjmtcfc926a52ssk2mw0ekmz6q";
    cleanup_addresses(&h.pool, &[ADDR_A, ADDR_B]).await;

    let window = build_window(&h).await;
    seed_share(&window, ADDR_A, 60.0, 1_700_000_000_001).await;
    seed_share(&window, ADDR_B, 40.0, 1_700_000_000_002).await;

    let result = h.builder.build(312_500_000, None).await.expect("build ok");
    assert_eq!(result.distribution.reference_revenue_sats, 312_500_000);
    assert!(
        result.distribution.published().count() > 0,
        "expected published payout weights"
    );
    let addr_a_id = AddressId::new(ADDR_A).unwrap();
    let addr_b_id = AddressId::new(ADDR_B).unwrap();
    for id in [&addr_a_id, &addr_b_id] {
        assert!(
            result.distribution.entries.iter().any(|e| &e.address == id),
            "share-holder must be in the distribution entries"
        );
    }
    // 60/40 share split → 60/40 score weights.
    let score_of = |id: &AddressId| {
        result
            .distribution
            .entries
            .iter()
            .find(|e| &e.address == id)
            .map(|e| e.score_weight)
            .unwrap_or(0)
    };
    assert!(score_of(&addr_a_id) > score_of(&addr_b_id));

    // The schema-2 snapshot must be readable from Redis.
    let snapshot = window
        .read_weight_snapshot_for(&result.payouts_fingerprint())
        .await
        .expect("read snapshot ok");
    let parsed = snapshot.expect("snapshot persisted");
    assert_eq!(parsed.reference_revenue_sats, 312_500_000);
    assert_eq!(parsed.entries.len(), result.distribution.entries.len());

    cleanup_addresses(&h.pool, &[ADDR_A, ADDR_B]).await;
}

// ── Test 2 — open-balance ledger is folded into the distribution ────

#[tokio::test]
async fn build_folds_open_balances_into_distribution() {
    let h = match connect_or_skip(9, "test_dist_ledger_").await {
        Some(h) => h,
        None => return,
    };

    // Use valid Bitcoin addresses so they survive the payout-address filter.
    const ADDR_MINER: &str = "bc1qymzqcsak0z8zxlxyrqtdw0tvke9sma0gwfaplq";
    const ADDR_DEBTOR: &str = "bc1qhntpnmga5u3c96wqvtxx4a429u5l23unmde3g0";
    cleanup_addresses(&h.pool, &[ADDR_MINER, ADDR_DEBTOR]).await;

    let window = build_window(&h).await;
    seed_share(&window, ADDR_MINER, 100.0, 1_700_000_000_001).await;

    // Debtor has a -5_000 balance (owes the pool from a previous trim
    // bonus). When debtor is NOT in the current window, the distribution
    // should still consider them.
    seed_open_balance(&h.pool, ADDR_DEBTOR, -5_000, 0).await;

    let result = h.builder.build(312_500_000, None).await.expect("build ok");
    let debtor_id = AddressId::new(ADDR_DEBTOR).unwrap();
    let debtor = result
        .distribution
        .entries
        .iter()
        .find(|e| e.address == debtor_id)
        .expect("open-balance debtor must be in the distribution entries");
    assert_eq!(debtor.balance_sats, -5_000, "debt carried for settlement");
    assert_eq!(debtor.wire_weight, 0, "no shares + debt → no output");

    cleanup_addresses(&h.pool, &[ADDR_MINER, ADDR_DEBTOR]).await;
}

// ── Test 3 — concurrent callers dedup via inflight cache ────────────

#[tokio::test]
async fn concurrent_builds_for_same_reward_share_one_compute() {
    let h = match connect_or_skip(10, "test_dist_dedup_").await {
        Some(h) => h,
        None => return,
    };

    // A REAL address, or the sanitize pass drops it and every build below
    // dedups an EMPTY distribution — see `cleanup_addresses`.
    const ADDR: &str = "bc1q69dqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq4vkle";
    cleanup_addresses(&h.pool, &[ADDR]).await;

    let window = build_window(&h).await;
    seed_share(&window, ADDR, 100.0, 1_700_000_000_001).await;

    let builder = Arc::new(h.builder.clone());
    let mut handles = Vec::new();
    for _ in 0..8 {
        let b = builder.clone();
        handles.push(tokio::spawn(
            async move { b.build(312_500_000, None).await },
        ));
    }

    let mut shared_result: Option<Arc<DistributionResult>> = None;
    for handle in handles {
        let result = handle.await.unwrap().expect("ok");
        if let Some(ref prev) = shared_result {
            assert!(
                Arc::ptr_eq(prev, &result),
                "concurrent callers should share the same Arc (in-flight dedup)"
            );
        } else {
            shared_result = Some(result);
        }
    }
    assert!(shared_result.is_some());

    cleanup(&h.pool, &h.address_prefix).await;
}

// ── Test 4 — invalidate_all triggers fresh compute on next call ─────

#[tokio::test]
async fn invalidate_all_triggers_fresh_compute() {
    let h = match connect_or_skip(11, "test_dist_inval_").await {
        Some(h) => h,
        None => return,
    };

    // A REAL address — a prefix placeholder is dropped by the sanitize
    // pass and the cache identity below would be compared over an EMPTY
    // distribution.
    const ADDR: &str = "bc1q6fdqqqqqqqqqqqqqqqqqqqqqqqqqqqqqz09s7u";
    cleanup_addresses(&h.pool, &[ADDR]).await;

    let window = build_window(&h).await;
    seed_share(&window, ADDR, 100.0, 1_700_000_000_001).await;

    let r1 = h.builder.build(312_500_000, None).await.expect("ok");
    let r2 = h.builder.build(312_500_000, None).await.expect("ok");
    assert!(
        Arc::ptr_eq(&r1, &r2),
        "cached call returns the same Arc as the first"
    );

    h.builder.invalidate_all();
    let r3 = h.builder.build(312_500_000, None).await.expect("ok");
    assert!(
        !Arc::ptr_eq(&r1, &r3),
        "post-invalidate, the cache returns a freshly-built result"
    );

    cleanup(&h.pool, &h.address_prefix).await;
}

// ── Test 5 — different rewards run independently ────────────────────

#[tokio::test]
async fn distinct_rewards_each_get_their_own_compute() {
    let h = match connect_or_skip(12, "test_dist_rew_").await {
        Some(h) => h,
        None => return,
    };

    // A REAL address, for the same reason as the two tests above.
    const ADDR: &str = "bc1q6ddqqqqqqqqqqqqqqqqqqqqqqqqqqqqqm7zjxl";
    cleanup_addresses(&h.pool, &[ADDR]).await;

    let window = build_window(&h).await;
    seed_share(&window, ADDR, 50.0, 1_700_000_000_001).await;

    let r1 = h.builder.build(300_000_000, None).await.expect("ok");
    let r2 = h.builder.build(312_500_000, None).await.expect("ok");
    assert_eq!(r1.distribution.reference_revenue_sats, 300_000_000);
    assert_eq!(r2.distribution.reference_revenue_sats, 312_500_000);
    assert!(!Arc::ptr_eq(&r1, &r2));

    cleanup(&h.pool, &h.address_prefix).await;
}

// ── Test 7 — distinct references share ONE window+ledger load ───────
//
// The reference-independent half — the Redis window read and the
// Postgres ledger query — is identical for every build and must be
// loaded once, not once per caller. (The push model removed the old
// per-JDC-value burst; distinct references now only arise across
// template changes, but the dedup still has to hold.)

#[tokio::test]
async fn concurrent_distinct_rewards_share_one_inputs_load() {
    let h = match connect_or_skip(14, "test_dist_inputs_").await {
        Some(h) => h,
        None => return,
    };

    // Valid Bitcoin addresses so they survive payout-address sanitisation.
    const ADDR_A: &str = "bc1q0fqgxscjqch5tmqvnf50e0uyfem9sr432z2zx9";
    const ADDR_B: &str = "bc1qvuv4q5jp34mlvc97fh0r4l00jrllmunezpl69k";
    cleanup_addresses(&h.pool, &[ADDR_A, ADDR_B]).await;

    let window = build_window(&h).await;
    seed_share(&window, ADDR_A, 100.0, 1_700_000_000_001).await;
    seed_share(&window, ADDR_B, 50.0, 1_700_000_000_002).await;

    let before = h.builder.inputs_loads();
    let builder = Arc::new(h.builder.clone());
    let mut handles = Vec::new();
    // 16 callers, 16 distinct rewards — no per-reward cache hit possible.
    for i in 0..16u64 {
        let b = builder.clone();
        handles.push(tokio::spawn(async move {
            b.build(312_500_000 + i * 137, None).await
        }));
    }
    for (i, handle) in handles.into_iter().enumerate() {
        let r = handle.await.unwrap().expect("build ok");
        assert_eq!(
            r.distribution.reference_revenue_sats,
            312_500_000 + i as u64 * 137
        );
        assert!(r.distribution.published().count() > 0);
    }

    let loads = h.builder.inputs_loads() - before;
    assert!(
        loads <= 2,
        "16 concurrent builds for distinct rewards should share the window+ledger \
         load (allowing one straggler that arrives after the leader published); \
         got {loads} loads"
    );

    // Sanity: a build after an invalidation must load fresh again.
    h.builder.invalidate_all();
    let _ = h.builder.build(999_000_000, None).await.expect("ok");
    assert!(
        h.builder.inputs_loads() - before > loads,
        "invalidate_all must force the next build to reload the inputs"
    );

    cleanup_addresses(&h.pool, &[ADDR_A, ADDR_B]).await;
    cleanup(&h.pool, &h.address_prefix).await;
}

// ── Test 8 — distinct references share ONE fingerprint + snapshot ───
//
// The weights fingerprint hashes the settlement INPUTS (scores,
// balances, fee, dust limits) — not the reference revenue the wire
// boosts were projected against. Two builds over the same window at
// different references therefore name the SAME snapshot, which is what
// lets one snapshot settle the pool's own templates and every JDC's
// independently-valued job alike.

#[tokio::test]
async fn distinct_references_share_one_fingerprinted_snapshot() {
    let h = match connect_or_skip(15, "test_dist_fp_").await {
        Some(h) => h,
        None => return,
    };

    const ADDR_A: &str = "bc1qcv9d0s4mrcpczumg8cr9tj34l0gw29scz552yd";
    const ADDR_B: &str = "bc1qy5jzxu2u6jeps9duq8fgdxuacjan93vy6yg4j8";
    cleanup_addresses(&h.pool, &[ADDR_A, ADDR_B]).await;

    let window = build_window(&h).await;
    seed_share(&window, ADDR_A, 70.0, 1_700_000_000_001).await;
    seed_share(&window, ADDR_B, 30.0, 1_700_000_000_002).await;

    let first = h
        .builder
        .build(312_500_000, None)
        .await
        .expect("first build ok");
    let second = h
        .builder
        .build(312_499_137, None)
        .await
        .expect("second build ok");

    assert_eq!(
        first.payouts_fingerprint(),
        second.payouts_fingerprint(),
        "same settlement inputs must share one snapshot identity"
    );

    let snap = window
        .read_weight_snapshot_for(&first.payouts_fingerprint())
        .await
        .expect("read ok")
        .expect("snapshot present");
    // Last writer wins on the shared identity — either reference is a
    // valid projection base; settlement never reads it as an amount.
    assert!(
        snap.reference_revenue_sats == 312_500_000 || snap.reference_revenue_sats == 312_499_137
    );
    assert_eq!(snap.entries.len(), first.distribution.entries.len());

    cleanup_addresses(&h.pool, &[ADDR_A, ADDR_B]).await;
    cleanup(&h.pool, &h.address_prefix).await;
}

// ── Test 6 — empty window: refused here, bootstrapped per-miner ──────

/// MONEY: an empty window must not produce a servable distribution.
///
/// It used to return an entry list with nothing in it, and that is not a
/// harmless empty answer: `weight_P` floors at 1 and §4 makes the pool
/// output the residual, so `payout_entries_at` yielded a SINGLE output
/// paying the WHOLE block to the fee address — and the list is not empty,
/// so the job path served it and settlement then booked nothing (every
/// claim is 0 at `score_total == 0`).
///
/// The shared build is the one that must refuse: its result is cached by
/// revenue alone and handed to every PPLNS connection, so it has no single
/// miner it could name as the claimant.
///
/// The test this replaces asserted `entries.is_empty()` — i.e. it pinned
/// the defect as the contract.
#[tokio::test]
async fn an_empty_window_is_refused_by_the_shared_build() {
    let h = match connect_or_skip(13, "test_dist_empty_").await {
        Some(h) => h,
        None => return,
    };
    let err = h
        .builder
        .build(312_500_000, None)
        .await
        .expect_err("an empty window must not yield a shared distribution");
    assert!(
        matches!(
            &*err,
            DistributionError::WeightBuild(bp_pplns::WeightBuildError::NoScoredMiners)
        ),
        "expected NoScoredMiners, got {err:?}"
    );

    cleanup(&h.pool, &h.address_prefix).await;
}

/// The other half: the per-miner bootstrap build DOES answer, and it pays
/// the asking miner rather than the pool.
///
/// This is what keeps the refusal above from bricking a fresh pool — the
/// window fills only from accepted shares, and shares come only from jobs.
#[tokio::test]
async fn the_bootstrap_build_pays_the_asking_miner() {
    let h = match connect_or_skip(4, "test_dist_boot_").await {
        Some(h) => h,
        None => return,
    };
    const ADDR: &str = "bc1q69dqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq4vkle";
    const T: u64 = 312_500_000;
    cleanup_addresses(&h.pool, &[ADDR]).await;
    let claimant = AddressId::new(ADDR).unwrap();

    // Precondition: the shared build really has nothing to work with, so
    // what follows is the bootstrap and not an ordinary window read.
    assert!(
        h.builder.build(T, None).await.is_err(),
        "window must be empty"
    );

    let result = h
        .builder
        .build_bootstrap(T, &claimant)
        .await
        .expect("the bootstrap build must answer");
    // The claimant carries the whole SCORE space — it is the only scored
    // address. (`entries` may hold more than one: `pplns_balance` is
    // global to this Postgres and other tests leave open-balance rows
    // behind, which legitimately enter as balance-only candidates.)
    let entry = result
        .distribution
        .entries
        .iter()
        .find(|e| e.address == claimant)
        .expect("the claimant must be an entry");
    assert_eq!(
        entry.score_weight, result.distribution.score_total,
        "the claimant holds the entire score space"
    );
    assert!(entry.wire_weight > 0, "and is published");

    let paid = result.distribution.payout_entries_at(T).expect("§4 vector");
    let of = |a: &str| -> u64 {
        paid.iter()
            .filter(|(addr, _)| addr.as_str() == a)
            .map(|(_, s)| *s)
            .sum()
    };
    // THE money assertion: the pool takes its fee and not the block.
    // `weight_P` carries only the fee whatever the ledger owes, so this
    // holds regardless of any leftover balance rows.
    let fee_only = T * u64::from(result.distribution.fee_ppm) / 1_000_000;
    assert!(
        of(FEE_ADDR).abs_diff(fee_only) <= 2 + paid.len() as u64,
        "pool took {} where its fee is {fee_only} — the old behaviour took all {T}",
        of(FEE_ADDR)
    );
    assert!(
        of(ADDR) > 0,
        "the asking miner must actually be paid, got {}",
        of(ADDR)
    );
    assert_eq!(paid.iter().map(|(_, s)| *s).sum::<u64>(), T, "Σ == T");

    // And it is bookable — the snapshot landed under its own fingerprint,
    // so a block found on this job settles like any other.
    assert!(result.snapshot_written);
    let window = build_window(&h).await;
    assert!(window
        .read_weight_snapshot_for(&result.payouts_fingerprint())
        .await
        .expect("read ok")
        .is_some());

    cleanup_addresses(&h.pool, &[ADDR]).await;
    cleanup(&h.pool, &h.address_prefix).await;
}

// ── Helper — build a fresh WindowStore over the same connection ─────
//
// The Harness owns the WindowStore inside the DistributionBuilder, but
// tests need a separate handle to seed shares + read the snapshot
// directly. Constructing a parallel WindowStore against the same
// connection is fine (ConnectionManager is multiplexed).

async fn build_window(h: &Harness) -> WindowStore {
    // Tests need a parallel WindowStore against the same Redis DB the
    // harness chose so they can seed shares + inspect the snapshot
    // directly. The harness's builder owns its WindowStore internally;
    // making a sibling against the same DB is fine because
    // ConnectionManager is multiplexed.
    let redis_base = std::env::var("BP_REDIS_URL").unwrap_or_else(|_| REDIS_URL.to_string());
    let db = redis_db_for_prefix(&h.address_prefix);
    let db =
        bp_test_support::redis_db_in_range(bp_test_support::redis_db::PPLNS_DISTRIBUTION, db).await;
    let url = format!("{redis_base}/{db}");
    let client = Client::open(url).expect("client");
    let conn = ConnectionManager::new(client).await.expect("conn");
    let nd = NetworkDifficulty::new(1_000_000.0);
    WindowStore::new(conn, 4.0, 100, nd)
}

// ── Finder bonus ────────────────────────────────────────────────────

/// 10 % of the miners' cut to the finder, 60/40 window.
///
/// The number this pins is NOT "the finder got 10 % more". It is that
/// the finder's own dilution is paid for: the bonus lands as score
/// weight `b = S·f/(1−f)`, so after re-dividing by `S + b` the finder
/// holds exactly `f + (1−f)·u/S` of the miners' cut. A naive `b = S·f`
/// leaves them short, and the shortfall grows with `f` — at 10 % on a
/// 60 % miner it is ~1.1 M sats of a 3.125 BTC block, which is the kind
/// of error that looks like a rounding bug in a payout audit.
///
/// Asserted as FRACTIONS of the miners' cut rather than absolute sats so
/// the test says what the model promises, not what one revenue happens
/// to produce.
#[tokio::test]
async fn finder_bonus_pays_the_finder_their_share_plus_the_bonus_of_the_rest() {
    const BONUS_PPM: u32 = 100_000; // 10 %
    let h = match connect_or_skip_with_bonus(0, "test_dist_bonus_", BONUS_PPM).await {
        Some(h) => h,
        None => return,
    };
    // Own pair, per `cleanup_addresses`' doc — `pplns_balance` is keyed
    // on the address alone, so sharing literals with another test in this
    // binary means its cleanup deletes rows this one is asserting on.
    const FINDER: &str = "bc1qerdj5ax2gu9jmy9nrjfmadauevy4kqap2dj750";
    const PEER: &str = "bc1qjsucu9x4ktuus4w3le07fnyh6g7cl0pl684vrw";
    cleanup_addresses(&h.pool, &[FINDER, PEER]).await;

    let window = build_window(&h).await;
    seed_share(&window, FINDER, 60.0, 1_700_000_000_001).await;
    seed_share(&window, PEER, 40.0, 1_700_000_000_002).await;

    const T: u64 = 312_500_000;
    let finder_id = AddressId::new(FINDER).unwrap();
    let built = h
        .builder
        .build(T, Some(&finder_id))
        .await
        .expect("build with bonus");
    let entries = built
        .distribution
        .payout_entries_at(T)
        .expect("§4 payout vector");

    // The finder is ONE output, not two: the bonus is score weight, so
    // it merges into the entry they already had. The old sats-denominated
    // bonus emitted a second output and every consumer had to fold it.
    let finder_outputs: Vec<u64> = entries
        .iter()
        .filter(|(a, _)| a.as_str() == FINDER)
        .map(|(_, s)| *s)
        .collect();
    assert_eq!(
        finder_outputs.len(),
        1,
        "the bonus must merge into the finder's own entry, not add an output: {entries:?}"
    );

    let paid_finder = finder_outputs[0] as f64;
    let paid_peer = entries
        .iter()
        .find(|(a, _)| a.as_str() == PEER)
        .map(|(_, s)| *s)
        .expect("peer is paid") as f64;
    // The miners' cut is what the bonus is a fraction OF — derive it
    // from what the miners were actually paid rather than re-deriving
    // the fee, so this does not silently track a fee-model change.
    let miner_pot = paid_finder + paid_peer;

    let f = BONUS_PPM as f64 / 1_000_000.0;
    // finder: f + (1−f)·0.60   peer: (1−f)·0.40
    let want_finder = f + (1.0 - f) * 0.60;
    let want_peer = (1.0 - f) * 0.40;
    let got_finder = paid_finder / miner_pot;
    let got_peer = paid_peer / miner_pot;
    assert!(
        (got_finder - want_finder).abs() < 1e-6,
        "finder fraction {got_finder} != {want_finder} (paid {paid_finder} of {miner_pot})"
    );
    assert!(
        (got_peer - want_peer).abs() < 1e-6,
        "peer fraction {got_peer} != {want_peer} (paid {paid_peer} of {miner_pot})"
    );

    // And the naive `b = S·f` form would have failed the above — pin the
    // gap so a "simplification" back to it cannot pass silently.
    let naive_finder_fraction = (0.60 + f) / (1.0 + f);
    assert!(
        (naive_finder_fraction - want_finder).abs() > 1e-3,
        "this test cannot distinguish the correct closed form from the naive one"
    );

    cleanup(&h.pool, &h.address_prefix).await;
    cleanup_addresses(&h.pool, &[FINDER, PEER]).await;
}

/// Two miners, same revenue, bonus ON — each must be served the
/// distribution built for THEM finding the block.
///
/// The failure this guards is silent: with the finder dropped from the
/// cache key whichever miner asks first mints the entry, and every other
/// miner is then served a coinbase paying that first miner the bonus.
/// Nothing errors, because the booking is internally consistent with the
/// coinbase that was actually mined — it is a misdirection of the bonus,
/// not a crash.
#[tokio::test]
async fn with_the_bonus_on_each_finder_gets_their_own_distribution() {
    let h = match connect_or_skip_with_bonus(1, "test_dist_bonuskey_", 100_000).await {
        Some(h) => h,
        None => return,
    };
    const A: &str = "bc1qz73djgeh27nlhfrs8jz5sfwr84gxlwgxhqjc6h";
    const B: &str = "bc1qrzc80e3q4vf3qcccrzegvvr7zgkrlsawx42vhf";
    cleanup_addresses(&h.pool, &[A, B]).await;

    let window = build_window(&h).await;
    seed_share(&window, A, 50.0, 1_700_000_000_001).await;
    seed_share(&window, B, 50.0, 1_700_000_000_002).await;

    const T: u64 = 312_500_000;
    let a_id = AddressId::new(A).unwrap();
    let b_id = AddressId::new(B).unwrap();
    let for_a = h.builder.build(T, Some(&a_id)).await.expect("build for A");
    let for_b = h.builder.build(T, Some(&b_id)).await.expect("build for B");

    // Distinct fingerprints: the fingerprint covers per-entry score
    // weight, and the bonus IS score weight, so naming a different
    // finder is a different distribution by identity — which is also
    // what makes each block book against its own snapshot.
    assert_ne!(
        for_a.payouts_fingerprint(),
        for_b.payouts_fingerprint(),
        "a per-finder build must not collide with another finder's"
    );

    let paid = |built: &Arc<DistributionResult>, who: &str| -> u64 {
        built
            .distribution
            .payout_entries_at(T)
            .expect("§4")
            .iter()
            .find(|(a, _)| a.as_str() == who)
            .map(|(_, s)| *s)
            .expect("paid")
    };
    // Equal shares, so whoever is named finder must out-earn the other
    // by exactly the bonus — and symmetrically.
    assert!(
        paid(&for_a, A) > paid(&for_a, B),
        "A's own build must pay A the bonus"
    );
    assert!(
        paid(&for_b, B) > paid(&for_b, A),
        "B's own build must pay B the bonus"
    );
    assert_eq!(
        paid(&for_a, A),
        paid(&for_b, B),
        "the bonus is symmetric between equal-weight finders"
    );

    cleanup(&h.pool, &h.address_prefix).await;
    cleanup_addresses(&h.pool, &[A, B]).await;
}

/// Bonus OFF — the finder argument must not fragment the cache.
///
/// This is the regression that would tax every pool that never enabled
/// the feature: a per-finder key with a finder-independent result means N
/// connected miners mint N identical entries, each holding a full payout
/// list, turning an O(N) snapshot footprint into O(N²) for no benefit.
/// One shared entry is the pre-bonus behaviour, and it is preserved
/// deliberately rather than by accident.
#[tokio::test]
async fn with_the_bonus_off_different_finders_share_one_build() {
    let h = match connect_or_skip(2, "test_dist_nobonus_").await {
        Some(h) => h,
        None => return,
    };
    const A: &str = "bc1q7nnuxx6mnlms6g3j52ugnxn5yvpwevx20e86qd";
    const B: &str = "bc1qwdtvdmlqcs0w70ek862chanyv7py0n4knzmq2k";
    cleanup_addresses(&h.pool, &[A, B]).await;

    let window = build_window(&h).await;
    seed_share(&window, A, 50.0, 1_700_000_000_001).await;
    seed_share(&window, B, 50.0, 1_700_000_000_002).await;

    const T: u64 = 312_500_000;
    let a_id = AddressId::new(A).unwrap();
    let b_id = AddressId::new(B).unwrap();
    let loads_before = h.builder.inputs_loads();
    let for_a = h.builder.build(T, Some(&a_id)).await.expect("build for A");
    let for_b = h.builder.build(T, Some(&b_id)).await.expect("build for B");
    let for_none = h.builder.build(T, None).await.expect("build for nobody");

    // Same fingerprint AND the same cached Arc: with no bonus the finder
    // is not part of the key at all, so these are one entry.
    assert_eq!(
        for_a.payouts_fingerprint(),
        for_b.payouts_fingerprint(),
        "no bonus configured — the build cannot depend on who is asking"
    );
    assert!(
        Arc::ptr_eq(&for_a, &for_b) && Arc::ptr_eq(&for_a, &for_none),
        "no bonus configured — every caller at one revenue must share ONE cache entry"
    );
    assert_eq!(
        h.builder.inputs_loads() - loads_before,
        1,
        "one window+ledger load for all three callers"
    );

    cleanup(&h.pool, &h.address_prefix).await;
    cleanup_addresses(&h.pool, &[A, B]).await;
}

/// Bonus ON, but nobody named — the build must still SUCCEED, and pay no
/// bonus.
///
/// This is the pool-wide JDP publisher's call, and it is load-bearing far
/// beyond PPLNS. `build_pool_wide` fills the slot `current_pool_wide()`
/// answers, which is the only thing that makes `distribution_available`
/// true, which is the only thing that lets ext 0x0003 be negotiated — on
/// every session, in every mode. A configured bonus that made this build
/// unavailable would take non-custodial payout enforcement down pool-wide
/// and carry Solo and Group-Solo with it, and it would do so silently: the
/// publisher's `else { continue }` logs nothing. It would also strand the
/// per-finder path this feature exists for, because `build_for_miner` is
/// reached only after that same negotiation succeeds.
///
/// Both directions are asserted in one test, so neither half can pass on a
/// precondition that quietly did not hold: the same builder that serves an
/// unnamed caller a bonus-free list must serve a named one a boosted one.
#[tokio::test]
async fn with_the_bonus_on_an_unnamed_caller_still_gets_a_bonus_free_build() {
    const BONUS_PPM: u32 = 100_000; // 10 %
    let h = match connect_or_skip_with_bonus(16, "test_dist_bonusnone_", BONUS_PPM).await {
        Some(h) => h,
        None => return,
    };
    const A: &str = "bc1qk39gw6gqt2kufm2y3qxc8mejl8sqgykgvrfc8u";
    const B: &str = "bc1qdglsutw8wh9jucdsqse6cangz8nxj2rucvc00u";
    cleanup_addresses(&h.pool, &[A, B]).await;

    let window = build_window(&h).await;
    seed_share(&window, A, 50.0, 1_700_000_000_001).await;
    seed_share(&window, B, 50.0, 1_700_000_000_002).await;

    const T: u64 = 312_500_000;
    let paid = |built: &Arc<DistributionResult>, who: &str| -> u64 {
        built
            .distribution
            .payout_entries_at(T)
            .expect("§4")
            .iter()
            .find(|(a, _)| a.as_str() == who)
            .map(|(_, s)| *s)
            .expect("paid")
    };

    // The publisher's call: a bonus IS configured, and it passes `None`.
    let pool_wide = h
        .builder
        .build(T, None)
        .await
        .expect("a configured bonus must not make the pool-wide build unavailable");
    assert!(
        h.builder.finder_bonus_active(),
        "precondition: this harness must have the bonus ON, or the test proves nothing"
    );
    // Equal weights in, so equal sats out — nobody was named, so nobody is
    // boosted. This is the property that makes the fallback safe: the worst
    // it can do is pay a finder no bonus, never pay the wrong miner one.
    assert_eq!(
        paid(&pool_wide, A),
        paid(&pool_wide, B),
        "no finder named — two equal-weight miners must be paid equally, with no bonus \
         going to whichever one happened to be built first"
    );

    // The negative control, same builder: naming a finder DOES boost them.
    // Without this the assertion above would also hold on a build where the
    // bonus silently never applied at all.
    let a_id = AddressId::new(A).unwrap();
    let for_a = h.builder.build(T, Some(&a_id)).await.expect("build for A");
    assert!(
        paid(&for_a, A) > paid(&for_a, B),
        "the bonus must still reach a NAMED finder — otherwise this test is asserting \
         equality on a feature that is simply inert"
    );
    assert_ne!(
        pool_wide.payouts_fingerprint(),
        for_a.payouts_fingerprint(),
        "the unnamed build and A's build are different distributions"
    );

    cleanup(&h.pool, &h.address_prefix).await;
    cleanup_addresses(&h.pool, &[A, B]).await;
}

fn redis_db_for_prefix(prefix: &str) -> u8 {
    // Mirror the manual db assignments in `#[tokio::test]`s above.
    // Brittle but kept obvious — change this table if you renumber the
    // tests.
    match prefix {
        "test_dist_e2e_" => 8,
        "test_dist_ledger_" => 9,
        "test_dist_dedup_" => 10,
        "test_dist_inval_" => 11,
        "test_dist_rew_" => 12,
        "test_dist_empty_" => 13,
        "test_dist_inputs_" => 14,
        "test_dist_fp_" => 15,
        "test_dist_oom_" => 6,
        "test_dist_nopg_" => 5,
        "test_dist_nowin_" => 7,
        "test_dist_boot_" => 4,
        "test_dist_bonus_" => 0,
        "test_dist_bonuskey_" => 1,
        "test_dist_nobonus_" => 2,
        "test_dist_bonusnone_" => 16,
        other => panic!("unknown test prefix: {other}"),
    }
}

// ── A failed snapshot write must not collapse the distribution ──────
//
// `pplns_payouts` turns any `build_distribution` error into "serve no job",
// so a build that returns `Err` leaves every miner in the window without one
// until it recovers. A Redis blip on the snapshot write must therefore not
// fail the build: the distribution is correct and is about to become a
// coinbase. Losing the snapshot costs a reprocess; losing the distribution
// costs the pool's miners their hashing time over a fault that changed
// nothing about who is owed what.
//
// The write is made to fail for real, not mocked — but the injection has to
// be LOCAL. `CONFIG SET maxmemory 1` was used here originally and is
// server-global: while it was in force, every other test writing to this
// Redis — in this file and in other crates running concurrently — failed with
// OOM. That was the whole flake.
//
// Occupying the snapshot key with a wrong-typed value was the next attempt.
// It cannot work: `write_weight_snapshot` issues `DEL` before its `HSET`, so
// it clears the obstacle itself — and it aims at `pplns:snapshot:<fingerprint>`
// anyway, not the bare prefix. Measured 2026-08-03: the write succeeded and
// the test asserted nothing about its own subject.
//
// A read-only Redis ACL user is the injection that holds. It is scoped to
// THIS connection, so concurrent tests are untouched, and it reproduces the
// production shape exactly: reads keep working, every write is refused.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_write_failure_still_returns_the_pplns_distribution() {
    let h = match connect_or_skip(6, "test_dist_oom_").await {
        Some(h) => h,
        None => return,
    };
    const ADDR_A: &str = "bc1q5ndveg7ps2wulxxmsdlqgekduxcsys43pxlxjr";
    const ADDR_B: &str = "bc1qzlx2znc9s9cga7g8836gpej6p9pc94k3xgylrh";
    cleanup_addresses(&h.pool, &[ADDR_A, ADDR_B]).await;

    let window = build_window(&h).await;
    // Seed while writes still work.
    seed_share(&window, ADDR_A, 60.0, 1_700_000_000_001).await;
    seed_share(&window, ADDR_B, 40.0, 1_700_000_000_002).await;

    const ACL_USER: &str = "bp_test_readonly_snapshot";
    let redis_base = std::env::var("BP_REDIS_URL").unwrap_or_else(|_| REDIS_URL.to_string());
    // The SAME logical DB the harness seeded into. Hardcoding the local
    // number here worked only while local numbers WERE the raw index; the
    // per-binary ranges made that silently point at an empty database and
    // the distribution came back empty.
    let db = redis_db_for_prefix(&h.address_prefix);
    let db =
        bp_test_support::redis_db_in_range(bp_test_support::redis_db::PPLNS_DISTRIBUTION, db).await;
    let client = Client::open(format!("{redis_base}/{db}")).expect("client");
    let mut admin = ConnectionManager::new(client).await.expect("admin conn");

    // Everything except writes. `-@write` covers DEL/HSET/EXPIRE, so the
    // snapshot write fails at its first command; HGETALL stays allowed.
    if redis::cmd("ACL")
        .arg("SETUSER")
        .arg(ACL_USER)
        .arg("on")
        .arg(">readonlypw")
        .arg("~*")
        .arg("&*")
        .arg("+@all")
        .arg("-@write")
        .query_async::<()>(&mut admin)
        .await
        .is_err()
    {
        eprintln!("redis ACL unavailable — skipping");
        cleanup_addresses(&h.pool, &[ADDR_A, ADDR_B]).await;
        return;
    }

    let ro_url = redis_base.replacen("redis://", &format!("redis://{ACL_USER}:readonlypw@"), 1);
    let ro_client = Client::open(format!("{ro_url}/{db}")).expect("read-only client");
    let ro_conn = ConnectionManager::new(ro_client)
        .await
        .expect("read-only conn");
    let ro_builder = DistributionBuilder::new(
        h.pool.clone(),
        WindowStore::new(ro_conn, 4.0, 100, NetworkDifficulty::new(1_000_000.0)),
        DistributionConfig::from_engine_config(&PplnsEngineConfig {
            fee_address: Some(AddressId::new(FEE_ADDR).unwrap()),
            ..PplnsEngineConfig::default()
        }),
    );

    // Window read + ledger read still work; only the snapshot write is
    // rejected. The build must survive it.
    let result = ro_builder
        .build(312_500_000, None)
        .await
        .expect("a rejected snapshot write must not fail the distribution build");

    assert!(
        !result.snapshot_written,
        "the read-only user must have rejected the snapshot write — without \
         that this test never exercises its subject"
    );
    assert!(
        result.distribution.published().count() >= 2,
        "the real PPLNS distribution must come back, not a solo fallback: {:?}",
        result.distribution.entries
    );
    assert!(
        result
            .distribution
            .published()
            .any(|e| e.address.as_str() == ADDR_B),
        "every miner in the window must still be paid — a solo fallback would \
         leave only the requesting address"
    );

    let _: () = redis::cmd("ACL")
        .arg("DELUSER")
        .arg(ACL_USER)
        .query_async(&mut admin)
        .await
        .expect("drop the read-only user");
    cleanup_addresses(&h.pool, &[ADDR_A, ADDR_B]).await;
}

// ── The two input reads must fail differently ───────────────────────
//
// `load_inputs` reads the window from Redis and the open-balance ledger
// from Postgres, and the two are not interchangeable.
//
// The window IS the shares: without it there is nothing to distribute and
// nothing may be invented, so the build fails and the resolver serves no
// job. The ledger is a set of PROMISES on top of that split, and a promise
// that cannot be read right now is not a promise that is lost — it still
// sits in `pplns_balance`. Failing the build over it would blank every
// PPLNS job in the pool during a Postgres fault that costs nothing but a
// one-block delay in repayments, while `record_share` (Redis only) keeps
// crediting every miner correctly throughout.
//
// The pair below pins both halves, with a control on each so neither can
// pass for an unrelated reason.

/// A `PgPool` that will never connect. Used to make the ledger read fail
/// for real rather than mocking the decision away.
fn unreachable_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(300))
        .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/nope")
        .expect("a lazily-connected pool parses its url")
}

#[tokio::test]
async fn an_unreadable_ledger_degrades_to_a_score_only_distribution() {
    let h = match connect_or_skip(5, "test_dist_nopg_").await {
        Some(h) => h,
        None => return,
    };
    // Own pair, per the rule above: these appear nowhere else that writes
    // `pplns_balance`. Reusing another test's addresses had this test
    // deleting its own seeded balance out from under itself.
    const ADDR_A: &str = "bc1q307hujcervvdfr73ntlam2f7w65j6gs9zcnf39";
    const ADDR_B: &str = "bc1qywnf55acqpxr0lekg2gmy2s46pzxqze99j0u9y";
    const REWARD: u64 = 312_500_000;
    const OWED: i64 = 1_000_000;
    cleanup_addresses(&h.pool, &[ADDR_A, ADDR_B]).await;

    let window = build_window(&h).await;
    seed_share(&window, ADDR_A, 60.0, 1_700_000_000_001).await;
    seed_share(&window, ADDR_B, 40.0, 1_700_000_000_002).await;
    seed_open_balance(&h.pool, ADDR_A, OWED, 0).await;

    // ── Control: with the ledger readable, the promise is IN the build ──
    let normal = h
        .builder
        .build(REWARD, None)
        .await
        .expect("build with ledger");
    let a_id = AddressId::new(ADDR_A).unwrap();
    let a_normal = normal
        .distribution
        .entries
        .iter()
        .find(|e| e.address == a_id)
        .expect("the owed miner is in the distribution");
    assert_eq!(
        a_normal.balance_sats, OWED,
        "precondition: a readable ledger puts the promise in the snapshot inputs"
    );
    assert!(
        a_normal.wire_weight > a_normal.score_weight,
        "precondition: the promise BOOSTS the published weight ({} vs score {}) —          without this the degraded case below would be indistinguishable",
        a_normal.wire_weight,
        a_normal.score_weight
    );

    // ── Subject: same window, ledger unreachable ────────────────────
    let degraded_builder = DistributionBuilder::new(
        unreachable_pool(),
        build_window(&h).await,
        DistributionConfig::from_engine_config(&PplnsEngineConfig {
            fee_address: Some(AddressId::new(FEE_ADDR).unwrap()),
            ..PplnsEngineConfig::default()
        }),
    );
    let degraded = degraded_builder.build(REWARD, None).await.expect(
        "an unreadable ledger must NOT fail the build — every PPLNS miner \
                 would lose their job over a fault that costs one block of delay",
    );

    assert_eq!(
        degraded.distribution.extras_total, 0,
        "a score-only distribution promises nothing"
    );
    for entry in &degraded.distribution.entries {
        assert_eq!(
            entry.balance_sats,
            0,
            "{} must carry no promise when the ledger could not be read",
            entry.address.as_str()
        );
        assert_eq!(
            entry.wire_weight,
            entry.score_weight,
            "{} must be published at its pure score weight",
            entry.address.as_str()
        );
    }
    // Both miners are still paid — this is a real distribution, not a
    // stand-in, and a block found on it settles from its own snapshot.
    for addr in [ADDR_A, ADDR_B] {
        let id = AddressId::new(addr).unwrap();
        assert!(
            degraded.distribution.published().any(|e| e.address == id),
            "{addr} must still be paid by score"
        );
    }
    assert!(
        degraded.snapshot_written,
        "the degraded distribution is bookable like any other"
    );

    // And the ledger row is untouched: the promise waits for a later block.
    let still_owed: (i64,) =
        sqlx::query_as(r#"SELECT "balanceSats" FROM pplns_balance WHERE address = $1"#)
            .bind(ADDR_A)
            .fetch_one(&h.pool)
            .await
            .expect("balance row");
    assert_eq!(
        still_owed.0, OWED,
        "the unread promise must survive — it is repaid from a later block"
    );

    cleanup_addresses(&h.pool, &[ADDR_A, ADDR_B]).await;
}

#[tokio::test]
async fn an_unreadable_window_fails_the_build_instead_of_degrading() {
    let h = match connect_or_skip(7, "test_dist_nowin_").await {
        Some(h) => h,
        None => return,
    };
    const ADDR: &str = "bc1qy5jzxu2u6jeps9duq8fgdxuacjan93vy6yg4j8";
    const REWARD: u64 = 312_500_000;
    cleanup_addresses(&h.pool, &[ADDR]).await;

    let window = build_window(&h).await;
    seed_share(&window, ADDR, 100.0, 1_700_000_000_001).await;

    // Control: it builds while the window is readable.
    h.builder
        .build(REWARD, None)
        .await
        .expect("precondition: the build works before the window is broken");

    // Break the window read for real: overwrite the aggregate HASH with a
    // STRING, so `HGETALL` answers WRONGTYPE. Deterministic, and local to
    // this test's Redis DB — unlike a server-global knob.
    let mut raw = raw_conn(&h).await;
    let _: () = redis::cmd("SET")
        .arg(bp_pplns_engine::window::KEY_WINDOW_BY_ADDRESS)
        .arg("not-a-hash")
        .query_async(&mut raw)
        .await
        .expect("clobber the window key");
    h.builder.invalidate_all();

    let err = h.builder.build(REWARD, None).await.expect_err(
        "an unreadable window must FAIL the build — there are no shares \
                     to distribute, and inventing a distribution would pay the wrong \
                     miners",
    );
    let msg = err.to_string();
    assert!(
        msg.contains("window read"),
        "the failure must name the window, not be some other error: {msg}"
    );

    let _: () = redis::cmd("DEL")
        .arg(bp_pplns_engine::window::KEY_WINDOW_BY_ADDRESS)
        .query_async(&mut raw)
        .await
        .expect("drop the clobbered key");
    cleanup_addresses(&h.pool, &[ADDR]).await;
}

/// A raw connection to the same Redis DB the harness chose — for the
/// tests that need to corrupt a key rather than go through `WindowStore`.
async fn raw_conn(h: &Harness) -> ConnectionManager {
    let redis_base = std::env::var("BP_REDIS_URL").unwrap_or_else(|_| REDIS_URL.to_string());
    let db = redis_db_for_prefix(&h.address_prefix);
    let db =
        bp_test_support::redis_db_in_range(bp_test_support::redis_db::PPLNS_DISTRIBUTION, db).await;
    let client = Client::open(format!("{redis_base}/{db}")).expect("client");
    ConnectionManager::new(client).await.expect("conn")
}
