// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(clippy::print_stderr)]
#![allow(clippy::needless_return)]

//! The duplicate-finder ledger booking, against docker-PG + docker-Redis.
//!
//! A PPLNS distribution that carries a finder bonus names the finder
//! **twice**: once for the dedicated bonus output
//! (`bp_pplns::distribution` `:649-657`) and once for their proportional
//! share (`:669-678`). Both are valid `TxOut`s and the block pays both
//! on-chain — but the ledger keys on `address`, so the booking path has
//! to fold them together before it writes.
//!
//! `build_writes_from_snapshot` (`bp-pplns-engine/src/engine.rs`) does
//! that fold via `merge_distribution_by_address`. It did not always, and
//! without it the duplicate reaches Postgres intact, where both writes in
//! `apply_distribution`'s single transaction (`ledger/mod.rs:92`)
//! mishandle it in different ways:
//!
//! - `pplns_balance` is `ON CONFLICT (address) DO UPDATE`
//!   (`bp-db/src/pplns.rs:318`) → Postgres raises `ON CONFLICT DO UPDATE
//!   command cannot affect row a second time` (SQLSTATE 21000), the
//!   transaction aborts, and nothing is booked. The block paid on-chain
//!   and the pool has no record of it.
//! - `pplns_payout_history` is `ON CONFLICT ("blockHeight", address) DO
//!   NOTHING` (`:416`) → no error at all; the second row is silently
//!   dropped and the audit trail under-counts what the coinbase paid.
//!
//! Both halves are covered here, because fixing one leaves the other
//! broken and the balance-side abort *masks* the audit-side loss: the
//! first test drives the real engine path end to end, and the second
//! drives the DB primitive directly with an unmerged list to show what
//! the merge is standing between the pool and.
//!
//! **Pool size is pinned deliberately.** The bonus output is carved out
//! before the proportional split and the greedy-largest-first weight
//! trim keeps the largest targets first, so above roughly 500 addresses
//! at the 50,000 WU budget floor the trim eats the finder's
//! proportional entry, the duplicate never forms, and a test at that
//! size passes for the wrong reason. Measured with
//! `bp-pplns/tests/spike_bonus_sizing_modes.rs`: `finder_entries == 2`
//! for every pool from 5 to 400 addresses, and `== 1` at 500 and 1000.
//! [`POOL_ADDRESSES`] sits inside the reproducing band, and
//! [`finder_appears_twice`] fails the test if the duplicate did not
//! actually form.
//!
//! Gated on the same local services as the sibling integration tests
//! (`BP_PG_URL` / `BP_REDIS_URL`); skips cleanly via `eprintln!` +
//! early return when either is unavailable.

use std::collections::{HashMap, HashSet};

use bp_common::{AddressId, Sats};
use bp_pplns::{
    build_coinbase_distribution, CoinbaseDistributionEntry, CoinbaseDistributionInput,
    DEFAULT_COINBASE_WEIGHT_BUDGET, DEFAULT_MIN_PAYOUT_SATS,
};
use bp_pplns_engine::config::PplnsEngineConfig;
use bp_pplns_engine::engine::PplnsEngine;
use bp_pplns_engine::ledger::{apply_distribution, coinbase_row, BalanceWrite};
use bp_pplns_engine::window::snapshot::StoredSnapshot;
use bp_pplns_engine::window::NetworkDifficulty;
use bp_share::payouts_fingerprint_from_parts;
use bp_test_support::{connect_pg_or_skip, connect_redis_or_skip, deterministic_p2wpkh_regtest};
use sqlx::PgPool;

/// Logical Redis DB for this file. Test binaries run one at a time under
/// `cargo test`, so reusing a number another file also takes is safe;
/// what matters is that no two tests *within* a binary share one, since
/// each `connect_redis_or_skip` does a `FLUSHDB`.
const REDIS_TEST_DB: u8 = 12;

/// Synthetic block heights, above any real chain height and distinct per
/// test so concurrent tests in this file can't collide on the
/// `(blockHeight, address)` UNIQUE index.
const BLOCK_HEIGHT_DUPLICATE: i32 = 9_996_001;
const BLOCK_HEIGHT_AUDIT_ONLY: i32 = 9_996_002;

/// Miner addresses in the window. Inside the measured 5..=400 band where
/// the finder's proportional entry survives the trim and the duplicate
/// actually forms — see the module docs.
const POOL_ADDRESSES: usize = 200;

/// 0.1776 BTC — the bonus this feature ships with.
const FINDER_BONUS_SATS: i64 = 17_760_000;

/// Current subsidy plus a plausible fee total. Any reward works; this one
/// keeps the numbers recognisable against the spike measurements.
const BLOCK_REWARD_SATS: u64 = 317_500_000;

/// The autoscaler's floor, and the value the duplicate-formation band was
/// measured at.
const WEIGHT_BUDGET: u32 = DEFAULT_COINBASE_WEIGHT_BUDGET;

/// Engine config mirroring a production PPLNS deployment: fee address
/// set, background tasks quiet for the duration of the test.
fn test_engine_config(fee_addr: &str) -> PplnsEngineConfig {
    PplnsEngineConfig {
        dust_sweep_enabled: false,
        touch_flush_interval_secs: 3_600,
        fee_address: Some(AddressId::new(fee_addr.to_string()).expect("fee addr valid")),
        fee_percent: 1.5,
        min_payout_sats: Sats(DEFAULT_MIN_PAYOUT_SATS as i64),
        coinbase_weight_budget: WEIGHT_BUDGET,
        ..PplnsEngineConfig::default()
    }
}

/// `POOL_ADDRESSES` distinct, genuinely payable regtest P2WPKH addresses.
///
/// Real addresses rather than prefixed test strings: the distribution
/// input is filtered through `is_valid_payout_address`, and the weight
/// trim sizes each output from its actual script, so synthetic strings
/// would take a different path than production.
///
/// `set` partitions the address space per test. The two tests here run
/// concurrently and both accumulate `totalPaidSats`, so sharing one set
/// would let each test's booking inflate the other's expected lifetime
/// total.
fn pool_addresses(set: u8) -> Vec<AddressId> {
    (0..POOL_ADDRESSES)
        .map(|i| {
            let mut seed = [0u8; 32];
            seed[0] = (i & 0xff) as u8;
            seed[1] = ((i >> 8) & 0xff) as u8;
            seed[2] = set;
            // Distinguish this file's addresses from the sibling regtest
            // tests', which seed with single repeated bytes.
            seed[31] = 0xdd;
            AddressId::new(deterministic_p2wpkh_regtest(seed)).expect("generated address is valid")
        })
        .collect()
}

/// The pool-fee address for one test's address set — also partitioned,
/// for the same reason as [`pool_addresses`].
fn fee_address_for(set: u8) -> AddressId {
    let mut seed = [0xfe; 32];
    seed[2] = set;
    AddressId::new(deterministic_p2wpkh_regtest(seed)).expect("fee address valid")
}

/// Build the payout list the PPLNS path produces with the bonus
/// configured: equal shares for every address, the bonus carved out for
/// `finder`.
///
/// Calls the shared math directly rather than going through
/// `PplnsEngine::build_distribution`, which would drag in Redis, a share
/// window and a snapshot write for no gain here: what is under test is the
/// *booking* of a list in which the finder appears twice, and the list's
/// shape is fully determined by the inputs below. The engine's own
/// threading of the config through to those inputs is covered by
/// `distribution_integration::finder_bonus_pays_the_named_finder`.
fn distribution_with_finder_bonus(
    miners: &[AddressId],
    fee_address: &AddressId,
    finder: &AddressId,
) -> Vec<CoinbaseDistributionEntry> {
    let address_shares: HashMap<AddressId, f64> = miners.iter().map(|a| (a.clone(), 1.0)).collect();
    let balances: HashMap<AddressId, Sats> = HashMap::new();

    let result = build_coinbase_distribution(CoinbaseDistributionInput {
        address_shares: &address_shares,
        balances: &balances,
        block_reward_sats: Sats(BLOCK_REWARD_SATS as i64),
        fee_percent: 1.5,
        fee_address: Some(fee_address),
        coinbase_weight_budget: WEIGHT_BUDGET,
        suppress_matching_debits: false,
        min_payout_sats: Some(Sats(DEFAULT_MIN_PAYOUT_SATS as i64)),
        finder_bonus_sats: Some(Sats(FINDER_BONUS_SATS)),
        finder_address: Some(finder),
    });
    result.payouts
}

/// How many coinbase outputs pay `finder`. Two is the condition under
/// test; one means the trim ate the proportional entry and the test
/// would prove nothing.
fn finder_appears_twice(payouts: &[CoinbaseDistributionEntry], finder: &AddressId) -> bool {
    payouts.iter().filter(|p| &p.address == finder).count() == 2
}

async fn cleanup(pool: &PgPool, addresses: &[AddressId], block_height: i32) {
    let addrs: Vec<String> = addresses.iter().map(|a| a.as_str().to_string()).collect();
    let _ = sqlx::query(r#"DELETE FROM pplns_payout_history WHERE "blockHeight" = $1"#)
        .bind(block_height)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM pplns_balance WHERE address = ANY($1)")
        .bind(&addrs)
        .execute(pool)
        .await;
}

// ── The gate: a duplicate-finder distribution must book ──────────────

/// A PPLNS block whose distribution names the finder twice must book,
/// and the audit trail it leaves must account for every satoshi the
/// coinbase paid.
///
/// Two independent assertions, both failing today:
///
/// 1. `apply_prepared` succeeds. Fails on `ON CONFLICT DO UPDATE command
///    cannot affect row a second time` from the `pplns_balance` upsert,
///    which aborts the whole transaction — so the block is unbookable,
///    and `block_confirmation.rs:248` retries the deterministic error
///    every tick forever.
/// 2. `Σ(persisted coinbase audit rows) == Σ(coinbase outputs)`. Fails
///    independently on the `pplns_payout_history` `DO NOTHING` drop,
///    which loses the duplicate row and under-counts the chain.
///
/// Only `coinbase` rows are summed. `pending` rows carry a signed
/// *delta* against the prior balance rather than an on-chain amount
/// (`engine.rs:664-688`), so including them would compare two different
/// quantities.
#[tokio::test]
async fn duplicate_finder_distribution_books_and_conserves_sats() {
    let Some(redis_conn) = connect_redis_or_skip(REDIS_TEST_DB).await else {
        return;
    };
    let Some(pool) = connect_pg_or_skip().await else {
        return;
    };

    let miners = pool_addresses(0);
    let fee_address = fee_address_for(0);
    // The finder is an ordinary pool member — every address in the
    // window is equally likely to find the block.
    let finder = miners[0].clone();

    let mut all_addresses = miners.clone();
    all_addresses.push(fee_address.clone());
    cleanup(&pool, &all_addresses, BLOCK_HEIGHT_DUPLICATE).await;

    let payouts = distribution_with_finder_bonus(&miners, &fee_address, &finder);
    assert!(
        finder_appears_twice(&payouts, &finder),
        "test precondition: the finder must hold BOTH a bonus output and a \
         proportional output, otherwise the duplicate the ledger has to merge \
         never forms and this test proves nothing. Got {} entries for the \
         finder across {} payouts at {POOL_ADDRESSES} addresses / \
         {WEIGHT_BUDGET} WU — if the trim has started eating the proportional \
         entry at this pool size, lower POOL_ADDRESSES (measured band is \
         5..=400).",
        payouts.iter().filter(|p| p.address == finder).count(),
        payouts.len(),
    );

    // What the coinbase pays the finder, across both of its outputs.
    let finder_on_chain: i64 = payouts
        .iter()
        .filter(|p| p.address == finder)
        .map(|p| p.sats.0)
        .sum();
    // The whole coinbase. `build_coinbase_distribution` consumes the
    // entire reward, so this is also the on-chain total.
    let coinbase_total: i64 = payouts.iter().map(|p| p.sats.0).sum();
    assert_eq!(
        coinbase_total, BLOCK_REWARD_SATS as i64,
        "the payout list IS the coinbase — it must consume the whole reward"
    );

    let engine = PplnsEngine::spawn(
        test_engine_config(fee_address.as_str()),
        redis_conn,
        pool.clone(),
        NetworkDifficulty::new(1_000.0),
    )
    .await
    .expect("PplnsEngine::spawn");

    // Persist the distribution as the snapshot the block-found path will
    // read, under the fingerprint of its own payout list — the same key
    // `build_from_inputs` writes (`distribution.rs:367-381`) and the same
    // one a job carries.
    let considered: HashSet<AddressId> = miners.iter().cloned().collect();
    let balance_after: HashMap<AddressId, Sats> = HashMap::new();
    let snapshot = StoredSnapshot::from_math_with_before(
        &payouts,
        BLOCK_REWARD_SATS,
        &considered,
        &balance_after,
        &HashMap::new(),
    );
    let fingerprint = payouts_fingerprint_from_parts(
        BLOCK_REWARD_SATS,
        payouts
            .iter()
            .map(|p| (p.address.as_str(), p.sats.0.max(0) as u64)),
    );
    engine
        .window()
        .write_snapshot_for(&fingerprint, &snapshot, 600)
        .await
        .expect("snapshot write");

    let prepared = engine
        .prepare_block_found_for(BLOCK_HEIGHT_DUPLICATE, BLOCK_REWARD_SATS, Some(fingerprint))
        .await
        .expect("prepare_block_found_for must resolve the snapshot it just wrote");

    // ── Assertion 1 — the block books at all ────────────────────────
    //
    // The duplicate address reaches `bulk_upsert_pplns_balances` as two
    // rows in one statement; Postgres refuses the second ON CONFLICT hit
    // and the transaction carrying BOTH writes rolls back.
    let outcome = engine.apply_prepared(&prepared).await.expect(
        "a block whose coinbase legitimately pays the finder twice must book — \
         an error here means the pool paid the block on-chain and can never \
         record it (the balance upsert cannot take two rows for one address in \
         one statement, so the whole booking transaction aborts)",
    );

    // ── Assertion 2 — the audit trail matches the chain ─────────────
    //
    // Independent of assertion 1: even with the balance write fixed, the
    // history insert's DO NOTHING silently drops the finder's second row.
    let audit_total: (Option<i64>,) = sqlx::query_as(
        r#"SELECT SUM("paidSats")::bigint FROM pplns_payout_history
           WHERE "blockHeight" = $1 AND "rowType" = 'coinbase'"#,
    )
    .bind(BLOCK_HEIGHT_DUPLICATE)
    .fetch_one(&pool)
    .await
    .expect("read audit rows");
    let audit_total = audit_total.0.unwrap_or(0);
    assert_eq!(
        audit_total,
        coinbase_total,
        "the coinbase audit rows must account for every satoshi the coinbase \
         paid: chain paid {coinbase_total}, ledger recorded {audit_total} \
         (short by {}). The finder holds two outputs and the history insert's \
         ON CONFLICT (\"blockHeight\", address) DO NOTHING drops the second \
         one, so the audit trail under-counts the block.",
        coinbase_total - audit_total,
    );

    // The finder's own two outputs must be folded into one row carrying
    // their sum — the merge has to combine, not pick one.
    let finder_row: (i64, i64) = sqlx::query_as(
        r#"SELECT count(*)::bigint, COALESCE(SUM("paidSats"), 0)::bigint
           FROM pplns_payout_history
           WHERE "blockHeight" = $1 AND address = $2 AND "rowType" = 'coinbase'"#,
    )
    .bind(BLOCK_HEIGHT_DUPLICATE)
    .bind(finder.as_str())
    .fetch_one(&pool)
    .await
    .expect("read finder audit row");
    assert_eq!(
        finder_row.0, 1,
        "the finder's bonus and proportional outputs must collapse to exactly \
         one audit row (the table's UNIQUE (blockHeight, address) permits no \
         more)"
    );
    assert_eq!(
        finder_row.1, finder_on_chain,
        "the finder's single audit row must carry the SUM of both their \
         coinbase outputs ({finder_on_chain}), not just one of them"
    );

    // And the balance side must credit the same total against their
    // lifetime `totalPaidSats`.
    let finder_paid: (i64,) =
        sqlx::query_as(r#"SELECT "totalPaidSats" FROM pplns_balance WHERE address = $1"#)
            .bind(finder.as_str())
            .fetch_one(&pool)
            .await
            .expect("finder must have a balance row");
    assert_eq!(
        finder_paid.0, finder_on_chain,
        "lifetime totalPaidSats must credit both of the finder's coinbase \
         outputs, not just the last write to win the upsert"
    );

    assert!(
        outcome.history_inserted >= 1,
        "the booking must have written audit rows"
    );

    engine.shutdown();
    cleanup(&pool, &all_addresses, BLOCK_HEIGHT_DUPLICATE).await;
}

// ── Why the merge has to cover the audit list too ────────────────────

/// The `pplns_payout_history` silent drop — the second, independent
/// reason `build_writes_from_snapshot` must merge per address.
///
/// This one is easy to miss. The balance upsert aborts the shared
/// transaction *first*, so while that defect is live it masks this one
/// entirely: the history insert never commits, and you cannot see what it
/// would have written. Fix only the `BalanceWrite` list and the sibling
/// test above goes green while the audit trail quietly under-counts the
/// chain on every block the pool ever finds.
///
/// So this bypasses [`build_writes_from_snapshot`] and drives
/// [`apply_distribution`] directly with the unmerged audit list the
/// engine used to produce, alongside a hand-merged balance list so the
/// transaction actually commits. What lands in `pplns_payout_history` is
/// then exactly what `ON CONFLICT ("blockHeight", address) DO NOTHING`
/// decided to keep — one row short, and short by real satoshi.
///
/// It asserts the loss rather than its absence: this is the behaviour of
/// a DB primitive that no amount of engine code changes, and it is what
/// makes the merge load-bearing. If a future change to
/// `bulk_insert_pplns_payout_history` makes duplicates survive (an
/// `ON CONFLICT … DO UPDATE SET "paidSats" = … + EXCLUDED."paidSats"`,
/// say), this test fails and tells you the invariant moved.
#[tokio::test]
async fn unmerged_audit_rows_would_be_silently_dropped_from_history() {
    let Some(pool) = connect_pg_or_skip().await else {
        return;
    };

    let miners = pool_addresses(1);
    let fee_address = fee_address_for(1);
    let finder = miners[0].clone();

    let mut all_addresses = miners.clone();
    all_addresses.push(fee_address.clone());
    cleanup(&pool, &all_addresses, BLOCK_HEIGHT_AUDIT_ONLY).await;

    let payouts = distribution_with_finder_bonus(&miners, &fee_address, &finder);
    assert!(
        finder_appears_twice(&payouts, &finder),
        "test precondition: the finder must hold two coinbase outputs — see \
         the sibling test's note on POOL_ADDRESSES"
    );
    let coinbase_total: i64 = payouts.iter().map(|p| p.sats.0).sum();

    // One audit row per coinbase output, the duplicate intact — the shape
    // `build_writes_from_snapshot` produced before it merged per address.
    let audit_rows: Vec<_> = payouts.iter().map(coinbase_row).collect();

    // The balance list, merged by hand so the upsert is well-formed and
    // this test isolates the history insert. `merge_distribution_by_address`
    // produces this shape for BOTH lists.
    let mut merged_paid: HashMap<String, i64> = HashMap::new();
    let mut merged_order: Vec<AddressId> = Vec::new();
    for entry in &payouts {
        let key = entry.address.as_str().to_string();
        if !merged_paid.contains_key(&key) {
            merged_order.push(entry.address.clone());
        }
        *merged_paid.entry(key).or_insert(0) += entry.sats.0;
    }
    let balance_writes: Vec<BalanceWrite> = merged_order
        .iter()
        .map(|address| BalanceWrite {
            address: address.clone(),
            balance_sats: Sats(0),
            total_paid_sats: Sats(merged_paid[address.as_str()]),
        })
        .collect();

    let outcome = apply_distribution(
        &pool,
        BLOCK_HEIGHT_AUDIT_ONLY,
        &audit_rows,
        &balance_writes,
        1_700_000_000_000,
    )
    .await
    .expect(
        "with a pre-merged balance list the transaction must commit — if it \
             does not, the balance side is not the only problem",
    );

    let audit_total: (Option<i64>,) = sqlx::query_as(
        r#"SELECT SUM("paidSats")::bigint FROM pplns_payout_history
           WHERE "blockHeight" = $1 AND "rowType" = 'coinbase'"#,
    )
    .bind(BLOCK_HEIGHT_AUDIT_ONLY)
    .fetch_one(&pool)
    .await
    .expect("read audit rows");
    let audit_total = audit_total.0.unwrap_or(0);

    // The finder's SECOND output is the one lost: the bonus entry is
    // pushed ahead of the sorted miners (`bp-pplns/src/distribution.rs`
    // `:651` vs `:669`), so it wins the DO NOTHING race and the
    // proportional entry is discarded.
    let finder_proportional = payouts
        .iter()
        .filter(|p| p.address == finder)
        .map(|p| p.sats.0)
        .next_back()
        .expect("finder has outputs");

    assert_eq!(
        outcome.history_inserted,
        audit_rows.len() as u64 - 1,
        "exactly one row must go missing — {} rows in, {} landed",
        audit_rows.len(),
        outcome.history_inserted,
    );
    assert_eq!(
        audit_total,
        coinbase_total - finder_proportional,
        "the dropped row is the finder's proportional output \
         ({finder_proportional} sats): the chain paid {coinbase_total} and \
         an unmerged audit list records only {audit_total}. This is why \
         `merge_distribution_by_address` has to fold the audit list too — \
         a merge of only the balance list leaves the pool's own books \
         permanently short of what it paid out."
    );

    cleanup(&pool, &all_addresses, BLOCK_HEIGHT_AUDIT_ONLY).await;
}
