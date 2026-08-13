// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(clippy::print_stderr)]
#![allow(clippy::needless_return)]

//! Integration tests for `bp-pplns-engine::ledger` against docker-PG.
//!
//! Gated on a local Postgres at
//! `postgres://postgres:postgres@localhost:15433/public_pool`
//! (override with `BP_PG_URL`). Tests skip cleanly via `eprintln!` +
//! early return if the instance isn't reachable.
//!
//! Each test seeds with a unique address-prefix (per-test-name) so
//! parallel runs don't collide on the shared `pplns_*` tables. Tests
//! clean up after themselves with a DELETE in a final block.

use bp_common::{AddressId, Sats};
use bp_pplns_engine::ledger::{
    apply_distribution, coinbase_row, pending_row, touch_buffer::flush_once,
    touch_buffer::TouchBuffer, ApplyDistributionResult, AuditRow, BalanceWrite, PayoutRowType,
};
use sqlx::{postgres::PgPoolOptions, PgPool};

/// `apply_distribution` takes the caller's transaction now, because the
/// balance read it settles against has to be locked in the same one (see
/// the function's docs). These tests hand it their own, which is exactly
/// what the engine does.
async fn apply_in_tx(
    pool: &PgPool,
    block_height: i32,
    rows: &[AuditRow],
    balances: &[BalanceWrite],
    now_ms: i64,
) -> ApplyDistributionResult {
    let mut tx = pool.begin().await.expect("begin");
    let out = apply_distribution(&mut tx, block_height, rows, balances, now_ms)
        .await
        .expect("apply_distribution");
    tx.commit().await.expect("commit");
    out
}

const DEFAULT_URL: &str = "postgres://postgres:postgres@localhost:15433/public_pool";

async fn connect_or_skip() -> Option<PgPool> {
    let url = std::env::var("BP_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(std::time::Duration::from_secs(2))
            .connect(&url),
    )
    .await
    {
        Ok(Ok(p)) => Some(p),
        Ok(Err(e)) => {
            eprintln!("PG connect failed for {url}: {e} — skipping integration test");
            return None;
        }
        Err(_) => {
            eprintln!("PG connect timed out — skipping");
            return None;
        }
    }
}

async fn cleanup(pool: &PgPool, address_prefix: &str, block_heights: &[i32]) {
    let like_pattern = format!("{address_prefix}%");
    let _ = sqlx::query("DELETE FROM pplns_payout_history WHERE \"blockHeight\" = ANY($1)")
        .bind(block_heights)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM pplns_balance WHERE address LIKE $1")
        .bind(&like_pattern)
        .execute(pool)
        .await;
}

// ── Test 1 — apply_distribution writes both tables atomically ──────

#[tokio::test]
async fn apply_distribution_writes_history_and_balance() {
    let pool = match connect_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let block_height = 9_998_001;
    let prefix = "test_apply_dist_";
    cleanup(&pool, prefix, &[block_height]).await;

    let addr_a = AddressId::new(format!("{prefix}aaa")).unwrap();
    let addr_b = AddressId::new(format!("{prefix}bbb")).unwrap();

    let rows = vec![
        AuditRow {
            address: addr_a.clone(),
            paid_sats: Sats(150_000),
            percent: 60.0,
            row_type: PayoutRowType::Coinbase,
        },
        AuditRow {
            address: addr_b.clone(),
            paid_sats: Sats(100_000),
            percent: 40.0,
            row_type: PayoutRowType::Coinbase,
        },
    ];
    let balances = vec![
        BalanceWrite {
            address: addr_a.clone(),
            balance_sats: Sats(0),
            total_paid_sats: Sats(150_000),
        },
        BalanceWrite {
            address: addr_b.clone(),
            balance_sats: Sats(0),
            total_paid_sats: Sats(100_000),
        },
    ];

    let result = apply_in_tx(&pool, block_height, &rows, &balances, 1_700_000_000_000).await;
    assert_eq!(result.history_inserted, 2);
    assert_eq!(result.balances_affected, 2);

    // Verify both tables.
    let hist_count: (i64,) =
        sqlx::query_as(r#"SELECT count(*) FROM pplns_payout_history WHERE "blockHeight" = $1"#)
            .bind(block_height)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(hist_count.0, 2);

    let bal: Vec<(String, i64, i64)> = sqlx::query_as(
        r#"SELECT address, "balanceSats", "totalPaidSats"
           FROM pplns_balance WHERE address LIKE $1 ORDER BY address"#,
    )
    .bind(format!("{prefix}%"))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(bal.len(), 2);
    assert_eq!(bal[0].2, 150_000);
    assert_eq!(bal[1].2, 100_000);

    cleanup(&pool, prefix, &[block_height]).await;
}

// ── Test 2 — apply_distribution replay is idempotent ────────────────

#[tokio::test]
async fn apply_distribution_replay_idempotent() {
    let pool = match connect_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let block_height = 9_998_002;
    let prefix = "test_replay_";
    cleanup(&pool, prefix, &[block_height]).await;

    let addr = AddressId::new(format!("{prefix}miner")).unwrap();
    let rows = vec![AuditRow {
        address: addr.clone(),
        paid_sats: Sats(500_000),
        percent: 100.0,
        row_type: PayoutRowType::Coinbase,
    }];
    let balances = vec![BalanceWrite {
        address: addr.clone(),
        balance_sats: Sats(0),
        total_paid_sats: Sats(500_000),
    }];

    let first = apply_in_tx(&pool, block_height, &rows, &balances, 1_700_000_000_000).await;
    assert_eq!(first.history_inserted, 1);

    // Replay: same block_height + same address triggers the
    // (blockHeight, address) UNIQUE-collision-DO-NOTHING path. The balance
    // upsert is now SKIPPED (gated on a non-zero history insert), so a replay
    // can never double-count the accumulated totalPaidSats.
    let second = apply_in_tx(&pool, block_height, &rows, &balances, 1_700_000_060_000).await;
    assert_eq!(
        second.history_inserted, 0,
        "replay must not duplicate history rows"
    );
    assert_eq!(
        second.balances_affected, 0,
        "replay must skip the balance upsert (idempotency gate)"
    );

    // Verify exactly 1 history row, and totalPaidSats still 500k (not doubled).
    let hist_count: (i64,) =
        sqlx::query_as(r#"SELECT count(*) FROM pplns_payout_history WHERE "blockHeight" = $1"#)
            .bind(block_height)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(hist_count.0, 1);
    let total_paid: (i64,) =
        sqlx::query_as(r#"SELECT "totalPaidSats" FROM pplns_balance WHERE address = $1"#)
            .bind(addr.as_str())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        total_paid.0, 500_000,
        "replay must not inflate totalPaidSats"
    );

    cleanup(&pool, prefix, &[block_height]).await;
}

// ── Test 3 — apply_distribution with mixed audit row types ──────────

#[tokio::test]
async fn apply_distribution_mixed_row_types() {
    let pool = match connect_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let block_height = 9_998_003;
    let prefix = "test_mixed_";
    cleanup(&pool, prefix, &[block_height]).await;

    let addr_a = AddressId::new(format!("{prefix}coinbase")).unwrap();
    let addr_b = AddressId::new(format!("{prefix}pending")).unwrap();
    let addr_c = AddressId::new(format!("{prefix}debit")).unwrap();

    let rows = vec![
        AuditRow {
            address: addr_a.clone(),
            paid_sats: Sats(80_000),
            percent: 80.0,
            row_type: PayoutRowType::Coinbase,
        },
        pending_row(addr_b.clone(), Sats(1_500)), // sub-dust credit
        pending_row(addr_c.clone(), Sats(-1_500)), // matching debit
    ];
    let balances = vec![
        BalanceWrite {
            address: addr_a.clone(),
            balance_sats: Sats(0),
            total_paid_sats: Sats(80_000),
        },
        BalanceWrite {
            address: addr_b.clone(),
            balance_sats: Sats(1_500),
            total_paid_sats: Sats(0),
        },
        BalanceWrite {
            address: addr_c.clone(),
            balance_sats: Sats(-1_500),
            total_paid_sats: Sats(0),
        },
    ];

    let result = apply_in_tx(&pool, block_height, &rows, &balances, 1_700_000_000_000).await;
    assert_eq!(result.history_inserted, 3);

    // Verify ledger symmetry holds in the persisted state.
    let signed_sum: (Option<i64>,) = sqlx::query_as(
        r#"SELECT SUM("balanceSats")::bigint FROM pplns_balance WHERE address LIKE $1"#,
    )
    .bind(format!("{prefix}%"))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        signed_sum.0.unwrap_or(0),
        0,
        "signed ledger Σ balanceSats must be 0 (credit ↔ debit pair)"
    );

    // Verify rowType wire strings match expected values.
    let row_types: Vec<(String, String)> = sqlx::query_as(
        r#"SELECT address, "rowType" FROM pplns_payout_history
           WHERE "blockHeight" = $1 ORDER BY address"#,
    )
    .bind(block_height)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(row_types.len(), 3);
    // Sorted by address: coinbase, debit, pending
    let addr_a_str = addr_a.as_str().to_string();
    let addr_b_str = addr_b.as_str().to_string();
    let addr_c_str = addr_c.as_str().to_string();
    let lookup: std::collections::HashMap<String, String> = row_types.into_iter().collect();
    assert_eq!(lookup[&addr_a_str], "coinbase");
    assert_eq!(lookup[&addr_b_str], "pending");
    assert_eq!(lookup[&addr_c_str], "pending");

    cleanup(&pool, prefix, &[block_height]).await;
}

// ── Test 4 — coinbase_row constructor matches manual build ─────────

#[tokio::test]
async fn coinbase_row_constructor_roundtrips_via_apply_distribution() {
    let pool = match connect_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let block_height = 9_998_004;
    let prefix = "test_cbrow_";
    cleanup(&pool, prefix, &[block_height]).await;

    let addr = AddressId::new(format!("{prefix}miner")).unwrap();
    let entry = bp_pplns::CoinbaseDistributionEntry {
        address: addr.clone(),
        percent: 33.33,
        sats: Sats(83_333),
    };
    let row = coinbase_row(&entry);

    let result = apply_in_tx(
        &pool,
        block_height,
        &[row],
        &[BalanceWrite {
            address: addr.clone(),
            balance_sats: Sats(0),
            total_paid_sats: Sats(83_333),
        }],
        1_700_000_000_000,
    )
    .await;
    assert_eq!(result.history_inserted, 1);

    let row: (String, i64, f32, String) = sqlx::query_as(
        r#"SELECT address, "paidSats", percent, "rowType"
           FROM pplns_payout_history WHERE "blockHeight" = $1"#,
    )
    .bind(block_height)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, addr.as_str());
    assert_eq!(row.1, 83_333);
    assert!((row.2 - 33.33).abs() < 1e-3);
    assert_eq!(row.3, "coinbase");

    cleanup(&pool, prefix, &[block_height]).await;
}

// ── Test 5 — touch_buffer flush_once writes to PG ──────────────────

#[tokio::test]
async fn touch_buffer_flush_once_updates_existing_rows() {
    let pool = match connect_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let prefix = "test_touch_flush_";
    cleanup(&pool, prefix, &[]).await;

    // Seed two balance rows so the flush has rows to touch.
    let seed_addr_a = format!("{prefix}aa");
    let seed_addr_b = format!("{prefix}bb");
    sqlx::query(
        r#"INSERT INTO pplns_balance (address, "balanceSats", "totalPaidSats", "updatedAt")
           VALUES ($1, 0, 0, 0), ($2, 0, 0, 0)"#,
    )
    .bind(&seed_addr_a)
    .bind(&seed_addr_b)
    .execute(&pool)
    .await
    .unwrap();

    let buf = TouchBuffer::new();
    buf.mark(&seed_addr_a, 1_700_000_500_000);
    buf.mark(&seed_addr_b, 1_700_000_600_000);
    buf.mark(&format!("{prefix}nonexistent"), 1_700_000_700_000);

    let n = flush_once(&pool, &buf).await.expect("flush ok");
    assert_eq!(
        n, 2,
        "expected 2 rows updated (nonexistent address silently skipped)"
    );
    assert!(buf.is_empty(), "buffer drained after successful flush");

    let stamps: Vec<(String, Option<i64>)> = sqlx::query_as(
        r#"SELECT address, "lastAcceptedShareAt"
           FROM pplns_balance WHERE address LIKE $1 ORDER BY address"#,
    )
    .bind(format!("{prefix}%"))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(stamps[0].1, Some(1_700_000_500_000));
    assert_eq!(stamps[1].1, Some(1_700_000_600_000));

    cleanup(&pool, prefix, &[]).await;
}

// ── Test 6 — touch_buffer empty drain is noop ───────────────────────

#[tokio::test]
async fn touch_buffer_flush_once_empty_returns_zero() {
    let pool = match connect_or_skip().await {
        Some(p) => p,
        None => return,
    };
    let buf = TouchBuffer::new();
    let n = flush_once(&pool, &buf).await.expect("flush ok");
    assert_eq!(n, 0);
}

// ── The settlement must LOCK the balances it reads ──────────────────
//
// The block-found balance write is absolute (`current + delta`). If
// `current` is read outside the transaction that writes it, anything
// committing in between is silently undone — and there IS another writer,
// the daily dust sweep, whose targets (open balance, no recent shares) are
// exactly the balance-only entries a distribution carries.
//
// The read therefore happens inside the apply transaction under
// `FOR UPDATE`. This proves the lock is really taken: a second connection
// asking for the same row with a short `lock_timeout` must be refused.

#[tokio::test]
async fn the_settlement_read_locks_the_rows_it_will_write() {
    let Some(pool) = connect_or_skip().await else {
        return;
    };
    let address = "test_ledger_locked_addr";
    let _ = sqlx::query("DELETE FROM pplns_balance WHERE address = $1")
        .bind(address)
        .execute(&pool)
        .await;
    sqlx::query(
        r#"INSERT INTO pplns_balance (address, "balanceSats", "totalPaidSats", "updatedAt")
           VALUES ($1, 5000, 0, 0)"#,
    )
    .bind(address)
    .execute(&pool)
    .await
    .expect("seed");

    // Control FIRST: with nothing holding the row, the competing update
    // succeeds — so a failure below is the lock and not a broken query.
    assert!(
        competing_update(&pool, address).await,
        "precondition: an unlocked row IS updatable within the timeout"
    );

    let mut tx = pool.begin().await.expect("begin");
    let locked = bp_db::find_pplns_balances_for_addresses_locked(&mut *tx, &[address.to_string()])
        .await
        .expect("locked read");
    assert_eq!(locked.len(), 1, "precondition: the row was read");

    assert!(
        !competing_update(&pool, address).await,
        "a row the settlement is about to write must be LOCKED — otherwise the \
         dust sweep commits into the gap and its work is undone by the absolute \
         write that follows"
    );

    tx.rollback().await.expect("rollback");
    // And released again once the transaction ends.
    assert!(
        competing_update(&pool, address).await,
        "the lock must not outlive the transaction"
    );

    let _ = sqlx::query("DELETE FROM pplns_balance WHERE address = $1")
        .bind(address)
        .execute(&pool)
        .await;
}

/// Try to take the row from a second connection with a short
/// `lock_timeout`. `true` = got it, `false` = refused (55P03).
async fn competing_update(pool: &PgPool, address: &str) -> bool {
    let mut other = pool.begin().await.expect("begin competitor");
    sqlx::query("SET LOCAL lock_timeout = '250ms'")
        .execute(&mut *other)
        .await
        .expect("set lock_timeout");
    let got = sqlx::query(r#"UPDATE pplns_balance SET "balanceSats" = 4000 WHERE address = $1"#)
        .bind(address)
        .execute(&mut *other)
        .await
        .is_ok();
    let _ = other.rollback().await;
    got
}

// ── A DIFFERENT block at an already-booked height ────────────────────
//
// `pplns_payout_history` has no `blockHash` column and is UNIQUE on
// `(blockHeight, address)`, so "this height has rows" conflates a harmless
// redelivery with a reorg that replaced the booked block. `ON CONFLICT DO
// NOTHING` cannot tell them apart on its own: it would keep the first block's
// amounts for every shared address and insert any address only the new block
// pays, leaving the height holding a set that reconciles to neither — and here,
// unlike Group-Solo, that is not only a damaged record. PPLNS keeps balances,
// so the accompanying upsert applies `totalPaidSats` a second time.
//
// Both directions are asserted in each test below. A gate that refuses too
// little books the wrong thing; a gate that refuses too much parks the ordinary
// redelivery the confirmation watcher produces whenever its post-apply
// `remove_pending_block` fails, and a real block is then never recorded.

/// Real P2WPKH regtest addresses, checksums verified. Nothing here parses them,
/// but a fixture that could not survive the renderer has quietly stopped
/// matching production.
///
/// One pair per test, never shared. `pplns_balance` is keyed by address alone —
/// no height, no block — so two tests in this file that name the same address
/// are writing to the same row and deleting it out from under each other on
/// cleanup, whatever heights they use. That is a flake, and it is the reason
/// the rest of the file partitions by address prefix.
const COLLIDE_A: &str = "bcrt1qgph8hukx60a8hezl94k25nd4wn85hecek785v7";
const COLLIDE_B: &str = "bcrt1qlx5arkhdegeuzjjartdc428nur5g49yn4j40ut";
const LATE_ARRIVER_PAID: &str = "bcrt1qjrum4cf6m8jm0gdtk6dv35xy3q493e5t8qlcsa";
const LATE_ARRIVER_PENDING: &str = "bcrt1qawrzh064ju8kupx429v5xkkdxny8xcw9ldjjxn";

/// Unlike `apply_in_tx`, this hands the error back instead of unwrapping, and
/// rolls the transaction back — which is what the engine does, and what makes
/// the "nothing reached the table" assertions below mean anything.
async fn try_apply_in_tx(
    pool: &PgPool,
    block_height: i32,
    rows: &[AuditRow],
    balances: &[BalanceWrite],
    now_ms: i64,
) -> Result<ApplyDistributionResult, bp_pplns_engine::ledger::LedgerError> {
    let mut tx = pool.begin().await.expect("begin");
    match apply_distribution(&mut tx, block_height, rows, balances, now_ms).await {
        Ok(out) => {
            tx.commit().await.expect("commit");
            Ok(out)
        }
        Err(e) => {
            tx.rollback().await.expect("rollback");
            Err(e)
        }
    }
}

fn cb(address: &str, sats: i64, percent: f32) -> AuditRow {
    AuditRow {
        address: AddressId::new(address).expect("valid address"),
        paid_sats: Sats(sats),
        percent,
        row_type: PayoutRowType::Coinbase,
    }
}

fn credit(address: &str, total_paid: i64) -> BalanceWrite {
    BalanceWrite {
        address: AddressId::new(address).expect("valid address"),
        balance_sats: Sats(0),
        total_paid_sats: Sats(total_paid),
    }
}

async fn cleanup_addresses(pool: &PgPool, block_heights: &[i32], addresses: &[&str]) {
    let owned: Vec<String> = addresses.iter().map(|a| (*a).to_string()).collect();
    let _ = sqlx::query(r#"DELETE FROM pplns_payout_history WHERE "blockHeight" = ANY($1)"#)
        .bind(block_heights)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM pplns_balance WHERE address = ANY($1)")
        .bind(&owned)
        .execute(pool)
        .await;
}

#[tokio::test]
async fn a_different_block_at_a_booked_height_is_refused_and_the_balance_is_untouched() {
    let Some(pool) = connect_or_skip().await else {
        return;
    };
    let block_height = 9_998_005;
    let addrs = [COLLIDE_A, COLLIDE_B];
    cleanup_addresses(&pool, &[block_height], &addrs).await;

    let block_a = [cb(COLLIDE_A, 300_000, 60.0), cb(COLLIDE_B, 200_000, 40.0)];
    let bal_a = [credit(COLLIDE_A, 300_000), credit(COLLIDE_B, 200_000)];
    let first = try_apply_in_tx(&pool, block_height, &block_a, &bal_a, 1)
        .await
        .expect("the first apply at a fresh height must succeed");
    assert_eq!(
        first.history_inserted, 2,
        "precondition: block A really booked both rows, or nothing below is \
         about a booked height"
    );

    // Direction 1 — ordinary idempotency is untouched.
    let replay = try_apply_in_tx(&pool, block_height, &block_a, &bal_a, 2)
        .await
        .expect("a redelivery of the SAME block must still be Ok, not a conflict");
    assert_eq!(replay.history_inserted, 0);
    assert_eq!(
        replay.balances_affected, 0,
        "and it must not re-apply the balance"
    );

    // Direction 2 — a different coinbase at the same height.
    let block_b = [cb(COLLIDE_A, 250_000, 50.0), cb(COLLIDE_B, 250_000, 50.0)];
    let bal_b = [credit(COLLIDE_A, 250_000), credit(COLLIDE_B, 250_000)];
    let err = try_apply_in_tx(&pool, block_height, &block_b, &bal_b, 3)
        .await
        .expect_err("a DIFFERENT block at a booked height must be refused, not absorbed");
    match err {
        bp_pplns_engine::ledger::LedgerError::HeightBookedByAnotherBlock {
            block_height: h,
            booked_rows,
            incoming_rows,
        } => {
            assert_eq!(h, block_height);
            assert_eq!(booked_rows, 2);
            assert_eq!(incoming_rows, 2);
        }
        other => panic!("wrong error: {other}"),
    }
    // The booked rows do not change on a retry, so the refusal has to park the
    // block rather than make the confirmation watcher — whose only exit for a
    // failing apply is `is_terminal()` — spin on it every tick.
    let again = try_apply_in_tx(&pool, block_height, &block_b, &bal_b, 4)
        .await
        .expect_err("still refused");
    assert!(again.is_terminal(), "must park, not retry forever: {again}");

    // Block A's booking is the record, and its credit was applied once.
    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"SELECT address, "paidSats" FROM pplns_payout_history
            WHERE "blockHeight" = $1 ORDER BY address"#,
    )
    .bind(block_height)
    .fetch_all(&pool)
    .await
    .expect("read history");
    assert_eq!(
        rows,
        vec![
            (COLLIDE_A.to_string(), 300_000),
            (COLLIDE_B.to_string(), 200_000)
        ],
        "a refused apply must roll back entirely"
    );
    let total_paid: (i64,) =
        sqlx::query_as(r#"SELECT "totalPaidSats" FROM pplns_balance WHERE address = $1"#)
            .bind(COLLIDE_A)
            .fetch_one(&pool)
            .await
            .expect("read balance");
    assert_eq!(
        total_paid.0, 300_000,
        "the second block must not have added its own credit on top of the \
         first block's — that is the double-apply this gate prevents"
    );

    cleanup_addresses(&pool, &[block_height], &addrs).await;
}

/// A 0-sat `pending` row must not turn a redelivery into a conflict.
///
/// PPLNS appends one per address live in the window but absent from the
/// distribution, and the window moves between delivery attempts — so a single
/// miner arriving in that gap would otherwise park an ordinary replay and the
/// block would never be booked. This is why the comparison looks only at
/// value-bearing rows.
#[tokio::test]
async fn a_late_arriver_pending_row_does_not_turn_a_redelivery_into_a_conflict() {
    let Some(pool) = connect_or_skip().await else {
        return;
    };
    let block_height = 9_998_006;
    let addrs = [LATE_ARRIVER_PAID, LATE_ARRIVER_PENDING];
    cleanup_addresses(&pool, &[block_height], &addrs).await;

    let first_delivery = [cb(LATE_ARRIVER_PAID, 400_000, 100.0)];
    let bal = [credit(LATE_ARRIVER_PAID, 400_000)];
    let first = try_apply_in_tx(&pool, block_height, &first_delivery, &bal, 1)
        .await
        .expect("first apply");
    assert_eq!(first.history_inserted, 1, "precondition");

    let redelivery = [
        cb(LATE_ARRIVER_PAID, 400_000, 100.0),
        pending_row(
            AddressId::new(LATE_ARRIVER_PENDING).expect("valid address"),
            Sats(0),
        ),
    ];
    let replay = try_apply_in_tx(&pool, block_height, &redelivery, &bal, 2)
        .await
        .expect("a row that moves no value cannot make this a different block");
    assert_eq!(replay.history_inserted, 0);

    cleanup_addresses(&pool, &[block_height], &addrs).await;
}
