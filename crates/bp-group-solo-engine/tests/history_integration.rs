// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(clippy::print_stderr)]

//! Integration tests for `bp_group_solo_engine::history::apply_distribution`
//! against docker-PG.
//!
//! The thing under test is the gate that tells a redelivery of the SAME block
//! apart from a DIFFERENT block at the same height. Both arrive as "this
//! `(group, height)` already has rows", and `ON CONFLICT DO NOTHING` — which is
//! all that stood here before — absorbs the two identically and reports `Ok`.
//!
//! Every test below asserts BOTH directions, because the failure modes are
//! symmetric and each is as bad as the other. A gate that refuses too little
//! lets a second block's booking merge into the first one's rows and calls it
//! success; a gate that refuses too much parks an ordinary redelivery, which
//! stops a real block from ever being recorded. A test that only proved the
//! refusal would pass just as happily against `Err(…)` returned
//! unconditionally.
//!
//! No Redis here — `apply_distribution` takes a `PgPool` and nothing else, so
//! there is no round store to isolate and no DB range to claim. Each test uses
//! a freshly generated group UUID, so tests cannot collide with each other even
//! at the same block height.

use bp_common::{AddressId, Sats};
use bp_group_solo_engine::history::{
    apply_distribution, AuditRow, GroupPayoutRowType, LedgerError,
};
use sqlx::{postgres::PgPoolOptions, PgPool};
use uuid::Uuid;

const PG_URL: &str = "postgres://postgres:postgres@localhost:15433/public_pool";

// Real P2WPKH regtest addresses, checksums verified. Nothing in this file
// parses them — `apply_distribution` stores whatever `AddressId` accepted — but
// a fixture that could not survive being handed to the renderer is a fixture
// that has quietly stopped matching production.
const ADDR_A: &str = "bcrt1q7mxmz5pdfer7vsr4t8kgwqrt0v8pup4wuk9utr";
const ADDR_B: &str = "bcrt1qa790dma3sxjlv79vsa8s47pek77yr90wa6v5th";
const ADDR_C: &str = "bcrt1qa0vp72rm44tcseucxwwwfmq4ra8cetnqh83y4m";

async fn connect_or_skip() -> Option<PgPool> {
    let pg_url = std::env::var("BP_PG_URL").unwrap_or_else(|_| PG_URL.to_string());
    match tokio::time::timeout(
        std::time::Duration::from_secs(2),
        PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(std::time::Duration::from_secs(2))
            .connect(&pg_url),
    )
    .await
    {
        Ok(Ok(p)) => Some(p),
        Ok(Err(e)) => {
            eprintln!("PG connect failed: {e} — skipping");
            None
        }
        Err(_) => {
            eprintln!("PG connect timed out — skipping");
            None
        }
    }
}

async fn seed_group(pool: &PgPool, group_id: Uuid) {
    sqlx::query(
        r#"INSERT INTO pplns_group
             (id, name, "creatorAddress", "adminTokenHash", active,
              "createdAt", "updatedAt", "isPublic", "lastRoundResetAt",
              "roundResetPreset", "roundResetTimezone", "roundResetIntervalDays")
           VALUES ($1, $2, $3, $4, true, 0, 0, false, NULL, 'daily', 'UTC', NULL)"#,
    )
    .bind(group_id)
    .bind(format!("test-group-{group_id}"))
    .bind(format!("test_grp_hist_creator_{group_id}"))
    .bind(format!("hash-{group_id}"))
    .execute(pool)
    .await
    .expect("seed group");
}

async fn cleanup_group(pool: &PgPool, group_id: Uuid) {
    let _ = sqlx::query(r#"DELETE FROM pplns_group_block_history WHERE "groupId" = $1"#)
        .bind(group_id)
        .execute(pool)
        .await;
    let _ = sqlx::query(r#"DELETE FROM pplns_group WHERE id = $1"#)
        .bind(group_id)
        .execute(pool)
        .await;
}

fn coinbase(address: &str, sats: i64) -> AuditRow {
    AuditRow {
        address: AddressId::new(address).expect("valid address"),
        paid_sats: Sats(sats),
        percent: 0.0,
        shares_in_round: 1,
        total_shares_in_round: 2,
        row_type: GroupPayoutRowType::Coinbase,
    }
}

/// What the height actually holds, so an assertion can be about the stored
/// record rather than about the return value that described it.
async fn booked_rows(pool: &PgPool, group_id: Uuid, height: i32) -> Vec<(String, i64)> {
    sqlx::query_as::<_, (String, i64)>(
        r#"SELECT address, "paidSats" FROM pplns_group_block_history
            WHERE "groupId" = $1 AND "blockHeight" = $2 ORDER BY address"#,
    )
    .bind(group_id)
    .bind(height)
    .fetch_all(pool)
    .await
    .expect("read booked rows")
}

/// Same members, different amounts — a different coinbase at the same height.
///
/// This is the shape a reorg produces: the pool's block at height H is replaced
/// by another of its own blocks at H, paying the same group a different split.
#[tokio::test]
async fn a_different_block_at_a_booked_height_is_refused_while_a_replay_still_passes() {
    let Some(pool) = connect_or_skip().await else {
        return;
    };
    let group_id = Uuid::new_v4();
    seed_group(&pool, group_id).await;
    let height = 800_001;

    let block_a = [coinbase(ADDR_A, 300_000), coinbase(ADDR_B, 200_000)];
    let first = apply_distribution(&pool, group_id, height, &block_a, 1)
        .await
        .expect("the first apply at a fresh height must succeed");
    assert_eq!(
        first.history_inserted, 2,
        "precondition: block A has to have really booked both rows, or the rest \
         of this test proves nothing about a booked height"
    );

    // Direction 1 — the gate must NOT have broken ordinary idempotency. The
    // confirmation watcher redelivers a block whenever its post-apply
    // `remove_pending_block` fails, so this is the common path, not the edge.
    let replay = apply_distribution(&pool, group_id, height, &block_a, 2)
        .await
        .expect("a redelivery of the SAME block must still be Ok, not a conflict");
    assert_eq!(
        replay.history_inserted, 0,
        "and it must still report having written nothing"
    );

    // Direction 2 — a different block at the same height.
    let block_b = [coinbase(ADDR_A, 250_000), coinbase(ADDR_B, 250_000)];
    let err = apply_distribution(&pool, group_id, height, &block_b, 3)
        .await
        .expect_err("a DIFFERENT block at a booked height must be refused, not absorbed");
    match err {
        LedgerError::HeightBookedByAnotherBlock {
            block_height,
            booked_rows,
            incoming_rows,
        } => {
            assert_eq!(block_height, height);
            assert_eq!(booked_rows, 2);
            assert_eq!(incoming_rows, 2);
        }
        other => panic!("wrong error: {other}"),
    }
    // The refusal has to be terminal or the confirmation watcher, whose only
    // exit for a failing apply is `is_terminal()`, retries it every tick
    // forever behind a repeating warning.
    let refusal = apply_distribution(&pool, group_id, height, &block_b, 4)
        .await
        .expect_err("still refused");
    assert!(
        refusal.is_terminal(),
        "the booked rows do not change on a retry, so this must park the block \
         rather than spin: {refusal}"
    );

    // And nothing of block B reached the table — block A's amounts are intact.
    assert_eq!(
        booked_rows(&pool, group_id, height).await,
        vec![(ADDR_A.to_string(), 300_000), (ADDR_B.to_string(), 200_000)],
        "a refused apply must roll back; block A's booking is the record"
    );

    cleanup_group(&pool, group_id).await;
}

/// The case that reported SUCCESS before the gate existed, and the reason
/// `history_inserted > 0` was never evidence of anything.
///
/// Block B pays everyone block A did, plus one more member. Every address they
/// share hits `ON CONFLICT DO NOTHING` and keeps block A's amount, while the new
/// member inserts — so the apply used to return `Ok` with `history_inserted == 1`
/// and leave the height holding a three-row set that reconciles to neither
/// block. The confirmation watcher read that `Ok` as a booking, fired the
/// settlement and dropped the parked block.
#[tokio::test]
async fn a_block_that_adds_a_member_cannot_merge_itself_into_the_booked_rows() {
    let Some(pool) = connect_or_skip().await else {
        return;
    };
    let group_id = Uuid::new_v4();
    seed_group(&pool, group_id).await;
    let height = 800_002;

    let block_a = [coinbase(ADDR_A, 300_000), coinbase(ADDR_B, 200_000)];
    apply_distribution(&pool, group_id, height, &block_a, 1)
        .await
        .expect("first apply");

    let block_b = [
        coinbase(ADDR_A, 300_000),
        coinbase(ADDR_B, 200_000),
        coinbase(ADDR_C, 100_000),
    ];
    let err = apply_distribution(&pool, group_id, height, &block_b, 2)
        .await
        .expect_err("a superset booking is still a different block");
    assert!(
        matches!(
            err,
            LedgerError::HeightBookedByAnotherBlock {
                booked_rows: 2,
                incoming_rows: 3,
                ..
            }
        ),
        "the counts name what disagreed: {err}"
    );

    let rows = booked_rows(&pool, group_id, height).await;
    assert_eq!(
        rows.len(),
        2,
        "the extra member must NOT have been inserted next to block A's rows — \
         that is the merged, unreconcilable record this gate exists to prevent: \
         {rows:?}"
    );
    assert!(
        !rows.iter().any(|(a, _)| a == ADDR_C),
        "and specifically not the address only block B pays: {rows:?}"
    );

    cleanup_group(&pool, group_id).await;
}

/// A 0-sat row must not turn a redelivery into a conflict.
///
/// This is the trap the PPLNS gate documents: it appends one 0-sat `pending` row
/// per address live in the window but absent from the distribution, and the
/// window moves between delivery attempts. Group-Solo does not write those rows
/// today, but the comparison is shared code, and a gate that counted them would
/// park an ordinary replay the moment one arrived. Pinning it here means the
/// shared filter cannot be dropped without a Group-Solo test going red too.
#[tokio::test]
async fn a_zero_sat_row_appearing_on_redelivery_is_still_a_replay() {
    let Some(pool) = connect_or_skip().await else {
        return;
    };
    let group_id = Uuid::new_v4();
    seed_group(&pool, group_id).await;
    let height = 800_003;

    let first_delivery = [coinbase(ADDR_A, 300_000), coinbase(ADDR_B, 200_000)];
    apply_distribution(&pool, group_id, height, &first_delivery, 1)
        .await
        .expect("first apply");

    let redelivery = [
        coinbase(ADDR_A, 300_000),
        coinbase(ADDR_B, 200_000),
        coinbase(ADDR_C, 0),
    ];
    let replay = apply_distribution(&pool, group_id, height, &redelivery, 2)
        .await
        .expect("a row that moves no value cannot make this a different block");
    assert_eq!(replay.history_inserted, 0);

    cleanup_group(&pool, group_id).await;
}
