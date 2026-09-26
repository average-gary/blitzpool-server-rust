// SPDX-License-Identifier: AGPL-3.0-or-later

//! Postgres-backed payout history for Group-Solo.
//!
//! One table, `pplns_group_block_history`: auto-id rows with UNIQUE
//! `(groupId, blockHeight, address)`, plus `sharesInRound` +
//! `totalSharesInRound` (the PROP-round audit detail PPLNS does not
//! have). The bulk insert lives in `bp-db`; this module wraps it in the
//! per-block transaction the block-found path needs.
//!
//! There is no balance table here. Group-Solo pays what the coinbase
//! pays and owes nothing afterwards — see the crate docs — so these rows
//! are a record of what happened, never an obligation. That is also why
//! the writer can be plainly idempotent instead of accumulating: a
//! redelivered block-found must leave the history exactly as the first
//! delivery did, and the UNIQUE index is enough to guarantee it.

use bp_common::{AddressId, Sats};
use bp_db::{bulk_insert_pplns_group_block_history, GroupPayoutHistoryInsert};
use sqlx::PgPool;
use uuid::Uuid;

// Shared with PPLNS — one source of truth for the rowType wire strings
// + apply result / error shapes. Group-Solo's audit rows add
// `sharesInRound` fields (see [`AuditRow`]) but the discriminator
// itself is identical, so we alias the shared enum.
pub use bp_coinbase_snapshot::{
    ApplyDistributionResult, LedgerError, PayoutRowType as GroupPayoutRowType,
};

/// One row in the payout history. Group-Solo's `sharesInRound` +
/// `totalSharesInRound` slots are preserved here so the log reflects the
/// PROP-round split the coinbase was built from.
#[derive(Clone, Debug)]
pub struct AuditRow {
    pub address: AddressId,
    pub paid_sats: Sats,
    pub percent: f32,
    pub shares_in_round: i64,
    pub total_shares_in_round: i64,
    pub row_type: GroupPayoutRowType,
}

/// Write one block's payout history for one group inside a single PG
/// transaction. Idempotent on replay via the
/// `(groupId, blockHeight, address)` UNIQUE constraint: a redelivered
/// block-found inserts nothing and reports `history_inserted == 0`.
pub async fn apply_distribution(
    pool: &PgPool,
    group_id: Uuid,
    block_height: i32,
    rows: &[AuditRow],
    now_ms: i64,
) -> Result<ApplyDistributionResult, LedgerError> {
    let mut tx = pool.begin().await?;

    let history_rows: Vec<GroupPayoutHistoryInsert> = rows
        .iter()
        .map(|r| GroupPayoutHistoryInsert {
            group_id,
            block_height,
            address: r.address.as_str().to_string(),
            paid_sats: r.paid_sats.0,
            percent: r.percent,
            shares_in_round: r.shares_in_round,
            total_shares_in_round: r.total_shares_in_round,
            row_type: r.row_type.as_wire().to_string(),
            created_at_ms: now_ms,
        })
        .collect();

    let history_inserted = bulk_insert_pplns_group_block_history(&mut *tx, &history_rows).await?;

    tx.commit().await?;
    Ok(ApplyDistributionResult {
        history_inserted,
        balances_affected: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payout_row_type_wire_strings_are_stable() {
        assert_eq!(GroupPayoutRowType::Coinbase.as_wire(), "coinbase");
        assert_eq!(GroupPayoutRowType::Pending.as_wire(), "pending");
        assert_eq!(GroupPayoutRowType::DustSweep.as_wire(), "dust-sweep");
    }
}
