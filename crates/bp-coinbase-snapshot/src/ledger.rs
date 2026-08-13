// SPDX-License-Identifier: AGPL-3.0-or-later

//! Shared apply-distribution ledger primitives.
//!
//! The per-engine `apply_distribution` orchestrators (PPLNS signed
//! credit/debit, Group-Solo unsigned pending) build mode-specific audit
//! rows, but the row-type discriminator, the result counts, and the
//! error type are identical — hoisted here so the wire strings the DB
//! column + UI depend on stay one source of truth.

use bp_db::DbError;
use thiserror::Error;

/// Row-type discriminator for the payout-history tables.
///
/// Single source of truth for the wire value: the strings
/// (`coinbase` | `pending` | `dust-sweep`), the schema columns
/// are `varchar(16)`, and the UI styles + filters on the literal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayoutRowType {
    /// Paid on-chain via the block's coinbase tx.
    Coinbase,
    /// Ledger change without an on-chain output (sub-dust /
    /// weight-trimmed credit, matching debit, or member-kick
    /// redistribution).
    Pending,
    /// Absorbed by the daily sweep cron after the abandonment period.
    DustSweep,
}

impl PayoutRowType {
    pub fn as_wire(self) -> &'static str {
        match self {
            Self::Coinbase => "coinbase",
            Self::Pending => "pending",
            Self::DustSweep => "dust-sweep",
        }
    }

    /// Inverse of [`Self::as_wire`]. `None` for an unrecognised string.
    /// Used when reconstructing a frozen distribution (e.g. a
    /// confirmation-gated block-found) from its serialized wire form.
    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "coinbase" => Some(Self::Coinbase),
            "pending" => Some(Self::Pending),
            "dust-sweep" => Some(Self::DustSweep),
            _ => None,
        }
    }
}

/// Error from an apply-distribution transaction.
#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("db: {0}")]
    Db(#[from] DbError),
    #[error("sqlx: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// Height `block_height` already carries payout rows that do NOT match
    /// what this apply would write — so a DIFFERENT block was booked at
    /// this height and a reorg replaced it with the one being applied now.
    ///
    /// `pplns_payout_history` has no `blockHash` column and is UNIQUE on
    /// `(blockHeight, address)`, so the ledger cannot hold both. This apply
    /// therefore books nothing, and saying so as an error is the whole
    /// point: the previous shape returned `Ok` with zero counts, which the
    /// confirmation watcher read as success — it fired the settlement,
    /// logged "payout history applied" and dropped the parked block. A
    /// block whose coinbase paid miners on-chain vanished with it.
    ///
    /// Terminal by nature: the recorded rows will not change on a retry.
    /// The caller parks the block in the unbookable store instead, where
    /// the frozen distribution survives for an operator reprocess.
    #[error(
        "block height {block_height} already carries {booked_rows} payout rows from a different \
         block; this apply would have written {incoming_rows} — the ledger keys payout history by \
         height, so it cannot hold both"
    )]
    HeightBookedByAnotherBlock {
        block_height: i32,
        booked_rows: usize,
        incoming_rows: usize,
    },
}

impl LedgerError {
    /// Would retrying this ever succeed?
    ///
    /// Only [`Self::HeightBookedByAnotherBlock`] is a verdict; the rest are
    /// infrastructure and clear on their own. Kept here rather than in each
    /// engine's `is_terminal` so the two cannot disagree about it.
    pub fn is_terminal(&self) -> bool {
        match self {
            LedgerError::HeightBookedByAnotherBlock { .. } => true,
            LedgerError::Db(_) | LedgerError::Sqlx(_) => false,
        }
    }
}

/// Row counts affected by one apply-distribution transaction.
#[derive(Clone, Debug)]
pub struct ApplyDistributionResult {
    pub history_inserted: u64,
    pub balances_affected: u64,
}

/// What a height's already-booked payout rows mean for the apply about to run.
///
/// See [`classify_booked_height`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BookedHeightVerdict {
    /// Nothing value-bearing recorded at this height yet — this apply is the
    /// first one and should write its rows.
    Fresh,
    /// Already booked, and byte-for-byte what this apply would write. Replaying
    /// moves nothing either way, so the caller returns success having written
    /// nothing.
    Replay,
    /// Already booked, and by something else. The caller must refuse — see
    /// [`LedgerError::HeightBookedByAnotherBlock`], whose two counts these are.
    Conflict {
        booked_rows: usize,
        incoming_rows: usize,
    },
}

/// Decide whether an apply at a height that already has payout rows is a
/// harmless replay or a second, different block.
///
/// **Both payout-history tables key a booked block by HEIGHT alone.** Neither
/// `pplns_payout_history` nor `pplns_group_block_history` has a `blockHash`
/// column, and both are UNIQUE on a tuple that starts with the height. So
/// "this height has rows" answers two very different questions at once: a
/// harmless redelivery of the SAME block, which must pass silently, and a
/// DIFFERENT block at the same height, whose miners were paid on-chain and
/// whose settlement is about to be skipped. (`blockparty_block_history` is the
/// exception — it is UNIQUE on `(groupId, blockHash)` and so cannot pose the
/// question.)
///
/// They are told apart by what the booking WOULD be rather than by which block
/// it is, which is the question that actually matters: if the rows already
/// recorded match the value-bearing rows this apply would write, replaying
/// moves nothing — even for a genuinely different block, because two blocks
/// that pay the same coinbase settle the same deltas and booking them twice
/// would double-apply them.
///
/// **Only value-bearing rows count, on both sides.** A row is not an
/// accounting: PPLNS writes a 0-sat `pending` row for every address live in the
/// window but absent from the block's distribution, and the window moves
/// between attempts, so comparing all rows would call an ordinary replay a
/// conflict the moment one new miner arrived. The 0-sat filter is why the
/// booked side is read with a `"paidSats" <> 0` predicate, and this function
/// applies the same rule to the incoming side so the two cannot drift.
///
/// **Both sides are sorted here, in Rust, and that is deliberate.** The obvious
/// alternative is to lean on the query's `ORDER BY address` for the booked side
/// and sort only the incoming one, which is what this logic did while it lived
/// inline in PPLNS. That silently makes the comparison depend on the *database*
/// collation of the `address` column agreeing with Rust's byte ordering, and the
/// two do diverge: under an ICU collation Postgres orders `1aBc` before `1Abc`,
/// byte order the reverse. Base58 addresses are mixed-case, so a pair differing
/// only in case pattern is possible, and on an ICU-collated column it would make
/// an ordinary replay compare unequal and read as a conflict. That errs in the
/// safe direction — a block gets parked for an operator instead of passing
/// silently — and it is vanishingly unlikely, but it costs nothing to not
/// depend on it.
///
/// Lives here rather than in either engine because both modes need the same
/// answer from the same shape of data, and the subtle parts — the 0-sat filter
/// and the ordering the comparison rests on — are exactly what a second
/// hand-written copy gets slightly differently.
pub fn classify_booked_height<'a, I>(
    mut booked: Vec<(String, i64)>,
    incoming: I,
) -> BookedHeightVerdict
where
    I: IntoIterator<Item = (&'a str, i64)>,
{
    if booked.is_empty() {
        return BookedHeightVerdict::Fresh;
    }
    let mut want: Vec<(String, i64)> = incoming
        .into_iter()
        .filter(|(_, sats)| *sats != 0)
        .map(|(addr, sats)| (addr.to_string(), sats))
        .collect();
    booked.sort();
    want.sort();
    if booked == want {
        BookedHeightVerdict::Replay
    } else {
        BookedHeightVerdict::Conflict {
            booked_rows: booked.len(),
            incoming_rows: want.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payout_row_type_wire_strings_are_correct() {
        assert_eq!(PayoutRowType::Coinbase.as_wire(), "coinbase");
        assert_eq!(PayoutRowType::Pending.as_wire(), "pending");
        assert_eq!(PayoutRowType::DustSweep.as_wire(), "dust-sweep");
    }

    fn booked(rows: &[(&str, i64)]) -> Vec<(String, i64)> {
        rows.iter().map(|(a, s)| (a.to_string(), *s)).collect()
    }

    #[test]
    fn an_empty_height_is_fresh() {
        assert_eq!(
            classify_booked_height(vec![], [("bc1qa", 1_000)]),
            BookedHeightVerdict::Fresh
        );
    }

    #[test]
    fn the_same_booking_again_is_a_replay() {
        assert_eq!(
            classify_booked_height(
                booked(&[("bc1qa", 1_000), ("bc1qb", 2_000)]),
                [("bc1qa", 1_000), ("bc1qb", 2_000)]
            ),
            BookedHeightVerdict::Replay
        );
    }

    /// The distinction the whole function exists for: same addresses, different
    /// amounts, is a different block's coinbase and must not pass as a replay.
    #[test]
    fn the_same_addresses_for_different_amounts_is_a_conflict() {
        assert_eq!(
            classify_booked_height(
                booked(&[("bc1qa", 1_000), ("bc1qb", 2_000)]),
                [("bc1qa", 1_000), ("bc1qb", 2_001)]
            ),
            BookedHeightVerdict::Conflict {
                booked_rows: 2,
                incoming_rows: 2
            }
        );
    }

    /// A member who was paid by the booked block and is absent from the
    /// incoming one. This is the shape `ON CONFLICT DO NOTHING` absorbs
    /// silently, so it has to be a conflict here.
    #[test]
    fn a_dropped_member_is_a_conflict_not_a_replay() {
        assert_eq!(
            classify_booked_height(
                booked(&[("bc1qa", 1_000), ("bc1qb", 2_000)]),
                [("bc1qa", 1_000)]
            ),
            BookedHeightVerdict::Conflict {
                booked_rows: 2,
                incoming_rows: 1
            }
        );
    }

    /// 0-sat rows account for nothing and must not tip a replay into a
    /// conflict. PPLNS writes one per address live in the window but absent
    /// from the distribution, and the window moves between attempts — so
    /// without this filter a single new miner arriving between two delivery
    /// attempts would park an ordinary replay.
    #[test]
    fn zero_sat_rows_do_not_make_a_replay_look_like_a_conflict() {
        assert_eq!(
            classify_booked_height(
                booked(&[("bc1qa", 1_000)]),
                [("bc1qa", 1_000), ("bc1q_late_arriver", 0)]
            ),
            BookedHeightVerdict::Replay,
            "a 0-sat late-arriver row moves no value, so this is the same booking"
        );
    }

    /// The booked side must not depend on the DB handing back rows in Rust's
    /// order. It comes out of SQL, and a SQL `ORDER BY` follows the column's
    /// collation, which is not byte ordering: under ICU `unicode` Postgres
    /// orders these two `1aBc, 1Abc`, while Rust orders them `1Abc, 1aBc`
    /// (`A` is 0x41, `a` is 0x61). That divergence was checked in psql, not
    /// assumed. So the booked rows arrive here in the collation's order and
    /// the incoming ones already in Rust's — only sorting `booked` locally
    /// makes the two meet, which is why this does not lean on `ORDER BY`.
    ///
    /// These are not real addresses, and deliberately so: nothing in
    /// `classify_booked_height` parses one — it compares strings — and showing
    /// the two orderings apart needs a case difference at the same position,
    /// which bech32's all-lowercase alphabet cannot produce. Base58 addresses
    /// are mixed-case, and this table holds them.
    #[test]
    fn the_comparison_does_not_depend_on_the_database_collation() {
        assert_eq!(
            classify_booked_height(
                // The order an ICU collation returns…
                booked(&[("1aBc", 500), ("1Abc", 400)]),
                // …against the order Rust's own comparison would have.
                [("1Abc", 400), ("1aBc", 500)]
            ),
            BookedHeightVerdict::Replay,
            "the same two rows; the DB's idea of sorted is not Rust's, and that \
             is not a different block"
        );
    }

    /// And the incoming side must not depend on arriving sorted either. It is
    /// the distribution's row order, which follows the split — a distribution
    /// is essentially never address-ordered, so without this sort a replay of
    /// very nearly every real block would read as a conflict. Here the booked
    /// rows are already in Rust's order and the incoming ones are not, so only
    /// sorting `want` locally makes them meet.
    #[test]
    fn the_comparison_does_not_depend_on_the_incoming_rows_arriving_sorted() {
        assert_eq!(
            classify_booked_height(
                booked(&[("1Abc", 400), ("1aBc", 500), ("1zzz", 600)]),
                [("1zzz", 600), ("1Abc", 400), ("1aBc", 500)]
            ),
            BookedHeightVerdict::Replay,
            "the same three rows in the distribution's order — still one booking"
        );
    }
}
