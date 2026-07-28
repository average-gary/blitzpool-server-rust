// SPDX-License-Identifier: AGPL-3.0-or-later
//! SPIKE — absolute sats vs percent-of-miner-cut, at the operator's two
//! candidate sizings: 0.1776 BTC flat, or 17.76% of the miner cut.
//!
//! Four questions:
//!   1. When does each mode hit the 95%-of-miner-cut clamp?
//!   2. How does the non-finder haircut drift across halvings in each?
//!   3. Fee sensitivity — percent mode scales with mempool fees, absolute
//!      does not. How much does the jackpot swing on a high-fee block?
//!   4. Can either mode fall under `min_payout_sats`, where the bonus is
//!      dropped SILENTLY?
//!
//! Eras are indexed by halving/subsidy, NOT by calendar year: the point
//! is the subsidy schedule, and putting dates on it invites an off-by-one
//! on the halving cadence.
//!
//! Run: cargo test -p bp-pplns --test spike_bonus_sizing_modes --release -- --nocapture

use std::collections::HashMap;

use bp_common::{AddressId, Sats};
use bp_pplns::{build_coinbase_distribution, CoinbaseDistributionInput};

const COIN: i64 = 100_000_000;
const ABS_1776: i64 = 17_760_000; // 0.1776 BTC
const PCT_1776: f64 = 0.1776; // 17.76% of the miner cut
const DUST: i64 = 546;
const FEE_PERCENT: f64 = 1.5;
const FEES_SATS: i64 = 5_000_000; // 0.05 BTC of tx fees

fn synth_addresses(n: usize) -> Vec<AddressId> {
    const CS: &[u8] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
    let mut out = Vec::with_capacity(n);
    let mut i = 0usize;
    while out.len() < n {
        let mut s = String::from("bc1q");
        let mut v = i;
        for _ in 0..38 {
            s.push(CS[v % CS.len()] as char);
            v = v / CS.len() + 7;
        }
        if let Ok(a) = AddressId::new(s) {
            out.push(a);
        }
        i += 1;
        if i > n * 80 {
            break;
        }
    }
    out
}

struct Row {
    /// The actual bonus output, extracted positionally (see `run`).
    bonus_paid: i64,
    miner_cut: i64,
    /// A non-finder miner's total take.
    nonfinder: i64,
    finder_entries: usize,
    conserved: bool,
}

/// `bonus` is resolved by the caller — that is the whole point of the
/// comparison: absolute mode passes a constant, percent mode derives it
/// from the miner cut at build time.
///
/// Bonus extraction is POSITIONAL, not `max()`. `distribution.rs` pushes
/// the fee output (`:641`), then the bonus output (`:651`), then the kept
/// miners sorted descending (`:673`). So the bonus is the first non-fee
/// entry. Using `max()` silently returns the finder's *proportional*
/// share whenever that exceeds the bonus — which happens in any small
/// pool — and misreports the thing under test.
fn run(n_miners: usize, reward: i64, bonus: i64, budget: u32) -> Row {
    let addrs = synth_addresses(n_miners + 1);
    let fee = addrs[0].clone();
    let miners: Vec<AddressId> = addrs[1..].to_vec();
    let mut shares = HashMap::new();
    for m in &miners {
        shares.insert(m.clone(), 1.0);
    }
    let balances = HashMap::new();
    let finder = miners[0].clone();

    let res = build_coinbase_distribution(CoinbaseDistributionInput {
        address_shares: &shares,
        balances: &balances,
        block_reward_sats: Sats(reward),
        fee_percent: FEE_PERCENT,
        fee_address: Some(&fee),
        coinbase_weight_budget: budget,
        suppress_matching_debits: false,
        min_payout_sats: Some(Sats(DUST)),
        finder_bonus_sats: if bonus > 0 { Some(Sats(bonus)) } else { None },
        finder_address: if bonus > 0 { Some(&finder) } else { None },
    });

    let fee_out: i64 = res
        .payouts
        .iter()
        .filter(|p| p.address == fee)
        .map(|p| p.sats.to_i64())
        .sum();
    let finder_entries = res.payouts.iter().filter(|p| p.address == finder).count();

    // First non-fee entry is the bonus output, iff one was emitted.
    let first_non_fee = res.payouts.iter().find(|p| p.address != fee);
    let bonus_paid = match first_non_fee {
        Some(p) if bonus > 0 && p.address == finder => p.sats.to_i64(),
        _ => 0,
    };

    let nonfinder = res
        .payouts
        .iter()
        .filter(|p| p.address == miners[1])
        .map(|p| p.sats.to_i64())
        .sum();
    let total: i64 = res.payouts.iter().map(|p| p.sats.to_i64()).sum();

    Row {
        bonus_paid,
        miner_cut: reward - fee_out,
        nonfinder,
        finder_entries,
        conserved: total == reward,
    }
}

/// What percent mode resolves to at build time: a fraction of the miner
/// cut (reward minus the pool fee), mirroring how the existing 95% cap is
/// computed at `distribution.rs:172`.
fn percent_to_sats(reward: i64, pct: f64) -> i64 {
    let fee = ((FEE_PERCENT / 100.0) * reward as f64).floor() as i64;
    (((reward - fee) as f64) * pct).floor() as i64
}

/// (halving index from now, subsidy sats, block reward sats)
fn eras() -> Vec<(usize, i64, i64)> {
    let mut out = Vec::new();
    let mut subsidy = 312_500_000i64;
    for h in 0..9 {
        out.push((h, subsidy, subsidy + FEES_SATS));
        subsidy /= 2;
    }
    out
}

/// A non-finder's payout with the bonus disabled, for haircut baselines.
fn baseline_nonfinder(n_miners: usize, reward: i64) -> i64 {
    run(n_miners, reward, 0, 200_000).nonfinder
}

#[test]
fn absolute_vs_percent_across_halvings() {
    for n_miners in [8usize, 500] {
        println!(
            "\n################  POOL SIZE = {n_miners} equal miners  \
             ({FEE_PERCENT}% pool fee, 0.05 BTC tx fees)  ################"
        );

        println!(
            "\n=== 0.1776 BTC ABSOLUTE ===\n\
             halving  subsidy_BTC  miner_cut_BTC   bonus_BTC  %of_cut  clamped  haircut  entries"
        );
        for (h, subsidy, reward) in eras() {
            let r = run(n_miners, reward, ABS_1776, 200_000);
            let base = baseline_nonfinder(n_miners, reward);
            let haircut = 100.0 * (1.0 - r.nonfinder as f64 / base as f64);
            println!(
                "  +{h}     {:10.6}  {:13.6}  {:10.6}  {:6.2}%  {:7}  {haircut:5.1}%   {}",
                subsidy as f64 / COIN as f64,
                r.miner_cut as f64 / COIN as f64,
                r.bonus_paid as f64 / COIN as f64,
                100.0 * r.bonus_paid as f64 / r.miner_cut as f64,
                r.bonus_paid < ABS_1776,
                r.finder_entries,
            );
            assert!(r.conserved, "+{h}: absolute mode must conserve reward");
            assert!(r.bonus_paid > 0, "+{h}: bonus output must be emitted");
            // Either it paid in full, or the clamp bound it to 95% of cut.
            let cap = ((r.miner_cut as f64) * 0.95).floor() as i64;
            assert!(
                r.bonus_paid == ABS_1776 || r.bonus_paid == cap,
                "+{h}: bonus {} is neither the request {ABS_1776} nor the cap {cap}",
                r.bonus_paid
            );
        }

        println!(
            "\n=== 17.76% OF MINER CUT ===\n\
             halving  subsidy_BTC  miner_cut_BTC   bonus_BTC  %of_cut  clamped  haircut  entries"
        );
        for (h, subsidy, reward) in eras() {
            let bonus = percent_to_sats(reward, PCT_1776);
            let r = run(n_miners, reward, bonus, 200_000);
            let base = baseline_nonfinder(n_miners, reward);
            let haircut = 100.0 * (1.0 - r.nonfinder as f64 / base as f64);
            println!(
                "  +{h}     {:10.6}  {:13.6}  {:10.6}  {:6.2}%  {:7}  {haircut:5.1}%   {}",
                subsidy as f64 / COIN as f64,
                r.miner_cut as f64 / COIN as f64,
                r.bonus_paid as f64 / COIN as f64,
                100.0 * r.bonus_paid as f64 / r.miner_cut as f64,
                r.bonus_paid < bonus,
                r.finder_entries,
            );
            assert!(r.conserved, "+{h}: percent mode must conserve reward");
            // The whole point of percent mode: 17.76 < 95, so the clamp
            // can never bind, at any subsidy.
            assert_eq!(
                r.bonus_paid, bonus,
                "+{h}: 17.76% must never hit the 95% clamp"
            );
        }
    }
}

/// Percent mode scales with mempool fees; absolute does not. On a
/// high-fee block the advertised jackpot moves.
#[test]
fn fee_spike_sensitivity() {
    let subsidy = 312_500_000i64;
    println!(
        "\n=== fee sensitivity at the current 3.125 BTC subsidy (500 miners) ===\n\
         tx_fees_BTC  reward_BTC   ABSOLUTE_bonus  PERCENT_bonus  percent/absolute"
    );
    for fees_btc in [0.0f64, 0.05, 0.25, 1.0, 2.0, 5.0] {
        let reward = subsidy + (fees_btc * COIN as f64) as i64;
        let a = run(500, reward, ABS_1776, 200_000);
        let pbonus = percent_to_sats(reward, PCT_1776);
        let p = run(500, reward, pbonus, 200_000);
        println!(
            "{fees_btc:10.2}   {:10.4}   {:14.6}  {:13.6}  {:6.2}x",
            reward as f64 / COIN as f64,
            a.bonus_paid as f64 / COIN as f64,
            p.bonus_paid as f64 / COIN as f64,
            p.bonus_paid as f64 / a.bonus_paid as f64,
        );
        assert!(a.conserved && p.conserved);
        assert_eq!(
            a.bonus_paid, ABS_1776,
            "absolute mode must be fee-invariant at this subsidy"
        );
    }
}

/// The duplicate-finder-entry hazard, restated at the NEW bonus sizes.
///
/// When the finder gets both a bonus output and a proportional output,
/// `finder_entries == 2`, and the downstream ledger write
/// (`bp-pplns-engine/src/engine.rs:611-700`) emits two rows for the same
/// address in one upsert → `ON CONFLICT DO UPDATE cannot affect row a
/// second time` → the whole booking transaction aborts. That is the
/// Phase 0 blocker.
///
/// At 1.776 BTC the hazard was INTERMITTENT: under a starved weight
/// budget the greedy-largest-first trim ate the finder's proportional
/// share, leaving `entries == 1` and hiding the bug. This checks whether
/// a 10x-smaller bonus changes that, i.e. whether the bug is now more or
/// less reliably reproducible.
#[test]
fn duplicate_entry_hazard_at_new_sizes() {
    println!(
        "\n=== finder_entries by weight budget — 2 means the ledger aborts ===\n\
         500 miners, current subsidy. 'trimmed' = pool larger than the budget allows."
    );
    let reward = 317_500_000i64;
    for (label, bonus) in [
        ("0.1776 BTC abs ", ABS_1776),
        ("17.76% of cut  ", percent_to_sats(reward, PCT_1776)),
        ("1.776 BTC (old)", 177_600_000i64),
    ] {
        print!("  {label}: ");
        for budget in [200_000u32, 50_000, 25_000, 12_000, 6_000] {
            let r = run(500, reward, bonus, budget);
            print!("{budget}WU→{} ", r.finder_entries);
            assert!(r.conserved, "{label} @ {budget}WU must conserve reward");
        }
        println!();
    }
    println!(
        "  → wherever entries==2, the duplicate-address ledger write aborts the\n    \
         booking transaction; wherever entries==1, the trim hid it."
    );

    // Which pool sizes actually abort, at the autoscaler's 50,000 WU floor?
    // This is the production-realistic question: the bug is not rare, it is
    // conditional on the pool fitting inside the budget.
    println!(
        "\n=== at the 50,000 WU autoscaler floor: which pool sizes abort? ===\n\
         (0.1776 BTC absolute, current subsidy)"
    );
    for n in [5usize, 10, 25, 50, 100, 200, 300, 400, 500, 1000] {
        let r = run(n, reward, ABS_1776, 50_000);
        println!(
            "  miners={n:5} finder_entries={} → {}",
            r.finder_entries,
            if r.finder_entries >= 2 {
                "LEDGER ABORTS"
            } else {
                "trim hid the duplicate"
            }
        );
        assert!(r.conserved);
    }
}

/// The dust-suppression path: `bonus_emitted = capped_bonus >= min_payout`
/// (`distribution.rs:175`). Below the floor the bonus is dropped SILENTLY
/// — no warn, no error, the finder just never gets the jackpot output.
#[test]
fn dust_suppression_reachability() {
    println!("\n=== can the bonus be silently suppressed below min_payout? ===");
    let mut subsidy = 312_500_000i64;
    let mut h = 0usize;
    let mut pct_floor_h = None;
    while subsidy > 0 {
        let reward = subsidy + FEES_SATS;
        if percent_to_sats(reward, PCT_1776) < DUST && pct_floor_h.is_none() {
            pct_floor_h = Some(h);
        }
        subsidy /= 2;
        h += 1;
    }
    println!(
        "  17.76% of cut drops below the {DUST}-sat floor at: {}",
        pct_floor_h.map_or_else(
            || "never within the subsidy schedule".to_string(),
            |h| format!("halving +{h}")
        )
    );
    println!(
        "  0.1776 BTC flat drops below the {DUST}-sat floor at: never \
         — it is a constant 17,760,000 sats"
    );

    // Prove the path really is silent when a config DOES reach it.
    let r = run(8, 317_500_000, 100, 200_000); // 100 sats < 546 floor
    println!(
        "  sanity: a 100-sat bonus request → finder_entries={} bonus_output={} \
         (dropped, no error surfaced, reward still conserved={})",
        r.finder_entries, r.bonus_paid, r.conserved
    );
    assert_eq!(r.bonus_paid, 0, "a sub-dust bonus must not be emitted");
    assert_eq!(
        r.finder_entries, 1,
        "finder keeps only its proportional entry when the bonus is suppressed"
    );
    assert!(r.conserved);
}
