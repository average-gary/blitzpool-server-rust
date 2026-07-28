// SPDX-License-Identifier: AGPL-3.0-or-later
//! SPIKE — what does a 1.776 BTC finder bonus actually do to a PPLNS
//! distribution, at today's subsidy and across the next two halvings?
//!
//! Three questions the plan needs answered against real code, not
//! arithmetic:
//!   1. Does the 95%-of-miner-cut cap bind at 1.776 BTC, and when?
//!   2. What happens to the non-finder remainder when it does?
//!   3. Does the "exactly one output differs per finder" property still
//!      hold at a bonus this large, including when the cap binds?
//!
//! Run: cargo test -p bp-pplns --test spike_bonus_1776 --release -- --nocapture

use std::collections::HashMap;

use bp_common::{AddressId, Sats};
use bp_pplns::{build_coinbase_distribution, CoinbaseDistributionInput};

const COIN: i64 = 100_000_000;
const BONUS_1776: i64 = 177_600_000;

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
        if i > n * 50 {
            break;
        }
    }
    out
}

fn dist(
    shares: &HashMap<AddressId, f64>,
    balances: &HashMap<AddressId, Sats>,
    fee: &AddressId,
    finder: Option<&AddressId>,
    bonus: i64,
    reward: i64,
    budget: u32,
) -> Vec<(String, i64)> {
    build_coinbase_distribution(CoinbaseDistributionInput {
        address_shares: shares,
        balances,
        block_reward_sats: Sats(reward),
        fee_percent: 1.5,
        fee_address: Some(fee),
        coinbase_weight_budget: budget,
        suppress_matching_debits: false,
        min_payout_sats: Some(Sats(546)),
        finder_bonus_sats: finder.map(|_| Sats(bonus)),
        finder_address: finder,
    })
    .payouts
    .into_iter()
    .map(|p| (p.address.as_str().to_string(), p.sats.to_i64()))
    .collect()
}

/// Q1 + Q2: cap behaviour and remainder erosion across halvings.
#[test]
fn bonus_1776_across_halvings() {
    let addrs = synth_addresses(9);
    let fee = &addrs[0];
    let miners = &addrs[1..9]; // 8 equal miners
    let mut shares = HashMap::new();
    for m in miners {
        shares.insert(m.clone(), 1.0);
    }
    let balances = HashMap::new();

    // (label, subsidy sats) — 0.05 BTC of fees assumed in every era.
    let eras: &[(&str, i64)] = &[
        ("2026 (3.125)", 312_500_000),
        ("2028 (1.5625)", 156_250_000),
        ("2032 (0.78125)", 78_125_000),
    ];

    println!("\n=== 1.776 BTC bonus vs miner cut, 8 equal miners, 1.5% fee ===");
    for (label, subsidy) in eras {
        let reward = subsidy + 5_000_000;
        let with = dist(
            &shares,
            &balances,
            fee,
            Some(&miners[0]),
            BONUS_1776,
            reward,
            200_000,
        );
        let without = dist(&shares, &balances, fee, None, 0, reward, 200_000);

        let paid_total: i64 = with.iter().map(|(_, s)| s).sum();
        let fee_out: i64 = with
            .iter()
            .filter(|(a, _)| a == fee.as_str())
            .map(|(_, s)| s)
            .sum();
        let finder_total: i64 = with
            .iter()
            .filter(|(a, _)| a == miners[0].as_str())
            .map(|(_, s)| s)
            .sum();
        let finder_entries = with.iter().filter(|(a, _)| a == miners[0].as_str()).count();
        // A non-finder: pick miners[1].
        let nonfinder_with: i64 = with
            .iter()
            .filter(|(a, _)| a == miners[1].as_str())
            .map(|(_, s)| s)
            .sum();
        let nonfinder_without: i64 = without
            .iter()
            .filter(|(a, _)| a == miners[1].as_str())
            .map(|(_, s)| s)
            .sum();
        let miner_cut = reward - fee_out;
        // Largest single output attributable to the finder = the bonus output.
        let bonus_paid = with
            .iter()
            .filter(|(a, _)| a == miners[0].as_str())
            .map(|(_, s)| *s)
            .max()
            .unwrap_or(0);

        let haircut = 100.0 * (1.0 - nonfinder_with as f64 / nonfinder_without as f64);
        println!(
            "{label:16} reward={:.4} miner_cut={:.4} bonus_paid={:.6} ({:.1}% of cut) capped={} \
             finder_outputs={} finder_total={:.4} nonfinder {} -> {} ({haircut:.1}% haircut) \
             conserved={}",
            reward as f64 / COIN as f64,
            miner_cut as f64 / COIN as f64,
            bonus_paid as f64 / COIN as f64,
            100.0 * bonus_paid as f64 / miner_cut as f64,
            bonus_paid < BONUS_1776,
            finder_entries,
            finder_total as f64 / COIN as f64,
            nonfinder_without,
            nonfinder_with,
            paid_total == reward,
        );
        assert_eq!(paid_total, reward, "{label}: reward must be conserved exactly");
    }
}

/// Q3: does the one-output-differs property survive a cap-binding bonus?
#[test]
fn positional_divergence_at_1776_including_capped() {
    let addrs = synth_addresses(201);
    let fee = &addrs[0];
    let miners = &addrs[1..201];
    let mut shares = HashMap::new();
    for (i, m) in miners.iter().enumerate() {
        shares.insert(m.clone(), 1.0 + (i as f64 % 7.0));
    }
    let balances = HashMap::new();

    println!("\n=== positional divergence between two finders, 200 miners ===");
    // Roomy budget at today's subsidy (cap does not bind), and a
    // post-halving reward where it does.
    for (label, reward, budget) in [
        ("2026 roomy", 317_500_000i64, 200_000u32),
        ("2028 roomy (cap binds)", 161_250_000, 200_000),
        ("2026 starved 12k WU", 317_500_000, 12_000),
        ("2028 starved 12k WU (cap binds)", 161_250_000, 12_000),
    ] {
        let a = dist(
            &shares,
            &balances,
            fee,
            Some(&miners[0]),
            BONUS_1776,
            reward,
            budget,
        );
        let b = dist(
            &shares,
            &balances,
            fee,
            Some(&miners[1]),
            BONUS_1776,
            reward,
            budget,
        );
        let n = a.len().max(b.len());
        let mut diffs = Vec::new();
        for i in 0..n {
            if a.get(i) != b.get(i) {
                diffs.push(i);
            }
        }
        let sum_a: i64 = a.iter().map(|(_, s)| s).sum();
        let sum_b: i64 = b.iter().map(|(_, s)| s).sum();
        let dup_a = a.iter().filter(|(x, _)| x == miners[0].as_str()).count();
        println!(
            "{label:34} len {}/{} differing_positions={} at {:?} finder_entries={} \
             conserved={}",
            a.len(),
            b.len(),
            diffs.len(),
            &diffs[..diffs.len().min(6)],
            dup_a,
            sum_a == reward && sum_b == reward,
        );
        assert_eq!(sum_a, reward, "{label}: A must conserve reward");
        assert_eq!(sum_b, reward, "{label}: B must conserve reward");
    }
}

/// The cap is a silent mutation into near-solo. Pin the exact boundary so
/// Phase 1 config validation can reject it loudly instead.
#[test]
fn cap_binding_boundary_is_silent() {
    let addrs = synth_addresses(9);
    let fee = &addrs[0];
    let miners = &addrs[1..9];
    let mut shares = HashMap::new();
    for m in miners {
        shares.insert(m.clone(), 1.0);
    }
    let balances = HashMap::new();

    // Post-halving: 1.5625 + 0.05 fees.
    let reward = 161_250_000i64;
    let out = dist(
        &shares,
        &balances,
        fee,
        Some(&miners[0]),
        BONUS_1776,
        reward,
        200_000,
    );
    let fee_out: i64 = out
        .iter()
        .filter(|(a, _)| a == fee.as_str())
        .map(|(_, s)| s)
        .sum();
    let miner_cut = reward - fee_out;
    let bonus_paid = out
        .iter()
        .filter(|(a, _)| a == miners[0].as_str())
        .map(|(_, s)| *s)
        .max()
        .unwrap();

    // The clamp is exactly 95% of the miner cut, floored.
    let expected_cap = ((miner_cut as f64) * 0.95).floor() as i64;
    assert_eq!(
        bonus_paid, expected_cap,
        "post-halving the requested 1.776 BTC must clamp to 95% of the miner cut"
    );
    assert!(
        bonus_paid < BONUS_1776,
        "cap must actually bind post-halving"
    );

    // And here is the consequence worth failing config over: the seven
    // non-finders split 5% of the cut.
    let remainder = miner_cut - bonus_paid;
    println!(
        "\n=== cap-binding consequence (post-2028) ===\nminer_cut={:.4} BTC  bonus_paid={:.6} BTC \
         (requested {:.4})  remainder_for_ALL_non_finders={:.6} BTC ({} sats)",
        miner_cut as f64 / COIN as f64,
        bonus_paid as f64 / COIN as f64,
        BONUS_1776 as f64 / COIN as f64,
        remainder as f64 / COIN as f64,
        remainder,
    );
    println!(
        "That is {:.2}% of the miner cut shared by every miner who did not find the block.",
        100.0 * remainder as f64 / miner_cut as f64
    );
}
