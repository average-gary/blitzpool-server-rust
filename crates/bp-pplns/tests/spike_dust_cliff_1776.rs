// SPDX-License-Identifier: AGPL-3.0-or-later
//! SPIKE — does a 1.776 BTC bonus starve the remainder below the dust
//! floor, and at what pool size?
//!
//! This is the question that decides whether the scheme is still
//! "coinbase-direct" in any meaningful sense. The whole premise of
//! rejecting Parasite's model was that its remainder is custodial. If
//! the bonus leaves a remainder too small to clear `min_payout_sats`
//! for most miners, their share becomes pending ledger credit — an
//! internal IOU — which is the same custodial outcome by a different
//! route.
//!
//! Run: cargo test -p bp-pplns --test spike_dust_cliff_1776 --release -- --nocapture

use std::collections::HashMap;

use bp_common::{AddressId, Sats};
use bp_pplns::{build_coinbase_distribution, CoinbaseDistributionInput};

const COIN: i64 = 100_000_000;
const BONUS_1776: i64 = 177_600_000;
const DUST: i64 = 546;

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

struct Outcome {
    on_chain_miners: usize,
    finder_bonus: i64,
    remainder_on_chain: i64,
    pending_addresses: usize,
}

fn run(n_miners: usize, reward: i64, bonus: i64, budget: u32) -> Outcome {
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
        fee_percent: 1.5,
        fee_address: Some(&fee),
        coinbase_weight_budget: budget,
        suppress_matching_debits: false,
        min_payout_sats: Some(Sats(DUST)),
        finder_bonus_sats: Some(Sats(bonus)),
        finder_address: Some(&finder),
    });

    let finder_bonus = res
        .payouts
        .iter()
        .filter(|p| p.address == finder)
        .map(|p| p.sats.to_i64())
        .max()
        .unwrap_or(0);
    // Miner outputs = everything that is not the fee output.
    let miner_outputs: Vec<i64> = res
        .payouts
        .iter()
        .filter(|p| p.address != fee)
        .map(|p| p.sats.to_i64())
        .collect();
    let remainder_on_chain: i64 = miner_outputs.iter().sum::<i64>() - finder_bonus;
    // Addresses left holding non-zero pending credit.
    let pending_addresses = res
        .balance_after
        .values()
        .filter(|s| s.to_i64() != 0)
        .count();

    Outcome {
        on_chain_miners: miner_outputs.len(),
        finder_bonus,
        remainder_on_chain,
        pending_addresses,
    }
}

#[test]
fn dust_cliff_by_pool_size() {
    println!(
        "\n=== 1.776 BTC bonus: how many miners still get PAID ON-CHAIN? ===\n\
         budget 200k WU, 1.5% fee, equal shares, dust floor {DUST} sats"
    );
    for (era, reward) in [
        ("2026 (3.125+.05)", 317_500_000i64),
        ("2028 (1.5625+.05)", 161_250_000),
        ("2032 (0.78125+.05)", 83_125_000),
    ] {
        println!("\n-- {era} --");
        for n in [10usize, 100, 500, 2000] {
            let o = run(n, reward, BONUS_1776, 200_000);
            let per = if o.on_chain_miners > 1 {
                o.remainder_on_chain / (o.on_chain_miners as i64 - 1).max(1)
            } else {
                0
            };
            println!(
                "  miners={n:5} on_chain_outputs={:5} bonus={:.6} BTC remainder_on_chain={:11} \
                 (~{per:9} sats each) pending_addrs={:5}",
                o.on_chain_miners,
                o.finder_bonus as f64 / COIN as f64,
                o.remainder_on_chain,
                o.pending_addresses,
            );
        }
    }

    println!(
        "\n=== baseline for comparison: SAME pools, bonus disabled ===\n\
         (isolates what the bonus itself costs in on-chain reach)"
    );
    for n in [10usize, 100, 500, 2000] {
        let o = run(n, 317_500_000, 0, 200_000);
        println!(
            "  miners={n:5} on_chain_outputs={:5} pending_addrs={:5}",
            o.on_chain_miners, o.pending_addresses
        );
    }
}
