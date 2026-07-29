// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(clippy::print_stderr)]

//! Phase 4 — the finder bonus, end to end against a real `bitcoin-node`.
//!
//! Everything upstream of this test proves a piece: the pure math carves the
//! bonus out (`bp-pplns`), the config reaches the builder
//! (`distribution_integration`), the ledger survives an address appearing
//! twice (`finder_bonus_ledger_integration`). None of them prove the
//! composite is a *block*. A coinbase paying one address in two outputs is
//! the kind of thing consensus could reject for reasons no unit test models
//! — and a bonus that makes the sat sums drift by one is `bad-cb-amount`, a
//! block the pool built and the network threw away.
//!
//! So this test mines one:
//!
//! 1. Three miners with distinct addresses and unequal share weights, one
//!    of them named as the prospective finder.
//! 2. `build_distribution` with `finder_bonus_sats` configured → a payout
//!    list in which the finder appears **twice**: its bonus output and its
//!    proportional share.
//! 3. That list becomes a real coinbase; bitcoin-core must accept the block.
//! 4. The ledger books it, and every audit row is checked against an actual
//!    output of the coinbase transaction the chain accepted — same script,
//!    same satoshis.
//! 5. The finder's booked total must be its proportional share plus
//!    exactly the bonus — read back off the ledger, not recomputed.
//!
//! Skips cleanly when `bitcoin-node` / Redis / PG aren't present.

use std::collections::HashMap;
use std::time::Duration;

use bitcoin::consensus::Decodable;
use bitcoin::Network;
use bp_common::{AddressId, Sats};
use bp_mining_job::{
    build_mining_job_from_tdp, merkle_root_from_coinbase, PayoutEntry, TdpCoinbaseTemplate,
    EXTRANONCE_SLOT_LEN,
};
use bp_pplns::DEFAULT_MIN_PAYOUT_SATS;
use bp_pplns_engine::config::PplnsEngineConfig;
use bp_pplns_engine::engine::PplnsEngine;
use bp_pplns_engine::window::NetworkDifficulty;
use bp_regtest_harness::{RegtestConfig, RegtestNode};
use bp_share::Target;
use bp_template_distribution::{TdpConfig, TdpHandle};
use sqlx::PgPool;

use bp_test_support::{
    brute_force_nonce, connect_pg_or_skip, connect_redis_or_skip, deterministic_p2wpkh_regtest,
    poll_for_height, wait_for_paired_template,
};

/// Own logical DB — 2 is free across this crate's suites.
const REDIS_TEST_DB: u8 = 2;

/// 0.1776 BTC — the bonus this feature ships with.
const FINDER_BONUS_SATS: i64 = 17_760_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finder_bonus_coinbase_accepted_by_core_and_booked() {
    let regtest_cfg = RegtestConfig::default();
    if !regtest_cfg.is_available() {
        eprintln!(
            "skipping finder-bonus e2e regtest — bitcoin-node not found at {} \
             (set BITCOIN_NODE_PATH)",
            regtest_cfg.bitcoin_node_path.display()
        );
        return;
    }
    let Some(redis_conn) = connect_redis_or_skip(REDIS_TEST_DB).await else {
        return;
    };
    let Some(pg) = connect_pg_or_skip().await else {
        return;
    };

    // Distinct from every sibling regtest test's addresses — they share the
    // `pplns_payout_history` / `pplns_balance` tables.
    let addr_finder = deterministic_p2wpkh_regtest([0xb1; 32]);
    let addr_bob = deterministic_p2wpkh_regtest([0xb2; 32]);
    let addr_carol = deterministic_p2wpkh_regtest([0xb3; 32]);
    let addr_fee = deterministic_p2wpkh_regtest([0xbf; 32]);

    let engine = PplnsEngine::spawn(
        bonus_engine_config(&addr_fee),
        redis_conn,
        pg.clone(),
        NetworkDifficulty::new(1_000.0),
    )
    .await
    .expect("PplnsEngine::spawn");

    // Unequal weights: the finder is deliberately the SMALLEST shareholder,
    // so a bonus that silently failed to land could not be mistaken for its
    // proportional share being large.
    let now_ms = chrono::Utc::now().timestamp_millis() as u64;
    for (addr, weight) in [
        (&addr_finder, 100.0),
        (&addr_bob, 200.0),
        (&addr_carol, 300.0),
    ] {
        engine
            .record_share(None, addr, weight, now_ms)
            .await
            .expect("seed share");
    }

    // ── Boot bitcoin-core, mine past IBD, attach TDP ──────────────────
    let node = RegtestNode::start_with(regtest_cfg)
        .await
        .expect("regtest start");
    node.generate_to_self(101)
        .await
        .expect("mine 101 for IBD-exit + coinbase maturity");
    let tdp = TdpHandle::spawn(
        TdpConfig::new(node.ipc_socket_path())
            .with_fee_threshold(1)
            .with_min_interval_secs(1),
    )
    .expect("TdpHandle::spawn against regtest IPC");
    let mut rx = tdp.subscribe();
    let _ = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if rx.recv().await.is_err() {
                break;
            }
        }
    })
    .await;
    node.generate_to_self(1)
        .await
        .expect("mine 1 more for a fresh NewTemplate");
    let (template, prev_hash) = wait_for_paired_template(&mut rx).await;

    // ── The distribution, built for this finder ───────────────────────
    let reward_sats = template.coinbase_tx_value_remaining;
    let finder = AddressId::new(addr_finder.clone()).expect("finder address valid");
    let dist = engine
        .build_distribution(reward_sats, &finder)
        .await
        .expect("build_distribution");
    let fingerprint = dist.payouts_fingerprint;

    // The shape the whole feature exists to produce: the finder twice.
    let finder_entries: Vec<i64> = dist
        .payouts
        .iter()
        .filter(|p| p.address == finder)
        .map(|p| p.sats.0)
        .collect();
    assert_eq!(
        finder_entries.len(),
        2,
        "the finder must appear twice — bonus output plus proportional share. \
         Got {finder_entries:?} in a {}-output list; one entry means the bonus \
         never landed (or was merged early, which would hide the duplicate the \
         ledger has to handle)",
        dist.payouts.len()
    );
    assert!(
        finder_entries.contains(&FINDER_BONUS_SATS),
        "one of the finder's two outputs must be exactly the configured \
         {FINDER_BONUS_SATS}-sat bonus; got {finder_entries:?}"
    );
    let total_payout: i64 = dist.payouts.iter().map(|p| p.sats.0).sum();
    assert_eq!(
        total_payout as u64, reward_sats,
        "the coinbase must pay out exactly the reward — any drift is \
         bad-cb-amount and the block is thrown away"
    );

    // ── Build the coinbase + find a regtest-target nonce ──────────────
    let payouts: Vec<PayoutEntry> = dist
        .payouts
        .iter()
        .map(|p| PayoutEntry {
            address: p.address.as_str().to_string(),
            sats: p.sats.0 as u64,
        })
        .collect();
    let coinbase_template = TdpCoinbaseTemplate {
        coinbase_prefix: &template.coinbase_prefix,
        coinbase_tx_version: template.coinbase_tx_version,
        coinbase_tx_input_sequence: template.coinbase_tx_input_sequence,
        coinbase_tx_value_remaining: template.coinbase_tx_value_remaining,
        coinbase_tx_outputs: &template.coinbase_tx_outputs,
        coinbase_tx_outputs_count: template.coinbase_tx_outputs_count,
        coinbase_tx_locktime: template.coinbase_tx_locktime,
    };
    let job = build_mining_job_from_tdp(
        Network::Regtest,
        &payouts,
        &coinbase_template,
        "finder-bonus-regtest",
        EXTRANONCE_SLOT_LEN,
    )
    .expect(
        "a payout list naming one address twice must still assemble into a \
         coinbase — two outputs to the same script is legal",
    );
    // The booking path finds the snapshot by this identity; engine and job
    // derive it independently and must agree.
    assert_eq!(
        job.payouts_fingerprint(),
        &fingerprint,
        "engine-side and job-side fingerprints must be byte-identical, \
         duplicate finder entry included"
    );

    let en1 = [0u8; 4];
    let en2 = [0u8; 8];
    let coinbase_txid = job.coinbase_txid_with_extranonce(&en1, &en2);
    let merkle_root = merkle_root_from_coinbase(&coinbase_txid, &template.merkle_path);
    let target = Target::from_le_bytes(prev_hash.target);
    let nonce = brute_force_nonce(
        template.version,
        &prev_hash.prev_hash,
        &merkle_root,
        prev_hash.header_timestamp,
        prev_hash.n_bits,
        &target,
    )
    .expect("find a regtest-target nonce within 1M tries");

    // ── Submit: consensus is the judge ────────────────────────────────
    let witness_coinbase = job.witness_coinbase_with_extranonce(&en1, &en2);
    let before_height = node.current_height().await.expect("current_height");
    tdp.submit_solution(
        template.template_id,
        template.version,
        prev_hash.header_timestamp,
        nonce,
        witness_coinbase.clone(),
    )
    .await
    .expect("submit_solution");
    let height = poll_for_height(&node, before_height + 1, Duration::from_secs(20))
        .await
        .expect(
            "bitcoin-core must accept a coinbase carrying the finder bonus — a \
             stuck tip means the bonus made the block invalid (sat drift, dust \
             output, or the duplicate-address outputs being rejected)",
        );
    assert_eq!(height, before_height + 1);

    // ── Book it ───────────────────────────────────────────────────────
    let prepared = engine
        .prepare_block_found_for(height as i32, reward_sats, Some(fingerprint))
        .await
        .expect("the mined job's own distribution must resolve by fingerprint");
    engine.apply_prepared(&prepared).await.expect(
        "the booking must survive the finder appearing twice in the payout \
             list — an unmerged duplicate aborts the write with a Postgres \
             cardinality_violation",
    );

    // ── Every audit row must match a real output of the accepted tx ───
    let coinbase_tx = bitcoin::Transaction::consensus_decode(&mut witness_coinbase.as_slice())
        .expect("submitted coinbase must decode");
    let rows: Vec<(String, i64)> = sqlx::query_as(
        r#"SELECT address, "paidSats" FROM pplns_payout_history
           WHERE "blockHeight" = $1 AND "rowType" = 'coinbase'"#,
    )
    .bind(height as i32)
    .fetch_all(&pg)
    .await
    .expect("read audit rows");
    assert!(!rows.is_empty(), "coinbase audit rows must exist");

    // The finder's two coinbase outputs are one audit row for their sum —
    // that merge is what keeps the ledger write legal. So compare per
    // address against the SUM of that address's coinbase outputs, not
    // against any single output.
    let mut on_chain: HashMap<Vec<u8>, u64> = HashMap::new();
    for o in &coinbase_tx.output {
        *on_chain
            .entry(o.script_pubkey.as_bytes().to_vec())
            .or_default() += o.value.to_sat();
    }
    for (address, paid_sats) in &rows {
        let script = bp_mining_job::address_to_script(Network::Regtest, address)
            .expect("audit-row address must be a payable script");
        let chain_total = on_chain.get(script.as_bytes()).copied().unwrap_or(0);
        assert_eq!(
            chain_total, *paid_sats as u64,
            "ledger booked {paid_sats} sat for {address}, but the accepted \
             coinbase pays that script {chain_total} sat in total"
        );
    }

    // And the ledger has to reflect that the finder was paid extra — by
    // exactly the bonus, and no more.
    //
    // Bob's weight is exactly twice the finder's, and both are diluted by
    // the same fee and the same bonus carve-out, so `bob_paid / 2` IS the
    // finder's proportional share, derived from the books rather than
    // recomputed from the config. Whatever the finder holds beyond that is
    // the bonus.
    //
    // Stated as equality rather than `>`: a mere inequality would also pass
    // if the bonus were carved out and then paid twice, or paid without
    // being carved out of the shared pot first (which would overpay the
    // coinbase and make the block invalid — the sum assertion above already
    // rules that out, but this pins the split itself).
    //
    // Note the bonus is only ~0.35% of a 50 BTC regtest subsidy, against
    // ~5.6% of a post-halving mainnet reward, so it cannot be expected to
    // out-weigh a proportional-share gap here.
    let booked: HashMap<&str, i64> = rows.iter().map(|(a, s)| (a.as_str(), *s)).collect();
    let finder_paid = *booked
        .get(addr_finder.as_str())
        .expect("the finder must have a coinbase audit row");
    let bob_paid = *booked
        .get(addr_bob.as_str())
        .expect("Bob must have a coinbase audit row");
    let finder_proportional = bob_paid / 2;
    let premium = finder_paid - finder_proportional;
    // The per-miner split floors, so allow the last-satoshi slack that
    // halving Bob's row can introduce.
    assert!(
        (premium - FINDER_BONUS_SATS).abs() <= 2,
        "the finder must be booked its proportional share plus exactly the \
         bonus: finder={finder_paid}, its share (= bob/2, Bob holding twice \
         the weight) = {finder_proportional}, so the premium is {premium} \
         where the configured bonus is {FINDER_BONUS_SATS} (bob={bob_paid})"
    );

    engine.shutdown();
    tdp.shutdown().expect("TDP clean shutdown");
    node.shutdown().await.expect("regtest clean shutdown");
    let _ = sqlx::query(r#"DELETE FROM pplns_payout_history WHERE "blockHeight" = $1"#)
        .bind(height as i32)
        .execute(&pg)
        .await;
    cleanup_pplns_state(&pg, &payouts).await;
}

fn bonus_engine_config(fee_addr: &str) -> PplnsEngineConfig {
    PplnsEngineConfig {
        dust_sweep_enabled: false,
        touch_flush_interval_secs: 3_600,
        fee_address: Some(AddressId::new(fee_addr.to_string()).expect("fee addr valid")),
        fee_percent: 1.5,
        min_payout_sats: Sats(DEFAULT_MIN_PAYOUT_SATS as i64),
        finder_bonus_sats: Some(Sats(FINDER_BONUS_SATS)),
        ..PplnsEngineConfig::default()
    }
}

async fn cleanup_pplns_state(pool: &PgPool, payouts: &[PayoutEntry]) {
    for p in payouts {
        let _ = sqlx::query("DELETE FROM pplns_payout_history WHERE address = $1")
            .bind(&p.address)
            .execute(pool)
            .await;
        let _ = sqlx::query("DELETE FROM pplns_balance WHERE address = $1")
            .bind(&p.address)
            .execute(pool)
            .await;
    }
}
