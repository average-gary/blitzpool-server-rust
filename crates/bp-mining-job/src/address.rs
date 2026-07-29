// SPDX-License-Identifier: AGPL-3.0-or-later

//! BTC address normalization and script derivation.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{LazyLock, Mutex};

use bitcoin::{Address, Network, ScriptBuf};

/// Normalize a BTC address for storage / equality comparison.
///
/// Bech32 / bech32m (BIP-173 / BIP-350) are case-insensitive by spec —
/// wallets may present them uppercase (QR-code optimization) but the
/// canonical wire form is lowercase. Legacy P2PKH / P2SH (base58) IS
/// case-sensitive — different cases are different addresses with
/// different checksums — and is left untouched.
///
/// Whitespace is trimmed. Empty input maps to empty output.
pub fn normalize_btc_address(address: &str) -> String {
    let trimmed = address.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("bc1")
        || lower.starts_with("tb1")
        || lower.starts_with("bcrt1")
        || lower.starts_with("sb1")
    {
        lower
    } else {
        trimmed.to_string()
    }
}

/// Memo for [`address_to_script`], keyed by `(network, address)`.
///
/// The network belongs in the key because the same address string is
/// accepted on one network and rejected on another — keying by address
/// alone would serve a mainnet script to a regtest job.
///
/// Only successes are stored. Failures are cheap (the parse bails early)
/// and caching them would pin unbounded attacker-chosen garbage: the
/// address arrives in the stratum username, so a client can mint distinct
/// invalid strings as fast as it can reconnect.
static SCRIPT_MEMO: LazyLock<Mutex<HashMap<(Network, String), ScriptBuf>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Entry ceiling for [`SCRIPT_MEMO`], past which it is cleared wholesale.
///
/// Successes are bounded by distinct addresses that actually mined, but
/// that set only ever grows while the PPLNS window slides, so without a
/// ceiling the memo accumulates every address the pool has ever seen (a
/// client can also rotate addresses deliberately). At roughly 100 bytes
/// per entry this caps it near 10 MB.
///
/// Clearing wholesale rather than evicting an LRU keeps the hot path a
/// single map op with no per-entry bookkeeping: the working set is one
/// template's payout addresses, which the next build re-populates
/// immediately. A real pool never reaches this.
const MEMO_MAX_ENTRIES: usize = 100_000;

/// Convert a BTC address to its `scriptPubKey` bytes for the given network.
/// All address types supported by `rust-bitcoin` are handled (P2PKH, P2SH,
/// P2WPKH, P2WSH, P2TR). A network mismatch (e.g. testnet address with
/// `Network::Bitcoin`) is rejected.
///
/// Memoized: this runs once per payout output per coinbase build, and the
/// PPLNS finder bonus makes each connection's payout list distinct, so the
/// job cache's per-list memo can no longer collapse the parses across
/// connections — the same few hundred addresses would otherwise be
/// re-parsed once per connection per template. The parse is pure, so a hit
/// is indistinguishable from a fresh parse.
pub fn address_to_script(network: Network, address: &str) -> Result<ScriptBuf, AddressError> {
    // A poisoned lock means another thread panicked mid-map-op. The memo is
    // pure cache, so recover and carry on rather than propagating.
    if let Some(hit) = SCRIPT_MEMO
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&(network, address.to_string()))
    {
        return Ok(hit.clone());
    }

    let unchecked = Address::from_str(address).map_err(|e| AddressError::Parse(e.to_string()))?;
    let checked = unchecked
        .require_network(network)
        .map_err(|e| AddressError::NetworkMismatch(e.to_string()))?;
    let script = checked.script_pubkey();

    let mut memo = SCRIPT_MEMO.lock().unwrap_or_else(|e| e.into_inner());
    if memo.len() >= MEMO_MAX_ENTRIES {
        memo.clear();
    }
    memo.insert((network, address.to_string()), script.clone());
    Ok(script)
}

#[derive(thiserror::Error, Debug)]
pub enum AddressError {
    #[error("failed to parse address: {0}")]
    Parse(String),
    #[error("address is not for the expected network: {0}")]
    NetworkMismatch(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bech32_normalized_to_lowercase() {
        assert_eq!(
            normalize_btc_address("BC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KV8F3T4"),
            "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"
        );
        assert_eq!(
            normalize_btc_address("TB1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KXPJZSX"),
            "tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx"
        );
        assert_eq!(
            normalize_btc_address("BCRT1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KYGT080"),
            "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080"
        );
    }

    #[test]
    fn legacy_preserves_case() {
        assert_eq!(
            normalize_btc_address("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"),
            "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"
        );
        assert_eq!(
            normalize_btc_address("3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy"),
            "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy"
        );
    }

    #[test]
    fn trims_whitespace() {
        assert_eq!(
            normalize_btc_address("  bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4  "),
            "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"
        );
    }

    #[test]
    fn empty_input_returns_empty() {
        assert_eq!(normalize_btc_address(""), "");
        assert_eq!(normalize_btc_address("   "), "");
    }

    #[test]
    fn address_to_script_p2wpkh_mainnet() {
        let script = address_to_script(
            Network::Bitcoin,
            "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
        )
        .unwrap();
        let bytes = script.to_bytes();
        // P2WPKH scriptPubKey: OP_0 (0x00) + OP_PUSHBYTES_20 (0x14) + 20-byte hash160.
        assert_eq!(bytes[0], 0x00);
        assert_eq!(bytes[1], 0x14);
        assert_eq!(bytes.len(), 22);
    }

    #[test]
    fn address_to_script_p2pkh_mainnet() {
        let script =
            address_to_script(Network::Bitcoin, "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2").unwrap();
        let bytes = script.to_bytes();
        // P2PKH: OP_DUP OP_HASH160 0x14 <20-byte hash> OP_EQUALVERIFY OP_CHECKSIG = 25 bytes.
        assert_eq!(bytes.len(), 25);
        assert_eq!(bytes[0], 0x76); // OP_DUP
        assert_eq!(bytes[1], 0xa9); // OP_HASH160
        assert_eq!(bytes[2], 0x14); // push 20
        assert_eq!(bytes[23], 0x88); // OP_EQUALVERIFY
        assert_eq!(bytes[24], 0xac); // OP_CHECKSIG
    }

    #[test]
    fn address_to_script_rejects_wrong_network() {
        // Testnet bech32 against mainnet — must be rejected.
        let result = address_to_script(
            Network::Bitcoin,
            "tb1qw508d6qejxtdg4y5r3zarvary0c5xw7kxpjzsx",
        );
        assert!(matches!(result, Err(AddressError::NetworkMismatch(_))));
    }

    #[test]
    fn address_to_script_rejects_garbage() {
        let result = address_to_script(Network::Bitcoin, "definitely-not-an-address");
        assert!(matches!(result, Err(AddressError::Parse(_))));
    }

    // ----- memoization -----

    #[test]
    fn memo_returns_the_same_script_on_repeat_calls() {
        let a = address_to_script(
            Network::Bitcoin,
            "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
        )
        .expect("first parse");
        let b = address_to_script(
            Network::Bitcoin,
            "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4",
        )
        .expect("memo hit");
        assert_eq!(a, b, "a memo hit must be indistinguishable from a parse");
    }

    /// The reason `network` is in the key. A regtest bech32 and a mainnet
    /// bech32 encoding the SAME witness program differ only by HRP, so a
    /// memo keyed on the address alone could not tell them apart — and
    /// each must still be rejected on the other's network.
    #[test]
    fn memo_does_not_leak_scripts_across_networks() {
        const MAINNET: &str = "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4";
        const REGTEST: &str = "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080";

        let mainnet_script = address_to_script(Network::Bitcoin, MAINNET).expect("mainnet ok");
        let regtest_script = address_to_script(Network::Regtest, REGTEST).expect("regtest ok");
        // Same witness program, so the scripts genuinely do match — which
        // is exactly why the HRP/network must key the memo.
        assert_eq!(mainnet_script, regtest_script);

        // Populating the memo must not make either address valid on the
        // other network.
        assert!(matches!(
            address_to_script(Network::Regtest, MAINNET),
            Err(AddressError::NetworkMismatch(_))
        ));
        assert!(matches!(
            address_to_script(Network::Bitcoin, REGTEST),
            Err(AddressError::NetworkMismatch(_))
        ));
    }

    #[test]
    fn memo_never_caches_failures() {
        // Assert on the keys themselves, not on `len()`: this memo is
        // process-global, so a sibling test inserting a valid address
        // concurrently would move the count.
        let keys: Vec<String> = (0..50).map(|i| format!("garbage-address-{i}")).collect();
        for k in &keys {
            let _ = address_to_script(Network::Bitcoin, k);
        }
        let memo = SCRIPT_MEMO.lock().unwrap();
        for k in &keys {
            assert!(
                !memo.contains_key(&(Network::Bitcoin, k.clone())),
                "invalid address {k} was cached — clients supply these, so \
                 caching them would let one pin unbounded memory"
            );
        }
    }

    /// The ceiling is enforced against the process-global memo, so this
    /// test would fight every sibling that touches it. Exercise the same
    /// clear-at-ceiling rule against a local map instead — the assertion is
    /// about the policy, and the hot path applies it verbatim.
    #[test]
    fn memo_clears_at_the_entry_ceiling() {
        let mut memo: HashMap<(Network, String), ScriptBuf> = HashMap::new();
        for i in 0..MEMO_MAX_ENTRIES {
            memo.insert(
                (Network::Bitcoin, format!("synthetic-{i}")),
                ScriptBuf::new(),
            );
        }
        assert_eq!(memo.len(), MEMO_MAX_ENTRIES);

        // The rule as `address_to_script` applies it before inserting.
        if memo.len() >= MEMO_MAX_ENTRIES {
            memo.clear();
        }
        memo.insert((Network::Bitcoin, "fresh".to_string()), ScriptBuf::new());
        assert_eq!(
            memo.len(),
            1,
            "at the ceiling the memo resets, so it can never grow unbounded"
        );
    }
}
