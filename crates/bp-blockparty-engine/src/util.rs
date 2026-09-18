// SPDX-License-Identifier: AGPL-3.0-or-later

//! Service-private helpers.

use bp_common::AddressId;

use crate::error::BlockpartyServiceError;

/// [`bp_common::normalized_address_id`] with the error mapped into this
/// crate's type.
///
/// The rule itself is NOT here. It used to be — a hand copy, because
/// `bp-mining-job` (where the rule lived) is only a dev-dependency of this
/// crate, so it could not be imported. What that copy cost is on record in its
/// own doc comment: an unconditional `to_ascii_lowercase` corrupted Base58 case,
/// so a signature-/email-verified legacy address never matched the
/// case-preserved row the verify path wrote, and any Base58 coinbase output was
/// built from a mangled address. The rule now lives in `bp-common` — a real
/// dependency of every crate that needs it — with a test that pinned all three
/// old copies to it before they were deleted.
pub(crate) fn normalize_address(raw: &str) -> Result<AddressId, BlockpartyServiceError> {
    bp_common::normalized_address_id(raw).map_err(|_| BlockpartyServiceError::InvalidAddress)
}

/// [`normalize_address`] for every path that puts an address INTO a party:
/// creation (the admin row), the admin's add, and the self-join link.
///
/// A Blockparty pays fixed addresses only. The payout resolver refuses a
/// distribution holding a rotating identity and serves the whole party no job
/// while that miner is connected, so admitting one here would stall every
/// member's mining on one enrolment. Refused at the door instead.
///
/// Not used by `remove_member`: an id enrolled before this check existed has
/// to stay removable.
pub(crate) fn normalize_enrolled_address(raw: &str) -> Result<AddressId, BlockpartyServiceError> {
    let address = normalize_address(raw)?;
    if bp_payout_descriptor::is_payout_id(address.as_str()) {
        return Err(BlockpartyServiceError::RotatingIdentity);
    }
    Ok(address)
}

#[cfg(test)]
mod tests {
    use super::normalize_address;

    #[test]
    fn preserves_base58_case_and_lowercases_bech32() {
        // Legacy Base58 is case-sensitive → MUST be preserved verbatim, so a
        // signature-/email-verified `1BvBM…` row is found by the gate (the bug
        // this fixes lowercased it to `1bvbm…` and missed).
        let base58 = "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2";
        assert_eq!(normalize_address(base58).unwrap().as_str(), base58);
        let p2sh = "3J98t1WpEZ73CNmQviecrnyiWrnqRhWNLy";
        assert_eq!(normalize_address(p2sh).unwrap().as_str(), p2sh);

        // bech32 is case-insensitive → canonicalise to lowercase, matching the
        // stratum/payout + group-solo normalizer byte-for-byte.
        assert_eq!(
            normalize_address("BC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KV8F3T4")
                .unwrap()
                .as_str(),
            "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"
        );

        assert!(normalize_address("   ").is_err());
    }
}
