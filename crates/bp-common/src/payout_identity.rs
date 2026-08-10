// SPDX-License-Identifier: AGPL-3.0-or-later

//! [`PayoutIdentity`] — what a miner presented on the wire, as the thing that
//! decides where its money goes.
//!
//! Today every identity is a literal address: one string that serves as ledger
//! key, mode key, and coinbase script source at once. A rotating identity
//! (an xpub the pool derives a fresh script from per block) splits those roles
//! apart — the ledger key must stay fixed or `pplns_balance`'s
//! `PRIMARY KEY (address)` grows a row per block, while the script must change
//! or rotation is not happening.
//!
//! This module holds the type that makes those two roles different values, the
//! one implementation of the `address.worker` split, and nothing else.
//! Descriptor parsing and script derivation are deliberately elsewhere: they
//! need `bitcoin`/`miniscript`, `bp-common` is a dependency of 21 crates, and
//! the rule that a script is derived at a *height* belongs next to
//! `address_to_script` where the only coinbase-path script derivation already
//! lives.
//!
//! **`Rotating` cannot be constructed.** Not by convention — see
//! [`RotatingDescriptor`]. That is what makes introducing this type a refactor
//! with a compiler-checked no-op guarantee instead of a feature behind a flag:
//! no behaviour can differ if the second variant cannot exist, while every
//! `match` on it still enumerates the sites the next phase has to fill in.

use crate::{normalize_btc_address, AddressId, InvalidAddressError};

/// An uninhabited type. No value of it exists, so nothing containing one by
/// value can be constructed.
///
/// `!` would say this directly but is not stable in field position.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Never {}

/// The descriptor a [`PayoutIdentity::Rotating`] derives its per-block script
/// from — **not yet representable**.
///
/// The field is [`Never`], which is uninhabited, so this struct cannot be built
/// *anywhere*, including inside this module. That is deliberate and it is
/// stronger than omitting a public constructor: "we did not write a way to make
/// one" is a claim about the code as it stands, and the next person to add a
/// helper breaks it silently. "The type has no values" is checked by the
/// compiler on every build.
///
/// Rust still requires a `match` arm for an uninhabited variant on stable, so
/// the exhaustiveness this buys is real: every site that will have to answer
/// "what is the script for a rotating identity?" has to write the arm now, and
/// the arm it writes now is `unreachable`-by-construction rather than a guess.
///
/// The field becomes `Box<miniscript::Descriptor<DescriptorPublicKey>>` when a
/// mode is ready to *pay* one. At that point this type gains values and every
/// arm written against it starts running — which is why those arms must be
/// written to be *correct*, not merely to compile.
///
/// **Intake landing is not that moment**, and the distinction is the whole
/// reason the phases are separate. `bp_payout_descriptor::RotatingPayout` now
/// exists and validates a real xpub, but nothing converts one into this variant:
/// the conversion is what makes `absurd()` unwritable, and the compile errors it
/// produces have to be answered with a real derivation at the coinbase seam, not
/// with a stub. So intake can validate and store a rotating identity while the
/// payout path still provably cannot see one.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RotatingDescriptor {
    _not_yet: Never,
}

impl RotatingDescriptor {
    /// Discharge a `Rotating` arm that cannot be reached.
    ///
    /// Returns `!` by matching an uninhabited value with zero arms, so the
    /// *compiler* certifies the arm is dead. Use this instead of
    /// `unreachable!()`: a panic macro is a promise a human made, and it stays
    /// compiling — and silently becomes reachable — the moment this type gains
    /// values.
    ///
    /// **This method is the Phase-2/3 worklist.** When `_not_yet` becomes a real
    /// descriptor, `absurd` can no longer be written, and every
    /// `descriptor.absurd()` in the workspace turns into a compile error that
    /// has to be replaced with the real derivation. That is the whole reason to
    /// land the type before the feature: the list of sites is produced by the
    /// compiler, now, rather than by grepping during the feature.
    pub fn absurd(self) -> ! {
        match self._not_yet {}
    }
}

/// Where a miner's share of a coinbase goes.
///
/// The two roles this separates:
///
/// 1. **[`payout_id`](Self::payout_id)** — the ledger key and the mode key.
///    Height-invariant by construction.
/// 2. **the payout script** — varies by height, and only for `Rotating`.
///    Derived in `bp-mining-job`, which owns the `bitcoin` dependency.
///
/// For `Static` the two coincide, which is why a `String` has sufficed so far.
///
/// # Why a sum type and not `Option<Descriptor>`
///
/// `descriptor.is_some()` reads as "is this miner rotating?" and answers "did
/// someone populate this field?". A row with both fields set pays whichever
/// branch the reader happened to check, and there are two readers (the coinbase
/// builder and whatever books the ledger). `CLAUDE.md` names this: *"The same
/// goes for `is_some()` on a per-mode field: it reads as a mode test and answers
/// a different question."*
///
/// The precedent is in this repo's history, 2026-08-03: a found block's
/// settlement inputs were stamped by `if resolved.mode == MiningMode::GroupSolo`,
/// PPLNS fell out of that `if`, and roughly half its blocks lost the inputs for
/// good against a 20-minute TTL. Several reviews passed over the line. An
/// `Option` on a per-mode field is that defect with a different keyword —
/// `is_some()` is no more exhaustive than `if mode ==`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PayoutIdentity {
    /// A literal Bitcoin address, paid verbatim at every height.
    Static {
        /// **Not an [`AddressId`], and that is load-bearing.**
        ///
        /// This is the same unconstrained `String` the coinbase seam carries
        /// today. Narrowing it to `AddressId` would impose that type's 62-char
        /// cap here, and a **regtest P2TR address is 64 characters**
        /// (`bcrt1p` + 1 + 52 + 6; the same formula gives 44 for `bcrt1q`,
        /// which matches what the node hands the regtests). Several regtests
        /// pay one today and pass. So the cap is not a latent break at this
        /// seam — it is a live one, and importing it here would fail passing
        /// tests inside a refactor that is supposed to change no behaviour.
        ///
        /// Widening `AddressId` and the 27 `varchar(62)` columns behind it is
        /// its own change with its own migration and rollback story. It does
        /// not ride along in a payout-identity diff.
        address: String,
    },
    /// An extended public key the pool derives a fresh script from per block.
    ///
    /// **Unconstructible** — see [`RotatingDescriptor`].
    Rotating {
        /// The descriptor to derive from.
        descriptor: RotatingDescriptor,
        /// The height-invariant ledger key. A rotating miner's script changes
        /// every block; this does not, or PPLNS carry-forward silently stops
        /// working (`pplns_balance` is `PRIMARY KEY (address)`).
        payout_id: AddressId,
    },
}

impl PayoutIdentity {
    /// A literal address, normalized ([`normalize_btc_address`]) but **not**
    /// shape-validated — matching what the coinbase seam accepts today.
    ///
    /// Callers that want the shape check keep doing what they do now: the
    /// authorize/channel-open probes run `address_to_script`, which is a
    /// stronger check than `AddressId` anyway (it parses the address and
    /// verifies the network).
    pub fn static_address(address: impl AsRef<str>) -> Self {
        PayoutIdentity::Static {
            address: normalize_btc_address(address.as_ref()),
        }
    }

    /// A literal address taken verbatim, with no normalization.
    ///
    /// For the paths that already normalized (or that deliberately preserve a
    /// caller's bytes, like the pool's own fee addresses read from config).
    /// Prefer [`static_address`](Self::static_address) when the string came off
    /// the wire.
    pub fn static_address_verbatim(address: impl Into<String>) -> Self {
        PayoutIdentity::Static {
            address: address.into(),
        }
    }

    /// The ledger key and the mode key — **height-invariant**.
    ///
    /// This is what goes in `pplns_balance.address`,
    /// `pplns_payout_history.address`, `worker_shares_entity.address`, and
    /// every mode lookup. It is NOT what goes in a coinbase output: for
    /// `Rotating` those are different values, which is the entire reason this
    /// type is a sum type.
    ///
    /// Returns `&str` rather than `&AddressId` because the `Static` arm cannot
    /// hold an `AddressId` — see [`PayoutIdentity::Static::address`]. Narrowing
    /// it belongs with the 62-char column widening, not here.
    pub fn payout_id(&self) -> &str {
        match self {
            PayoutIdentity::Static { address } => address,
            PayoutIdentity::Rotating { payout_id, .. } => payout_id.as_str(),
        }
    }

    /// Is this identity paid a different script per block?
    ///
    /// A `match`, so it cannot answer a stale question if a third variant is
    /// added. Read-only classification — it is NOT a substitute for matching
    /// where behaviour differs, and a caller that branches on it to *choose a
    /// script* has reintroduced exactly the `is_some()` defect this type
    /// exists to prevent. Intended for logging and metrics.
    pub fn rotates(&self) -> bool {
        match self {
            PayoutIdentity::Static { .. } => false,
            PayoutIdentity::Rotating { .. } => true,
        }
    }
}

/// Split a wire identity into its payout part and its worker part.
///
/// **The one implementation of this rule.** It existed four times — once per
/// protocol path that reads a `user_identity`:
///
/// | Site | Form |
/// |---|---|
/// | `bp-stratum-v1/src/frame.rs` | `split_once('.')`, worker defaults to `"worker"` |
/// | `bp-stratum-v2/src/mining/client.rs` | `find('.')`, worker defaults to `"default"` |
/// | `bp-stratum-v2/src/jdp/client.rs` | `find('.')`, worker discarded |
/// | `bp-stratum-v2/src/extensions.rs` | `find('.')` for the Worker-ID TLV, worker attribution only |
///
/// All four agreed on the rule and each said so in its own words. The JDP one
/// records the money consequence of getting it wrong: *"otherwise the trailing
/// `.worker` makes `address_to_script` reject the address at coinbase-output
/// encode time, collapsing the pool payout to an empty output set
/// (`coinbase_tx_outputs = 0x00`)."*
///
/// Split on the **first** dot; the worker name keeps any further dots.
///
/// The worker part is `Option`, not a defaulted `&str`, because *no dot* and *an
/// empty worker after a dot* are different inputs and the four sites do not
/// treat them the same way — SV1 defaults `"addr"` to worker `"worker"` but
/// leaves `"addr."` as the empty string. Returning `""` for both would silently
/// change SV1's behaviour. Nothing is trimmed: the address part is trimmed
/// downstream by [`normalize_btc_address`], and trimming the worker part here
/// would change what SV2 reports as a worker name.
///
/// This is the split ONLY. It does not decide whether the payout part is an
/// address or an xpub — that is [`parse_payout_identity`]'s job, so the four
/// sites do not each have to learn a new grammar.
pub fn split_identity_and_worker(raw: &str) -> (&str, Option<&str>) {
    match raw.find('.') {
        Some(idx) => (&raw[..idx], Some(&raw[idx + 1..])),
        None => (raw, None),
    }
}

/// Why a wire identity could not become a [`PayoutIdentity`].
///
/// Deliberately narrow, and deliberately carrying no borrowed text from a
/// parser. When descriptor intake lands, the error text is a hazard rather than
/// a convenience: `Descriptor::from_str` on a bare `xprv` returns a 131-char
/// error that **contains the whole private key**. The local idiom next door
/// (`Address::from_str(a).map_err(|e| AddressError::Parse(e.to_string()))`) is
/// correct for a public address and would write a spendable key into the pool's
/// logs if copied here.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum IdentityParseError {
    /// Nothing before the first dot, or nothing at all.
    #[error("identity has no payout part")]
    Empty,
    /// The payout part is not a usable address shape.
    #[error("identity payout part is not a valid address: {0}")]
    InvalidAddress(InvalidAddressError),
}

/// Parse a wire identity (`<payout>` or `<payout>.<worker>`) into a
/// [`PayoutIdentity`] plus the raw worker part.
///
/// **Only ever returns `Static`.** The grammar for a rotating identity is not
/// implemented here yet, on purpose: the point of landing this function now is
/// that when it is, it is one place rather than four, and the four call sites
/// need no further change.
///
/// Shape-validates through [`AddressId`], which is what the sites that call
/// this already do. It does NOT parse the address or check the network — that is
/// `address_to_script`, and the two sites that run it keep running it, so their
/// rejection behaviour is unchanged.
pub fn parse_payout_identity(
    raw: &str,
) -> Result<(PayoutIdentity, Option<&str>), IdentityParseError> {
    let (payout_part, worker) = split_identity_and_worker(raw);
    let normalized = normalize_btc_address(payout_part);
    if normalized.is_empty() {
        return Err(IdentityParseError::Empty);
    }
    // Shape-check, then keep the normalized string rather than the `AddressId`:
    // `Static` holds an unconstrained `String` so a 64-char regtest P2TR still
    // works. The check is what the sites do today; the storage is what the
    // coinbase seam does today.
    AddressId::new(normalized.clone()).map_err(IdentityParseError::InvalidAddress)?;
    Ok((PayoutIdentity::static_address_verbatim(normalized), worker))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- split_identity_and_worker ----

    #[test]
    fn split_takes_the_first_dot_and_the_worker_keeps_the_rest() {
        assert_eq!(
            split_identity_and_worker("bc1qfoo.rig1.board2"),
            ("bc1qfoo", Some("rig1.board2"))
        );
    }

    #[test]
    fn split_distinguishes_no_dot_from_an_empty_worker() {
        // The distinction SV1 depends on: no dot defaults the worker to
        // "worker", a trailing dot leaves it empty. Collapsing both to `""`
        // would change SV1's authorize behaviour.
        assert_eq!(split_identity_and_worker("bc1qfoo"), ("bc1qfoo", None));
        assert_eq!(split_identity_and_worker("bc1qfoo."), ("bc1qfoo", Some("")));
    }

    #[test]
    fn split_reports_an_empty_payout_part_rather_than_guessing() {
        assert_eq!(split_identity_and_worker(".rig1"), ("", Some("rig1")));
        assert_eq!(split_identity_and_worker(""), ("", None));
    }

    #[test]
    fn split_does_not_trim() {
        // The address part is trimmed downstream by `normalize_btc_address`;
        // trimming the worker here would change what SV2 reports.
        assert_eq!(
            split_identity_and_worker("  bc1qfoo  .  rig1  "),
            ("  bc1qfoo  ", Some("  rig1  "))
        );
    }

    // ---- parse_payout_identity ----

    #[test]
    fn parse_yields_a_static_identity_and_normalizes_the_address() {
        let (identity, worker) =
            parse_payout_identity("BC1QW508D6QEJXTDG4Y5R3ZARVARY0C5XW7KV8F3T4.rig1").unwrap();
        assert_eq!(
            identity,
            PayoutIdentity::Static {
                address: "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4".to_string()
            }
        );
        assert_eq!(worker, Some("rig1"));
    }

    #[test]
    fn parse_preserves_base58_case() {
        let (identity, _) = parse_payout_identity("1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2.w").unwrap();
        assert_eq!(identity.payout_id(), "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2");
    }

    #[test]
    fn parse_rejects_an_empty_payout_part() {
        assert_eq!(parse_payout_identity(""), Err(IdentityParseError::Empty));
        assert_eq!(
            parse_payout_identity(".rig1"),
            Err(IdentityParseError::Empty)
        );
        assert_eq!(
            parse_payout_identity("   .rig1"),
            Err(IdentityParseError::Empty)
        );
    }

    #[test]
    fn parse_rejects_an_over_long_payout_part() {
        // A bare xpub is 111 chars. This is the rejection an xpub meets today,
        // at one of four differently-shaped layers; intake replaces it.
        let xpub = "x".repeat(111);
        assert_eq!(
            parse_payout_identity(&xpub),
            Err(IdentityParseError::InvalidAddress(
                InvalidAddressError::TooLong(111)
            ))
        );
    }

    // ---- PayoutIdentity ----

    #[test]
    fn static_address_normalizes_but_static_address_verbatim_does_not() {
        assert_eq!(
            PayoutIdentity::static_address("  BC1QFOO  ").payout_id(),
            "bc1qfoo"
        );
        assert_eq!(
            PayoutIdentity::static_address_verbatim("  BC1QFOO  ").payout_id(),
            "  BC1QFOO  "
        );
    }

    /// The 62-char cap is a **live** break at this seam, not a latent one.
    ///
    /// `Static` holds a `String` specifically so this works. If someone
    /// narrows it to `AddressId`, this test fails and points at the reason
    /// rather than at four regtests that suddenly stop paying.
    #[test]
    fn a_regtest_p2tr_address_is_64_chars_and_static_carries_it() {
        // A real bech32m P2TR on regtest: `bcrt1p` + 52 data + 6 checksum.
        let regtest_p2tr = "bcrt1p5d7rjq7g6rdk2yhzks9smlaqtedr4dekq08ge8ztwac72sfr9rusgm2jyk";
        assert_eq!(
            regtest_p2tr.len(),
            64,
            "regtest P2TR must be 64 chars for this test to mean anything"
        );

        // The cap this seam must NOT inherit.
        assert_eq!(
            AddressId::new(regtest_p2tr),
            Err(InvalidAddressError::TooLong(64)),
            "negative control: AddressId rejects it, so carrying one here would break payouts"
        );

        // ... and `Static` carries it unharmed.
        let identity = PayoutIdentity::static_address(regtest_p2tr);
        assert_eq!(identity.payout_id(), regtest_p2tr);
    }

    /// `parse_payout_identity` DOES apply the cap, because the sites it
    /// replaces do. Pinning it so the difference from the seam is deliberate
    /// and visible rather than an accident of which function was called.
    #[test]
    fn parse_applies_the_cap_that_the_coinbase_seam_does_not() {
        let regtest_p2tr = "bcrt1p5d7rjq7g6rdk2yhzks9smlaqtedr4dekq08ge8ztwac72sfr9rusgm2jyk";
        assert_eq!(
            parse_payout_identity(regtest_p2tr),
            Err(IdentityParseError::InvalidAddress(
                InvalidAddressError::TooLong(64)
            )),
            "intake keeps today's cap; only the coinbase seam is uncapped"
        );
    }
}
