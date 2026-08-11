// SPDX-License-Identifier: AGPL-3.0-or-later

//! The `payout_id → PayoutIdentity` directory, and the pool's one
//! [`RotatingIntake`].
//!
//! # The gap this closes
//!
//! Rotation splits one string into two values — a height-invariant `payout_id`
//! for the ledger and a per-height script for the coinbase (see
//! [`bp_common::PayoutIdentity`]). The protocol layer resolves the first at
//! authorize/channel-open and then forgets everything else: `resolve_payouts`
//! takes a `&str` (SV1) or an `&AddressId` (SV2), and nothing else. So the
//! descriptor needs somewhere to live between the two.
//!
//! ## Why a directory and not a wider `resolve_payouts`
//!
//! Threading the identity down the connection would serve Solo and **nothing
//! else**, because the shapes of the modes differ in the one way that matters:
//!
//! | Mode | Whose identities the coinbase pays |
//! |---|---|
//! | Solo | the connecting miner's |
//! | PPLNS | **every miner in the window** — hundreds, almost none connected to this session |
//! | Group-Solo | **every member of the group** |
//! | Blockparty | the admin's members', which are operator-entered and never rotate |
//!
//! PPLNS's distribution is a list of `AddressId`s out of Redis/Postgres. There is
//! no connection to have carried a descriptor for the other 499 miners in the
//! window, so a per-connection channel cannot answer PPLNS's question at all —
//! it would have to be joined by `payout_id` against something anyway. Building
//! the per-connection thing for Solo now and the lookup for PPLNS in Phase 4
//! would be two mechanisms for one question, which is `CLAUDE.md`'s opening
//! failure mode with the ink still wet.
//!
//! Phase 2 already staged this shape: `bp_db::find_rotating_identities` exists
//! and its doc says *"for the payout path to resolve descriptors in bulk"*. And
//! the in-repo precedent is [`crate::engines::BlitzpoolModeGate`] — a
//! payout-id-keyed, refcounted, in-memory map populated at authorize and read
//! synchronously by the payout resolver. This is the same pattern for the
//! adjacent fact.
//!
//! ## Why it is refcounted the same way the mode gate is
//!
//! Two connections can present the same xpub (a miner with two rigs). One
//! disconnecting must not remove the descriptor the other is still being paid
//! through — that is verbatim the reason `BlitzpoolModeGate` refcounts, and
//! getting it wrong here is worse than getting it wrong there: a missing mode
//! falls back to Solo, while a missing descriptor means a rotating miner's
//! entry cannot be built at all.
//!
//! ## What is deliberately NOT in here
//!
//! Static identities. The directory holds rotating ones only, and a lookup miss
//! is not an error — it is the answer *"this payout_id is a literal address, pay
//! it verbatim"*, which is what the pool has always done. Storing every static
//! address here too would make the map the size of the miner base to answer a
//! question the `payout_id` already answers by being an address.
//!
//! That asymmetry is the one thing in this module that could rot into the
//! `is_some()` defect `CLAUDE.md` names, so it is confined to
//! [`PayoutIdentityDirectory::identity_for`], which returns a `PayoutIdentity`
//! and never an `Option` — callers get an identity to `match` on, not a
//! "was it found?" boolean to branch on.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bp_common::{IdentityRefused, PayoutIdentity, RotatingIntake};
use bp_payout_descriptor::{intake_wire_identity, IntakeError};
use tracing::{debug, info, warn};

/// In-memory `payout_id → (rotating identity, refcount)`.
///
/// Cheap to read: the lock is held across a single `HashMap::get` plus a clone
/// of an `Arc`-backed identity, and it is read once per
/// `(template-broadcast × connection)` — the same cadence as the mode gate,
/// ~30 s per connection.
pub(crate) struct PayoutIdentityDirectory {
    inner: Mutex<HashMap<String, RefcountedIdentity>>,
}

#[derive(Debug)]
struct RefcountedIdentity {
    identity: PayoutIdentity,
    count: usize,
}

impl PayoutIdentityDirectory {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Publish a rotating identity, or bump its refcount if it is already here.
    ///
    /// Called from intake — the one moment an identity comes into existence —
    /// rather than from a session-registration hook, because intake is the only
    /// place that holds the descriptor. By the time `register_session` runs, the
    /// wire string has already been replaced by the `payout_id`.
    fn publish(&self, identity: PayoutIdentity) {
        let key = identity.payout_id().to_string();
        let mut guard = self.inner.lock().expect("payout-identity mutex poisoned");
        guard
            .entry(key)
            .and_modify(|e| e.count += 1)
            .or_insert(RefcountedIdentity { identity, count: 1 });
    }

    /// Drop one reference; remove the entry at zero.
    ///
    /// A `payout_id` that was never published is a no-op — the common case, since
    /// every static miner's disconnect reaches here too.
    pub(crate) fn release(&self, payout_id: &str) {
        let mut guard = self.inner.lock().expect("payout-identity mutex poisoned");
        if let Some(entry) = guard.get_mut(payout_id) {
            if entry.count <= 1 {
                guard.remove(payout_id);
            } else {
                entry.count -= 1;
            }
        }
    }

    /// **The payout path's question: how do I pay this `payout_id`?**
    ///
    /// Always an answer, never an `Option`. A hit is the rotating identity a
    /// connected miner presented; a miss means the id is a literal address, which
    /// is what every identity in this pool was before this feature. The caller
    /// gets a `PayoutIdentity` to `match` on either way, so there is no
    /// "found it?" branch for a mode to fall out of.
    ///
    /// Note what a miss for a *rotating* miner would mean: a `payout_id`
    /// (`xpb…` + a base58 hash) returned as a `Static` address, which
    /// `address_to_script` then refuses — so the coinbase fails to build rather
    /// than paying anything wrong. That is the correct failure direction, and it
    /// is why this can be a plain map lookup with no fallback logic. It should
    /// not happen: the entry is published before authorize returns and released
    /// only at disconnect.
    pub(crate) fn identity_for(&self, payout_id: &str) -> PayoutIdentity {
        let guard = self.inner.lock().expect("payout-identity mutex poisoned");
        match guard.get(payout_id) {
            Some(e) => e.identity.clone(),
            None => PayoutIdentity::static_address_verbatim(payout_id),
        }
    }

    /// How many rotating identities are currently published. Diagnostics and
    /// tests only.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner
            .lock()
            .expect("payout-identity mutex poisoned")
            .len()
    }
}

/// The pool's [`RotatingIntake`] — `bp_payout_descriptor`'s intake plus the
/// operator flag plus publication to the directory.
///
/// One implementation, shared by SV1's `mining.authorize` and SV2's
/// `OpenMiningChannel`. Both protocols get the same verdict on the same input
/// because there is only one thing to ask.
pub(crate) struct PoolRotatingIntake {
    directory: Arc<PayoutIdentityDirectory>,
    /// `[payout_identity] allow_rotating`, default `false`. Held here rather
    /// than read per call so the flag is fixed for the process lifetime — an
    /// operator turning it off does not orphan the descriptors of miners already
    /// connected under it.
    allow_rotating: bool,
}

impl PoolRotatingIntake {
    pub(crate) fn new(directory: Arc<PayoutIdentityDirectory>, allow_rotating: bool) -> Self {
        if allow_rotating {
            info!(
                descriptor_template = bp_payout_descriptor::POOL_DESCRIPTOR_TEMPLATE,
                derivation_path = bp_payout_descriptor::POOL_DERIVATION_PATH_BIP32,
                "payout-identity: rotating identities ENABLED"
            );
        }
        Self {
            directory,
            allow_rotating,
        }
    }
}

impl RotatingIntake for PoolRotatingIntake {
    fn intake(&self, payout_part: &str) -> Result<Option<PayoutIdentity>, IdentityRefused> {
        let rotating = match intake_wire_identity(payout_part, self.allow_rotating) {
            Ok(None) => return Ok(None),
            Ok(Some(r)) => r,
            // **The operator-facing line is written here and nowhere else.**
            // `IdentityRefused` carries no detail on purpose (a descriptor
            // parser's error text can contain a private key), so this is the
            // only place that knows which refusal it was — and `IntakeError`'s
            // `Display` is a fixed string per variant, never borrowed input.
            //
            // Deliberately not logging `payout_part`: it is an extended key. For
            // `FeatureDisabled` that is a public key and harmless, but
            // `NotAnXpub` covers the `xprv` case, and one call site that logs
            // the input is how the credential rule gets undone. The variant is
            // enough to support a miner.
            Err(e) => {
                match e {
                    IntakeError::FeatureDisabled => warn!(
                        "payout-identity: a miner presented an extended key but \
                         [payout_identity] allow_rotating is false; refusing the connection"
                    ),
                    other => warn!(
                        refusal = %other,
                        "payout-identity: refusing a rotating identity at intake"
                    ),
                }
                return Err(IdentityRefused);
            }
        };

        let identity = rotating.into_payout_identity();
        self.directory.publish(identity.clone());
        debug!(
            payout_id = identity.payout_id(),
            "payout-identity: rotating identity admitted"
        );
        Ok(Some(identity))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The BIP-32 test-vector master public keys, vectors 1 and 2. Real keys
    /// with real checksums — a made-up one fails `Xpub::from_str` and every
    /// "admitted" assertion below would pass vacuously against `Err`.
    ///
    /// Two *distinct* keys, so the two-miners tests are not accidentally
    /// testing one.
    const XPUB_A: &str = "xpub661MyMwAqRbcFtXgS5sYJABqqG9YLmC4Q1Rdap9gSE8NqtwybGhePY2gZ29ESFjqJoCu1Rupje8YtGqsefD265TMg7usUDFdp6W1EGMcet8";
    const XPUB_B: &str = "xpub661MyMwAqRbcFW31YEwpkMuc5THy2PSt5bDMsktWQcFF8syAmRUapSCGu8ED9W6oDMSgv6Zz8idoc4a6mr8BDzTJY47LJhkJ8UB7WEGuduB";

    fn directory_and_intake(allow: bool) -> (Arc<PayoutIdentityDirectory>, PoolRotatingIntake) {
        let dir = Arc::new(PayoutIdentityDirectory::new());
        (dir.clone(), PoolRotatingIntake::new(dir, allow))
    }

    /// A static address is not this module's business, and the directory must
    /// stay empty for it — the asymmetry the module doc describes.
    #[test]
    fn a_static_address_passes_through_and_is_not_published() {
        let (dir, intake) = directory_and_intake(true);
        let out = intake.intake("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4");
        assert_eq!(out, Ok(None), "a static address is not an intake attempt");
        assert_eq!(dir.len(), 0, "nothing static belongs in the directory");
    }

    /// The whole Phase 3 mechanism in one test: intake admits the key, the
    /// directory holds it, and `identity_for` gives back something that
    /// **rotates** — not the `payout_id` as an address.
    #[test]
    fn an_admitted_xpub_is_resolvable_from_the_directory_as_a_rotating_identity() {
        let (dir, intake) = directory_and_intake(true);
        let identity = intake
            .intake(XPUB_A)
            .expect("intake must not refuse a valid xpub")
            .expect("a valid xpub is an intake attempt");
        assert!(identity.rotates());
        let payout_id = identity.payout_id().to_string();

        let resolved = dir.identity_for(&payout_id);
        assert_eq!(resolved, identity, "same identity back out");
        assert!(
            resolved.script_at(842_000).is_some(),
            "a rotating identity resolved from the directory must derive a script; \
             a `Static` fallback here is the failure this test exists to catch"
        );
    }

    /// The negative control for the test above, in the same file: an id the
    /// directory has never seen comes back `Static`, so the assertion above is
    /// not passing on a default.
    #[test]
    fn an_unknown_payout_id_comes_back_static() {
        let dir = PayoutIdentityDirectory::new();
        let resolved = dir.identity_for("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4");
        assert!(!resolved.rotates());
        assert!(
            resolved.script_at(842_000).is_none(),
            "a static identity has no derivation"
        );
    }

    /// Two rigs, one xpub, one disconnect. The surviving connection must still
    /// be payable — the reason this is refcounted rather than a plain insert.
    #[test]
    fn one_of_two_connections_disconnecting_leaves_the_descriptor_in_place() {
        let (dir, intake) = directory_and_intake(true);
        let first = intake.intake(XPUB_A).unwrap().unwrap();
        let second = intake.intake(XPUB_A).unwrap().unwrap();
        assert_eq!(first, second, "one xpub is one identity");
        assert_eq!(dir.len(), 1, "and one directory entry");
        let payout_id = first.payout_id().to_string();

        dir.release(&payout_id);
        assert!(
            dir.identity_for(&payout_id).rotates(),
            "the second rig is still connected and still has to be paid"
        );

        dir.release(&payout_id);
        assert!(
            !dir.identity_for(&payout_id).rotates(),
            "with nobody connected the entry is gone"
        );
        assert_eq!(dir.len(), 0);
    }

    /// Releasing an id that was never published is the common case — every
    /// static miner's disconnect — and must not disturb anything.
    #[test]
    fn releasing_an_unpublished_id_is_a_no_op() {
        let (dir, intake) = directory_and_intake(true);
        let identity = intake.intake(XPUB_A).unwrap().unwrap();
        dir.release("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4");
        assert!(dir.identity_for(identity.payout_id()).rotates());
        assert_eq!(dir.len(), 1);
    }

    /// Two xpubs are two identities and two entries — the directory keys on the
    /// `payout_id`, and a collision here would pay one miner another's money.
    #[test]
    fn two_xpubs_are_two_entries() {
        let (dir, intake) = directory_and_intake(true);
        let a = intake.intake(XPUB_A).unwrap().unwrap();
        let b = intake.intake(XPUB_B).unwrap().unwrap();
        assert_ne!(a.payout_id(), b.payout_id());
        assert_eq!(dir.len(), 2);
        assert_eq!(dir.identity_for(a.payout_id()), a);
        assert_eq!(dir.identity_for(b.payout_id()), b);
    }

    /// Flag off: refused, and **not** passed through to the static path. The
    /// distinction is the whole reason `intake` returns `Err` rather than
    /// `Ok(None)` here — a fall-through would refuse this miner with a message
    /// about address length.
    #[test]
    fn the_flag_is_what_admits_an_xpub_and_a_refusal_publishes_nothing() {
        let (dir, intake) = directory_and_intake(false);
        assert_eq!(intake.intake(XPUB_A), Err(IdentityRefused));
        assert_eq!(dir.len(), 0, "a refused identity must not be published");
    }
}
