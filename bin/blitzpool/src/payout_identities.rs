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
//!
//! ## The other half: settlement
//!
//! The directory answers *"how do I pay this miner now"*, on the template path,
//! for a miner that is connected. [`PoolPaidAddresses`] answers the settlement
//! question — *"which address did this ledger key get paid at height H"* — ~100
//! blocks later, when the miner is usually gone and often the process with it.
//! It lives next to the directory because it reads it first, and it falls back to
//! `miner_identity`, which is why [`PoolRotatingIntake`] writes that row.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bitcoin::Network;
use bp_coinbase_snapshot::{PaidAddressResolver, PaidAtHeight, PaidAtHeightError};
use bp_common::{IdentityRefused, PayoutIdentity, RotatingIntake};
use bp_payout_descriptor::{intake_wire_identity, IntakeError};
use sqlx::PgPool;
use tracing::{debug, error, info, warn};

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

    /// Publish a rotating identity without going through intake, for tests in
    /// other modules that need a directory which answers `Rotating` — the
    /// Blockparty refusal in [`crate::payout_resolver`] is one.
    ///
    /// [`Self::publish`] stays private so that at runtime intake remains the only
    /// way an identity comes into existence.
    #[cfg(test)]
    pub(crate) fn publish_for_test(&self, identity: PayoutIdentity) {
        self.publish(identity);
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
    /// Where an admitted descriptor is written so **settlement** can still find
    /// it. See [`Self::persist`].
    pool: PgPool,
}

impl PoolRotatingIntake {
    pub(crate) fn new(
        directory: Arc<PayoutIdentityDirectory>,
        allow_rotating: bool,
        pool: PgPool,
    ) -> Self {
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
            pool,
        }
    }

    /// Persist an admitted identity so settlement can rehydrate it.
    ///
    /// **Not an optimisation — the only correct source at settlement time.** The
    /// directory is refcounted to *connection* lifetime and a found block is
    /// booked ~100 blocks later, so by then the miner has usually disconnected and
    /// the process may have restarted. `miner_identity` is the one place the
    /// descriptor survives that, and [`PoolPaidAddresses`] reads it there.
    ///
    /// Fire-and-forget because [`RotatingIntake::intake`] is synchronous: it runs
    /// on the authorize / channel-open path, which must not wait on Postgres to
    /// answer a miner. A lost write is not silent — the upsert is idempotent so
    /// every reconnect retries it, and a settlement that finds no row refuses the
    /// block by name (`PaidAtHeightError::Unresolvable`, non-terminal) rather than
    /// booking a rotating miner's claim twice.
    fn persist(&self, identity: &PayoutIdentity) {
        // Exhaustive on purpose. A third identity kind has to decide here whether
        // it leaves anything behind for settlement, rather than falling into a
        // silent "no" — `CLAUDE.md`'s 2026-08-03 entry is what an `if` costs.
        let (payout_id, descriptor) = match identity {
            // Nothing to store: the ledger key IS the address, and the resolver
            // maps it to itself without consulting a row.
            PayoutIdentity::Static { .. } => return,
            PayoutIdentity::Rotating {
                descriptor,
                payout_id,
            } => (
                payout_id.as_str().to_string(),
                descriptor.canonical_descriptor().to_string(),
            ),
        };
        let pool = self.pool.clone();
        tokio::spawn(async move {
            let now_ms = chrono::Utc::now().timestamp_millis();
            // The line carries the DbError and the `payout_id` (a published hash)
            // — never `descriptor`, which is in scope here and is a
            // wallet-watching capability over every address the pool will pay
            // this miner.
            match bp_db::upsert_rotating_identity(&pool, &payout_id, &descriptor, now_ms).await {
                Ok(()) => debug!(
                    payout_id,
                    "payout-identity: descriptor persisted for settlement"
                ),
                Err(err) => warn!(
                    %err,
                    payout_id,
                    "payout-identity: persisting the descriptor failed; settlement cannot \
                     rehydrate this miner until a reconnect re-writes the row"
                ),
            }
        });
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
        // Both halves, together: the directory serves the coinbase while this
        // connection lives, the row serves settlement after it is gone.
        self.persist(&identity);
        debug!(
            payout_id = identity.payout_id(),
            "payout-identity: rotating identity admitted"
        );
        Ok(Some(identity))
    }
}

/// **The pool's [`PaidAddressResolver`]: which address a ledger key was paid
/// under, at one block's height.**
///
/// Settlement is `claim − actually_paid` per ledger key, and `actually_paid` is
/// keyed on the address the coinbase output rendered to. A rotating miner's
/// ledger key is a `payout_id` — a hash that can never equal a rendered address —
/// so without this translation it settles at `claim − 0` (a full-claim credit for
/// money already paid) *and* the address the coinbase did pay falls into the
/// "outside the distribution" arm and mints a second row. One block, two wrong
/// rows, opposite directions. See [`bp_coinbase_snapshot::paid_at_height`].
///
/// ## Why it is built here
///
/// This is the only layer holding all of the inputs at once: the network
/// (`[network]`), the identity sources (the directory above and Postgres), and —
/// through the engine that calls it — the height. `bp-coinbase-snapshot` owns the
/// *type* because both engines need it and it already renders `paid_by_address`
/// through the same `Address::from_script`; this owns the *lookup*.
///
/// ## Directory, then Postgres, then refuse
///
/// 1. The directory, for a miner still connected — free, and content-addressed so
///    it cannot disagree with the row.
/// 2. `miner_identity`, in **one** bulk read per settlement
///    (`find_rotating_identities`), for everyone else. The descriptor is
///    rehydrated through `bp_payout_descriptor::rehydrate_stored_identity`, which
///    re-hashes it and refuses a row that does not reproduce the `payout_id` it
///    was fetched under.
/// 3. A key that resolves to neither is an **error** — never
///    `static_address_verbatim(payout_id)`. That fallback is what
///    `PayoutIdentityDirectory::identity_for` returns on a miss, and it is exactly
///    right for the coinbase path (an unpayable hash fails to build a block) and
///    exactly wrong here (a `payout_id` compared against `paid_by_address` finds
///    nothing and books the two rows above).
///
/// The classifier between (1) and (2) is `bp_pplns::is_valid_payout_address` —
/// called, not re-spelled. It is already the predicate the distribution build
/// gates on, and a second copy of "which strings can be paid" is the drift
/// `CLAUDE.md` opens with.
pub(crate) struct PoolPaidAddresses {
    directory: Arc<PayoutIdentityDirectory>,
    pool: PgPool,
    /// Rendering is network-dependent (`bcrt1…` vs `bc1…`), and the same script
    /// on the wrong network renders to a string this block's coinbase cannot
    /// contain — every rotating miner then looks unpaid. Threaded, never assumed.
    network: Network,
}

impl PoolPaidAddresses {
    pub(crate) fn new(
        directory: Arc<PayoutIdentityDirectory>,
        pool: PgPool,
        network: Network,
    ) -> Self {
        Self {
            directory,
            pool,
            network,
        }
    }

    /// Rebuild the identities for keys the directory did not hold, from
    /// `miner_identity`.
    ///
    /// One query for the whole settlement, not one per entry — which is what
    /// `find_rotating_identities`' own doc comment (*"for the payout path to
    /// resolve descriptors in bulk"*) was written for.
    async fn rehydrate_from_the_store(
        &self,
        keys: &[&String],
    ) -> Result<Vec<PayoutIdentity>, PaidAtHeightError> {
        let rows = bp_db::find_rotating_identities(&self.pool)
            .await
            .map_err(|err| {
                // The cause is logged HERE and dropped from the returned error:
                // `IdentityLookupFailed` carries no payload because the rows it
                // failed to read hold descriptors. `%err` is the DbError, and no
                // descriptor is interpolated into this line.
                error!(
                    %err,
                    unresolved = keys.len(),
                    "payout-identity: miner_identity could not be read; this block's \
                     settlement will retry"
                );
                PaidAtHeightError::IdentityLookupFailed
            })?;
        // `kind`, not `descriptor.is_some()`: reading the presence of a per-kind
        // field as the kind is the defect `CLAUDE.md` names, and the query's own
        // `WHERE` is not visible from here.
        let stored: HashMap<&str, &str> = rows
            .iter()
            .filter(|row| row.kind == bp_db::KIND_ROTATING)
            .filter_map(|row| {
                row.descriptor
                    .as_deref()
                    .map(|d| (row.payout_id.as_str(), d))
            })
            .collect();

        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let Some(descriptor) = stored.get(key.as_str()) else {
                warn!(
                    payout_id = key.as_str(),
                    "payout-identity: no identity for a ledger key in this block's \
                     distribution — refusing to settle it (retryable: the row can still arrive)"
                );
                return Err(PaidAtHeightError::Unresolvable {
                    payout_id: (*key).clone(),
                });
            };
            // Re-hashes the descriptor and refuses a row that does not reproduce
            // this key. Content-addressing is only a property of the system if
            // something checks it.
            let payout = bp_payout_descriptor::rehydrate_stored_identity(key, descriptor).map_err(
                |refusal| {
                    // `IntakeError` is `Copy` and its `Display` is a fixed string
                    // per variant — it cannot carry the row's descriptor.
                    error!(
                        payout_id = key.as_str(),
                        %refusal,
                        "payout-identity: the stored identity for a ledger key in this block's \
                         distribution was refused; the ledger and the row disagree about which \
                         wallet this miner is"
                    );
                    PaidAtHeightError::StoredIdentityRejected {
                        payout_id: (*key).clone(),
                    }
                },
            )?;
            out.push(payout.into_payout_identity());
        }
        Ok(out)
    }
}

/// Manual, so a `{:?}` of an engine cannot walk into the directory's
/// descriptors. `RotatingDescriptor`'s own `Debug` redacts, so this is the second
/// layer rather than the only one — the trait requires `Debug`, and the cheapest
/// way to keep a capability out of a log line is to have nothing to print.
impl fmt::Debug for PoolPaidAddresses {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolPaidAddresses")
            .field("network", &self.network)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl PaidAddressResolver for PoolPaidAddresses {
    async fn paid_at_height(
        &self,
        ledger_keys: &[String],
        height: u32,
    ) -> Result<PaidAtHeight, PaidAtHeightError> {
        let mut identities: Vec<PayoutIdentity> = Vec::with_capacity(ledger_keys.len());
        let mut needing_the_store: Vec<&String> = Vec::new();

        for key in ledger_keys {
            let identity = self.directory.identity_for(key);
            match &identity {
                // A connected rotating miner. The directory cannot hand back the
                // wrong descriptor: `payout_id` is its hash.
                PayoutIdentity::Rotating { .. } => identities.push(identity),
                PayoutIdentity::Static { address } => {
                    if bp_pplns::is_valid_payout_address(address) {
                        // Genuinely a literal address — what every identity in
                        // this pool was before rotation existed.
                        identities.push(identity);
                    } else {
                        // The directory's miss branch, handing back a `payout_id`
                        // dressed as an address. Postgres decides, not this.
                        needing_the_store.push(key);
                    }
                }
            }
        }

        if !needing_the_store.is_empty() {
            identities.extend(self.rehydrate_from_the_store(&needing_the_store).await?);
        }

        PaidAtHeight::resolve(identities.iter(), self.network, height)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// The BIP-32 test-vector master public keys, vectors 1 and 2. Real keys
    /// with real checksums — a made-up one fails `Xpub::from_str` and every
    /// "admitted" assertion below would pass vacuously against `Err`.
    ///
    /// Two *distinct* keys, so the two-miners tests are not accidentally
    /// testing one.
    const XPUB_A: &str = "xpub661MyMwAqRbcFtXgS5sYJABqqG9YLmC4Q1Rdap9gSE8NqtwybGhePY2gZ29ESFjqJoCu1Rupje8YtGqsefD265TMg7usUDFdp6W1EGMcet8";
    const XPUB_B: &str = "xpub661MyMwAqRbcFW31YEwpkMuc5THy2PSt5bDMsktWQcFF8syAmRUapSCGu8ED9W6oDMSgv6Zz8idoc4a6mr8BDzTJY47LJhkJ8UB7WEGuduB";

    /// The regtest addresses `XPUB_A` / `XPUB_B` derive to at [`HEIGHT`], from
    /// `RotatingPayout::address_at` — the other rendering path, so a resolver
    /// asserted against these is not being checked against itself.
    const HEIGHT: u32 = 842_000;
    const ADDR_A_AT_HEIGHT: &str = "bcrt1qq6eq76f0xuvmc5h9gqp8a7g6795csn39rd6ae5";

    /// A real regtest address, because [`bp_pplns::is_valid_payout_address`] is
    /// what tells a literal address from the directory's miss branch, and it
    /// parses. `format!("{prefix}aaa")` would classify as a miss.
    const STATIC_ADDR: &str = "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080";

    /// Ledger-key shaped, and the hash of nothing: no descriptor produces it, so
    /// no test and no run can have stored it. `Unresolvable` is what it must be.
    const ABSENT_ID: &str = "xpbZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ";

    /// Also ledger-key shaped, and the key a *stored* descriptor is deliberately
    /// filed under wrongly — `xpbGde…` is `XPUB_B`'s real id with one character
    /// changed, so the row is exactly the plausible corruption.
    const MISMATCHED_ID: &str = "xpbGde6AcmBoPFVk7mbMgRXUshU81Q7vkEb95vk51jQu1vb";

    /// Every intake test needs a runtime: admitting an xpub `tokio::spawn`s the
    /// row settlement will later read (see [`PoolRotatingIntake::persist`]), and
    /// `spawn` outside a runtime panics.
    ///
    /// The pool it gets is never reachable, on purpose. These tests are about the
    /// directory half; a live pool would make them depend on Postgres for facts
    /// that have nothing to do with it, and would silently pass if the write
    /// stopped happening. That the write happens, and that settlement can read it
    /// back, is
    /// `an_admitted_identity_is_readable_by_settlement_after_the_miner_disconnects`.
    fn directory_and_intake(allow: bool) -> (Arc<PayoutIdentityDirectory>, PoolRotatingIntake) {
        let dir = Arc::new(PayoutIdentityDirectory::new());
        (
            dir.clone(),
            PoolRotatingIntake::new(dir, allow, unreachable_pool()),
        )
    }

    /// Port 1 is privileged and unbound. `connect_lazy` does not dial, so this
    /// cannot fail here; anything that actually queries fails fast — which is how
    /// the resolver tests below prove they never queried.
    ///
    /// The short `acquire_timeout` is what makes "fast" true: sqlx retries a
    /// refused connection until the timeout, and the 30 s default turned one test
    /// into a 30 s test.
    fn unreachable_pool() -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(500))
            .connect_lazy("postgres://bp-test:bp-test@127.0.0.1:1/bp_never")
            .expect("a lazy pool does not connect, so it cannot fail to")
    }

    /// A static address is not this module's business, and the directory must
    /// stay empty for it — the asymmetry the module doc describes.
    #[tokio::test]
    async fn a_static_address_passes_through_and_is_not_published() {
        let (dir, intake) = directory_and_intake(true);
        let out = intake.intake("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4");
        assert_eq!(out, Ok(None), "a static address is not an intake attempt");
        assert_eq!(dir.len(), 0, "nothing static belongs in the directory");
    }

    /// The whole Phase 3 mechanism in one test: intake admits the key, the
    /// directory holds it, and `identity_for` gives back something that
    /// **rotates** — not the `payout_id` as an address.
    #[tokio::test]
    async fn an_admitted_xpub_is_resolvable_from_the_directory_as_a_rotating_identity() {
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
    #[tokio::test]
    async fn one_of_two_connections_disconnecting_leaves_the_descriptor_in_place() {
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
    #[tokio::test]
    async fn releasing_an_unpublished_id_is_a_no_op() {
        let (dir, intake) = directory_and_intake(true);
        let identity = intake.intake(XPUB_A).unwrap().unwrap();
        dir.release("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4");
        assert!(dir.identity_for(identity.payout_id()).rotates());
        assert_eq!(dir.len(), 1);
    }

    /// Two xpubs are two identities and two entries — the directory keys on the
    /// `payout_id`, and a collision here would pay one miner another's money.
    #[tokio::test]
    async fn two_xpubs_are_two_entries() {
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
    #[tokio::test]
    async fn the_flag_is_what_admits_an_xpub_and_a_refusal_publishes_nothing() {
        let (dir, intake) = directory_and_intake(false);
        assert_eq!(intake.intake(XPUB_A), Err(IdentityRefused));
        assert_eq!(dir.len(), 0, "a refused identity must not be published");
    }

    // ── Settlement attribution (plan Phase 4a) ─────────────────────────────

    /// The cheap path, and the one that runs when a block is found while the
    /// miner is still connected. The pool here is **unreachable**: if this
    /// resolver went to Postgres for a key the directory already covers, the
    /// call would fail instead of answering — so the assertion is also the
    /// proof that a settlement costs zero queries when nobody has left.
    #[tokio::test]
    async fn a_connected_rotating_miner_is_attributed_from_the_directory_without_a_query() {
        let (dir, intake) = directory_and_intake(true);
        let payout_id = intake
            .intake(XPUB_A)
            .unwrap()
            .unwrap()
            .payout_id()
            .to_string();

        let resolver = PoolPaidAddresses::new(dir, unreachable_pool(), Network::Regtest);
        let paid = resolver
            .paid_at_height(std::slice::from_ref(&payout_id), HEIGHT)
            .await
            .expect("a published identity needs no store");

        assert_eq!(
            paid.paid_address(&payout_id),
            Some(ADDR_A_AT_HEIGHT),
            "the address the coinbase paid at this height is the one the ledger \
             key must be credited under"
        );
        assert_eq!(paid.height(), HEIGHT);
    }

    /// Every miner in this pool before rotation existed, and most after: the
    /// ledger key **is** the address. No directory entry, no row, no query — and
    /// specifically not the `Unresolvable` a "not in the directory ⇒ look it up"
    /// resolver would produce for it.
    #[tokio::test]
    async fn a_literal_address_is_attributed_to_itself_without_a_query() {
        let resolver = PoolPaidAddresses::new(
            Arc::new(PayoutIdentityDirectory::new()),
            unreachable_pool(),
            Network::Regtest,
        );
        let paid = resolver
            .paid_at_height(&[STATIC_ADDR.to_string()], HEIGHT)
            .await
            .expect("a literal address is its own attribution");
        assert_eq!(paid.paid_address(STATIC_ADDR), Some(STATIC_ADDR));
    }

    /// Postgres unreachable, with a key only Postgres could answer.
    ///
    /// The distinction this pins is worth money: a *retryable* failure leaves the
    /// block pending and settles it on a later tick, while a terminal one parks it
    /// in `unbookable` and a human has to go and get it. A database that blinked
    /// must not do the second. *Mutation:* return `Unresolvable` here, or make
    /// `IdentityLookupFailed` terminal in `PaidAtHeightError::is_terminal`, and
    /// this fails.
    #[tokio::test]
    async fn a_store_that_cannot_be_read_is_retryable_and_leaks_nothing() {
        let resolver = PoolPaidAddresses::new(
            Arc::new(PayoutIdentityDirectory::new()),
            unreachable_pool(),
            Network::Regtest,
        );
        let err = resolver
            .paid_at_height(&[ABSENT_ID.to_string()], HEIGHT)
            .await
            .expect_err("an unresolvable key must not be attributed to itself");

        assert_eq!(err, PaidAtHeightError::IdentityLookupFailed);
        assert!(
            !err.is_terminal(),
            "a block must not be parked in unbookable because Postgres blinked"
        );
        let text = err.to_string();
        assert!(
            !text.contains("127.0.0.1") && !text.contains("bp-test"),
            "the sqlx cause is logged locally and dropped from the error; a \
             connection string carries credentials: {text}"
        );
    }

    /// Two refusals out of the real store, read together because the pair is the
    /// decision: a key with **no** row is retryable (the row can still arrive — a
    /// restarted process re-writes it when the miner reconnects), and a row that
    /// does **not** hash to the key it is filed under is terminal. The second is
    /// the one that would otherwise pay another wallet.
    #[tokio::test]
    async fn an_absent_row_is_retryable_and_a_row_under_the_wrong_key_is_not() {
        let Some(pool) = bp_test_support::connect_pg_or_skip().await else {
            return;
        };
        let resolver = PoolPaidAddresses::new(
            Arc::new(PayoutIdentityDirectory::new()),
            pool.clone(),
            Network::Regtest,
        );

        let absent = resolver
            .paid_at_height(&[ABSENT_ID.to_string()], HEIGHT)
            .await
            .expect_err("no row is no attribution");
        assert_eq!(
            absent,
            PaidAtHeightError::Unresolvable {
                payout_id: ABSENT_ID.to_string()
            }
        );
        assert!(
            !absent.is_terminal(),
            "the row can still arrive; settling later is right, parking is not"
        );

        // File a real descriptor under a key it does not hash to — the plausible
        // corruption, since the id differs from the real one by one character.
        let b = bp_payout_descriptor::RotatingPayout::from_xpub_str(XPUB_B)
            .expect("a BIP-32 vector is a valid xpub");
        assert_ne!(
            b.payout_id().as_str(),
            MISMATCHED_ID,
            "the precondition: this row is filed under the WRONG key"
        );
        bp_db::upsert_rotating_identity(&pool, MISMATCHED_ID, b.canonical_descriptor(), 1)
            .await
            .expect("write the corrupt row");

        let mismatch = resolver
            .paid_at_height(&[MISMATCHED_ID.to_string()], HEIGHT)
            .await
            .expect_err("a descriptor that does not hash to the key is not this miner's wallet");
        assert_eq!(
            mismatch,
            PaidAtHeightError::StoredIdentityRejected {
                payout_id: MISMATCHED_ID.to_string()
            }
        );
        assert!(
            mismatch.is_terminal(),
            "no later tick fixes a row that disagrees with the ledger; a human has to"
        );
    }

    /// **The reason `persist` exists, end to end.** Intake admits an xpub, the
    /// row lands, the miner disconnects and the directory forgets it — the state
    /// settlement actually runs in, ~100 blocks later and usually a process
    /// restart away — and attribution still names the address the coinbase paid.
    ///
    /// Two negative controls, and both were necessary:
    ///
    /// - The directory release. Without it this passes on the in-memory hit and
    ///   proves nothing about the row.
    /// - The `DELETE` below. The upsert is idempotent, so *an earlier run's row*
    ///   is indistinguishable from this run's — measured, not supposed: with the
    ///   `persist` call commented out this test still passed until the delete
    ///   existed, on a row a previous run had left behind.
    #[tokio::test]
    async fn an_admitted_identity_is_readable_by_settlement_after_the_miner_disconnects() {
        let Some(pool) = bp_test_support::connect_pg_or_skip().await else {
            return;
        };
        let expected_id = bp_payout_descriptor::RotatingPayout::from_xpub_str(XPUB_A)
            .expect("a BIP-32 vector is a valid xpub")
            .payout_id()
            .as_str()
            .to_string();
        // Not `query!`: a runtime-checked statement keeps a test-only DELETE out
        // of the `.sqlx` cache the production queries live in.
        sqlx::query(r#"DELETE FROM miner_identity WHERE "payoutId" = $1"#)
            .bind(&expected_id)
            .execute(&pool)
            .await
            .expect("clear any earlier run's row");

        let dir = Arc::new(PayoutIdentityDirectory::new());
        let intake = PoolRotatingIntake::new(dir.clone(), true, pool.clone());
        let payout_id = intake
            .intake(XPUB_A)
            .expect("the flag is on")
            .expect("an xpub is an intake attempt")
            .payout_id()
            .to_string();
        assert_eq!(
            payout_id, expected_id,
            "the row just deleted must be the row intake writes"
        );

        // The write is spawned, because `RotatingIntake::intake` is synchronous.
        // Wait for it rather than racing it.
        let mut row = None;
        for _ in 0..40 {
            row = bp_db::find_miner_identity(&pool, &payout_id)
                .await
                .expect("read miner_identity");
            if row.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let row = row.expect(
            "intake must persist the descriptor: without this row, a block found \
             after this miner disconnects cannot be settled at all",
        );
        assert_eq!(row.kind, bp_db::KIND_ROTATING);

        dir.release(&payout_id);
        assert!(
            !dir.identity_for(&payout_id).rotates(),
            "the precondition: the directory can no longer answer for this miner"
        );

        let resolver = PoolPaidAddresses::new(dir, pool, Network::Regtest);
        let paid = resolver
            .paid_at_height(std::slice::from_ref(&payout_id), HEIGHT)
            .await
            .expect("the stored descriptor is the whole reason it is stored");
        assert_eq!(
            paid.paid_address(&payout_id),
            Some(ADDR_A_AT_HEIGHT),
            "settlement must credit the ledger key under the same address the \
             coinbase paid at that height"
        );
    }
}
