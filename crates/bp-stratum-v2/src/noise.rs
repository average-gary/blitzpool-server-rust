// SPDX-License-Identifier: AGPL-3.0-or-later

//! Noise-XK handshake wiring + per-connection certificate validity.
//!
//! Thin wrapper over [`stratum_apps::network_helpers::accept_noise_connection`]
//! (runtime-dep) — we do **not** re-implement the Noise state machine
//! or the underlying handshake protocol. The `stratum-apps` crate owns the
//! wire format; this module owns the **pool's config + the "12h cert
//! validity" convention** that `bp-stratum-v2` commits to in production
//! wiring.
//!
//! ## "12h cert rotation"
//!
//! Each accepted Noise connection generates a fresh Responder via
//! `Responder::from_authority_kp(pub, prv, Duration::from_secs(cert_validity))`.
//! The `cert_validity` controls how long the cert the JDC/miner sees is
//! valid for. The pool's authority key-pair itself doesn't rotate —
//! only the per-connection cert. Setting [`DEFAULT_CERT_VALIDITY`] to
//! 12 hours means the cert presented to a freshly-connected miner is
//! valid for half a day; that matches the operational hand-off doc
//! and is short enough that a leaked cert is naturally retired by
//! daily-rolling-key practice without manual revocation.
//!
//! There is no shared mutable state to rotate centrally — every
//! [`accept_pool_noise`] call generates a fresh Responder with a fresh
//! 12h cert. The "rotation" is therefore inherent in the per-connection
//! generation; this module exposes the convention as a named constant.
//! It is not configurable.
//!
//! ## What this module wraps vs. what stays in `stratum-apps`
//!
//! - **In `stratum-apps`** (runtime-dep, MIT/Apache): the Noise-XK
//!   handshake state machine, framing, encoder/decoder, the
//!   `Responder::from_authority_kp` builder, the `NoiseTcpStream`
//!   read/write split.
//! - **In this module**: pool-side config (parsed authority keys), the
//!   re-exports (so consumers `use crate::noise::{NoiseConfig,
//!   NoiseTcpStream, ...}` without the deep `stratum_apps::network_helpers::*`
//!   path), and the [`DEFAULT_CERT_VALIDITY`] constant.
//!
//! We runtime-dep the protocol plumbing + write our own pool-side state
//! machine + config. This module is exactly that split applied to Noise.
//!
//! ## What this module does **not** do
//!
//! - **TCP-accept loop**: belongs in [`crate::server`] /
//!   [`crate::jdp_server`] — they own the listener, the per-connection
//!   task spawn, the cancellation token, and the fail-ban / rate-limit
//!   guards. This module is invoked one-call-per-accepted-connection
//!   from inside their loop.
//! - **Frame routing**: belongs to the per-connection task in
//!   `server.rs` / `jdp_server.rs`. Once [`accept_pool_noise`] returns
//!   a [`NoiseTcpStream`], the per-connection task drives the
//!   `read_frame()` / `write_frame()` loop.

use std::time::Duration;

use stratum_apps::key_utils::{Secp256k1PublicKey, Secp256k1SecretKey};
use stratum_apps::network_helpers::{accept_noise_connection, Error as NoiseHelpersError};
use tokio::net::TcpStream;

// ── Re-exports — let consumers import via `crate::noise::*` ─────────

pub use stratum_apps::network_helpers::noise_stream::{
    NoiseTcpReadHalf, NoiseTcpStream, NoiseTcpWriteHalf,
};

/// Errors propagated from [`accept_pool_noise`]. Re-exports the
/// upstream [`stratum_apps::network_helpers::Error`] under a
/// pool-side alias so error handling in `server.rs` / `jdp_server.rs`
/// doesn't depend on the deep path.
pub type NoiseError = NoiseHelpersError;

// ── Constants ───────────────────────────────────────────────────────

/// Cert validity used in production: 12 hours.
///
/// Rationale: per the operational hand-off doc, the pool issues
/// per-connection Noise certs that live for half a day. Connections
/// outliving this re-handshake on reconnect with a fresh cert. A
/// leaked cert is naturally retired by daily key practice without
/// requiring manual revocation tooling.
pub const DEFAULT_CERT_VALIDITY: Duration = Duration::from_secs(12 * 3600);

// ── NoiseConfig ─────────────────────────────────────────────────────

/// Pool-side Noise-handshake configuration: the parsed authority key-pair.
/// Every connection's cert is issued for [`DEFAULT_CERT_VALIDITY`].
/// Clone-able so the same config can be shared between the mining-server
/// and JDP-server accept loops without an `Arc`.
#[derive(Clone, Debug)]
pub struct NoiseConfig {
    authority_pub: Secp256k1PublicKey,
    authority_prv: Secp256k1SecretKey,
}

impl NoiseConfig {
    pub fn new(authority_pub: Secp256k1PublicKey, authority_prv: Secp256k1SecretKey) -> Self {
        Self {
            authority_pub,
            authority_prv,
        }
    }

    pub fn authority_pub(&self) -> &Secp256k1PublicKey {
        &self.authority_pub
    }

    pub fn authority_prv(&self) -> &Secp256k1SecretKey {
        &self.authority_prv
    }
}

// ── accept_pool_noise ───────────────────────────────────────────────

/// Accept a freshly-connected `TcpStream` as a Noise responder.
///
/// Thin wrapper over
/// [`stratum_apps::network_helpers::accept_noise_connection`] that
/// passes the pool's authority key-pair from [`NoiseConfig`] and
/// [`DEFAULT_CERT_VALIDITY`]. The handshake timeout is `stratum_apps`-internal
/// (10 s, fixed at the time of pinning); see
/// [`stratum_apps::network_helpers::noise_stream::NoiseTcpStream::accept`]
/// for the override path.
///
/// On success returns a [`NoiseTcpStream`] split-ready for
/// the per-connection task; on failure the IO layer closes the TCP
/// stream and increments a handshake-failure counter (per-IP fail-ban
/// is the listener-loop's concern, deferred to `server.rs`).
pub async fn accept_pool_noise(
    stream: TcpStream,
    config: &NoiseConfig,
) -> Result<NoiseTcpStream, NoiseError> {
    accept_noise_connection(
        stream,
        config.authority_pub,
        config.authority_prv,
        DEFAULT_CERT_VALIDITY.as_secs(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Public test key-pair from the standard SV2 example pool config.
    /// Safe to commit; matches the standard SV2 testnet/regtest
    /// reference fixtures.
    const TEST_PUB: &str = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72";
    const TEST_PRV: &str = "mkDLTBBRxdBv998612qipDYoTK3YUrqLe8uWw7gu3iXbSrn2n";

    #[test]
    fn default_cert_validity_is_12_hours() {
        assert_eq!(DEFAULT_CERT_VALIDITY, Duration::from_secs(12 * 3600));
    }

    #[test]
    fn new_holds_the_sri_test_keys() {
        let pub_k: Secp256k1PublicKey = TEST_PUB.parse().unwrap();
        let prv_k: Secp256k1SecretKey = TEST_PRV.parse().unwrap();
        let cfg = NoiseConfig::new(pub_k, prv_k);
        assert_eq!(cfg.authority_pub().into_bytes(), pub_k.into_bytes());
        assert_eq!(cfg.authority_prv().into_bytes(), prv_k.into_bytes());
        assert_ne!(pub_k.into_bytes(), [0u8; 32]);
    }

    /// `accept_pool_noise` is async + needs a real TCP-stream peer
    /// to handshake against. The full handshake is exercised in the
    /// regtest e2e tests (`tests/regtest_standard.rs` +
    /// `tests/regtest_extended.rs`) where a real miner client peers
    /// with the pool. Here we only assert the surface type — the
    /// function exists, takes our `&NoiseConfig`, and compiles
    /// against the upstream signature.
    #[test]
    fn accept_pool_noise_surface_type_compiles() {
        fn _assert_signature() {
            // Compile-time only — confirms accept_pool_noise's
            // signature still matches the upstream helper. Doesn't run.
            #[allow(dead_code)]
            async fn _example(stream: TcpStream, cfg: &NoiseConfig) {
                let _: Result<NoiseTcpStream, NoiseError> = accept_pool_noise(stream, cfg).await;
            }
        }
    }
}
