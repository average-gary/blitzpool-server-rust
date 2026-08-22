// SPDX-License-Identifier: AGPL-3.0-or-later

//! Wire-primitive conversions shared by both SV2 codecs.
//!
//! [`crate::server_codec`] (mining) and [`crate::jdp_server_codec`] (job
//! declaration) translate between the lifetime-bound `stratum_core` wire types
//! and the owned shapes the pure handlers take. The per-message mapping is
//! specific to each sub-protocol; the primitives underneath it are not — a
//! fixed-width byte field, a UTF-8 string, a token, a `Str0255` behave the same
//! whichever frame carries them.
//!
//! They used to be written out per codec, and the copies had already drifted:
//! `str0255` reached the same `CodecError::Conversion(format!("{e:?}"))` through
//! `CodecError::from_conv` on one side and through a locally re-declared
//! `conv` on the other, because `from_conv` was private to the mining codec and
//! the JDP codec could not see it. Counting the write paths, that one function
//! existed in four spellings.
//!
//! [`CodecError`] lives here for the same reason: both codecs return it, and
//! both server tasks wrap it in their own `WriteError`. It is not the mining
//! codec's type — it only used to be declared there.

use crate::tokens::Token;

// ── Errors ──────────────────────────────────────────────────────────

/// Codec-layer failures. Production wiring logs + drops the frame;
/// the per-connection task continues. None of these are connection-fatal
/// in the spec sense.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// Inbound message arrived on the wrong sub-protocol port —
    /// e.g. a JDP frame on the mining listener. Caller logs +
    /// ignores (the per-connection task already routed by port).
    ///
    /// Name and message are protocol-neutral because both codecs raise it:
    /// the JDP codec has done so all along, and while this variant lived in
    /// the mining codec it told an operator a JDP frame was "not relevant to
    /// mining server" — naming the listener that did not reject it.
    #[error("message type not served on this sub-protocol port: {0:?}")]
    NotForThisSubProtocol(&'static str),
    /// Sv2 wire type → owned-data conversion failure. Typically a
    /// length mismatch on a fixed-size byte field.
    #[error("conversion: {0}")]
    Conversion(String),
    /// A miner-supplied string failed UTF-8 validation. Caller
    /// reports + drops (a malicious miner can otherwise corrupt
    /// downstream string handling).
    #[error("invalid UTF-8: {0}")]
    InvalidUtf8(String),
    /// Outbound frame variant doesn't yet have a wire-codec
    /// implementation. Placeholder during the iterative build-out;
    /// disappears once every variant is covered.
    #[error("encode not yet implemented for variant: {0}")]
    EncodeUnimplemented(&'static str),
}

impl CodecError {
    /// Wrap any `Debug` conversion failure as [`CodecError::Conversion`].
    ///
    /// `pub(crate)` and not private: both codecs and both write paths need it,
    /// and being unreachable from three of the four is exactly how it came to
    /// be written out three more times.
    pub(crate) fn from_conv<E: core::fmt::Debug>(e: E) -> Self {
        CodecError::Conversion(format!("{e:?}"))
    }
}

// ── Wire primitives ─────────────────────────────────────────────────

pub(crate) fn utf8_from_bytes(b: &[u8]) -> Result<String, CodecError> {
    std::str::from_utf8(b)
        .map(|s| s.to_string())
        .map_err(|e| CodecError::InvalidUtf8(e.to_string()))
}

pub(crate) fn bytes_to_32(b: &[u8]) -> Result<[u8; 32], CodecError> {
    if b.len() != 32 {
        return Err(CodecError::Conversion(format!(
            "expected 32-byte field, got {}",
            b.len()
        )));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(b);
    Ok(arr)
}

pub(crate) fn token_from_bytes(b: &[u8]) -> Result<Token, CodecError> {
    if b.len() != crate::tokens::TOKEN_LEN {
        return Err(CodecError::Conversion(format!(
            "expected {}-byte token, got {}",
            crate::tokens::TOKEN_LEN,
            b.len()
        )));
    }
    let mut arr = [0u8; crate::tokens::TOKEN_LEN];
    arr.copy_from_slice(b);
    Ok(Token(arr))
}

pub(crate) fn str0255(s: String) -> Result<stratum_core::binary_sv2::Str0255<'static>, CodecError> {
    s.try_into().map_err(CodecError::from_conv)
}
