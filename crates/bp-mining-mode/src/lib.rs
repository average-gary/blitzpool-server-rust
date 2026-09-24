// SPDX-License-Identifier: AGPL-3.0-or-later

//! Per-address mining-mode value type plus the live-marker write debouncer.
//!
//! [`MiningModeResult`] is what a mode resolution hands around (mode plus
//! the group id for group modes). [`MarkDebouncer`] is an in-memory
//! rate-limiter for the live-marker write path (≤1 Redis write/min/address
//! for unchanged mode, immediate write on mode change).
//!
//! The resolution itself lives with its I/O: the Stratum front resolves in
//! the bin's session persistence, the API in `bp-api`'s `mode` module.

mod debouncer;
mod result;

pub use debouncer::{MarkDebouncer, DEFAULT_REFRESH_INTERVAL};
pub use result::MiningModeResult;
