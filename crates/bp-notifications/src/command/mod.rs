// SPDX-License-Identifier: AGPL-3.0-or-later

//! Bot-command parsing + dispatch.
//!
//! Telegram polling and ntfy SSE share one command surface: a single
//! [`Command`] enum models the wire form, [`CommandHandler`] holds the
//! bp-db pool + dispatcher and replies through the [`Transport`] the
//! command arrived on.

mod handler;
mod parser;
pub(crate) mod read;

pub use handler::{ChatLanguageMap, CommandHandler, Transport};
pub use parser::{
    parse_address_callback, parse_bestdiff_callback, parse_command, parse_hourly_callback,
    AddressCallback, Command, FlagToggle, HourlyTarget, LanguageSwitch,
};
