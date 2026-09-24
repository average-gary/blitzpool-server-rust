// SPDX-License-Identifier: AGPL-3.0-or-later

use chrono_tz::Tz;

/// Static knobs the dispatcher needs at startup that aren't bound to
/// any individual adapter. Adapters carry their own per-transport
/// config (`SmtpConfig`, `TelegramConfig`, …).
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// IANA timezone for device-status timestamps. Default
    /// `Europe/Zurich` — falls back to `UTC` on parse failure.
    pub timezone: Tz,
}

impl DispatcherConfig {
    /// Default — `Europe/Zurich` TZ.
    pub fn default_zurich() -> Self {
        Self {
            timezone: chrono_tz::Europe::Zurich,
        }
    }
}
