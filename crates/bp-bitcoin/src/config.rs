// SPDX-License-Identifier: AGPL-3.0-or-later

//! Connection configuration + auth modes.

use std::path::PathBuf;

/// Where the RPC client connects to and how it authenticates.
#[derive(Clone, Debug)]
pub struct BitcoinRpcConfig {
    /// Full base URL — `http://127.0.0.1:8332` for mainnet, `:18332`
    /// for testnet, `:18443` for regtest. No trailing slash.
    pub url: String,
    pub auth: RpcAuth,
    /// Per-request timeout. `None` = `reqwest` default (~30s).
    pub timeout: Option<std::time::Duration>,
}

/// JSON-RPC authentication mode.
#[derive(Clone, Debug)]
pub enum RpcAuth {
    /// Cookie file path, typically `<datadir>/.cookie`. Read on every
    /// connect so a restarting bitcoind (which rotates its cookie) doesn't
    /// strand long-lived clients. Format: `__cookie__:<random-hex>`.
    Cookie(PathBuf),
    /// Static credentials from `bitcoin.conf` (`rpcuser` / `rpcpassword`).
    UserPassword { user: String, password: String },
}
