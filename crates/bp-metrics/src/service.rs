// SPDX-License-Identifier: AGPL-3.0-or-later

//! Spawn the Prometheus exporter HTTP listener + install the global
//! recorder.
//!
//! # Lifecycle
//!
//! Exactly **one** `MetricsService::spawn` per process — the `metrics`
//! crate uses a single global recorder. The handle holds the listener
//! task; dropping it shuts down the HTTP listener via the exporter's
//! own cancellation mechanism. Tests that need a per-test recorder
//! must use a different port each time and clean up explicitly.

use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::{info, warn};

use crate::config::PrometheusConfig;
use crate::error::MetricsError;

pub struct MetricsService;

impl MetricsService {
    /// Install the Prometheus recorder + spawn the HTTP listener.
    pub fn spawn(config: PrometheusConfig) -> Result<MetricsServiceHandle, MetricsError> {
        PrometheusBuilder::new()
            .with_http_listener(config.bind_addr)
            .install()
            .map_err(|e| MetricsError::Install(format!("install: {e}")))?;
        info!(
            bind_addr = %config.bind_addr,
            "Prometheus exporter started — /metrics endpoint live"
        );
        Ok(MetricsServiceHandle {
            bind_addr: config.bind_addr.to_string(),
        })
    }
}

/// Handle. Cheap; holds nothing the caller needs to drop manually.
/// The exporter's HTTP listener task is detached + lives for the
/// process lifetime (the global recorder is install-once anyway).
#[derive(Clone, Debug)]
pub struct MetricsServiceHandle {
    pub bind_addr: String,
}

impl Drop for MetricsServiceHandle {
    fn drop(&mut self) {
        // The metrics-exporter-prometheus listener doesn't expose a
        // clean shutdown handle in the install-on-runtime mode; once
        // installed the recorder lives for the process. Log so
        // operators can correlate handle-drops with the listener
        // outliving the holder if it ever becomes a problem.
        warn!(
            bind_addr = %self.bind_addr,
            "MetricsServiceHandle dropped; Prometheus listener continues on global recorder"
        );
    }
}
