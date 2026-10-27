//! Daemon-side configuration loaded from `~/.config/k9rs/config.yaml`.
//!
//! The daemon reads the `k9rs.daemon` section for K8s API tuning
//! parameters. The TUI ignores this section — clean boundary.
//!
//! The load is EXPLICIT, from one call in `main`, and not a `LazyLock` that
//! reads `$HOME` on first touch. It was the latter, and that made the test
//! suite depend on the developer's config file: `watcher_grace()`,
//! `locals_grace()` and five `live_query` tuning values all read through
//! here, so the same `cargo test` produced `watcher_grace=60s,
//! page_size=1000` on one machine and `999s, 7` on another with a `daemon:`
//! section. Proven by pointing `HOME` at a fixture, not reasoned about.
//!
//! This is the second instance of that bug class — `overlay::OVERLAYS` was
//! the first, and was fixed in exactly this shape without the pattern being
//! swept for. The pin below asserts the MECHANISM (nothing has loaded under
//! test), not a value, because a value-based pin passes on a machine whose
//! config happens to match the defaults.

use std::sync::{LazyLock, OnceLock};

use serde::Deserialize;

/// The process's daemon config, set once by [`load_from_home`].
static CONFIG: OnceLock<DaemonConfig> = OnceLock::new();

/// What every reader sees until — and if — a load happens. Built in-process
/// from compile-time constants, so a test, a `--no-daemon` TUI and a
/// subcommand all agree without touching a filesystem.
static DEFAULTS: LazyLock<DaemonConfig> = LazyLock::new(DaemonConfig::default);

/// Read the `k9rs.daemon` section from `~/.config/k9rs/config.yaml`.
///
/// Called from exactly ONE place — `main`, before the subcommand dispatch —
/// so the daemon, the in-process `--no-daemon` TUI and any subcommand added
/// later all get it, and nothing else in the tree decides where this config
/// comes from. Idempotent: a second call is ignored, because the first reader
/// may already hold a `&'static` borrow.
///
/// LENIENT on invalid config (warn + defaults): the TUI is the loud gate — it
/// refuses to start on the same file's errors before ever spawning a daemon,
/// so a long-running daemon whose config went bad after launch degrades
/// rather than dying mid-flight.
pub fn load_from_home() {
    let _ = CONFIG.set(match k9rs_config::load_section::<DaemonConfig>("daemon") {
        Ok(Some(c)) => c,
        Ok(None) => DaemonConfig::default(),
        Err(e) => {
            tracing::warn!("config: {e} — daemon using defaults");
            DaemonConfig::default()
        }
    });
}

/// Access the daemon config — the loaded one, or the defaults when nothing
/// has been loaded.
pub fn daemon_config() -> &'static DaemonConfig {
    CONFIG.get().unwrap_or(&DEFAULTS)
}

/// Daemon-side tuning parameters.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct DaemonConfig {
    pub watcher_page_size: u32,
    pub discovery_refresh_secs: u64,
    /// Delta-broadcast ring depth per watcher (batches). A tokio broadcast
    /// ring retains each value until 63 newer ones overwrite it, so a
    /// larger ring both tolerates a slower subscriber before it must
    /// re-baseline (≈ ring × 200 ms flush) AND keeps that many batches
    /// resident — on a QUIET large resource the initial LIST's batches are
    /// never overwritten, so the whole dataset stays pinned. Lower this to
    /// cut daemon memory on big quiet resources; raise it if slow (SSH)
    /// clients re-baseline too often.
    pub fanout_ring: usize,
    /// How long a watcher stays warm after its last subscriber leaves. Bounds
    /// a row store plus an apiserver watch; see
    /// `DEFAULT_WATCHER_GRACE_SECS`.
    pub watcher_grace_secs: u64,
    /// How long a context's port-forwards survive after its last session
    /// detaches. A separate knob from `watcherGraceSecs` because it bounds a
    /// user-created side effect rather than a cache; see
    /// `DEFAULT_LOCALS_GRACE_SECS`.
    pub locals_grace_secs: u64,
    pub backoff: BackoffConfig,
    /// Exec-backed local resource sources. Each entry defines a command
    /// that is periodically run; its JSON stdout is parsed into table rows.
    #[serde(default)]
    pub exec_resources: Vec<crate::local::exec_source::ExecSourceConfig>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            watcher_page_size: 1000,
            discovery_refresh_secs: 300,
            fanout_ring: 64,
            watcher_grace_secs: crate::grace::DEFAULT_WATCHER_GRACE_SECS,
            locals_grace_secs: crate::grace::DEFAULT_LOCALS_GRACE_SECS,
            backoff: BackoffConfig::default(),
            exec_resources: Vec::new(),
        }
    }
}

/// Watcher retry backoff parameters.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct BackoffConfig {
    pub initial_ms: u64,
    pub max_ms: u64,
    pub max_elapsed_ms: u64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_ms: 300,
            max_ms: 30_000,
            max_elapsed_ms: 120_000,
        }
    }
}

#[cfg(test)]
#[path = "tests/daemon_config.rs"]
mod tests;
