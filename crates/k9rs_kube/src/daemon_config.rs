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
/// LENIENT on invalid config (keep going on defaults) because a daemon is a
/// long-running process: one whose config file goes bad after launch should
/// degrade, not die mid-flight with sessions attached.
///
/// This used to add "the TUI is the loud gate — it refuses to start on the
/// same file's errors before ever spawning a daemon". Nothing spawns a
/// daemon — `k9rs daemon` is a foreground command the user runs, possibly
/// first, possibly on a box where the TUI never runs — and the TUI's own
/// gate validates the `k9rs` section, not this one. There was no loud gate
/// anywhere.
///
/// So the complaint is KEPT rather than logged, and
/// [`load_problems`] hands it to whichever entry point called this, to
/// report once its logging exists. It was a bare `tracing::warn!` called
/// from `main` BEFORE any subscriber is installed — dropped on the floor,
/// not even `--log-file` caught it. Overlays survive the same ordering
/// because their warn macro also pushes to a `problems` list that `main`
/// surfaces as a flash; this had no second destination. Same shape as the
/// lost-daemon-diagnostics entry in the ledger.
pub fn load_from_home() {
    let mut problem = None;
    let _ = CONFIG.set(match k9rs_config::load_section::<DaemonConfig>("daemon") {
        Ok(Some(c)) => c,
        Ok(None) => DaemonConfig::default(),
        Err(e) => {
            problem = Some(format!("daemon config: {e} — using defaults"));
            DaemonConfig::default()
        }
    });
    if let Some(p) = problem {
        let _ = PROBLEMS.set(vec![p]);
    }
}

/// What [`load_from_home`] had to say about the config file, for the entry
/// point to report once it has somewhere to report to. Empty when the file
/// was fine, absent, or never loaded.
pub fn load_problems() -> &'static [String] {
    PROBLEMS.get().map_or(&[], |p| p.as_slice())
}

static PROBLEMS: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();

/// Access the daemon config — the loaded one, or the defaults when nothing
/// has been loaded.
pub fn daemon_config() -> &'static DaemonConfig {
    CONFIG.get().unwrap_or(&DEFAULTS)
}

/// Daemon-side tuning parameters.
///
/// The knobs for which zero is not a quantity but a malfunction are
/// `NonZero`, so serde refuses them where the file is read and
/// [`load_from_home`] reports it — the same shape `ExecSourceConfig`'s
/// `pollIntervalSecs` already had. They used to be plain integers with a
/// single `.max(1)` at one of the six readers: `fanoutRing: 0` PANICS
/// (`broadcast::channel(0)`), which is why that one was guarded, while
/// `discoveryRefreshSecs: 0` became a `sleep(0)` hot loop and
/// `backoff.initialMs: 0` made `(0 * 2).min(max)` a fixed point — the
/// documented ceiling unreachable and the retry loop spinning at
/// `now + 0ms`. Clamping at the readers would have been the same decision
/// written six times; a type states it once.
///
/// `watcherGraceSecs`, `localsGraceSecs` and `backoff.maxElapsedMs` stay
/// plain: zero is a real answer for each ("do not keep it warm", "give up
/// on the first failure"), and refusing it would take a choice away.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct DaemonConfig {
    pub watcher_page_size: std::num::NonZeroU32,
    pub discovery_refresh_secs: std::num::NonZeroU64,
    /// Delta-broadcast ring depth per watcher (batches). A tokio broadcast
    /// ring retains each value until 63 newer ones overwrite it, so a
    /// larger ring both tolerates a slower subscriber before it must
    /// re-baseline (≈ ring × 200 ms flush) AND keeps that many batches
    /// resident — on a QUIET large resource the initial LIST's batches are
    /// never overwritten, so the whole dataset stays pinned. Lower this to
    /// cut daemon memory on big quiet resources; raise it if slow (SSH)
    /// clients re-baseline too often.
    pub fanout_ring: std::num::NonZeroUsize,
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
            watcher_page_size: nonzero_u32(1000),
            discovery_refresh_secs: nonzero_u64(300),
            fanout_ring: std::num::NonZeroUsize::new(64).expect("64 is nonzero"),
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
    /// First retry delay. `NonZero` because the doubling is
    /// `(current * 2).min(max)`, which has a fixed point at zero: a `0`
    /// here made `max_ms` unreachable and the retry loop spin.
    pub initial_ms: std::num::NonZeroU64,
    /// Ceiling for the doubling.
    pub max_ms: std::num::NonZeroU64,
    /// How long a watch may keep failing before its rows are declared dead.
    /// Zero is allowed and means "the first failure is fatal".
    pub max_elapsed_ms: u64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_ms: nonzero_u64(300),
            max_ms: nonzero_u64(30_000),
            max_elapsed_ms: 120_000,
        }
    }
}

/// The two `NonZero` constructors the defaults need, with the literal right
/// next to the proof it is nonzero.
const fn nonzero_u32(n: u32) -> std::num::NonZeroU32 {
    match std::num::NonZeroU32::new(n) {
        Some(v) => v,
        None => panic!("a default must be nonzero"),
    }
}

const fn nonzero_u64(n: u64) -> std::num::NonZeroU64 {
    match std::num::NonZeroU64::new(n) {
        Some(v) => v,
        None => panic!("a default must be nonzero"),
    }
}

#[cfg(test)]
#[path = "tests/daemon_config.rs"]
mod tests;
