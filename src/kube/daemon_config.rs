//! Daemon-side configuration loaded from `~/.config/k9rs/config.yaml`.
//!
//! The daemon reads the `k9rs.daemon` section for K8s API tuning
//! parameters. The TUI ignores this section — clean boundary.

use std::sync::LazyLock;

use serde::Deserialize;

/// Global daemon config, loaded once at startup. LENIENT on invalid
/// config (warn + defaults): the TUI is the loud gate — it refuses to
/// start on the same file's errors before ever spawning a daemon, so a
/// long-running daemon whose config went bad after launch degrades
/// rather than dying mid-flight.
static CONFIG: LazyLock<DaemonConfig> = LazyLock::new(|| {
    match load_section::<DaemonConfig>("daemon") {
        Ok(Some(c)) => c,
        Ok(None) => DaemonConfig::default(),
        Err(e) => {
            tracing::warn!("config: {e} — daemon using defaults");
            DaemonConfig::default()
        }
    }
});

/// Access the daemon config.
pub fn daemon_config() -> &'static DaemonConfig {
    &CONFIG
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
    pub backoff: BackoffConfig,
    /// Exec-backed local resource sources. Each entry defines a command
    /// that is periodically run; its JSON stdout is parsed into table rows.
    #[serde(default)]
    pub exec_resources: Vec<crate::kube::local::exec_source::ExecSourceConfig>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            watcher_page_size: 1000,
            discovery_refresh_secs: 300,
            fanout_ring: 64,
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

// ---------------------------------------------------------------------------
// Shared config file loader
// ---------------------------------------------------------------------------

/// Read `~/.config/k9rs/config.yaml` and deserialize the `k9rs` root section
/// (or a sub-section) into `T`.
///
/// - `Ok(None)` — the file, the `k9rs` root, or the requested section is
///   ABSENT. Expected; callers use their defaults.
/// - `Err` — the file EXISTS but is invalid (unreadable, YAML syntax
///   error, or strict deserialization failed). The user wrote something
///   and it is being rejected — callers must never silently collapse
///   this into "use defaults": a one-letter typo in `keys:` would revert
///   the whole config (`readOnly` included) with zero feedback, since
///   the TUI's tracing writes to a sink without `--log-file`.
///
/// - `load_section::<AppConfig>("")` → deserializes the `k9rs` key
/// - `load_section::<DaemonConfig>("daemon")` → deserializes `k9rs.daemon`
pub(crate) fn load_section<T: serde::de::DeserializeOwned>(
    section: &str,
) -> Result<Option<T>, String> {
    let Ok(home) = std::env::var("HOME") else { return Ok(None) };
    let path = std::path::Path::new(&home).join(".config/k9rs/config.yaml");
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("cannot read {}: {}", path.display(), e)),
    };
    let yaml: serde_yaml::Value = serde_yaml::from_str(&content)
        .map_err(|e| format!("{}: YAML syntax error: {}", path.display(), e))?;
    let Some(root) = yaml.get("k9rs") else { return Ok(None) };
    let val = if section.is_empty() {
        root.clone()
    } else {
        match root.get(section) {
            Some(v) => v.clone(),
            None => return Ok(None),
        }
    };
    match serde_yaml::from_value(val) {
        Ok(v) => Ok(Some(v)),
        Err(e) => {
            let label = if section.is_empty() { "k9rs" } else { section };
            Err(format!("{}: invalid '{}' section: {}", path.display(), label, e))
        }
    }
}

#[cfg(test)]
#[path = "../tests/kube/daemon_config.rs"]
mod tests;
