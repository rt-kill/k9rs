//! Daemon-side configuration loaded from `~/.config/k9rs/config.yaml`.
//!
//! The daemon reads the `k9rs.daemon` section for K8s API tuning
//! parameters. The TUI ignores this section — clean boundary.

use std::sync::LazyLock;

use serde::Deserialize;

/// Global daemon config, loaded once at startup.
static CONFIG: LazyLock<DaemonConfig> = LazyLock::new(|| {
    load_section::<DaemonConfig>("daemon").unwrap_or_default()
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
/// (or a sub-section) into `T`. Returns `None` if the file is missing,
/// the section is absent, or deserialization fails.
///
/// - `load_section::<AppConfig>("")` → deserializes the `k9rs` key
/// - `load_section::<DaemonConfig>("daemon")` → deserializes `k9rs.daemon`
///
/// Used by both the TUI and daemon loaders.
pub(crate) fn load_section<T: serde::de::DeserializeOwned>(section: &str) -> Option<T> {
    let home = std::env::var("HOME").ok()?;
    let path = std::path::Path::new(&home).join(".config/k9rs/config.yaml");
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return None, // file missing — expected, no warning
    };
    let yaml: serde_yaml::Value = match serde_yaml::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("config: failed to parse {}: {}", path.display(), e);
            return None;
        }
    };
    let root = yaml.get("k9rs")?;
    let val = if section.is_empty() {
        root.clone()
    } else {
        root.get(section)?.clone()
    };
    match serde_yaml::from_value(val) {
        Ok(v) => Some(v),
        Err(e) => {
            let label = if section.is_empty() { "root" } else { section };
            tracing::warn!("config: failed to deserialize '{}' section: {}", label, e);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_config_defaults() {
        let cfg = DaemonConfig::default();
        assert_eq!(cfg.watcher_page_size, 1000);
        assert_eq!(cfg.discovery_refresh_secs, 300);
        assert_eq!(cfg.backoff.initial_ms, 300);
        assert_eq!(cfg.backoff.max_ms, 30_000);
        assert_eq!(cfg.backoff.max_elapsed_ms, 120_000);
    }

    #[test]
    fn daemon_config_partial_yaml() {
        let yaml = r#"
watcherPageSize: 2000
"#;
        let cfg: DaemonConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.watcher_page_size, 2000);
        // Missing fields use defaults
        assert_eq!(cfg.discovery_refresh_secs, 300);
        assert_eq!(cfg.backoff.initial_ms, 300);
    }

    #[test]
    fn daemon_config_full_yaml() {
        let yaml = r#"
watcherPageSize: 500
discoveryRefreshSecs: 60
backoff:
  initialMs: 100
  maxMs: 5000
  maxElapsedMs: 30000
"#;
        let cfg: DaemonConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.watcher_page_size, 500);
        assert_eq!(cfg.discovery_refresh_secs, 60);
        assert_eq!(cfg.backoff.initial_ms, 100);
        assert_eq!(cfg.backoff.max_ms, 5000);
        assert_eq!(cfg.backoff.max_elapsed_ms, 30000);
    }

    #[test]
    fn daemon_config_unknown_field_rejected() {
        let yaml = r#"
watcherPageSize: 1000
unknownField: true
"#;
        let result = serde_yaml::from_str::<DaemonConfig>(yaml);
        assert!(result.is_err(), "unknown fields should be rejected");
    }

    #[test]
    fn app_config_defaults() {
        let cfg = crate::app::AppConfig::default();
        assert!(!cfg.no_exit_on_ctrl_c);
        assert!(!cfg.read_only);
        assert_eq!(cfg.ui.page_scroll_lines, 40);
        assert_eq!(cfg.ui.flash.info_secs, 3);
        assert_eq!(cfg.ui.logs.max_lines, 50_000);
    }

    #[test]
    fn app_config_partial_yaml() {
        let yaml = r#"
readOnly: true
ui:
  pageScrollLines: 20
"#;
        let cfg: crate::app::AppConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.read_only);
        assert_eq!(cfg.ui.page_scroll_lines, 20);
        // Nested defaults preserved
        assert_eq!(cfg.ui.flash.info_secs, 3);
        assert_eq!(cfg.ui.logs.tail_lines, 100);
    }

    #[test]
    fn app_config_unknown_field_rejected() {
        let yaml = r#"
readOnly: true
typoField: 42
"#;
        let result = serde_yaml::from_str::<crate::app::AppConfig>(yaml);
        assert!(result.is_err(), "unknown fields should be rejected");
    }

    #[test]
    fn app_config_nested_unknown_rejected() {
        let yaml = r#"
ui:
  maxColumnWitdh: 32
"#;
        let result = serde_yaml::from_str::<crate::app::AppConfig>(yaml);
        assert!(result.is_err(), "typo in nested field should be rejected");
    }

    #[test]
    fn empty_yaml_uses_defaults() {
        let cfg: crate::app::AppConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg.ui.page_scroll_lines, 40);
        assert_eq!(cfg.ui.max_column_width, 64);
    }
}
