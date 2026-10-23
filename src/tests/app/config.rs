//! `AppConfig`'s strict parse of the shared config file.
//!
//! These were in `k9rs_kube`'s `daemon_config` mirror, next to the daemon's
//! half of the same file. `AppConfig` is the TUI's section, so the assertions
//! belong here — and a lower crate's tests reaching up is a dev-dependency
//! cycle that costs `cargo test -p k9rs_kube` the entire TUI.

use crate::app::AppConfig;

#[test]
fn app_config_defaults() {
    let cfg = AppConfig::default();
    assert!(!cfg.no_exit_on_ctrl_c);
    assert!(!cfg.read_only);
    assert_eq!(cfg.ui.flash.info_secs, 3);
    assert_eq!(cfg.ui.logs.max_lines, 50_000);
}

#[test]
fn app_config_partial_yaml() {
    let yaml = r#"
readOnly: true
ui:
  maxColumnWidth: 20
"#;
    let cfg: AppConfig = serde_yaml::from_str(yaml).unwrap();
    assert!(cfg.read_only);
    assert_eq!(cfg.ui.max_column_width, 20);
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
    let result = serde_yaml::from_str::<AppConfig>(yaml);
    assert!(result.is_err(), "unknown fields should be rejected");
}

#[test]
fn app_config_nested_unknown_rejected() {
    let yaml = r#"
ui:
  maxColumnWitdh: 32
"#;
    let result = serde_yaml::from_str::<AppConfig>(yaml);
    assert!(result.is_err(), "typo in nested field should be rejected");
}

#[test]
fn empty_yaml_uses_defaults() {
    let cfg: AppConfig = serde_yaml::from_str("{}").unwrap();
    assert_eq!(cfg.ui.max_column_width, 64);
}
