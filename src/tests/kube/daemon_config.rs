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
        let cfg: crate::app::AppConfig = serde_yaml::from_str(yaml).unwrap();
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
        assert_eq!(cfg.ui.max_column_width, 64);
    }
