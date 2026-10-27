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
fanoutRing: 16
watcherGraceSecs: 30
localsGraceSecs: 7200
backoff:
  initialMs: 100
  maxMs: 5000
  maxElapsedMs: 30000
"#;
        let cfg: DaemonConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.watcher_page_size, 500);
        assert_eq!(cfg.discovery_refresh_secs, 60);
        assert_eq!(cfg.fanout_ring, 16);
        // The two grace windows are SEPARATE knobs on purpose — a watcher is a
        // cache, a port-forward is something the user asked for.
        assert_eq!(cfg.watcher_grace_secs, 30);
        assert_eq!(cfg.locals_grace_secs, 7200);
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

    // Five `AppConfig` tests lived here, asserting the TUI's strict gate over
    // the `daemon:` section from the config crate's own mirror. They moved to
    // `src/tests/app/config.rs`: a low crate's tests asserting an upper
    // crate's behaviour makes `cargo test -p k9rs_kube` build the whole TUI.

    /// `pollIntervalSecs: 0` is rejected by the TYPE, not by a guard.
    ///
    /// Zero was catastrophic and silent: `RunDelay::Schedule(ZERO)` re-runs
    /// with no delay, and `timeout(ZERO, child)` fires before the child can
    /// emit a byte, so every poll `kill_on_drop`s its own subprocess at CPU
    /// speed while publishing nothing. `LocalRegistry::new` ran five guards
    /// over these configs — empty name, duplicate name, empty command, empty
    /// headers, header/key length — and none of them looked at the interval.
    #[test]
    fn a_zero_poll_interval_is_not_deserializable() {
        let yaml = r#"
execResources:
  - name: node-costs
    command: kubectl
    pollIntervalSecs: 0
    headers: ["NAME"]
    jsonFieldKeys: ["name"]
"#;
        let err = serde_yaml::from_str::<DaemonConfig>(yaml)
            .expect_err("zero must not parse");
        let msg = err.to_string();
        assert!(
            msg.contains("zero") || msg.contains("NonZero") || msg.contains("nonzero"),
            "the error should name the problem: {msg}",
        );

        // …and a sane value still parses.
        let ok: DaemonConfig = serde_yaml::from_str(
            &yaml.replace("pollIntervalSecs: 0", "pollIntervalSecs: 15"),
        )
        .expect("a non-zero interval parses");
        assert_eq!(ok.exec_resources[0].poll_interval_secs.get(), 15);
    }

/// A process that never calls `load_from_home` reads the DEFAULTS, so the
/// suite's results cannot depend on the developer's config file.
///
/// This was not true. `CONFIG` was a `LazyLock` that read `$HOME` on first
/// touch, and first touch happens deep inside `grace::watcher_grace()` and
/// five `live_query` tuning reads. Pointing `HOME` at a fixture containing
/// `watcherGraceSecs: 999, watcherPageSize: 7` changed the SAME
/// `cargo test -p k9rs_kube` run from `60s / 1000` to `999s / 7`.
///
/// The assertion is on the MECHANISM, not the values: `watcher_page_size ==
/// 1000` alone passes vacuously on a machine with no `daemon:` section — and
/// that is precisely the machine where this bug is invisible. Checking the
/// slot is empty holds everywhere, and is reachable only because this mirror
/// is a child module of `daemon_config` and can see a private static.
///
/// Second instance of this class; `overlay::OVERLAYS` was the first, and was
/// fixed in this exact shape without anyone sweeping for siblings.
#[test]
fn a_process_that_never_loads_its_config_uses_defaults() {
    assert!(
        CONFIG.get().is_none(),
        "something loaded the real config file during the test run — the \
         suite is no longer hermetic, and whichever test did it now depends \
         on whatever is in the developer's ~/.config/k9rs/config.yaml",
    );
    // And the reader agrees, so a future `get_or_init` cannot quietly make
    // the slot non-empty while still answering with defaults.
    assert_eq!(daemon_config().watcher_page_size, DaemonConfig::default().watcher_page_size);
}

