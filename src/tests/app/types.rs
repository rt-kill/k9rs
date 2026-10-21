use super::*;

// ---------------------------------------------------------------------------
// Connection — the unified link/reconnect lifecycle (2026-08 audit: the
// five-field constellation whose missed edges shipped two real bugs)
// ---------------------------------------------------------------------------

mod connection {
    use super::{Connection, LinkState};
    use std::time::{Duration, Instant};

    #[test]
    fn starts_connecting_and_never_connected() {
        let conn = Connection::new();
        assert!(conn.link() == LinkState::Connecting, "initial handshake hasn't landed");
        assert!(!conn.has_ever_connected(), "initial failure must read as fatal");
        assert!(!conn.rebuild_due(Instant::now()), "nothing to rebuild yet");
    }

    #[test]
    fn establish_goes_live_and_arms_the_fatality_rule() {
        let mut conn = Connection::new();
        conn.established();
        assert!(conn.link() != LinkState::Connecting);
        assert!(conn.has_ever_connected());
    }

    #[test]
    fn disconnect_shows_connecting_now_and_schedules_a_rebuild() {
        let mut conn = Connection::new();
        conn.established();
        conn.disconnected();
        assert!(conn.link() == LinkState::Connecting, "stale rows must gate THIS frame");
        assert!(conn.rebuild_due(Instant::now()));
    }

    #[test]
    fn begin_rebuild_consumes_the_plan_but_stays_connecting() {
        // THE mid-gap invariant: the plan is consumed at the START of the
        // rebuild, but the screen must not flicker back to stale rows
        // before the new connection is up — liveness is stored, not
        // derived from the plan.
        let mut conn = Connection::new();
        conn.established();
        conn.disconnected();
        conn.begin_rebuild();
        assert!(!conn.rebuild_due(Instant::now()), "plan consumed");
        assert!(conn.link() == LinkState::Connecting, "still gating until established()");
    }

    #[test]
    fn backoff_doubles_clamps_and_resets_on_establish() {
        let mut conn = Connection::new();
        conn.established();
        let t0 = Instant::now();

        conn.reconnect_failed_backoff(t0);
        assert!(!conn.rebuild_due(t0), "backed off — not due immediately");
        assert!(conn.rebuild_due(t0 + Duration::from_millis(500)));

        // Each failure doubles the NEXT delay: 500ms, 1s, 2s, 4s, 5s cap.
        conn.reconnect_failed_backoff(t0);
        assert!(!conn.rebuild_due(t0 + Duration::from_millis(999)));
        assert!(conn.rebuild_due(t0 + Duration::from_millis(1000)));
        conn.reconnect_failed_backoff(t0);
        assert!(conn.rebuild_due(t0 + Duration::from_secs(2)));
        conn.reconnect_failed_backoff(t0);
        assert!(conn.rebuild_due(t0 + Duration::from_secs(4)));
        conn.reconnect_failed_backoff(t0);
        assert!(!conn.rebuild_due(t0 + Duration::from_secs(4)), "clamped at 5s");
        assert!(conn.rebuild_due(t0 + Duration::from_secs(5)));

        // Success resets the ladder.
        conn.established();
        conn.reconnect_failed_backoff(t0);
        assert!(conn.rebuild_due(t0 + Duration::from_millis(500)));
    }

    #[test]
    fn switch_failure_falls_back_without_backoff() {
        // The daemon is UP (the switch TARGET was unreachable) — the
        // fallback reconnect to the previous context runs immediately.
        let mut conn = Connection::new();
        conn.established();
        conn.switch_failed_fallback();
        assert!(conn.rebuild_due(Instant::now()));
    }

    #[test]
    fn switch_request_gates_the_frame_without_a_plan() {
        let mut conn = Connection::new();
        conn.established();
        conn.switch_requested();
        assert!(conn.link() == LinkState::Connecting, "connecting screen this frame");
        assert!(
            !conn.rebuild_due(Instant::now()),
            "the SWITCH drives the rebuild, not the reconnect plan"
        );
    }
}

// ---------------------------------------------------------------------------
// NoContext — "nothing to connect to" is its own state, not a slow connect
// ---------------------------------------------------------------------------

mod no_context {
    use super::{Connection, LinkState};
    use std::time::Instant;

    #[test]
    fn no_context_is_terminal_until_the_user_picks_one() {
        // Saying "connecting…" forever would be the same lie the connecting
        // screen was invented to stop telling — nothing is being attempted.
        let mut conn = Connection::new();
        conn.no_context();
        assert_eq!(conn.link(), LinkState::NoContext);
        assert!(
            !conn.rebuild_due(Instant::now()),
            "a retry with no target to retry against is just a loop",
        );
    }

    #[test]
    fn picking_a_context_is_the_way_out() {
        let mut conn = Connection::new();
        conn.no_context();
        conn.switch_requested();
        assert_eq!(conn.link(), LinkState::Connecting, "now something IS being attempted");
    }

    #[test]
    fn a_dead_daemon_still_backs_off_from_no_context() {
        // The escape hatch must not disarm the normal machinery: once a
        // switch is under way, an ordinary failure has to back off as usual.
        let mut conn = Connection::new();
        conn.no_context();
        conn.switch_requested();
        conn.reconnect_failed_backoff(Instant::now());
        assert!(!conn.rebuild_due(Instant::now()), "backoff deadline not reached yet");
    }
}

// ---------------------------------------------------------------------------
// Modal — one slot, so two-modals-at-once is unrepresentable
// ---------------------------------------------------------------------------

mod modal {
    use crate::app::{App, Modal, TakeoverOp};

    #[test]
    fn opening_a_modal_displaces_whatever_was_open() {
        // These were four independent fields cleared by hand at 53 sites;
        // any path that forgot one left a modal buried under another,
        // invisible until the top one closed. Now it is an assignment.
        let mut app = App::new_for_test();
        app.ui.open(Modal::Command { input: crate::app::text_input::TextInput::seeded(":pods"), history_index: None });
        assert!(app.ui.command_input().is_some());

        app.ui.open(Modal::Takeover(TakeoverOp::Edit {
            target: crate::kube::protocol::ObjectRef::new(
                crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
                "p".to_string(),
                crate::kube::protocol::Namespace::Named("default".to_string()),
            ),
            state: crate::app::EditState::AwaitingYaml,
        }));
        assert!(app.ui.takeover().is_some());
        assert!(
            app.ui.command_input().is_none(),
            "the command prompt cannot still be open behind the overlay",
        );

        app.ui.close_modal();
        assert!(!app.ui.is_modal());
        assert!(app.ui.takeover().is_none());
    }

    #[test]
    fn exactly_one_accessor_answers_at_a_time() {
        // The accessors partition the slot: whatever is open, every OTHER
        // accessor must say no. This is the property the four-field version
        // could not state, let alone hold.
        let mut app = App::new_for_test();
        let opens: Vec<(&str, Modal)> = vec![
            ("command", Modal::Command { input: crate::app::text_input::TextInput::new(), history_index: None }),
            ("overlay", Modal::Takeover(TakeoverOp::Edit {
            target: crate::kube::protocol::ObjectRef::new(
                crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
                "p".to_string(),
                crate::kube::protocol::Namespace::Named("default".to_string()),
            ),
            state: crate::app::EditState::AwaitingYaml,
        })),
        ];
        for (label, modal) in opens {
            app.ui.open(modal);
            let answers = [
                app.ui.takeover().is_some(),
                app.ui.command_input().is_some(),
            ];
            assert_eq!(
                answers.iter().filter(|a| **a).count(),
                1,
                "{label}: exactly one modal accessor may answer",
            );
        }
    }
}

/// `entries()` is what `validate()` walks for reserved-chord and duplicate
/// checks, and it used to be a THIRD hand-written list parallel to the struct
/// fields and to `op_override` — so a binding added to those two and missed
/// here escaped both checks, in the function whose doc says the point is
/// "a startup error, not a mystery". It is derived now; this pins that.
#[test]
fn every_bindable_operation_reaches_the_validator() {
    use crate::kube::protocol::OperationKind as Op;

    // Give every bindable operation an override, then assert each one shows
    // up by name in the list `validate` sees.
    let keys = KeysConfig {
        describe: Some(KeyCombo::plain('1')),
        yaml: Some(KeyCombo::plain('2')),
        logs: Some(KeyCombo::plain('3')),
        previous_logs: Some(KeyCombo::plain('4')),
        shell: Some(KeyCombo::plain('5')),
        restart: Some(KeyCombo::plain('6')),
        scale: Some(KeyCombo::plain('7')),
        delete: Some(KeyCombo::plain('8')),
        force_kill: Some(KeyCombo::plain('9')),
        port_forward: Some(KeyCombo::plain('A')),
        show_node: Some(KeyCombo::plain('B')),
        node_shell: Some(KeyCombo::plain('C')),
        decode_secret: Some(KeyCombo::plain('D')),
        trigger_cron_job: Some(KeyCombo::plain('E')),
        toggle_suspend_cron_job: Some(KeyCombo::plain('F')),
        ..Default::default()
    };

    let seen: Vec<&str> = keys.entries().into_iter().map(|(n, _)| n).collect();
    for op in Op::BUILT_IN {
        let Some(name) = op.config_key() else { continue };
        assert!(
            seen.contains(&name),
            "{op:?} is bindable as `keys.{name}` but never reaches validate(): {seen:?}",
        );
    }
}

/// The view starts one shed level in: source tag and message, no timestamp.
///
/// Not two independent flags — `t` cycles ONE level, so the config names a
/// point on that cycle. Two booleans were four combinations for three
/// reachable states, and the fourth (a timestamp with no attribution) was
/// something a config could ask for and no keypress could produce.
///
/// The reasons for the level are not symmetric, which is why this is pinned:
/// the kubelet stamp is 31 of ~97 columns duplicating one the service prints
/// itself, while a source tag only EXISTS on a multi-source stream and is
/// then the one thing saying which replica spoke.
#[test]
fn the_log_view_starts_on_source_and_message() {
    let cfg = crate::app::LogConfig::default();
    assert_eq!(cfg.default_fields, crate::app::LogFields::Source);
    assert_eq!(cfg.initial_shed(), 1);
    use crate::kube::protocol::Role;
    assert!(!Role::Timestamp.shown(cfg.initial_shed()), "the stamp is shed first");
    assert!(Role::Source.shown(cfg.initial_shed()), "attribution survives it");
}

/// Every level of the cycle has a name a config can write. Adding a role to
/// `SHED_ORDER` creates a state, and this fails until someone names it —
/// which is the only thing stopping the config from being a stale parallel
/// list of the roles.
#[test]
fn log_fields_covers_every_shed_level() {
    use crate::app::LogFields;
    use crate::kube::protocol::Role;
    let named = [LogFields::All, LogFields::Source, LogFields::Message];
    assert_eq!(
        named.len(),
        Role::levels(),
        "a role was added to SHED_ORDER without naming the level it creates",
    );
    // …and the names map onto distinct, ordered levels.
    let mut levels: Vec<usize> = named.iter().map(|f| f.shed()).collect();
    let before = levels.clone();
    levels.sort_unstable();
    levels.dedup();
    assert_eq!(levels.len(), named.len(), "two names for one level");
    assert_eq!(before, levels, "the names are not in shed order");
}

/// The README's sample config must PARSE. Every config struct carries
/// `deny_unknown_fields` and `load_config` returns `Err` rather than falling
/// back to defaults — deliberately — so one stale key in the documented
/// sample is not a doc nit, it is an app that refuses to start for anyone who
/// copies it. The README carried `ui.pageScrollLines` for two months after
/// the Viewport rework deleted the concept.
///
/// The prior version of this test fed only the `keys:` and `daemon:` blocks,
/// so it pinned "the documented config shape" without ever parsing the `ui:`
/// block that had the dead key in it.
#[test]
fn the_readme_sample_config_actually_parses() {
    let readme = include_str!("../../../README.md");
    let block = readme
        .split("```yaml")
        .nth(1)
        .and_then(|b| b.split("```").next())
        .expect("README has a yaml config block");
    // The file is rooted at `k9rs:`; `load_section::<AppConfig>("")` reads
    // that section, so the test parses the same shape the app does.
    #[derive(serde::Deserialize)]
    struct Root {
        k9rs: AppConfig,
    }
    let cfg = serde_yaml::from_str::<Root>(block)
        .unwrap_or_else(|e| panic!("the documented config is rejected by the app: {e}\n{block}"))
        .k9rs;
    // …and the values it advertises are the ones the app actually defaults to,
    // where it claims to be showing defaults.
    assert_eq!(cfg.ui.logs.tail_lines, LogConfig::default().tail_lines);
    assert_eq!(cfg.ui.logs.default_fields, LogConfig::default().default_fields);
}

// ---------------------------------------------------------------------------
// delta_tracker_tests (mirrored out of types.rs)
// ---------------------------------------------------------------------------

    use crate::kube::protocol::{ObjectKey, RowChange};
    use crate::kube::resources::row::{CellValue, ResourceRow};

    fn row(name: &str, val: &str) -> ResourceRow {
        ResourceRow {
            name: name.into(),
            namespace: Some("ns".into()),
            cells: vec![CellValue::Text(val.into())],
            ..Default::default()
        }
    }
    fn key(name: &str) -> ObjectKey {
        ObjectKey::new("ns".to_string(), name.to_string())
    }

    /// E4: a metadata-only upsert (identical cells) must NOT flash — kube
    /// heartbeat churn would otherwise light the table permanently. A real
    /// change flashes; an insert doesn't (matches the baseline path);
    /// removes clear their state.
    #[test]
    fn apply_changes_flashes_only_genuine_changes() {
        let mut d = DeltaTracker::new();
        d.rebaseline(&[row("a", "1")]);
        assert!(d.changed_rows().is_empty(), "baseline seeds hashes, no flash");

        d.apply_changes(&[RowChange::Upsert(row("a", "1"))]); // heartbeat
        assert!(d.changed_rows().is_empty(), "identical cells: no flash");

        d.apply_changes(&[RowChange::Upsert(row("a", "2"))]); // real change
        assert!(d.changed_rows().contains_key(&key("a")));

        d.apply_changes(&[RowChange::Upsert(row("new", "1"))]); // insert
        assert!(!d.changed_rows().contains_key(&key("new")), "inserts don't flash");

        d.apply_changes(&[RowChange::Remove(key("a"))]);
        assert!(!d.changed_rows().contains_key(&key("a")), "remove clears state");
    }

    /// Cross-recovery continuity: hashes survive a re-baseline, so a row
    /// that changed while a watcher was down flashes after the recovery
    /// baseline — and unchanged rows do NOT mass-flash.
    #[test]
    fn rebaseline_keeps_continuity_without_mass_flash() {
        let mut d = DeltaTracker::new();
        d.rebaseline(&[row("a", "1"), row("b", "1")]);
        d.rebaseline(&[row("a", "1"), row("b", "2")]); // recovery baseline
        assert!(!d.changed_rows().contains_key(&key("a")), "unchanged: quiet");
        assert!(d.changed_rows().contains_key(&key("b")), "changed-during-gap: flash");
    }

// ---------------------------------------------------------------------------
// key_combo_tests (mirrored out of types.rs)
// ---------------------------------------------------------------------------


    fn parse(s: &str) -> Result<KeyCombo, String> {
        KeyCombo::try_from(s.to_string())
    }

    #[test]
    fn parses_plain_ctrl_and_case() {
        assert_eq!(parse("l"), Ok(KeyCombo::plain('l')));
        assert_eq!(parse("L"), Ok(KeyCombo::plain('L')));
        assert_eq!(parse("-"), Ok(KeyCombo::plain('-')), "a lone dash is a character");
        assert_eq!(parse(" "), Ok(KeyCombo::plain(' ')), "space is a character");
        assert_eq!(parse("ctrl-l"), Ok(KeyCombo { ctrl: true, ch: 'l' }));
        // Ctrl chords arrive lowercase from the terminal — an uppercase
        // spec is normalized so it can actually match.
        assert_eq!(parse("Ctrl-L"), Ok(KeyCombo { ctrl: true, ch: 'l' }));
        assert_eq!(parse("C-x"), Ok(KeyCombo { ctrl: true, ch: 'x' }));
        assert_eq!(parse(" ctrl-l "), Ok(KeyCombo { ctrl: true, ch: 'l' }), "trimmed");
    }

    #[test]
    fn rejects_malformed_specs() {
        for bad in ["", "ctrl-", "meta-l", "ll", "ctrl-ll", "alt-x"] {
            assert!(parse(bad).is_err(), "'{bad}' must be rejected");
        }
    }

    #[test]
    fn validate_rejects_reserved_and_duplicate_chords() {
        // Reserved: a binding an earlier dispatch layer always consumes.
        let keys = KeysConfig {
            logs: Some(KeyCombo { ctrl: true, ch: 'r' }),
            ..Default::default()
        };
        let err = keys.validate().unwrap_err();
        assert!(err.contains("keys.logs") && err.contains("reserved"), "{err}");

        // Duplicate chords across entries.
        let keys = KeysConfig {
            col_right: Some(KeyCombo::plain('x')),
            logs: Some(KeyCombo::plain('x')),
            ..Default::default()
        };
        let err = keys.validate().unwrap_err();
        assert!(err.contains("already bound"), "{err}");

        // The user's real shape is fine.
        let keys = KeysConfig {
            col_left: Some(KeyCombo::plain('h')),
            col_right: Some(KeyCombo::plain('l')),
            logs: Some(KeyCombo { ctrl: true, ch: 'l' }),
            ..Default::default()
        };
        assert!(keys.validate().is_ok());
    }

    #[test]
    fn op_key_override_replaces_default() {
        use crate::kube::protocol::OperationKind as Op;
        let mut keys = KeysConfig::default();
        // Default: descriptor key as a plain chord.
        assert_eq!(keys.op_key(&Op::StreamLogs), Some(KeyCombo::plain('l')));
        // Override REPLACES it (the default char is no longer the binding).
        keys.logs = Some(KeyCombo { ctrl: true, ch: 'l' });
        assert_eq!(keys.op_key(&Op::StreamLogs), Some(KeyCombo { ctrl: true, ch: 'l' }));
        // Structural-default ops have no chord unless the user adds one.
        assert_eq!(keys.op_key(&Op::Delete), None);
        keys.delete = Some(KeyCombo::plain('x'));
        assert_eq!(keys.op_key(&Op::Delete), Some(KeyCombo::plain('x')));
        // TakeoverOp-defined ops are never bindable here.
        assert_eq!(keys.op_key(&Op::Custom("z".into())), None);
    }

    #[test]
    fn labels_render_ctrl_and_shift() {
        assert_eq!(KeyCombo::plain('l').label(), "l");
        assert_eq!(KeyCombo::plain('L').label(), "Shift-l");
        assert_eq!(KeyCombo { ctrl: true, ch: 'l' }.label(), "Ctrl-l");
    }

    /// Pins the documented config shape (what `load_section` hands to
    /// AppConfig after stripping the `k9rs:` root) — INCLUDING a sibling
    /// `daemon:` section, which the README documents in the same file
    /// and must not be rejected as an unknown field.
    #[test]
    fn app_config_parses_keys_section() {
        let yaml = "keys:\n  colLeft: h\n  colRight: l\n  colFirst: \"0\"\n  colLast: \"$\"\n  namespaceAll: \")\"\n  logs: ctrl-l\n\
                    daemon:\n  watcherPageSize: 500\n";
        let cfg: AppConfig = serde_yaml::from_str(yaml).expect("keys + daemon sections parse");
        assert_eq!(cfg.keys.col_left, Some(KeyCombo::plain('h')));
        assert_eq!(cfg.keys.col_right, Some(KeyCombo::plain('l')));
        assert_eq!(cfg.keys.col_first, Some(KeyCombo::plain('0')));
        assert_eq!(cfg.keys.col_last, Some(KeyCombo::plain('$')));
        // Shift-0 (`)`) restores switch-to-all-namespaces once `0` is colFirst.
        assert_eq!(cfg.keys.namespace_all, Some(KeyCombo::plain(')')));
        // `0` / `$` / `)` are not reserved chords, so the binding validates.
        assert!(cfg.keys.validate().is_ok(), "vim 0/$ column jumps + namespaceAll must validate");
        assert_eq!(cfg.keys.logs, Some(KeyCombo { ctrl: true, ch: 'l' }));
        // A typo'd binding name is a load-time error (deny_unknown_fields),
        // not a silently ignored key.
        assert!(serde_yaml::from_str::<AppConfig>("keys:\n  colleft: h\n").is_err());
        // A malformed chord is too.
        assert!(serde_yaml::from_str::<AppConfig>("keys:\n  logs: meta-l\n").is_err());
        // And so is a typo inside the daemon section — the TUI's loud
        // startup validation covers the whole file.
        assert!(serde_yaml::from_str::<AppConfig>("daemon:\n  watcherPagesize: 5\n").is_err());
    }

// ---------------------------------------------------------------------------
// context_switch_tests (mirrored out of types.rs)
// ---------------------------------------------------------------------------

    use crate::kube::protocol::ContextName;

    #[test]
    fn stable_accepts_new_switches() {
        let s = ContextSwitchState::Stable;
        assert!(s.is_stable());
    }

    #[test]
    fn requested_rejects_new_switches() {
        let s = ContextSwitchState::Requested(ContextName::new("prod").unwrap());
        assert!(!s.is_stable());
    }

    #[test]
    fn in_flight_rejects_new_switches() {
        let s = ContextSwitchState::InFlight(ContextName::new("prod").unwrap());
        assert!(!s.is_stable());
    }

    #[test]
    fn target_names_the_context_in_flight() {
        assert_eq!(ContextSwitchState::Stable.target(), None);
        assert_eq!(
            ContextSwitchState::Requested(ContextName::new("prod").unwrap()).target().map(|c| c.as_str()),
            Some("prod")
        );
        assert_eq!(
            ContextSwitchState::InFlight(ContextName::new("prod").unwrap()).target().map(|c| c.as_str()),
            Some("prod")
        );
    }

    #[test]
    fn take_requested_transitions_requested_to_in_flight_keeping_target() {
        let mut s = ContextSwitchState::Requested(ContextName::new("prod").unwrap());
        assert_eq!(s.take_requested().as_ref().map(|c| c.as_str()), Some("prod"));
        // InFlight carries the target forward, so the failure path can name it.
        assert_eq!(s, ContextSwitchState::InFlight(ContextName::new("prod").unwrap()));
    }

    #[test]
    fn take_requested_is_noop_from_stable() {
        let mut s = ContextSwitchState::Stable;
        assert!(s.take_requested().is_none());
        assert_eq!(s, ContextSwitchState::Stable);
    }

    #[test]
    fn take_requested_is_noop_from_in_flight() {
        let mut s = ContextSwitchState::InFlight(ContextName::new("prod").unwrap());
        assert!(s.take_requested().is_none());
        assert_eq!(s, ContextSwitchState::InFlight(ContextName::new("prod").unwrap()));
    }

    #[test]
    fn settle_from_any_state() {
        for mut s in [
            ContextSwitchState::Stable,
            ContextSwitchState::Requested(ContextName::new("prod").unwrap()),
            ContextSwitchState::InFlight(ContextName::new("prod").unwrap()),
        ] {
            s.settle();
            assert_eq!(s, ContextSwitchState::Stable);
        }
    }

    #[test]
    fn settle_resolves_a_failed_switch() {
        // The failure edge: an in-flight switch whose connection never comes
        // up settles back to Stable, so the next switch is accepted (no lock).
        let mut s = ContextSwitchState::Requested(ContextName::new("does-not-exist").unwrap());
        s.take_requested();
        assert!(!s.is_stable(), "in flight while connecting");
        s.settle(); // ConnectionFailed for the target
        assert!(s.is_stable(), "a failed switch must not stay stuck InFlight");
    }

    #[test]
    fn full_lifecycle() {
        // Stable → Requested(name) → InFlight(name) → Stable
        let mut s = ContextSwitchState::Stable;
        assert!(s.is_stable());
        s = ContextSwitchState::Requested(ContextName::new("prod").unwrap());
        assert!(!s.is_stable());
        let taken = s.take_requested();
        assert_eq!(taken.as_ref().map(|c| c.as_str()), Some("prod"));
        assert_eq!(s, ContextSwitchState::InFlight(ContextName::new("prod").unwrap()));
        s.settle();
        assert!(s.is_stable());
    }

// ---------------------------------------------------------------------------
// The README is a hand-written copy of an exhaustive source
// ---------------------------------------------------------------------------

/// The README's "Rebindable operations:" list matches the code.
///
/// Fifteen names, written out in prose beside an exhaustive `config_key`
/// match — the "one decision, many copies" shape, with the copy in a file no
/// compiler reads. Adding an operation forces a `config_key` arm (the match
/// is exhaustive) but nothing forces the README, so this list drifts the
/// moment anyone adds one. The in-app surfaces already project from one
/// declaration; the docs were the last copy.
#[test]
fn the_readme_lists_exactly_the_rebindable_operations() {
    use crate::kube::protocol::OperationKind;

    let readme = include_str!("../../../README.md");
    let marker = "# Rebindable operations:";
    let at = readme.find(marker).expect("the README documents the rebindable set");
    // The sentence runs to its full stop, wrapped over comment lines.
    let tail = &readme[at + marker.len()..];
    let sentence = &tail[..tail.find('.').expect("the list ends in a full stop")];
    let documented: std::collections::BTreeSet<String> = sentence
        .split(',')
        .map(|w| w.trim().trim_start_matches('#').trim().to_string())
        .filter(|w| !w.is_empty())
        .collect();

    let actual: std::collections::BTreeSet<String> = OperationKind::BUILT_IN
        .iter()
        .filter_map(|op| op.config_key())
        .map(str::to_string)
        .collect();

    assert_eq!(
        documented, actual,
        "the README's rebindable list and `OperationKind::config_key` disagree \
         — documented-but-absent: {:?}; real-but-undocumented: {:?}",
        documented.difference(&actual).collect::<Vec<_>>(),
        actual.difference(&documented).collect::<Vec<_>>(),
    );
}
