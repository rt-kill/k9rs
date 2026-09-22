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
    use crate::app::{App, Modal, Overlay};

    #[test]
    fn opening_a_modal_displaces_whatever_was_open() {
        // These were four independent fields cleared by hand at 53 sites;
        // any path that forgot one left a modal buried under another,
        // invisible until the top one closed. Now it is an assignment.
        let mut app = App::new_for_test();
        app.ui.open(Modal::Command { input: ":pods".into(), history_index: None });
        assert!(app.ui.command_input().is_some());

        app.ui.open(Modal::Overlay(Overlay::ContainerSelect {
            target: crate::kube::protocol::ObjectRef::new(
                crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
                "p".to_string(),
                crate::kube::protocol::Namespace::Named("default".to_string()),
            ),
            choices: vec![crate::app::ContainerChoice {
                label: "web".into(),
                container: crate::kube::protocol::LogContainer::Named("web".into()),
            }],
            selected: 0,
            action: crate::app::ContainerAction::Logs,
        }));
        assert!(app.ui.overlay().is_some());
        assert!(
            app.ui.command_input().is_none(),
            "the command prompt cannot still be open behind the overlay",
        );

        app.ui.close_modal();
        assert!(!app.ui.is_modal());
        assert!(app.ui.overlay().is_none());
    }

    #[test]
    fn exactly_one_accessor_answers_at_a_time() {
        // The accessors partition the slot: whatever is open, every OTHER
        // accessor must say no. This is the property the four-field version
        // could not state, let alone hold.
        let mut app = App::new_for_test();
        let opens: Vec<(&str, Modal)> = vec![
            ("command", Modal::Command { input: String::new(), history_index: None }),
            ("overlay", Modal::Overlay(Overlay::ContainerSelect {
                target: crate::kube::protocol::ObjectRef::new(
                    crate::app::nav::rid(crate::kube::resource_def::BuiltInKind::Pod),
                    "p".to_string(),
                    crate::kube::protocol::Namespace::Named("default".to_string()),
                ),
                choices: vec![crate::app::ContainerChoice {
                    label: "web".into(),
                    container: crate::kube::protocol::LogContainer::Named("web".into()),
                }],
                selected: 0,
                action: crate::app::ContainerAction::Logs,
            })),
        ];
        for (label, modal) in opens {
            app.ui.open(modal);
            let answers = [
                app.ui.overlay().is_some(),
                app.ui.confirm_dialog().is_some(),
                app.ui.form_dialog().is_some(),
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
