use super::*;
use k9rs_proto::protocol::{Namespace, ObjectRef, ResourceId};
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};

fn make_key(code: KeyCode) -> KeyEvent {
    KeyEvent {
        code,
        modifiers: KeyModifiers::NONE,
        kind: KeyEventKind::Press,
        state: KeyEventState::NONE,
    }
}

/// Reset the nav stack to a test-only root list element for `kind`
/// (parked stream — no session, no daemon).
fn reset_to_kind(app: &mut App, kind: k9rs_proto::resource_def::BuiltInKind) {
    reset_to_rid(app, kind.to_resource_id());
}

fn reset_to_rid(app: &mut App, resource: k9rs_proto::protocol::ResourceId) {
    let label = resource.short_label().to_lowercase();
    let root = crate::element::Element::ResourceList(
        crate::element::ResourceList::open_for_test(
            crate::element::QuerySpec {
                rid: resource,
                namespace: k9rs_proto::protocol::Namespace::All,
                filter: None,
            },
            &app.kube.metrics,
            label,
        ),
    );
    app.nav.reset(root);
}

/// Push a content element (yaml/describe) as the top view.
fn push_content_element(app: &mut App, yaml: bool) {
    let target = ObjectRef::new(
        k9rs_proto::protocol::ResourceId::BuiltIn(k9rs_proto::resource_def::BuiltInKind::Pod),
        String::new(),
        k9rs_proto::protocol::Namespace::from_user_command(""),
    );
    let spec = if yaml {
        crate::element::ContentSpec::Yaml(target)
    } else {
        crate::element::ContentSpec::Describe(target)
    };
    app.nav.push(crate::element::Element::ContentView(
        crate::element::ContentView::new(spec, crate::ContentViewState::default(), crate::element::ContentPhase::Ready),
    ));
}

/// Push a log-session element (parked stream) as the top view.
fn push_log_element(app: &mut App) {
    app.nav.push(crate::element::Element::LogSession(Box::new(
        crate::element::LogSession::for_test(k9rs_proto::protocol::LogTarget::one("test-pod".to_string()), k9rs_proto::protocol::LogContainer::Default),
    )));
}

/// Push the `?` view the way `Action::Help` does.
fn push_help_element(app: &mut App) {
    let class = crate::keymap::ViewClass::of(app.nav.top());
    let caps = Some(app.current_capabilities());
    let store =
        app.core.seed_keybindings(caps, app.config.keys, class, app.nav.top().markable());
    let rid = k9rs_proto::local_types::LocalResourceKind::Keybinding.to_resource_id();
    app.nav.push(crate::element::Element::ResourceList(
        crate::element::ResourceList::client(
            crate::element::QuerySpec {
                rid,
                namespace: k9rs_proto::protocol::Namespace::All,
                filter: None,
            },
            store,
            &app.kube.metrics,
            "keys".to_string(),
        ),
    ));
}

fn make_resource_app() -> App {
    // The test root is already a pods list element — nothing to preload:
    // `current_capabilities()` computes straight from the typed rid.
    App::new_for_test()
}

fn make_ctrl_key(code: KeyCode) -> KeyEvent {
    KeyEvent {
        code,
        modifiers: KeyModifiers::CONTROL,
        kind: KeyEventKind::Press,
        state: KeyEventState::NONE,
    }
}

fn make_shift_key(code: KeyCode) -> KeyEvent {
    KeyEvent {
        code,
        modifiers: KeyModifiers::SHIFT,
        kind: KeyEventKind::Press,
        state: KeyEventState::NONE,
    }
}

#[test]
fn test_q_in_resource_view_quits_when_no_filter() {
    let app = make_resource_app();
    // No active filter, so `q` should quit.
    let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
    assert!(matches!(action, Some(Action::Quit)));
}

#[test]
fn test_ctrl_c_quit() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('c')));
    assert!(matches!(action, Some(Action::Quit)));
}

#[test]
fn test_help_key() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('?')));
    assert!(matches!(action, Some(Action::Help)));
}

#[test]
fn test_resource_view_navigation() {
    let app = make_resource_app();
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('j'))),
        Some(Action::NextItem)
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('k'))),
        Some(Action::PrevItem)
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Enter)),
        Some(Action::Enter)
    ));
}

#[test]
fn test_resource_view_describe_yaml() {
    let app = make_resource_app();
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('d'))),
        Some(Action::Describe)
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('y'))),
        Some(Action::Yaml)
    ));
}

#[test]
fn test_pods_logs_key() {
    // Logs are plain 'l' by default (k9s parity); column movement is
    // arrow-keys-only unless the user binds keys.colLeft/colRight.
    let mut app = make_resource_app();
    reset_to_kind(&mut app, k9rs_proto::resource_def::BuiltInKind::Pod);
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('l'))),
        Some(Action::Logs)
    ));
    // 'h' is unbound by default; arrows move the column cursor.
    assert!(handle_key_event(&app, make_key(KeyCode::Char('h'))).is_none());
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Left)),
        Some(Action::ColLeft)
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Right)),
        Some(Action::ColRight)
    ));
}

#[test]
fn test_workload_types_have_logs() {
    use k9rs_proto::resource_def::BuiltInKind::*;
    for kind in [Deployment, StatefulSet, DaemonSet, ReplicaSet, Job, CronJob] {
        let mut app = make_resource_app();
        reset_to_kind(&mut app, kind);
        assert!(
            matches!(handle_key_event(&app, make_key(KeyCode::Char('l'))), Some(Action::Logs)),
            "Expected Logs action for {:?}",
            kind,
        );
    }
}

#[test]
fn test_non_workload_no_logs() {
    use k9rs_proto::resource_def::BuiltInKind::*;
    for kind in [Service, Node, Namespace, ConfigMap, Secret] {
        let mut app = make_resource_app();
        reset_to_kind(&mut app, kind);
        assert!(
            !matches!(handle_key_event(&app, make_key(KeyCode::Char('l'))), Some(Action::Logs)),
            "Expected no Logs action for {:?}",
            kind,
        );
    }
}

/// User rebinds: `keys.logs: ctrl-l` MOVES the binding (plain `l` no
/// longer streams logs), and `keys.colLeft/colRight: h/l` add vim-style
/// column movement that shadows any op default on the same chord.
#[test]
fn test_key_rebinding_moves_and_shadows() {
    use crate::KeyCombo;
    let mut app = make_resource_app();
    reset_to_kind(&mut app, k9rs_proto::resource_def::BuiltInKind::Pod);
    app.config.keys.logs = Some(KeyCombo::ctrl('l'));
    app.config.keys.col_left = Some(KeyCombo::plain('h'));
    app.config.keys.col_right = Some(KeyCombo::plain('l'));

    assert!(matches!(
        handle_key_event(&app, make_ctrl_key(KeyCode::Char('l'))),
        Some(Action::Logs)
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('l'))),
        Some(Action::ColRight)
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('h'))),
        Some(Action::ColLeft)
    ));

    // Override alone (no col binding claiming `l`): the old default is
    // STILL unbound — a rebind replaces, never duplicates.
    app.config.keys.col_right = None;
    assert!(handle_key_event(&app, make_key(KeyCode::Char('l'))).is_none());
}

/// `keys.colFirst: "0"` / `keys.colLast: "$"` jump the column cursor to
/// the first / last column (vim 0/$). Matched before the structural
/// arms, so `0` shadows its default (switch to all namespaces).
#[test]
fn test_column_jump_bindings_shadow_defaults() {
    use crate::KeyCombo;
    let mut app = make_resource_app();
    reset_to_kind(&mut app, k9rs_proto::resource_def::BuiltInKind::Pod);

    // Unbound by default: `0` still switches to all namespaces; `$` is nothing.
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('0'))),
        Some(Action::SwitchNamespace(k9rs_proto::protocol::Namespace::All))
    ));
    assert!(handle_key_event(&app, make_key(KeyCode::Char('$'))).is_none());

    app.config.keys.col_first = Some(crate::KeyCombo::plain('0'));
    app.config.keys.col_last = Some(KeyCombo::plain('$'));

    // Now they move the column cursor, shadowing the all-namespaces default.
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('0'))),
        Some(Action::ColFirst)
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('$'))),
        Some(Action::ColLast)
    ));

    // With `0` taken by colFirst, `namespaceAll` restores the shadowed
    // switch-to-all-namespaces action on another key (Shift-0 = `)`).
    app.config.keys.namespace_all = Some(KeyCombo::plain(')'));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char(')'))),
        Some(Action::SwitchNamespace(k9rs_proto::protocol::Namespace::All))
    ));
    // …and `0` still moves the column cursor (the rebind does not reshadow it).
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('0'))),
        Some(Action::ColFirst)
    ));
}

/// An exact-chord miss must not alias: with logs on plain `l`, Ctrl-L is
/// nothing (the old global fired logs off the hovered row from ANY view
/// state — it is gone in favor of the binding system).
#[test]
fn test_ctrl_chord_does_not_alias_bare_char() {
    let mut app = make_resource_app();
    reset_to_kind(&mut app, k9rs_proto::resource_def::BuiltInKind::Pod);
    assert!(handle_key_event(&app, make_ctrl_key(KeyCode::Char('l'))).is_none());
}

#[test]
fn test_confirm_dialog_keys() {
    let mut app = App::new_for_test();
    app.nav.push(crate::element::Element::Confirm(Box::new(crate::ConfirmDialog {
        message: "Are you sure?".to_string(),
        pending: crate::PendingAction::Single {
            op: crate::SingleOp::Delete,
            target: ObjectRef::new(
                ResourceId::BuiltIn(k9rs_proto::resource_def::BuiltInKind::Pod),
                "test",
                Namespace::from_user_command("default"),
            ),
        },
        action_label: "Delete".to_string(),
        focused: crate::ConfirmFocus::Cancel,
    })));

    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('y'))),
        Some(Action::Confirm)
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('n'))),
        Some(Action::Cancel)
    ));

    // Non-sticky: any unrecognized key cancels (cancel is always the safe
    // direction), so a stray keystroke can't trap the user behind the modal.
    // In particular a spurious 'r' (a stale terminal response misparsed as a
    // key) opens the restart dialog, and the next key dismisses it instead of
    // being swallowed. A bare ':' likewise dismisses rather than being eaten.
    for stray in [KeyCode::Char('r'), KeyCode::Char(':'), KeyCode::Char('z'), KeyCode::Esc] {
        assert!(
            matches!(handle_key_event(&app, make_key(stray)), Some(Action::Cancel)),
            "expected stray key {:?} to cancel the confirm dialog",
            stray,
        );
    }
    // Focus-movement keys are not a cancel — they toggle the selected button.
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Tab)),
        Some(Action::ToggleDialogButton)
    ));
}

#[test]
fn test_ctrl_d_delete() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('d')));
    assert!(matches!(action, Some(Action::Delete)));
}

#[test]
fn test_ctrl_r_refresh() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('r')));
    assert!(matches!(action, Some(Action::Refresh)));
}

#[test]
fn test_resource_view_port_forward() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('f')));
    assert!(matches!(action, Some(Action::ShowPortForwards)));
    let action2 = handle_key_event(&app, make_key(KeyCode::Char('F')));
    assert!(matches!(action2, Some(Action::PortForward)));
}

#[test]
fn test_restart_on_deployments() {
    let mut app = make_resource_app();
    reset_to_kind(&mut app, k9rs_proto::resource_def::BuiltInKind::Deployment);
    let action = handle_key_event(&app, make_key(KeyCode::Char('r')));
    assert!(matches!(action, Some(Action::Restart)));
}

#[test]
fn test_q_goes_back_in_detail_view() {
    let mut app = App::new_for_test();
    push_content_element(&mut app, true);
    let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_log_view_s_toggles_follow() {
    let mut app = App::new_for_test();
    push_log_element(&mut app);
    let action = handle_key_event(&app, make_key(KeyCode::Char('s')));
    assert!(matches!(action, Some(Action::ToggleLogFollow)));
}

#[test]
fn test_log_view_t_toggles_timestamps() {
    let mut app = App::new_for_test();
    push_log_element(&mut app);
    let action = handle_key_event(&app, make_key(KeyCode::Char('t')));
    assert!(matches!(action, Some(Action::ToggleLogTimestamps)));
}

#[test]
fn test_log_view_shift_c_clears_logs() {
    let mut app = App::new_for_test();
    push_log_element(&mut app);
    let action = handle_key_event(&app, make_shift_key(KeyCode::Char('C')));
    assert!(matches!(action, Some(Action::ClearLogs)));
}

/// Help is a nav ELEMENT now, so leaving it is the ordinary pop — there is
/// no help-specific key path left to get wrong. Both `q` and Esc reach it
/// through the table handler, like every other drilled view.
#[test]
fn test_q_goes_back_in_help_view() {
    let mut app = App::new_for_test();
    push_help_element(&mut app);
    assert!(app.nav.top().is_keybindings_view());
    // `ClearFilter` IS the pop in a table view (`q` drilled) — the same
    // action every other drilled table uses, which is the point.
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('q'))),
        Some(Action::ClearFilter)
    ));
}

#[test]
fn test_esc_emits_clear_filter_in_resource_view() {
    let app = make_resource_app();
    // Esc always emits; whether it DOES anything is the action layer's call
    // ("nothing to pop at the root" lives there). Swallowing it here used to
    // hide the key from `select_gate`, which is what stopped Esc from ever
    // reaching select mode.
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::ClearFilter)));
}

#[test]
fn test_esc_goes_back_in_detail_view() {
    let mut app = App::new_for_test();
    push_content_element(&mut app, false);
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_esc_goes_back_in_log_view() {
    let mut app = App::new_for_test();
    push_log_element(&mut app);
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_esc_goes_back_in_help_view() {
    let mut app = App::new_for_test();
    push_help_element(&mut app);
    assert!(matches!(handle_key_event(&app, make_key(KeyCode::Esc)), Some(Action::ClearFilter)));
}

/// Help is a TABLE, so everything a table can do it can do: `/` opens the
/// filter, `j`/`k` move the cursor, `c` copies. That is the whole point of
/// modelling it as a resource rather than as a bespoke overlay.
#[test]
fn help_is_an_ordinary_table_view() {
    let mut app = App::new_for_test();
    push_help_element(&mut app);
    for (key, want) in [
        (KeyCode::Char('/'), Action::Filter(String::new())),
        (KeyCode::Char('j'), Action::NextItem),
        (KeyCode::Char('k'), Action::PrevItem),
        (KeyCode::Char('c'), Action::Copy),
    ] {
        assert_eq!(handle_key_event(&app, make_key(key)), Some(want), "{key:?}");
    }
    assert_eq!(
        crate::keymap::ViewClass::of(app.nav.top()),
        crate::keymap::ViewClass::Table,
        "help dispatches as a table, so it needs no class of its own",
    );
}

/// It floats: the view it documents stays on screen behind it.
#[test]
fn help_declares_itself_a_floating_view() {
    let mut app = App::new_for_test();
    assert!(!app.nav.top().floats(), "an ordinary list fills the frame");
    push_help_element(&mut app);
    assert!(app.nav.top().floats());
}

#[test]
fn test_q_goes_back_in_container_select() {
    let mut app = App::new_for_test();
    app.nav.push(crate::element::Element::Picker(Box::new(
        crate::ContainerPicker {
            target: k9rs_proto::protocol::ObjectRef::new(
                k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
                "p".to_string(),
                k9rs_proto::protocol::Namespace::Named("default".to_string()),
            ),
            choices: vec![
                crate::ContainerChoice {
                    label: "web".into(),
                    container: k9rs_proto::protocol::LogContainer::Named("web".into()),
                },
                crate::ContainerChoice {
                    label: "sidecar".into(),
                    container: k9rs_proto::protocol::LogContainer::Named("sidecar".into()),
                },
            ],
            selected: 0,
            action: crate::ContainerAction::Logs,
        },
    )));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('q'))),
        Some(Action::Back)
    ));
    // The picker keeps its own cursor keys, and `Enter` picks.
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('j'))),
        Some(Action::NextItem)
    ));
    assert!(matches!(handle_key_event(&app, make_key(KeyCode::Enter)), Some(Action::Enter)));
    // …and like every dialog it owns the globals too, rather than letting
    // `:` open the command prompt behind it.
    assert!(app.nav.top().captures_all_input());
    assert!(handle_key_event(&app, make_key(KeyCode::Char(':'))).is_none());
}

#[test]
fn test_log_view_home_end() {
    let mut app = App::new_for_test();
    push_log_element(&mut app);
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('g'))),
        Some(Action::Home)
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('G'))),
        Some(Action::End)
    ));
}

#[test]
fn test_detail_view_home_end() {
    let mut app = App::new_for_test();
    push_content_element(&mut app, true);
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('g'))),
        Some(Action::Home)
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('G'))),
        Some(Action::End)
    ));
}

// ---------------------------------------------------------------------------
// Global ctrl-key bindings
// ---------------------------------------------------------------------------

#[test]
fn test_ctrl_e_toggle_header() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('e')));
    assert!(matches!(action, Some(Action::ToggleHeader)));
}

#[test]
fn test_ctrl_s_save_table() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('s')));
    assert!(matches!(action, Some(Action::SaveTable)));
}

#[test]
fn test_ctrl_a_show_aliases() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('a')));
    assert!(matches!(action, Some(Action::ShowAliases)));
}

#[test]
fn test_ctrl_w_toggle_wide() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('w')));
    assert!(matches!(action, Some(Action::ToggleWide)));
}

#[test]
fn test_ctrl_z_toggle_fault_filter() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('z')));
    assert!(matches!(action, Some(Action::ToggleFaultFilter)));
}

#[test]
fn test_ctrl_space_span_mark() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char(' ')));
    assert!(matches!(action, Some(Action::SpanMark)));
}

#[test]
fn test_ctrl_backslash_clear_marks() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('\\')));
    assert!(matches!(action, Some(Action::ClearMarks)));
}

// ---------------------------------------------------------------------------
// Resource view keys
// ---------------------------------------------------------------------------

#[test]
fn test_resource_view_tab_cycling() {
    let app = make_resource_app();
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Tab)),
        Some(Action::NextTab)
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::BackTab)),
        Some(Action::PrevTab)
    ));
}

#[test]
fn test_resource_view_toggle_mark() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char(' ')));
    assert!(matches!(action, Some(Action::ToggleMark)));
}

#[test]
fn test_resource_view_switch_namespace_all() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('0')));
    assert!(matches!(action, Some(Action::SwitchNamespace(k9rs_proto::protocol::Namespace::All))));
}

#[test]
fn test_resource_view_edit() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('e')));
    assert!(matches!(action, Some(Action::Edit)));
}

#[test]
fn test_resource_view_copy() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('c')));
    assert!(matches!(action, Some(Action::Copy)));
}

#[test]
fn test_resource_view_jump_to_owner() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('J')));
    assert!(matches!(action, Some(Action::JumpToOwner)));
}

#[test]
fn test_resource_view_used_by() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('U')));
    assert!(matches!(action, Some(Action::UsedBy)));
}

#[test]
fn test_resource_view_column_filter() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('~')));
    assert!(matches!(action, Some(Action::ColumnFilter)));
}

#[test]
fn test_resource_view_toggle_last_view() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('-')));
    assert!(matches!(action, Some(Action::ToggleLastView)));
}

#[test]
fn test_resource_view_toggle_sort_direction() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('O')));
    assert!(matches!(action, Some(Action::ToggleSortDirection)));
}

/// `N` and `A` name a column; they do not guess an index.
///
/// These three tests used to assert `Column(1)` for pods, `Column(0)` for
/// nodes and `Last` for age — baking in the positional rule, and passing for
/// the right answer for the wrong reason. The rule was wrong for any table
/// whose shape didn't match: Events is namespaced but its headers are
/// `[NAMESPACE, TYPE, …]`, so `N` sorted by event TYPE. The key emits a NAME
/// now, and resolution happens against the live headers — see
/// `semantic_sort_resolves_against_the_real_headers`.
#[test]
fn sort_keys_name_a_column_rather_than_guessing_an_index() {
    use crate::SortTarget;
    let mut app = make_resource_app();

    for kind in [
        k9rs_proto::resource_def::BuiltInKind::Pod,
        k9rs_proto::resource_def::BuiltInKind::Node,
        k9rs_proto::resource_def::BuiltInKind::Event,
    ] {
        reset_to_kind(&mut app, kind);
        assert_eq!(
            handle_key_event(&app, make_key(KeyCode::Char('N'))),
            Some(Action::Sort(SortTarget::Named(crate::actions::COL_NAME))),
            "{kind:?}: N names NAME regardless of scope",
        );
        assert_eq!(
            handle_key_event(&app, make_key(KeyCode::Char('A'))),
            Some(Action::Sort(SortTarget::Named(crate::actions::COL_AGE))),
            "{kind:?}: A names AGE regardless of width",
        );
    }
}

// ---------------------------------------------------------------------------
// Batch capture (marks ∩ present rows)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Detail view keys (Yaml/Describe)
// ---------------------------------------------------------------------------

fn make_yaml_app() -> App {
    let mut app = App::new_for_test();
    push_content_element(&mut app, true);
    app
}

#[test]
fn test_detail_view_search_next_prev() {
    let app = make_yaml_app();
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('n'))),
        Some(Action::SearchNext)
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('N'))),
        Some(Action::SearchPrev)
    ));
}

#[test]
fn test_detail_view_ctrl_d_page_down() {
    let app = make_yaml_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('d')));
    assert!(matches!(action, Some(Action::PageDown)));
}

#[test]
fn test_detail_view_ctrl_u_page_up() {
    let app = make_yaml_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('u')));
    assert!(matches!(action, Some(Action::PageUp)));
}

#[test]
fn test_detail_view_copy() {
    let app = make_yaml_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('c')));
    assert!(matches!(action, Some(Action::Copy)));
}

#[test]
fn test_detail_view_slash_starts_search() {
    let app = make_yaml_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('/')));
    assert!(matches!(action, Some(Action::SearchStart)));
}

// ---------------------------------------------------------------------------
// Log view keys
// ---------------------------------------------------------------------------

fn make_log_app() -> App {
    let mut app = App::new_for_test();
    push_log_element(&mut app);
    app
}

#[test]
fn test_log_view_toggle_wrap() {
    let app = make_log_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('w')));
    assert!(matches!(action, Some(Action::ToggleLogWrap)));
}

#[test]
fn test_log_view_log_range_digits() {
    use crate::actions::LogRange;
    let app = make_log_app();
    for d in ['1', '3', '6'] {
        assert!(
            matches!(
                handle_key_event(&app, make_key(KeyCode::Char(d))),
                Some(Action::LogRange(LogRange::Since(_)))
            ),
            "`{d}` asks for a duration",
        );
    }
    // '0' is back to the tail.
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('0'))),
        Some(Action::LogRange(LogRange::Tail))
    ));
    // '7' is the end of the ladder: everything. This is the state the old
    // `Option<String>` payload could not express, which is why the digits
    // capped the reachable history at 24h.
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('7'))),
        Some(Action::LogRange(LogRange::Head))
    ));
}

#[test]
fn test_log_view_copy() {
    let app = make_log_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('c')));
    assert!(matches!(action, Some(Action::Copy)));
}

#[test]
fn test_log_view_ctrl_s_save_logs() {
    // Ctrl+S in log view should produce SaveLogs, not the global SaveTable.
    let app = make_log_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('s')));
    assert!(matches!(action, Some(Action::SaveLogs)));
}

#[test]
fn test_resource_view_ctrl_s_save_table() {
    // Ctrl+S in resource view should produce SaveTable.
    let app = make_resource_app();
    let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('s')));
    assert!(matches!(action, Some(Action::SaveTable)));
}

#[test]
fn test_log_view_n_and_shift_n_unbound() {
    // n/N are NOT bound in log views: logs have no separate search (their `/`
    // grep IS the filter), and the old branch compared a logical line index to
    // a physical wrap-offset (broken under wrap). The binding was removed.
    let app = make_log_app();
    assert!(handle_key_event(&app, make_key(KeyCode::Char('n'))).is_none());
    assert!(handle_key_event(&app, make_key(KeyCode::Char('N'))).is_none());
}

#[test]
fn test_log_view_slash_starts_search() {
    let app = make_log_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('/')));
    assert!(matches!(action, Some(Action::SearchStart)));
}


/// `h`/`l` and the arrows pan the log horizontally, and they do it through
/// the SAME actions the tables use for their column cursor — one "move left
/// in this view" intent, answered by each element in its own units. That is
/// also why a user who rebound `colLeft`/`colRight` reaches the log pan with
/// no second config key.
#[test]
fn test_log_view_h_l_and_arrows_pan() {
    let app = make_log_app();
    for (key, want) in [
        (KeyCode::Char('h'), Action::ColLeft),
        (KeyCode::Char('l'), Action::ColRight),
        (KeyCode::Left, Action::ColLeft),
        (KeyCode::Right, Action::ColRight),
    ] {
        assert_eq!(handle_key_event(&app, make_key(key)), Some(want), "{key:?}");
    }
}

/// THE bug: Esc in a form did nothing. The typing handler claimed it by
/// calling `UiState::close_modal`, which clears the modal SLOT — and a form
/// has been a nav ELEMENT since `Modal::Form` was removed, so Esc cleared an
/// already-empty slot, the dialog stayed on screen, and OK was the only exit.
/// Nothing tested it, and `FormDialog::focused`'s own doc asserted the
/// opposite ("Esc cancels regardless of focus").
///
/// Esc must come back as `Action::Back` — the one undo path, the same pop
/// that leaves every other element.
#[test]
fn esc_in_a_form_pops_it() {
    let mut app = App::new_for_test();
    app.nav.push(form_element());
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Esc)),
        Some(Action::Back)
    ));
}

/// The other half of the same bug is the fall-through: Esc only reaches the
/// action layer if no input sink claims it first. The sink that could is
/// `modal_without_key_handler`, and a form does not live in the modal slot
/// at all — which is the same fact that made `close_modal()` a no-op.
///
/// `handle_form_dialog_key` itself cannot be called from a sync test: it
/// takes a `ClientSession`, which spawns onto a runtime, and no test in this
/// crate constructs one (tests never touch the daemon). So the `return
/// false` in its Esc arm is covered by this invariant plus its own comment,
/// not by a direct call.
#[test]
fn no_modal_sink_claims_a_forms_keys() {
    let mut app = App::new_for_test();
    app.nav.push(form_element());
    assert!(
        !app.ui.modal_without_key_handler(),
        "a form is a nav element, so no modal sink may swallow its Esc",
    );
}

/// Enter on Cancel means exactly what Esc means — it is not a second way
/// out with its own implementation.
#[test]
fn enter_on_cancel_pops_but_enter_on_ok_does_not() {
    let mut app = App::new_for_test();
    app.nav.push(form_element());

    app.nav.top_mut().form_dialog_mut().unwrap().focused = crate::FormFocus::Cancel;
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Enter)),
        Some(Action::Back)
    ));

    // On OK, Enter is the typing handler's business (it submits), so the
    // action layer must stay out of it.
    app.nav.top_mut().form_dialog_mut().unwrap().focused = crate::FormFocus::Ok;
    assert!(handle_key_event(&app, make_key(KeyCode::Enter)).is_none());
}

fn form_element() -> crate::element::Element {
    crate::element::Element::Form(Box::new(crate::FormDialog {
        submit: crate::FormSubmit::Scale,
        title: "Scale".to_string(),
        subtitle: String::new(),
        target: ObjectRef::new(
            ResourceId::BuiltIn(k9rs_proto::resource_def::BuiltInKind::Deployment),
            "api",
            Namespace::from_user_command("default"),
        ),
        fields: vec![crate::FormFieldState {
            name: "replicas".to_string(),
            label: "Replicas".to_string(),
            kind: crate::FormFieldKind::Number { min: 0, max: 100 },
            value: crate::text_input::TextInput::seeded("3"),
        }],
        focused: crate::FormFocus::Field(0),
    }))
}

fn make_alt_key(code: KeyCode) -> KeyEvent {
    KeyEvent {
        code,
        modifiers: KeyModifiers::ALT,
        kind: KeyEventKind::Press,
        state: KeyEventState::NONE,
    }
}

/// No Alt chord is its bare character — in ANY view class.
///
/// `combo_of` refuses Alt with exactly this reasoning ("an Alt-modified key
/// is a different chord, not its bare character"), but only the data-driven
/// `keys.*` lookups go through it. The structural `match key.code` arms do
/// not, and each view class guarded CONTROL only — so every Alt chord fell
/// through to the literal arm for its bare letter. **`Alt-q` quit the app**,
/// `Alt-g` jumped Home, `Alt-d` deleted.
///
/// Swept over every letter rather than spot-checked on `q`: the bug was one
/// missing modifier in a guard, so any letter with a literal arm had it, and
/// a fix that only taught `q` about Alt would leave the rest.
#[test]
fn an_alt_chord_is_never_its_bare_character() {
    let mut app = make_resource_app();
    for class in ["resource list", "overview"] {
        let mut app = App::new_for_test();
        if class == "overview" {
            app.nav.reset(crate::element::Element::Overview(Default::default()));
        }
        for c in 'a'..='z' {
            let alt = handle_key_event(&app, make_alt_key(KeyCode::Char(c)));
            assert!(
                alt.is_none(),
                "{class}: Alt-{c:?} produced {alt:?} — a chord is not its bare character",
            );
        }
        for code in [KeyCode::Enter, KeyCode::Home, KeyCode::End, KeyCode::Esc] {
            let alt = handle_key_event(&app, make_alt_key(code));
            assert!(alt.is_none(), "{class}: Alt-{code:?} produced {alt:?}");
        }
    }

    // The sibling rule the four copies DID get right, kept here so a future
    // consolidation cannot quietly drop one half of it. `Ctrl-d`/`Ctrl-u` are
    // claimed in detail views, so this asks the resource list.
    let _ = &mut app;
    for c in ['q', 'g', 'd', 'y'] {
        let ctrl = handle_key_event(&app, make_ctrl_key(KeyCode::Char(c)));
        assert!(
            ctrl.is_none() || !matches!(ctrl, Some(Action::Quit)),
            "Ctrl-{c:?} produced {ctrl:?}",
        );
    }
}

// ---------------------------------------------------------------------------
// The four column chords, and what a rebind may shadow
// ---------------------------------------------------------------------------

/// All FOUR configured column chords work in a log view, not three.
///
/// `colLast` was the missing one — `keys: { colLast: "e" }` jumped columns
/// in a table and did nothing in a log, under a comment saying these are "a
/// property of the ACTION rather than of one view".
#[test]
fn every_configured_column_chord_works_in_a_log_view() {
    use crate::actions::Action;
    let wanted = [
        ('a', Action::ColLeft),
        ('b', Action::ColRight),
        ('c', Action::ColFirst),
        ('e', Action::ColLast),
    ];
    for (ch, expected) in wanted {
        let mut app = App::new_for_test();
        push_log_element(&mut app);
        app.config.keys.col_left = Some(crate::KeyCombo::plain('a'));
        app.config.keys.col_right = Some(crate::KeyCombo::plain('b'));
        app.config.keys.col_first = Some(crate::KeyCombo::plain('c'));
        app.config.keys.col_last = Some(crate::KeyCombo::plain('e'));
        assert_eq!(
            handle_key_event(&app, make_key(KeyCode::Char(ch))),
            Some(expected.clone()),
            "`{ch}` must reach {expected:?} in a log view, as it does in a table",
        );
        // …and the same binding in a table, so this really is one decision.
        let mut table = App::new_for_test();
        table.config.keys.col_left = Some(crate::KeyCombo::plain('a'));
        table.config.keys.col_right = Some(crate::KeyCombo::plain('b'));
        table.config.keys.col_first = Some(crate::KeyCombo::plain('c'));
        table.config.keys.col_last = Some(crate::KeyCombo::plain('e'));
        assert_eq!(handle_key_event(&table, make_key(KeyCode::Char(ch))), Some(expected));
    }
}

/// `colFirst: "0"` does NOT cost the log view its range ladder.
///
/// The ladder is advertised in the hint bar as `<0-7> range` and has no
/// other key; column panning has `^` and `$`, which are structural. On a
/// table the same rebind costs nothing, because switch-to-all-namespaces
/// keeps its own `namespaceAll` escape hatch — so the digits win here and
/// the table keeps its rebind.
#[test]
fn rebinding_col_first_to_zero_keeps_the_log_range_ladder() {
    use crate::actions::{Action, LogRange};
    let mut app = App::new_for_test();
    push_log_element(&mut app);
    app.config.keys.col_first = Some(crate::KeyCombo::plain('0'));

    assert_eq!(
        handle_key_event(&app, make_key(KeyCode::Char('0'))),
        Some(Action::LogRange(LogRange::Tail)),
        "`0` is the tail range, which nothing else reaches",
    );
    assert_eq!(
        handle_key_event(&app, make_key(KeyCode::Char('7'))),
        Some(Action::LogRange(LogRange::Head)),
    );
    // And the panning the rebind was for is still reachable structurally.
    assert_eq!(handle_key_event(&app, make_key(KeyCode::Char('^'))), Some(Action::ColFirst));

    // On a TABLE the rebind still takes effect — the asymmetry is deliberate
    // and this is the half that says so.
    let mut table = App::new_for_test();
    table.config.keys.col_first = Some(crate::KeyCombo::plain('0'));
    assert_eq!(handle_key_event(&table, make_key(KeyCode::Char('0'))), Some(Action::ColFirst));
}

/// A chord is not its bare character in a picker either.
///
/// The four per-class handlers got this guard when `Alt-q` was found
/// quitting the app; the picker's was reached through `handle_dialog_keys`
/// and was not among them, so `Ctrl-j` moved the selection and `Ctrl-q`
/// left the picker.
#[test]
fn a_chord_is_not_its_bare_character_in_a_picker() {
    use crossterm::event::KeyModifiers as M;
    let mut app = App::new_for_test();
    app.nav.push(crate::element::Element::Picker(Box::new(crate::ContainerPicker {
        target: ObjectRef::new(
            k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
            "canary".to_string(),
            Namespace::Named("default".to_string()),
        ),
        choices: vec![crate::ContainerChoice {
            label: "web".into(),
            container: k9rs_proto::protocol::LogContainer::Named("web".into()),
        }],
        selected: 0,
        action: crate::ContainerAction::Logs,
    })));

    // The bare keys still work.
    assert_eq!(
        handle_key_event(&app, make_key(KeyCode::Char('j'))),
        Some(crate::actions::Action::NextItem),
    );
    // The chords are nothing.
    for (ch, mods) in [('j', M::CONTROL), ('k', M::CONTROL), ('q', M::CONTROL), ('q', M::ALT)] {
        let key = KeyEvent {
            code: KeyCode::Char(ch),
            modifiers: mods,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        };
        assert_eq!(
            handle_key_event(&app, key),
            None,
            "{mods:?}-{ch} must not alias to the bare `{ch}`",
        );
    }
}

/// `:` under a dialog cancels it; it does NOT open the command prompt.
///
/// This is what keeps `Modal::Command` and `Element::Confirm` from
/// co-existing — a state the types DO allow. If the prompt could open under
/// a dialog, `edit_command_key` would consume every key while
/// `command_input` is `Some`, whatever is on the nav stack, and the box
/// would be unanswerable. The guarantee lives in the input router, so this
/// is where it is checked.
#[test]
fn a_colon_under_a_dialog_cancels_it_rather_than_opening_the_prompt() {
    let mut app = App::new_for_test();
    app.nav.push(crate::element::Element::Confirm(Box::new(crate::ConfirmDialog {
        message: "Delete pod/canary?".to_string(),
        action_label: "Delete".to_string(),
        pending: crate::PendingAction::Single {
            op: crate::SingleOp::Delete,
            target: ObjectRef::new(
                k9rs_proto::resource_def::BuiltInKind::Pod.to_resource_id(),
                "canary".to_string(),
                Namespace::Named("default".to_string()),
            ),
        },
        focused: crate::ConfirmFocus::Cancel,
    })));
    assert!(app.ui.command_input().is_none(), "no prompt to begin with");
    assert_eq!(
        handle_key_event(&app, make_key(KeyCode::Char(':'))),
        Some(crate::actions::Action::Cancel),
        "`:` must reach the dialog's own keymap, not the global that opens the prompt",
    );
}
