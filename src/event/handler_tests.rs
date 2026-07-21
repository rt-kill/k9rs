use super::*;
use crate::kube::protocol::{Namespace, ObjectRef, ResourceId};
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
fn reset_to_kind(app: &mut App, kind: crate::kube::resource_def::BuiltInKind) {
    reset_to_rid(app, rid(kind));
}

fn reset_to_rid(app: &mut App, resource: crate::kube::protocol::ResourceId) {
    let label = resource.short_label().to_lowercase();
    let root = crate::app::element::Element::ResourceList(
        crate::app::element::ResourceList::open_for_test(
            crate::app::element::QuerySpec {
                rid: resource,
                namespace: crate::kube::protocol::Namespace::All,
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
        crate::kube::protocol::ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
        String::new(),
        crate::kube::protocol::Namespace::from_user_command(""),
    );
    let spec = if yaml {
        crate::app::element::ContentSpec::Yaml(target)
    } else {
        crate::app::element::ContentSpec::Describe(target)
    };
    app.nav.push(crate::app::element::Element::ContentView(
        crate::app::element::ContentView::new(spec, crate::app::ContentViewState::default(), false),
    ));
}

/// Push a log-session element (parked stream) as the top view.
fn push_log_element(app: &mut App) {
    app.nav.push(crate::app::element::Element::LogSession(Box::new(
        crate::app::element::LogSession::for_test(ContainerRef::new(
            "test-pod",
            "default",
            crate::kube::protocol::LogContainer::Default,
        )),
    )));
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
    reset_to_kind(&mut app, crate::kube::resource_def::BuiltInKind::Pod);
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
    use crate::kube::resource_def::BuiltInKind::*;
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
    use crate::kube::resource_def::BuiltInKind::*;
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
    use crate::app::KeyCombo;
    let mut app = make_resource_app();
    reset_to_kind(&mut app, crate::kube::resource_def::BuiltInKind::Pod);
    app.config.keys.logs = Some(KeyCombo { ctrl: true, ch: 'l' });
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
    use crate::app::KeyCombo;
    let mut app = make_resource_app();
    reset_to_kind(&mut app, crate::kube::resource_def::BuiltInKind::Pod);

    // Unbound by default: `0` still switches to all namespaces; `$` is nothing.
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('0'))),
        Some(Action::SwitchNamespace(crate::kube::protocol::Namespace::All))
    ));
    assert!(handle_key_event(&app, make_key(KeyCode::Char('$'))).is_none());

    app.config.keys.col_first = Some(KeyCombo::plain('0'));
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
}

/// An exact-chord miss must not alias: with logs on plain `l`, Ctrl-L is
/// nothing (the old global fired logs off the hovered row from ANY view
/// state — it is gone in favor of the binding system).
#[test]
fn test_ctrl_chord_does_not_alias_bare_char() {
    let mut app = make_resource_app();
    reset_to_kind(&mut app, crate::kube::resource_def::BuiltInKind::Pod);
    assert!(handle_key_event(&app, make_ctrl_key(KeyCode::Char('l'))).is_none());
}

#[test]
fn test_confirm_dialog_keys() {
    let mut app = App::new_for_test();
    app.ui.confirm_dialog = Some(crate::app::ConfirmDialog {
        message: "Are you sure?".to_string(),
        pending: crate::app::PendingAction::Single {
            op: crate::app::SingleOp::Delete,
            target: ObjectRef::new(
                ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
                "test",
                Namespace::from_user_command("default"),
            ),
        },
        action_label: "Delete".to_string(),
        action_focused: false,
    });

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
    reset_to_kind(&mut app, crate::kube::resource_def::BuiltInKind::Deployment);
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

#[test]
fn test_q_goes_back_in_help_view() {
    let mut app = App::new_for_test();
    app.ui.overlay = Some(crate::app::Overlay::Help { scroll: 0 });
    let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_q_goes_back_in_contexts_view() {
    let mut app = App::new_for_test();
    app.nav.push(crate::app::element::Element::ContextList(
        crate::app::element::ContextList::new(Vec::new()),
    ));
    let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_esc_noop_in_resource_view_no_filter() {
    let app = make_resource_app();
    // No active filter, Esc is a no-op at root.
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(action.is_none());
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
    app.ui.overlay = Some(crate::app::Overlay::Help { scroll: 0 });
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_esc_goes_back_in_contexts_view() {
    let mut app = App::new_for_test();
    app.nav.push(crate::app::element::Element::ContextList(
        crate::app::element::ContextList::new(Vec::new()),
    ));
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_q_goes_back_in_container_select() {
    let mut app = App::new_for_test();
    app.ui.overlay = Some(crate::app::Overlay::ContainerSelect {
        target: crate::kube::protocol::ObjectRef::new(
            rid(crate::kube::resource_def::BuiltInKind::Pod),
            String::new(),
            crate::kube::protocol::Namespace::All,
        ),
        containers: Vec::new(),
        selected: 0,
        action: crate::app::ContainerAction::Logs,
    });
    let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
    assert!(matches!(action, Some(Action::Back)));
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
    assert!(matches!(action, Some(Action::SwitchNamespace(crate::kube::protocol::Namespace::All))));
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

#[test]
fn test_resource_view_sort_by_name() {
    let mut app = make_resource_app();
    // Pods are namespaced, so NAME is column 1.
    reset_to_kind(&mut app, crate::kube::resource_def::BuiltInKind::Pod);
    let action = handle_key_event(&app, make_key(KeyCode::Char('N')));
    assert!(matches!(action, Some(Action::Sort(crate::app::SortTarget::Column(1)))));
}

#[test]
fn test_resource_view_sort_by_name_cluster_scoped() {
    let mut app = make_resource_app();
    // Nodes are cluster-scoped, so NAME is column 0.
    reset_to_kind(&mut app, crate::kube::resource_def::BuiltInKind::Node);
    let action = handle_key_event(&app, make_key(KeyCode::Char('N')));
    assert!(matches!(action, Some(Action::Sort(crate::app::SortTarget::Column(0)))));
}

#[test]
fn test_resource_view_sort_by_age() {
    let app = make_resource_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('A')));
    assert!(matches!(action, Some(Action::Sort(crate::app::SortTarget::Last))));
}

// ---------------------------------------------------------------------------
// Batch capture (marks ∩ present rows)
// ---------------------------------------------------------------------------

/// Batch targets are captured as marks ∩ PRESENT rows. During the Ctrl-R
/// window (marks survive `clear()`, rows unknown/Initializing) capture is
/// honestly empty — nothing can operate on an identity the store cannot
/// vouch for. The recovery baseline re-anchors and capture works again.
#[test]
fn test_batch_capture_is_empty_during_refresh_window() {
    use crate::app::store::StorePayload;
    use crate::kube::protocol::{ObjectKey, TableBaseline};
    use crate::kube::resources::row::{CellValue, ResourceRow};

    let row = |name: &str| ResourceRow {
        cells: vec![CellValue::Text(name.into())],
        name: name.into(),
        namespace: Some("ns".into()),
        ..Default::default()
    };
    let baseline = |rows: Vec<ResourceRow>| {
        StorePayload::Baseline(TableBaseline {
            resource: rid(crate::kube::resource_def::BuiltInKind::Pod),
            headers: vec!["NAME".into()],
            rows,
        })
    };

    let app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
    store.apply(1, baseline(vec![row("a"), row("b")]));
    store.toggle_mark(&ObjectKey::new("ns".to_string(), "a".to_string()));

    let captured = crate::kube::session_actions::get_marked_resource_infos(&app);
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].name, "a");

    // Ctrl-R: rows drop, marks survive — capture must go empty, and the
    // app is STILL in select mode (the title says so; batch keys flash).
    store.clear();
    assert!(app.select_mode());
    assert!(crate::kube::session_actions::get_marked_resource_infos(&app).is_empty());

    // Recovery baseline re-anchors the surviving mark.
    store.apply(2, baseline(vec![row("a")]));
    let captured = crate::kube::session_actions::get_marked_resource_infos(&app);
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].name, "a");
}

/// ONE batch at a time: while a tracker is outstanding, a second batch
/// dispatch is refused (a silent tracker replacement would duplicate the
/// sends and let the first batch's late results clobber the second's
/// summary).
#[test]
fn test_second_batch_refused_while_one_outstanding() {
    use crate::app::store::StorePayload;
    use crate::kube::protocol::{ObjectKey, TableBaseline};
    use crate::kube::resources::row::{CellValue, ResourceRow};

    let row = |name: &str| ResourceRow {
        cells: vec![CellValue::Text(name.into())],
        name: name.into(),
        namespace: Some("ns".into()),
        ..Default::default()
    };
    let mut app = App::new_for_test();
    let store = std::sync::Arc::clone(app.nav.top().data_store().unwrap());
    store.apply(1, StorePayload::Baseline(TableBaseline {
        resource: rid(crate::kube::resource_def::BuiltInKind::Pod),
        headers: vec!["NAME".into()],
        rows: vec![row("a")],
    }));
    store.toggle_mark(&ObjectKey::new("ns".to_string(), "a".to_string()));

    // Outstanding tracker → refuse, no dialog, selection untouched.
    let t = crate::kube::protocol::ObjectRef::new(
        rid(crate::kube::resource_def::BuiltInKind::Pod),
        "a".to_string(),
        crate::kube::protocol::Namespace::from_row("ns"),
    );
    app.pending_batch = Some(crate::app::BatchTracker::new(
        "Deleted", "pod".to_string(),
        rid(crate::kube::resource_def::BuiltInKind::Pod),
        std::slice::from_ref(&t), 0, std::sync::Weak::new(),
    ));
    crate::kube::session_actions::handle_batch_op(&mut app, Action::BatchDelete);
    assert!(app.ui.confirm_dialog.is_none());
    assert!(app.ui.flash.as_ref().is_some_and(|f| f.message.contains("in flight")));
    assert!(app.nav.top().has_marks(), "refusal leaves the selection untouched");

    // Tracker cleared → the same dispatch opens the confirm dialog.
    app.pending_batch = None;
    crate::kube::session_actions::handle_batch_op(&mut app, Action::BatchDelete);
    assert!(app.ui.confirm_dialog.is_some());
}

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
fn test_log_view_log_since_digits() {
    let app = make_log_app();
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('1'))),
        Some(Action::LogSince(Some(_)))
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('3'))),
        Some(Action::LogSince(Some(_)))
    ));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('6'))),
        Some(Action::LogSince(Some(_)))
    ));
    // '0' clears the time range.
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('0'))),
        Some(Action::LogSince(None))
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
fn test_log_view_search_next_prev() {
    let app = make_log_app();
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
fn test_log_view_slash_starts_search() {
    let app = make_log_app();
    let action = handle_key_event(&app, make_key(KeyCode::Char('/')));
    assert!(matches!(action, Some(Action::SearchStart)));
}
