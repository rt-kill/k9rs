use super::*;
use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};

fn make_key(code: KeyCode) -> KeyEvent {
    KeyEvent {
        code,
        modifiers: KeyModifiers::NONE,
        kind: KeyEventKind::Press,
        state: KeyEventState::NONE,
    }
}

fn make_resource_app() -> App {
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::Resources;
    // `populate_capabilities` used to seed `app.capabilities` for every
    // built-in kind so `app.current_capabilities()` returned a real manifest
    // during tests. Gone now — `current_capabilities()` computes straight
    // from the typed rid via `ResourceId::capabilities()`, so there's
    // nothing to preload.
    app
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
    // Logs are Shift+L (uppercase), plain 'l' is column-right.
    let mut app = make_resource_app();
    app.nav.reset(rid(crate::kube::resource_def::BuiltInKind::Pod));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('L'))),
        Some(Action::Logs)
    ));
    // Plain 'l' should be ColRight, not Logs.
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('l'))),
        Some(Action::ColRight)
    ));
}

#[test]
fn test_workload_types_have_logs() {
    use crate::kube::resource_def::BuiltInKind::*;
    for kind in [Deployment, StatefulSet, DaemonSet, ReplicaSet, Job, CronJob] {
        let mut app = make_resource_app();
        app.nav.reset(rid(kind));
        assert!(
            matches!(handle_key_event(&app, make_key(KeyCode::Char('L'))), Some(Action::Logs)),
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
        app.nav.reset(rid(kind));
        assert!(
            !matches!(handle_key_event(&app, make_key(KeyCode::Char('L'))), Some(Action::Logs)),
            "Expected no Logs action for {:?}",
            kind,
        );
    }
}

#[test]
fn test_confirm_dialog_keys() {
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    use crate::kube::protocol::{Namespace, ObjectRef, ResourceId};
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
    app.nav.reset(rid(crate::kube::resource_def::BuiltInKind::Deployment));
    let action = handle_key_event(&app, make_key(KeyCode::Char('r')));
    assert!(matches!(action, Some(Action::Restart)));
}

#[test]
fn test_q_goes_back_in_detail_view() {
    use crate::kube::protocol::{Namespace, ObjectRef, ResourceId};
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::ContentView {
        kind: crate::app::ContentViewKind::Yaml,
        target: Some(ObjectRef::new(
            ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
            String::new(),
            Namespace::from_user_command(""),
        )),
        awaiting_response: false,
        state: crate::app::ContentViewState::default(),
    };
    let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_log_view_s_toggles_follow() {
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::Logs {
        target: ContainerRef::new(String::new(), String::new(), crate::kube::protocol::LogContainer::Default),
        state: Box::new(crate::app::LogState::new()), stream: None,
    };
    let action = handle_key_event(&app, make_key(KeyCode::Char('s')));
    assert!(matches!(action, Some(Action::ToggleLogFollow)));
}

#[test]
fn test_log_view_t_toggles_timestamps() {
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::Logs {
        target: ContainerRef::new(String::new(), String::new(), crate::kube::protocol::LogContainer::Default),
        state: Box::new(crate::app::LogState::new()), stream: None,
    };
    let action = handle_key_event(&app, make_key(KeyCode::Char('t')));
    assert!(matches!(action, Some(Action::ToggleLogTimestamps)));
}

#[test]
fn test_log_view_shift_c_clears_logs() {
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::Logs {
        target: ContainerRef::new(String::new(), String::new(), crate::kube::protocol::LogContainer::Default),
        state: Box::new(crate::app::LogState::new()), stream: None,
    };
    let action = handle_key_event(&app, make_shift_key(KeyCode::Char('C')));
    assert!(matches!(action, Some(Action::ClearLogs)));
}

#[test]
fn test_q_goes_back_in_help_view() {
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::Help;
    let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_q_goes_back_in_contexts_view() {
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::Contexts;
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
    use crate::kube::protocol::{Namespace, ObjectRef, ResourceId};
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::ContentView {
        kind: crate::app::ContentViewKind::Describe,
        target: Some(ObjectRef::new(
            ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
            String::new(),
            Namespace::from_user_command(""),
        )),
        awaiting_response: false,
        state: crate::app::ContentViewState::default(),
    };
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_esc_goes_back_in_log_view() {
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::Logs {
        target: ContainerRef::new(String::new(), String::new(), crate::kube::protocol::LogContainer::Default),
        state: Box::new(crate::app::LogState::new()), stream: None,
    };
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_esc_goes_back_in_help_view() {
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::Help;
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_esc_goes_back_in_contexts_view() {
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::Contexts;
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_q_goes_back_in_container_select() {
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::ContainerSelect {
        target: crate::kube::protocol::ObjectRef::new(
            rid(crate::kube::resource_def::BuiltInKind::Pod),
            String::new(),
            crate::kube::protocol::Namespace::All,
        ),
        selected: 0,
        action: crate::app::ContainerAction::Logs,
    };
    let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_log_view_home_end() {
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::Logs {
        target: ContainerRef::new(String::new(), String::new(), crate::kube::protocol::LogContainer::Default),
        state: Box::new(crate::app::LogState::new()), stream: None,
    };
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
    use crate::kube::protocol::{Namespace, ObjectRef, ResourceId};
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::ContentView {
        kind: crate::app::ContentViewKind::Yaml,
        target: Some(ObjectRef::new(
            ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
            String::new(),
            Namespace::from_user_command(""),
        )),
        awaiting_response: false,
        state: crate::app::ContentViewState::default(),
    };
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
    app.nav.reset(rid(crate::kube::resource_def::BuiltInKind::Pod));
    let action = handle_key_event(&app, make_key(KeyCode::Char('N')));
    assert!(matches!(action, Some(Action::Sort(crate::app::SortTarget::Column(1)))));
}

#[test]
fn test_resource_view_sort_by_name_cluster_scoped() {
    let mut app = make_resource_app();
    // Nodes are cluster-scoped, so NAME is column 0.
    app.nav.reset(rid(crate::kube::resource_def::BuiltInKind::Node));
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
// Detail view keys (Yaml/Describe)
// ---------------------------------------------------------------------------

fn make_yaml_app() -> App {
    use crate::kube::protocol::{Namespace, ObjectRef, ResourceId};
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::ContentView {
        kind: crate::app::ContentViewKind::Yaml,
        target: Some(ObjectRef::new(
            ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
            String::new(),
            Namespace::from_user_command(""),
        )),
        awaiting_response: false,
        state: crate::app::ContentViewState::default(),
    };
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
    let mut app = App::new(crate::kube::protocol::ContextName::default(), String::new());
    app.route = Route::Logs {
        target: ContainerRef::new(String::new(), String::new(), crate::kube::protocol::LogContainer::Default),
        state: Box::new(crate::app::LogState::new()),
        stream: None,
    };
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
