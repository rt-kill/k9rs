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
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::Resources;
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
    let mut app = make_resource_app();
    app.nav.reset(rid("pods"));
    assert!(matches!(
        handle_key_event(&app, make_key(KeyCode::Char('l'))),
        Some(Action::Logs)
    ));
}

#[test]
fn test_workload_types_have_logs() {
    // All workload-related resource tabs should support logs.
    // Services are excluded because kubectl logs doesn't work on services.
    for alias in ["deployments", "statefulsets", "daemonsets", "replicasets", "jobs", "cronjobs"] {
        let mut app = make_resource_app();
        app.nav.reset(rid(alias));
        assert!(
            matches!(handle_key_event(&app, make_key(KeyCode::Char('l'))), Some(Action::Logs)),
            "Expected Logs action for {}",
            alias,
        );
    }
}

#[test]
fn test_non_workload_no_logs() {
    // Non-workload resource tabs should NOT produce a Logs action.
    // Instead they produce a FlashInfo message.
    for alias in ["services", "nodes", "namespaces", "configmaps", "secrets"] {
        let mut app = make_resource_app();
        app.nav.reset(rid(alias));
        assert!(
            !matches!(handle_key_event(&app, make_key(KeyCode::Char('l'))), Some(Action::Logs)),
            "Expected no Logs action for {}",
            alias,
        );
    }
}

#[test]
fn test_confirm_dialog_keys() {
    let mut app = App::new(String::new(), Vec::new(), String::new());
    use crate::kube::protocol::{Namespace, ObjectRef};
    app.confirm_dialog = Some(crate::app::ConfirmDialog {
        message: "Are you sure?".to_string(),
        pending: crate::app::PendingAction::Single {
            action: Action::Delete,
            target: ObjectRef::from_parts("pod", "test", Namespace::from("default")),
        },
        yes_selected: false,
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
    assert!(matches!(action, Some(Action::PortForward)));
}

#[test]
fn test_restart_on_deployments() {
    let mut app = make_resource_app();
    app.nav.reset(rid("deployments"));
    let action = handle_key_event(&app, make_key(KeyCode::Char('r')));
    assert!(matches!(action, Some(Action::Restart)));
}

#[test]
fn test_q_goes_back_in_detail_view() {
    use crate::kube::protocol::{Namespace, ObjectRef};
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::Yaml {
        target: ObjectRef::from_parts("", String::new(), Namespace::from("")),
        awaiting_response: false,
        state: crate::app::ContentViewState::default(),
    };
    let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_log_view_s_toggles_follow() {
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::Logs {
        target: ContainerRef::new(String::new(), String::new(), String::new()),
        state: Box::new(crate::app::LogState::new()),
    };
    let action = handle_key_event(&app, make_key(KeyCode::Char('s')));
    assert!(matches!(action, Some(Action::ToggleLogFollow)));
}

#[test]
fn test_log_view_t_toggles_timestamps() {
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::Logs {
        target: ContainerRef::new(String::new(), String::new(), String::new()),
        state: Box::new(crate::app::LogState::new()),
    };
    let action = handle_key_event(&app, make_key(KeyCode::Char('t')));
    assert!(matches!(action, Some(Action::ToggleLogTimestamps)));
}

#[test]
fn test_log_view_shift_c_clears_logs() {
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::Logs {
        target: ContainerRef::new(String::new(), String::new(), String::new()),
        state: Box::new(crate::app::LogState::new()),
    };
    let action = handle_key_event(&app, make_shift_key(KeyCode::Char('C')));
    assert!(matches!(action, Some(Action::ClearLogs)));
}

#[test]
fn test_q_goes_back_in_help_view() {
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::Help;
    let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_q_goes_back_in_contexts_view() {
    let mut app = App::new(String::new(), Vec::new(), String::new());
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
    use crate::kube::protocol::{Namespace, ObjectRef};
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::Describe {
        target: ObjectRef::from_parts("", String::new(), Namespace::from("")),
        awaiting_response: false,
        state: crate::app::ContentViewState::default(),
    };
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_esc_goes_back_in_log_view() {
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::Logs {
        target: ContainerRef::new(String::new(), String::new(), String::new()),
        state: Box::new(crate::app::LogState::new()),
    };
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_esc_goes_back_in_help_view() {
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::Help;
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_esc_goes_back_in_contexts_view() {
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::Contexts;
    let action = handle_key_event(&app, make_key(KeyCode::Esc));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_q_goes_back_in_container_select() {
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::ContainerSelect {
        pod: String::new(),
        namespace: String::new(),
        selected: 0,
        for_shell: false,
    };
    let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
    assert!(matches!(action, Some(Action::Back)));
}

#[test]
fn test_log_view_home_end() {
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::Logs {
        target: ContainerRef::new(String::new(), String::new(), String::new()),
        state: Box::new(crate::app::LogState::new()),
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
    use crate::kube::protocol::{Namespace, ObjectRef};
    let mut app = App::new(String::new(), Vec::new(), String::new());
    app.route = Route::Yaml {
        target: ObjectRef::from_parts("", String::new(), Namespace::from("")),
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
