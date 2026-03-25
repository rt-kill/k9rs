use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::app::actions::Action;
use crate::app::{App, ResourceTab, Route};

/// Maps a `KeyEvent` to an `Action` based on the current application state.
/// Returns `None` if the key has no binding in the current context.
///
/// Note: filter mode and command mode input are handled directly in the main
/// event loop (main.rs) before this function is called. This function only
/// handles normal and confirmation-dialog key mappings.
pub fn handle_key_event(app: &App, key: KeyEvent) -> Option<Action> {
    // -----------------------------------------------------------------------
    // Confirmation dialog: only y/n/Enter/Esc.
    // -----------------------------------------------------------------------
    if app.confirm_dialog.is_some() {
        return handle_confirm_dialog(app, key);
    }

    // -----------------------------------------------------------------------
    // Detail views override `/` to start search instead of filter.
    // -----------------------------------------------------------------------
    if matches!(app.route, Route::Yaml { .. } | Route::Describe { .. } | Route::Aliases) {
        if key.code == KeyCode::Char('/') {
            return Some(Action::SearchStart);
        }
    }

    // -----------------------------------------------------------------------
    // Global keys (available in every view).
    // -----------------------------------------------------------------------
    if let Some(action) = handle_global_keys(app, key) {
        return Some(action);
    }

    // -----------------------------------------------------------------------
    // Route-specific keys.
    // -----------------------------------------------------------------------
    match &app.route {
        Route::Resources => handle_resource_view_keys(app, key),
        Route::Yaml { .. } | Route::Describe { .. } => handle_detail_view_keys(key),
        Route::Logs { .. } => handle_log_view_keys(key),
        Route::Help => handle_help_view_keys(key),
        Route::Contexts => handle_contexts_view_keys(key),
        Route::Shell { .. } => handle_log_view_keys(key),
        Route::ContainerSelect { .. } => handle_container_select_keys(key),
        Route::Aliases => handle_detail_view_keys(key),
    }
}

// ---------------------------------------------------------------------------
// Confirm dialog
// ---------------------------------------------------------------------------

fn handle_confirm_dialog(app: &App, key: KeyEvent) -> Option<Action> {
    match key.code {
        KeyCode::Char('y') | KeyCode::Char('Y') => Some(Action::Confirm),
        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => Some(Action::Cancel),
        KeyCode::Enter => {
            // Confirm or cancel based on which button is selected
            if app.confirm_dialog.as_ref().map_or(false, |d| d.yes_selected) {
                Some(Action::Confirm)
            } else {
                Some(Action::Cancel)
            }
        }
        KeyCode::Left | KeyCode::Right | KeyCode::Tab => Some(Action::ToggleDialogButton),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Global keys
// ---------------------------------------------------------------------------

fn handle_global_keys(app: &App, key: KeyEvent) -> Option<Action> {
    // Ctrl-C: quit (unless noExitOnCtrlC is set).
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        if !app.no_exit_on_ctrl_c {
            return Some(Action::Quit);
        } else {
            return None;
        }
    }

    // Ctrl-R: force refresh (global).
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('r') {
        return Some(Action::Refresh);
    }

    // Ctrl-E: toggle header (global).
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('e') {
        return Some(Action::ToggleHeader);
    }

    // Ctrl-S: save/dump table to file.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('s') {
        return Some(Action::SaveTable);
    }

    // Ctrl-A: show aliases view.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('a') {
        return Some(Action::ShowAliases);
    }

    // Ctrl-W: toggle wide column mode.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('w') {
        return Some(Action::ToggleWide);
    }

    // Ctrl-Z: toggle fault filter.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('z') {
        return Some(Action::ToggleFaultFilter);
    }

    // Ctrl-L: toggle full-fetch mode.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('l') {
        return Some(Action::ToggleFullFetch);
    }

    match key.code {
        // `q` is NOT global quit — it is context-sensitive (handled per-view).
        KeyCode::Char(':') => Some(Action::CommandMode),
        KeyCode::Char('/') => Some(Action::Filter(String::new())),
        KeyCode::Char('?') => Some(Action::Help),
        // Esc is NOT global — it is context-sensitive (clear filter in resource
        // view, go back in sub-views). Handled per-view below.
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Resource list view
// ---------------------------------------------------------------------------

fn handle_resource_view_keys(app: &App, key: KeyEvent) -> Option<Action> {
    // Ctrl-D: delete with confirmation.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('d') {
        if app.read_only {
            return Some(Action::FlashInfo("Read-only mode".to_string()));
        }
        return Some(Action::Delete);
    }

    // Ctrl-K: force-kill pod.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('k') {
        if app.read_only {
            return Some(Action::FlashInfo("Read-only mode".to_string()));
        }
        if app.resource_tab != ResourceTab::Pods {
            return Some(Action::FlashInfo("Force-kill is only available on Pods".to_string()));
        }
        return Some(Action::ForceKill);
    }

    match key.code {
        // `q` in resource view: if filter is active, clear it. Otherwise quit
        // (resource view is the root, so going back means quitting).
        KeyCode::Char('q') => {
            if !app.active_filter_text().is_empty() {
                Some(Action::ClearFilter)
            } else {
                Some(Action::Quit)
            }
        }

        // Esc in resource view: clear filter if active, otherwise no-op.
        KeyCode::Esc => {
            if !app.active_filter_text().is_empty() {
                Some(Action::ClearFilter)
            } else {
                None
            }
        }

        // Navigation.
        KeyCode::Down | KeyCode::Char('j') => Some(Action::NextItem),
        KeyCode::Up | KeyCode::Char('k') => Some(Action::PrevItem),
        KeyCode::PageDown => Some(Action::PageDown),
        KeyCode::PageUp => Some(Action::PageUp),
        KeyCode::Home | KeyCode::Char('g') => Some(Action::Home),
        KeyCode::End | KeyCode::Char('G') => Some(Action::End),

        // Drill down.
        KeyCode::Enter => Some(Action::Enter),

        // Detail views.
        KeyCode::Char('d') => Some(Action::Describe),
        KeyCode::Char('y') => Some(Action::Yaml),
        KeyCode::Char('e') => {
            if app.read_only {
                Some(Action::FlashInfo("Read-only mode".to_string()))
            } else {
                Some(Action::Edit)
            }
        }

        // Logs: available on pods and workload types (like k9s).
        // Services are excluded because kubectl logs doesn't work on services.
        KeyCode::Char('l') => {
            match app.resource_tab {
                ResourceTab::Pods | ResourceTab::Deployments | ResourceTab::StatefulSets
                | ResourceTab::DaemonSets | ResourceTab::ReplicaSets
                | ResourceTab::Jobs | ResourceTab::CronJobs => Some(Action::Logs),
                _ => Some(Action::FlashInfo("Logs not available for this resource type".to_string())),
            }
        }
        KeyCode::Char('s') => {
            if app.resource_tab == ResourceTab::Pods {
                if app.read_only {
                    Some(Action::FlashInfo("Read-only mode".to_string()))
                } else {
                    Some(Action::Shell)
                }
            } else if matches!(
                app.resource_tab,
                ResourceTab::Deployments | ResourceTab::StatefulSets | ResourceTab::ReplicaSets
            ) {
                if app.read_only {
                    Some(Action::FlashInfo("Read-only mode".to_string()))
                } else {
                    Some(Action::Scale)
                }
            } else {
                None
            }
        }

        // Restart (deployments/statefulsets/daemonsets).
        KeyCode::Char('r') => {
            if matches!(
                app.resource_tab,
                ResourceTab::Deployments | ResourceTab::StatefulSets | ResourceTab::DaemonSets
            ) {
                if app.read_only {
                    Some(Action::FlashInfo("Read-only mode".to_string()))
                } else {
                    Some(Action::Restart)
                }
            } else {
                None
            }
        }

        // Port-forward (pods only — the implementation only handles pods).
        KeyCode::Char('f') => {
            if app.resource_tab == ResourceTab::Pods {
                Some(Action::PortForward)
            } else {
                Some(Action::FlashInfo("Port-forward is only available on Pods".to_string()))
            }
        }

        // Previous logs: available on pods and workload types.
        KeyCode::Char('p') => {
            match app.resource_tab {
                ResourceTab::Pods | ResourceTab::Deployments | ResourceTab::StatefulSets
                | ResourceTab::DaemonSets | ResourceTab::ReplicaSets
                | ResourceTab::Jobs | ResourceTab::CronJobs => Some(Action::PreviousLogs),
                _ => Some(Action::FlashInfo("Logs not available for this resource type".to_string())),
            }
        }

        // Show node for a pod.
        KeyCode::Char('o') => {
            if app.resource_tab == ResourceTab::Pods {
                Some(Action::ShowNode)
            } else {
                Some(Action::FlashInfo("Show node is only available on Pods".to_string()))
            }
        }

        // Toggle between last two views.
        KeyCode::Char('-') => Some(Action::ToggleLastView),

        // Sort by current column (toggle direction).
        KeyCode::Char('O') => Some(Action::ToggleSortDirection),

        // Sort by NAME (column 1, or 0 if no NS).
        KeyCode::Char('N') => Some(Action::Sort(1)),

        // Sort by AGE (last column — sentinel value usize::MAX).
        KeyCode::Char('A') => Some(Action::Sort(usize::MAX)),

        // Sort by STATUS (column 3, meaningful for Pods).
        KeyCode::Char('S') if matches!(app.resource_tab, ResourceTab::Pods) => Some(Action::Sort(3)),

        // Copy.
        KeyCode::Char('c') => Some(Action::Copy),

        // Mark/select rows.
        KeyCode::Char(' ') => Some(Action::ToggleMark),

        // 0: switch to all namespaces (like k9s)
        KeyCode::Char('0') => Some(Action::SwitchNamespace("all".to_string())),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// YAML / Describe detail views
// ---------------------------------------------------------------------------

fn handle_detail_view_keys(key: KeyEvent) -> Option<Action> {
    // Ctrl-d / Ctrl-u for half-page scroll (vim-style)
    if key.modifiers.contains(KeyModifiers::CONTROL) {
        return match key.code {
            KeyCode::Char('d') => Some(Action::PageDown),
            KeyCode::Char('u') => Some(Action::PageUp),
            _ => None,
        };
    }

    match key.code {
        // `q` or Esc in detail views goes back (also clears search).
        KeyCode::Char('q') | KeyCode::Esc => Some(Action::Back),

        // Navigation.
        KeyCode::Down | KeyCode::Char('j') => Some(Action::NextItem),
        KeyCode::Up | KeyCode::Char('k') => Some(Action::PrevItem),
        KeyCode::PageDown => Some(Action::PageDown),
        KeyCode::PageUp => Some(Action::PageUp),
        KeyCode::Home | KeyCode::Char('g') => Some(Action::Home),
        KeyCode::End | KeyCode::Char('G') => Some(Action::End),

        // Search navigation.
        KeyCode::Char('n') => Some(Action::SearchNext),
        KeyCode::Char('N') => Some(Action::SearchPrev),

        // Copy.
        KeyCode::Char('c') => Some(Action::Copy),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Log view
// ---------------------------------------------------------------------------

fn handle_log_view_keys(key: KeyEvent) -> Option<Action> {
    // Shift-C: clear logs.
    if key.modifiers.contains(KeyModifiers::SHIFT) && key.code == KeyCode::Char('C') {
        return Some(Action::ClearLogs);
    }

    match key.code {
        // `q` or Esc in log view goes back.
        KeyCode::Char('q') | KeyCode::Esc => Some(Action::Back),

        // Scrolling.
        KeyCode::Down | KeyCode::Char('j') => Some(Action::ScrollDown(1)),
        KeyCode::Up | KeyCode::Char('k') => Some(Action::ScrollUp(1)),
        KeyCode::PageDown => Some(Action::PageDown),
        KeyCode::PageUp => Some(Action::PageUp),
        KeyCode::Home | KeyCode::Char('g') => Some(Action::Home),
        KeyCode::End | KeyCode::Char('G') => Some(Action::End),

        // Log-specific toggles.
        KeyCode::Char('s') => Some(Action::ToggleLogFollow),
        KeyCode::Char('w') => Some(Action::ToggleLogWrap),
        KeyCode::Char('t') => Some(Action::ToggleLogTimestamps),

        // Copy.
        KeyCode::Char('c') => Some(Action::Copy),

        // Digits 0-6: log time range selection.
        KeyCode::Char('0') => Some(Action::LogSince(None)),
        KeyCode::Char('1') => Some(Action::LogSince(Some("1m".to_string()))),
        KeyCode::Char('2') => Some(Action::LogSince(Some("5m".to_string()))),
        KeyCode::Char('3') => Some(Action::LogSince(Some("15m".to_string()))),
        KeyCode::Char('4') => Some(Action::LogSince(Some("30m".to_string()))),
        KeyCode::Char('5') => Some(Action::LogSince(Some("1h".to_string()))),
        KeyCode::Char('6') => Some(Action::LogSince(Some("24h".to_string()))),

        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Help view
// ---------------------------------------------------------------------------

fn handle_help_view_keys(key: KeyEvent) -> Option<Action> {
    match key.code {
        // `q` or Esc in help view goes back.
        KeyCode::Char('q') | KeyCode::Esc => Some(Action::Back),

        KeyCode::Down | KeyCode::Char('j') => Some(Action::NextItem),
        KeyCode::Up | KeyCode::Char('k') => Some(Action::PrevItem),
        KeyCode::PageDown => Some(Action::PageDown),
        KeyCode::PageUp => Some(Action::PageUp),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Context selector
// ---------------------------------------------------------------------------

fn handle_contexts_view_keys(key: KeyEvent) -> Option<Action> {
    match key.code {
        // `q` or Esc in context view goes back.
        KeyCode::Char('q') | KeyCode::Esc => Some(Action::Back),

        // Navigation.
        KeyCode::Down | KeyCode::Char('j') => Some(Action::NextItem),
        KeyCode::Up | KeyCode::Char('k') => Some(Action::PrevItem),
        KeyCode::PageDown => Some(Action::PageDown),
        KeyCode::PageUp => Some(Action::PageUp),
        KeyCode::Home | KeyCode::Char('g') => Some(Action::Home),
        KeyCode::End | KeyCode::Char('G') => Some(Action::End),

        // Switch to selected context.
        KeyCode::Enter => Some(Action::Enter),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Container select view
// ---------------------------------------------------------------------------

fn handle_container_select_keys(key: KeyEvent) -> Option<Action> {
    match key.code {
        // `q` or Esc in container select goes back.
        KeyCode::Char('q') | KeyCode::Esc => Some(Action::Back),

        KeyCode::Down | KeyCode::Char('j') => Some(Action::NextItem),
        KeyCode::Up | KeyCode::Char('k') => Some(Action::PrevItem),
        KeyCode::Enter => Some(Action::Enter),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
        let app = App::new(String::new(), Vec::new(), String::new());
        // No active filter, so `q` should quit.
        let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
        assert!(matches!(action, Some(Action::Quit)));
    }

    #[test]
    fn test_ctrl_c_quit() {
        let app = App::new(String::new(), Vec::new(), String::new());
        let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('c')));
        assert!(matches!(action, Some(Action::Quit)));
    }

    #[test]
    fn test_help_key() {
        let app = App::new(String::new(), Vec::new(), String::new());
        let action = handle_key_event(&app, make_key(KeyCode::Char('?')));
        assert!(matches!(action, Some(Action::Help)));
    }

    #[test]
    fn test_resource_view_navigation() {
        let app = App::new(String::new(), Vec::new(), String::new());
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
        let app = App::new(String::new(), Vec::new(), String::new());
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
        let mut app = App::new(String::new(), Vec::new(), String::new());
        app.resource_tab = ResourceTab::Pods;
        assert!(matches!(
            handle_key_event(&app, make_key(KeyCode::Char('l'))),
            Some(Action::Logs)
        ));
    }

    #[test]
    fn test_workload_types_have_logs() {
        // All workload-related resource tabs should support logs.
        // Services are excluded because kubectl logs doesn't work on services.
        for tab in [
            ResourceTab::Deployments,
            ResourceTab::StatefulSets,
            ResourceTab::DaemonSets,
            ResourceTab::ReplicaSets,
            ResourceTab::Jobs,
            ResourceTab::CronJobs,
        ] {
            let mut app = App::new(String::new(), Vec::new(), String::new());
            app.resource_tab = tab;
            assert!(
                matches!(handle_key_event(&app, make_key(KeyCode::Char('l'))), Some(Action::Logs)),
                "Expected Logs action for {:?}",
                tab,
            );
        }
    }

    #[test]
    fn test_non_workload_no_logs() {
        // Non-workload resource tabs should NOT produce a Logs action.
        // Instead they produce a FlashInfo message.
        for tab in [
            ResourceTab::Services,
            ResourceTab::Nodes,
            ResourceTab::Namespaces,
            ResourceTab::ConfigMaps,
            ResourceTab::Secrets,
        ] {
            let mut app = App::new(String::new(), Vec::new(), String::new());
            app.resource_tab = tab;
            assert!(
                !matches!(handle_key_event(&app, make_key(KeyCode::Char('l'))), Some(Action::Logs)),
                "Expected no Logs action for {:?}",
                tab,
            );
        }
    }

    #[test]
    fn test_confirm_dialog_keys() {
        let mut app = App::new(String::new(), Vec::new(), String::new());
        app.confirm_dialog = Some(crate::app::ConfirmDialog {
            message: "Are you sure?".to_string(),
            action: Action::Delete,
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
        let app = App::new(String::new(), Vec::new(), String::new());
        let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('d')));
        assert!(matches!(action, Some(Action::Delete)));
    }

    #[test]
    fn test_ctrl_r_refresh() {
        let app = App::new(String::new(), Vec::new(), String::new());
        let action = handle_key_event(&app, make_ctrl_key(KeyCode::Char('r')));
        assert!(matches!(action, Some(Action::Refresh)));
    }

    #[test]
    fn test_resource_view_port_forward() {
        let app = App::new(String::new(), Vec::new(), String::new());
        let action = handle_key_event(&app, make_key(KeyCode::Char('f')));
        assert!(matches!(action, Some(Action::PortForward)));
    }

    #[test]
    fn test_restart_on_deployments() {
        let mut app = App::new(String::new(), Vec::new(), String::new());
        app.resource_tab = ResourceTab::Deployments;
        let action = handle_key_event(&app, make_key(KeyCode::Char('r')));
        assert!(matches!(action, Some(Action::Restart)));
    }

    #[test]
    fn test_q_goes_back_in_detail_view() {
        let mut app = App::new(String::new(), Vec::new(), String::new());
        app.route = Route::Yaml {
            resource: String::new(),
            name: String::new(),
            namespace: String::new(),
        };
        let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
        assert!(matches!(action, Some(Action::Back)));
    }

    #[test]
    fn test_log_view_s_toggles_follow() {
        let mut app = App::new(String::new(), Vec::new(), String::new());
        app.route = Route::Logs {
            pod: String::new(),
            container: String::new(),
            namespace: String::new(),
        };
        let action = handle_key_event(&app, make_key(KeyCode::Char('s')));
        assert!(matches!(action, Some(Action::ToggleLogFollow)));
    }

    #[test]
    fn test_log_view_t_toggles_timestamps() {
        let mut app = App::new(String::new(), Vec::new(), String::new());
        app.route = Route::Logs {
            pod: String::new(),
            container: String::new(),
            namespace: String::new(),
        };
        let action = handle_key_event(&app, make_key(KeyCode::Char('t')));
        assert!(matches!(action, Some(Action::ToggleLogTimestamps)));
    }

    #[test]
    fn test_log_view_shift_c_clears_logs() {
        let mut app = App::new(String::new(), Vec::new(), String::new());
        app.route = Route::Logs {
            pod: String::new(),
            container: String::new(),
            namespace: String::new(),
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
        let app = App::new(String::new(), Vec::new(), String::new());
        // No active filter, so Esc should be no-op.
        let action = handle_key_event(&app, make_key(KeyCode::Esc));
        assert!(action.is_none());
    }

    #[test]
    fn test_esc_goes_back_in_detail_view() {
        let mut app = App::new(String::new(), Vec::new(), String::new());
        app.route = Route::Describe {
            resource: String::new(),
            name: String::new(),
            namespace: String::new(),
        };
        let action = handle_key_event(&app, make_key(KeyCode::Esc));
        assert!(matches!(action, Some(Action::Back)));
    }

    #[test]
    fn test_esc_goes_back_in_log_view() {
        let mut app = App::new(String::new(), Vec::new(), String::new());
        app.route = Route::Logs {
            pod: String::new(),
            container: String::new(),
            namespace: String::new(),
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
        };
        let action = handle_key_event(&app, make_key(KeyCode::Char('q')));
        assert!(matches!(action, Some(Action::Back)));
    }

    #[test]
    fn test_log_view_home_end() {
        let mut app = App::new(String::new(), Vec::new(), String::new());
        app.route = Route::Logs {
            pod: String::new(),
            container: String::new(),
            namespace: String::new(),
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
        let mut app = App::new(String::new(), Vec::new(), String::new());
        app.route = Route::Yaml {
            resource: String::new(),
            name: String::new(),
            namespace: String::new(),
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
}
