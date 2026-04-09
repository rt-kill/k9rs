use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::app::actions::Action;
use crate::app::{App, Route};
#[cfg(test)]
use crate::app::nav::rid;
#[cfg(test)]
use crate::app::ContainerRef;
use crate::kube::protocol::OperationKind;

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
    // Detail views and log view override `/` to start search instead of filter.
    // -----------------------------------------------------------------------
    if matches!(app.route, Route::Yaml { .. } | Route::Describe { .. } | Route::Aliases { .. } | Route::Logs { .. }) {
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
        Route::Overview => handle_overview_keys(key),
        Route::Resources => handle_resource_view_keys(app, key),
        Route::Yaml { .. } | Route::Describe { .. } => handle_detail_view_keys(key),
        Route::Logs { .. } => handle_log_view_keys(app, key),
        Route::Help => handle_help_view_keys(key),
        Route::Contexts => handle_contexts_view_keys(key),
        Route::Shell { .. } => handle_log_view_keys(app, key),
        Route::ContainerSelect { .. } => handle_container_select_keys(key),
        Route::Aliases { .. } => handle_detail_view_keys(key),
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

    // Ctrl-Space: span-mark (select range).
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char(' ') {
        return Some(Action::SpanMark);
    }

    // Ctrl-\: clear all marks.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('\\') {
        return Some(Action::ClearMarks);
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
        return Some(Action::Delete);
    }

    // Ctrl-K: force-kill pod.
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('k') {
        if !app.current_capabilities().supports(OperationKind::ForceKill) {
            return Some(Action::FlashInfo("Force-kill is only available on Pods".to_string()));
        }
        return Some(Action::ForceKill);
    }

    match key.code {
        // `q` in resource view: if drilled/filtered, pop one level. Otherwise quit.
        KeyCode::Char('q') => {
            if app.nav.is_drilled() {
                Some(Action::ClearFilter)
            } else {
                Some(Action::Quit)
            }
        }

        // Esc in resource view: pop one nav level if drilled, otherwise no-op.
        // Overview is only a startup page — `:overview` or `:home` to return.
        KeyCode::Esc => {
            if app.nav.is_drilled() {
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
        KeyCode::Char('e') => Some(Action::Edit),

        // Logs: available on pods and workload types. We consult both the
        // server-provided capabilities AND the static resource_types table —
        // capabilities arrive asynchronously, so checking only the manifest
        // would make `l` fail in the brief window after subscribe but before
        // the server's first ResourceCapabilities event lands.
        KeyCode::Char('l') => {
            let can_log = app.current_capabilities().supports(OperationKind::StreamLogs)
                || crate::kube::resource_types::find_by_plural(&app.nav.resource_id().plural)
                    .map_or(false, |meta| meta.supports_logs);
            if can_log {
                Some(Action::Logs)
            } else {
                Some(Action::FlashInfo("Logs not available for this resource type".to_string()))
            }
        }
        KeyCode::Char('s') => {
            let caps = app.current_capabilities();
            if caps.supports(OperationKind::Shell) {
                Some(Action::Shell)
            } else if caps.supports(OperationKind::Scale) {
                Some(Action::Scale)
            } else if caps.supports(OperationKind::ToggleSuspendCronJob) {
                Some(Action::SuspendCronJob)
            } else {
                None
            }
        }
        KeyCode::Char('x') => {
            if app.current_capabilities().supports(OperationKind::DecodeSecret) {
                Some(Action::DecodeSecret)
            } else {
                None
            }
        }
        KeyCode::Char('t') => {
            if app.current_capabilities().supports(OperationKind::TriggerCronJob) {
                Some(Action::TriggerCronJob)
            } else {
                None
            }
        }
        KeyCode::Char('r') => {
            if app.current_capabilities().supports(OperationKind::Restart) {
                Some(Action::Restart)
            } else {
                None
            }
        }

        // F: create a new port-forward (opens dialog).
        KeyCode::Char('F') => Some(Action::PortForward),
        // f: show active port-forwards for this resource.
        KeyCode::Char('f') => Some(Action::ShowPortForwards),

        // Previous logs: available on pods and workload types.
        KeyCode::Char('p') => {
            if app.current_capabilities().supports(OperationKind::PreviousLogs) {
                Some(Action::PreviousLogs)
            } else {
                Some(Action::FlashInfo("Logs not available for this resource type".to_string()))
            }
        }

        // Show node for a pod.
        KeyCode::Char('o') => {
            if app.current_capabilities().supports(OperationKind::ShowNode) {
                Some(Action::ShowNode)
            } else {
                Some(Action::FlashInfo("Show node is only available on Pods".to_string()))
            }
        }

        // Toggle between last two views.
        KeyCode::Char('-') => Some(Action::ToggleLastView),

        // Sort by current column (toggle direction).
        KeyCode::Char('O') => Some(Action::ToggleSortDirection),

        // Sort by NAME: column 0 for cluster-scoped (no NAMESPACE column),
        // column 1 for namespaced resources.
        KeyCode::Char('N') => {
            let col = if app.current_tab_is_cluster_scoped() { 0 } else { 1 };
            Some(Action::Sort(col))
        }

        // Sort by AGE (last column — sentinel value usize::MAX).
        KeyCode::Char('A') => Some(Action::Sort(usize::MAX)),

        // Sort by STATUS (column 3, meaningful for Pods).
        KeyCode::Char('S') if app.current_capabilities().supports(OperationKind::Shell) => Some(Action::Sort(3)),

        // Copy.
        KeyCode::Char('c') => Some(Action::Copy),

        // Mark/select rows.
        KeyCode::Char(' ') => Some(Action::ToggleMark),

        // 0: switch to all namespaces
        KeyCode::Char('0') => Some(Action::SwitchNamespace(crate::kube::protocol::Namespace::All)),

        // Tab cycling.
        KeyCode::Tab => Some(Action::NextTab),
        KeyCode::BackTab => Some(Action::PrevTab),

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

fn handle_log_view_keys(app: &App, key: KeyEvent) -> Option<Action> {
    // Shift-C: clear logs.
    if key.modifiers.contains(KeyModifiers::SHIFT) && key.code == KeyCode::Char('C') {
        return Some(Action::ClearLogs);
    }

    match key.code {
        // `q` in log view goes back.
        KeyCode::Char('q') => Some(Action::Back),
        // Esc: if filtering, cancel draft or pop filter; otherwise go back.
        KeyCode::Esc => {
            let has_log_filters = match &app.route {
                Route::Logs { ref state, .. } | Route::Shell { ref state, .. } => {
                    state.is_filtering() || !state.filters.is_empty()
                }
                _ => false,
            };
            if has_log_filters {
                Some(Action::ClearFilter)
            } else {
                Some(Action::Back)
            }
        }

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

        // Search navigation.
        KeyCode::Char('n') => Some(Action::SearchNext),
        KeyCode::Char('N') => Some(Action::SearchPrev),

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

fn handle_overview_keys(key: KeyEvent) -> Option<Action> {
    match key.code {
        KeyCode::Char('q') => Some(Action::Quit),
        // Tab goes to the first resource view
        KeyCode::Tab => Some(Action::NextTab),
        KeyCode::BackTab => Some(Action::PrevTab),
        _ => None,
    }
}

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

#[cfg(test)]
#[path = "handler_tests.rs"]
mod tests;
