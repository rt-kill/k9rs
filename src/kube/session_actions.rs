use tokio::sync::mpsc;

use crate::app::{App, ContainerRef, InputMode};
use crate::app::nav::rid;
use crate::event::AppEvent;
use crate::kube::client_session::ClientSession;
use crate::kube::protocol::{self, ObjectRef};
use crate::kube::resource_def::BuiltInKind;
use crate::kube::session::{ds_try, ActionResult, apply_nav_change};

const PAGE_SCROLL_LINES: usize = 40;
const HELP_PAGE_SCROLL_LINES: usize = 10;
const DEFAULT_TERMINAL_HEIGHT: usize = 24;
const CHROME_LINES: usize = 5;
const LOG_CHROME_LINES: usize = 4;

/// Render-clamp-aware max for `help_scroll`. Mirrors the formula the
/// help overlay uses at render time so action handlers can store a
/// stable max and PrevItem decrements move the scroll position
/// immediately instead of being absorbed by the render-time clamp.
fn help_max_scroll(caps: &crate::kube::protocol::ResourceCapabilities) -> usize {
    let h = crossterm::terminal::size()
        .map(|(_, h)| h)
        .unwrap_or(DEFAULT_TERMINAL_HEIGHT as u16);
    crate::ui::widgets::HelpOverlay::max_scroll(h, Some(caps))
}


// ---------------------------------------------------------------------------
// Main dispatcher — thin routing table, delegates to focused sub-functions
// ---------------------------------------------------------------------------

pub(crate) fn handle_action(
    app: &mut App,
    action: crate::app::actions::Action,
    event_tx: &mpsc::Sender<AppEvent>,
    data_source: &mut ClientSession,
) -> ActionResult {
    use crate::app::actions::Action;
    use crate::app::Route;

    match action {
        // --- Lifecycle ---
        Action::Quit => {
            app.exit_reason = Some(crate::app::ExitReason::UserQuit);
            app.should_quit = true;
        }
        Action::Back => {
            if let Some(route) = app.route_stack.pop() {
                app.route = route;
            }
        }
        Action::Help => {
            if matches!(app.route, Route::Help) {
                if let Some(route) = app.route_stack.pop() {
                    app.route = route;
                } else {
                    app.route = Route::Resources;
                }
            } else {
                app.navigate_to(Route::Help);
            }
        }

        // --- Tab switching ---
        Action::NextTab => {
            if matches!(app.route, Route::Overview) { app.route = Route::Resources; }
            let new_rid = app.next_tab();
            let change = app.nav.reset(new_rid);
            *app.nav.filter_input_mut() = Default::default();
            apply_nav_change(app, data_source, change);
        }
        Action::PrevTab => {
            if matches!(app.route, Route::Overview) { app.route = Route::Resources; }
            let new_rid = app.prev_tab();
            let change = app.nav.reset(new_rid);
            *app.nav.filter_input_mut() = Default::default();
            apply_nav_change(app, data_source, change);
        }

        // --- Scroll / in-view navigation ---
        a @ (Action::NextItem | Action::PrevItem | Action::PageUp | Action::PageDown
            | Action::Home | Action::End | Action::ScrollUp(_) | Action::ScrollDown(_)
            | Action::ColLeft | Action::ColRight) => {
            handle_scroll(app, a);
        }

        // --- Already-extracted resource actions ---
        Action::Enter => return handle_enter(app, data_source),
        Action::Describe => handle_describe(app, data_source),
        Action::Yaml => handle_yaml(app, data_source),
        Action::Logs => handle_logs(app, data_source),
        Action::PreviousLogs => handle_previous_logs(app, data_source),

        // --- Resource CRUD / mutation operations ---
        a @ (Action::Shell | Action::Delete | Action::Edit | Action::Scale
            | Action::Restart | Action::ForceKill | Action::DecodeSecret
            | Action::TriggerCronJob | Action::SuspendCronJob) => {
            return handle_resource_op(app, a, data_source);
        }

        // --- Confirmation dialog ---
        Action::Confirm => return handle_confirm_action(app, data_source),
        Action::Cancel => { app.ui.confirm_dialog = None; }
        Action::ToggleDialogButton => {
            if let Some(ref mut dialog) = app.ui.confirm_dialog {
                dialog.action_focused = !dialog.action_focused;
            }
        }

        // --- Filter & search ---
        a @ (Action::Filter(_) | Action::ColumnFilter | Action::ClearFilter
            | Action::ToggleFaultFilter | Action::SearchStart | Action::SearchExec(_)
            | Action::SearchNext | Action::SearchPrev | Action::SearchClear) => {
            handle_filter_search(app, a, data_source);
        }

        // --- Log view actions ---
        a @ (Action::ToggleLogFollow | Action::ToggleLogWrap | Action::ToggleLogTimestamps
            | Action::ClearLogs | Action::LogSince(_) | Action::SaveLogs) => {
            handle_log_action(app, a, data_source);
        }

        // --- Drill-down navigation ---
        a @ (Action::ShowNode | Action::UsedBy | Action::JumpToOwner | Action::NodeShell
            | Action::OverlayCapability(_)) => {
            return handle_drill(app, a, data_source);
        }

        // --- Clipboard / file I/O ---
        a @ (Action::Copy | Action::SaveTable) => {
            handle_io(app, a, event_tx, data_source);
        }

        // --- Simple inline actions ---
        Action::SwitchNamespace(ns) => do_switch_namespace(app, data_source, ns),
        Action::SwitchContext(ctx) => begin_context_switch(app, data_source, &ctx),
        Action::CommandMode => {
            app.ui.input_mode = InputMode::Command { input: String::new(), history_index: None };
        }
        Action::Sort(target) => app.sort_by(target),
        Action::ToggleSortDirection => app.toggle_sort_direction(),
        Action::ToggleHeader => { app.ui.show_header = !app.ui.show_header; }
        Action::ToggleWide => {
            app.ui.column_level = app.ui.column_level.next();
            app.ui.flash = Some(crate::app::FlashMessage::info(
                format!("Columns: {}", app.ui.column_level.label())
            ));
        }
        Action::ToggleMark => app.toggle_mark(),
        Action::SpanMark => app.span_mark(),
        Action::ClearMarks => app.clear_marks(),
        Action::ToggleLastView => {
            if let Some(last) = app.nav.prev_root().cloned() {
                let change = app.nav.reset(last);
                *app.nav.filter_input_mut() = Default::default();
                apply_nav_change(app, data_source, change);
            }
        }
        Action::ShowPortForwards => handle_show_port_forwards(app, data_source),
        Action::PortForward => {
            if let Some(dialog) = resolve_port_forward_dialog(app) {
                app.ui.form_dialog = Some(dialog);
            } else {
                app.ui.flash = Some(crate::app::FlashMessage::error(
                    "No target found for port-forward".to_string()
                ));
            }
        }
        Action::Refresh => handle_refresh(app, data_source),
        Action::ShowAliases => handle_show_aliases(app),
        Action::FlashInfo(msg) => {
            app.ui.flash = Some(crate::app::FlashMessage::info(msg));
        }
    }
    ActionResult::None
}

// ---------------------------------------------------------------------------
// Scroll / in-view navigation
// ---------------------------------------------------------------------------

fn handle_scroll(app: &mut App, action: crate::app::actions::Action) {
    use crate::app::actions::Action;
    use crate::app::Route;

    match action {
        Action::NextItem => {
            let caps = app.current_capabilities();
            match &mut app.route {
                Route::ContentView { ref mut state, .. } => {
                    let max = state.line_count().saturating_sub(1);
                    state.scroll = (state.scroll + 1).min(max);
                }
                Route::ContainerSelect { ref target, ref mut selected, .. } => {
                    let container_count = {
                        // Manual dual-path lookup: can't call app.table_for()
                        // because app.route is mutably borrowed by this match.
                        let table = if crate::app::nav::is_globally_stored(&target.resource) {
                            app.data.tables.get(&target.resource)
                        } else {
                            app.nav.find_table_for_resource(&target.resource)
                        };
                        table
                            .and_then(|t| t.items.iter().find(|p| {
                                p.name == target.name && p.namespace.as_deref() == target.namespace.as_option()
                            }))
                            .map(|p| p.containers.len())
                            .unwrap_or(0)
                    };
                    if container_count > 0 && *selected + 1 < container_count {
                        *selected += 1;
                    }
                }
                Route::Help => {
                    let max = help_max_scroll(&caps);
                    app.ui.help_scroll = (app.ui.help_scroll + 1).min(max);
                }
                _ => app.select_next(),
            }
        }
        Action::PrevItem => {
            match &mut app.route {
                Route::ContentView { ref mut state, .. } => state.scroll = state.scroll.saturating_sub(1),
                Route::ContainerSelect { ref mut selected, .. } => {
                    *selected = selected.saturating_sub(1);
                }
                Route::Help => {
                    app.ui.help_scroll = app.ui.help_scroll.saturating_sub(1);
                }
                _ => app.select_prev(),
            }
        }
        Action::PageUp => {
            match &mut app.route {
                Route::Logs { ref mut state, .. } => {
                    state.follow = false;
                    state.scroll = state.scroll.saturating_sub(PAGE_SCROLL_LINES);
                }
                Route::ContentView { ref mut state, .. } => state.scroll = state.scroll.saturating_sub(PAGE_SCROLL_LINES),
                Route::Help => {
                    app.ui.help_scroll = app.ui.help_scroll.saturating_sub(HELP_PAGE_SCROLL_LINES);
                }
                _ => app.page_up(),
            }
        }
        Action::PageDown => {
            let caps = app.current_capabilities();
            match &mut app.route {
                Route::Logs { ref mut state, .. } => {
                    state.follow = false;
                    let total = state.visible_count();
                    let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CHROME_LINES);
                    let max = total.saturating_sub(visible);
                    state.scroll = (state.scroll + PAGE_SCROLL_LINES).min(max);
                }
                Route::ContentView { ref mut state, .. } => {
                    let max = state.line_count().saturating_sub(1);
                    state.scroll = (state.scroll + PAGE_SCROLL_LINES).min(max);
                }
                Route::Help => {
                    let max = help_max_scroll(&caps);
                    app.ui.help_scroll = (app.ui.help_scroll + HELP_PAGE_SCROLL_LINES).min(max);
                }
                _ => app.page_down(),
            }
        }
        Action::Home => {
            match &mut app.route {
                Route::Logs { ref mut state, .. } => {
                    state.follow = false;
                    state.scroll = 0;
                }
                Route::ContentView { ref mut state, .. } => state.scroll = 0,
                Route::Help => app.ui.help_scroll = 0,
                _ => app.go_home(),
            }
        }
        Action::End => {
            let caps = app.current_capabilities();
            match &mut app.route {
                Route::Logs { ref mut state, .. } => {
                    let total = state.visible_count();
                    let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CHROME_LINES);
                    state.scroll = total.saturating_sub(visible);
                    state.follow = true;
                }
                Route::ContentView { ref mut state, .. } => {
                    state.scroll = state.line_count().saturating_sub(1);
                }
                Route::Help => {
                    app.ui.help_scroll = help_max_scroll(&caps);
                }
                _ => app.go_end(),
            }
        }
        Action::ScrollUp(n) => {
            if let Route::Logs { ref mut state, .. } = app.route {
                if state.follow {
                    let total = state.visible_count();
                    let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CHROME_LINES);
                    state.scroll = total.saturating_sub(visible);
                    state.follow = false;
                }
                state.scroll = state.scroll.saturating_sub(n);
            }
        }
        Action::ScrollDown(n) => {
            if let Route::Logs { ref mut state, .. } = app.route {
                if !state.follow {
                    let total = state.visible_count();
                    let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CHROME_LINES);
                    let max = total.saturating_sub(visible);
                    state.scroll = (state.scroll + n).min(max);
                }
            }
        }
        Action::ColLeft => app.col_left(),
        Action::ColRight => app.col_right(),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Resource CRUD / mutation operations
// ---------------------------------------------------------------------------

fn handle_resource_op(
    app: &mut App,
    action: crate::app::actions::Action,
    data_source: &mut ClientSession,
) -> ActionResult {
    use crate::app::actions::Action;
    use crate::app::Route;

    match action {
        Action::Shell => {
            if app.current_capabilities().supports(crate::kube::protocol::OperationKind::Shell) {
                let current_rid = app.nav.resource_id().clone();
                if let Some(row) = app.active_view_table().and_then(|t| t.selected_item()) {
                    let Some(pod_ns) = row.namespace.clone() else {
                        app.ui.flash = Some(crate::app::FlashMessage::error(
                            format!("Shell refused: pod/{} has no resolved namespace", row.name)
                        ));
                        return ActionResult::None;
                    };
                    if pod_ns.is_empty() {
                        app.ui.flash = Some(crate::app::FlashMessage::error(
                            format!("Shell refused: pod/{} has empty namespace", row.name)
                        ));
                        return ActionResult::None;
                    }
                    let pod_name = row.name.clone();
                    let containers = &row.containers;
                    let target = crate::kube::protocol::ObjectRef::new(
                        current_rid,
                        pod_name.clone(),
                        crate::kube::protocol::Namespace::from_row(&pod_ns),
                    );
                    if containers.len() > 1 {
                        app.navigate_to(Route::ContainerSelect {
                            target,
                            selected: 0,
                            action: crate::app::ContainerAction::Shell,
                        });
                    } else {
                        let container = containers.first().map(|c| c.name.clone()).unwrap_or_default();
                        return ActionResult::Shell(crate::kube::session::ExecTarget {
                            pod: pod_name,
                            namespace: pod_ns,
                            container,
                        });
                    }
                }
            }
        }
        Action::Delete => {
            let marked = get_marked_resource_infos(app);
            if !marked.is_empty() {
                let count = marked.len();
                let resource = marked[0].resource.display_label().to_string();
                app.ui.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Delete {} {}s?", count, resource),
                    action_label: "Delete".to_string(),
                    pending: crate::app::PendingAction::BatchDelete(marked),
                    action_focused: false,
                });
            } else if let Some(info) = get_selected_resource_info(app) {
                app.ui.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Delete {}/{}?", info.resource.display_label(), info.name),
                    action_label: "Delete".to_string(),
                    pending: crate::app::PendingAction::Single { op: crate::app::SingleOp::Delete, target: info },
                    action_focused: false,
                });
            }
        }
        Action::Edit => {
            if matches!(app.route, Route::EditingResource { .. }) {
                app.ui.flash = Some(crate::app::FlashMessage::warn("Edit already in progress".to_string()));
                return ActionResult::None;
            }
            if let Some(info) = get_selected_resource_info(app) {
                ds_try!(app, data_source.yaml(&info));
                app.navigate_to(Route::EditingResource {
                    target: info,
                    state: crate::app::EditState::AwaitingYaml,
                });
            }
        }
        Action::Scale => {
            if let Some(info) = get_selected_resource_info(app) {
                let current_replicas = {
                    let ready_col = app.active_view_descriptor()
                        .and_then(|d| d.col("READY"));
                    app.active_view_table()
                        .and_then(|t| t.selected_item())
                        .and_then(|row| {
                            ready_col
                                .and_then(|col| row.cells.get(col))
                                .map(|cell| {
                                    let s = cell.to_string();
                                    s.split('/').nth(1).unwrap_or("").trim().to_string()
                                })
                                .filter(|s| !s.is_empty())
                        })
                };
                app.ui.form_dialog = Some(build_scale_form(info, current_replicas.as_deref()));
            }
        }
        Action::Restart => {
            let marked = get_marked_resource_infos(app);
            if !marked.is_empty() {
                let count = marked.len();
                let resource = marked[0].resource.display_label().to_string();
                app.ui.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Restart {} {}s?", count, resource),
                    action_label: "Restart".to_string(),
                    pending: crate::app::PendingAction::BatchRestart(marked),
                    action_focused: false,
                });
            } else if let Some(info) = get_selected_resource_info(app) {
                app.ui.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Restart {}/{}?", info.resource.display_label(), info.name),
                    action_label: "Restart".to_string(),
                    pending: crate::app::PendingAction::Single { op: crate::app::SingleOp::Restart, target: info },
                    action_focused: false,
                });
            }
        }
        Action::ForceKill => {
            let marked = get_marked_resource_infos(app);
            if !marked.is_empty() {
                let count = marked.len();
                app.ui.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Force-kill {} pods?", count),
                    action_label: "Force Kill".to_string(),
                    pending: crate::app::PendingAction::BatchForceKill(marked),
                    action_focused: false,
                });
            } else if let Some(info) = get_selected_resource_info(app) {
                app.ui.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Force-kill {}/{}?", info.resource.display_label(), info.name),
                    action_label: "Force Kill".to_string(),
                    pending: crate::app::PendingAction::Single { op: crate::app::SingleOp::ForceKill, target: info },
                    action_focused: false,
                });
            }
        }
        Action::DecodeSecret => {
            if let Some(info) = get_selected_resource_info(app) {
                let mut state = crate::app::ContentViewState::default();
                state.set_content(format!("Decoding secret {}/{}...", info.namespace, info.name));
                ds_try!(app, data_source.decode_secret(&info));
                app.navigate_to(Route::ContentView {
                    kind: crate::app::ContentViewKind::Describe,
                    target: Some(info),
                    awaiting_response: false,
                    state,
                });
            }
        }
        Action::TriggerCronJob => {
            if let Some(info) = get_selected_resource_info(app) {
                let name = info.name.clone();
                ds_try!(app, data_source.trigger_cronjob(&info));
                app.ui.flash = Some(crate::app::FlashMessage::info(format!("Triggering CronJob: {}", name)));
            }
        }
        Action::SuspendCronJob => {
            if let Some(info) = get_selected_resource_info(app) {
                ds_try!(app, data_source.toggle_suspend_cronjob(&info));
            }
        }
        _ => {}
    }
    ActionResult::None
}

// ---------------------------------------------------------------------------
// Confirmation dialog
// ---------------------------------------------------------------------------

fn handle_confirm_action(
    app: &mut App,
    data_source: &mut ClientSession,
) -> ActionResult {
    if let Some(dialog) = app.ui.confirm_dialog.take() {
        app.kube.kubectl_cache.clear();
        match dialog.pending {
            crate::app::PendingAction::Single { op: crate::app::SingleOp::Delete, ref target } => {
                ds_try!(app, data_source.delete(target));
            }
            crate::app::PendingAction::Single { op: crate::app::SingleOp::Restart, ref target } => {
                ds_try!(app, data_source.restart(target));
            }
            crate::app::PendingAction::Single { op: crate::app::SingleOp::ForceKill, ref target } => {
                if target.namespace.as_option().is_none() {
                    app.ui.flash = Some(crate::app::FlashMessage::error(
                        format!("Force-kill refused: pod/{} has no resolved namespace", target.name)
                    ));
                    return ActionResult::None;
                }
                ds_try!(app, data_source.force_kill(target));
            }
            crate::app::PendingAction::BatchDelete(batch) => {
                app.clear_marks();
                for item in &batch {
                    ds_try!(app, data_source.delete(item));
                }
            }
            crate::app::PendingAction::BatchRestart(batch) => {
                app.clear_marks();
                for item in &batch {
                    ds_try!(app, data_source.restart(item));
                }
            }
            crate::app::PendingAction::BatchForceKill(batch) => {
                app.clear_marks();
                if let Some(bad) = batch.iter().find(|t| t.namespace.as_option().is_none()) {
                    app.ui.flash = Some(crate::app::FlashMessage::error(
                        format!("Batch force-kill refused: pod/{} has no resolved namespace", bad.name)
                    ));
                    return ActionResult::None;
                }
                for item in &batch {
                    ds_try!(app, data_source.force_kill(item));
                }
            }
        }
    }
    ActionResult::None
}

// ---------------------------------------------------------------------------
// Filter & search
// ---------------------------------------------------------------------------

fn handle_filter_search(
    app: &mut App,
    action: crate::app::actions::Action,
    data_source: &mut ClientSession,
) {
    use crate::app::actions::Action;
    use crate::app::Route;

    match action {
        Action::Filter(_) => {
            if matches!(app.route, Route::Resources) {
                let fi = app.nav.filter_input_mut();
                fi.active = true;
                fi.text.clear();
                fi.column = None;
            } else if let Route::Logs { ref mut state, .. } = app.route {
                state.start_filter();
            }
        }
        Action::ColumnFilter => {
            if matches!(app.route, Route::Resources) {
                let col = app.active_table_selected_col();
                let fi = app.nav.filter_input_mut();
                fi.active = true;
                fi.text.clear();
                fi.column = Some(col);
            }
        }
        Action::ClearFilter => {
            if let Route::Logs { ref mut state, .. } = app.route {
                if state.is_filtering() {
                    state.cancel_filter();
                } else if !state.filters.is_empty() {
                    state.pop_filter();
                }
            } else if let Some((popped, change)) = app.nav.pop() {
                apply_nav_change(app, data_source, change);
                if popped.resource != *app.nav.resource_id() {
                    let saved = app.nav.current().saved_selected;
                    app.select_in_active_table(saved);
                }
                app.reapply_nav_filters();
            } else {
                app.clear_filter();
            }
        }
        Action::ToggleFaultFilter => {
            if !app.nav.has_fault_filter() {
                let sel = app.active_table_selected();
                app.nav.save_selected(sel);
                let current_rid = app.nav.resource_id().clone();
                let change = app.nav.push(crate::app::nav::NavStep::new(
                    current_rid,
                    Some(crate::app::nav::NavFilter::Fault),
                ));
                apply_nav_change(app, data_source, change);
                app.reapply_nav_filters();
                app.ui.flash = Some(crate::app::FlashMessage::info("Fault filter ON"));
            } else {
                app.nav.pop_fault_filter();
                app.reapply_nav_filters();
                app.ui.flash = Some(crate::app::FlashMessage::info("Fault filter OFF"));
            }
        }
        Action::SearchStart => {
            match &mut app.route {
                Route::ContentView { ref mut state, .. } => {
                    state.search_input_active = true;
                    state.search_input.clear();
                }
                Route::Logs { ref mut state, .. } => {
                    state.start_filter();
                }
                _ => {}
            }
        }
        Action::SearchExec(term) => {
            if let Route::ContentView { ref mut state, .. } = app.route {
                state.search = Some(term.clone());
                state.update_search();
            }
        }
        Action::SearchNext => {
            match &mut app.route {
                Route::ContentView { ref mut state, .. } => {
                    state.next_match(40);
                }
                Route::Logs { ref mut state, .. } => {
                    let current_scroll = state.scroll;
                    if let Some(&next_idx) = state.filtered_indices.iter()
                        .find(|&&idx| idx > current_scroll)
                    {
                        state.scroll = next_idx;
                        state.follow = false;
                    } else if let Some(&first) = state.filtered_indices.first() {
                        state.scroll = first;
                        state.follow = false;
                    }
                }
                _ => {}
            }
        }
        Action::SearchPrev => {
            match &mut app.route {
                Route::ContentView { ref mut state, .. } => {
                    state.prev_match(40);
                }
                Route::Logs { ref mut state, .. } => {
                    let current_scroll = state.scroll;
                    if let Some(&prev_idx) = state.filtered_indices.iter().rev()
                        .find(|&&idx| idx < current_scroll)
                    {
                        state.scroll = prev_idx;
                        state.follow = false;
                    } else if let Some(&last) = state.filtered_indices.last() {
                        state.scroll = last;
                        state.follow = false;
                    }
                }
                _ => {}
            }
        }
        Action::SearchClear => {
            match &mut app.route {
                Route::ContentView { ref mut state, .. } => {
                    state.clear_search();
                }
                Route::Logs { .. } => {
                    // Log view uses stackable filters — cleared via Esc/pop_filter
                }
                _ => {}
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Log view actions
// ---------------------------------------------------------------------------

fn handle_log_action(
    app: &mut App,
    action: crate::app::actions::Action,
    data_source: &mut ClientSession,
) {
    use crate::app::actions::Action;
    use crate::app::Route;

    match action {
        Action::ToggleLogFollow => {
            if let Route::Logs { ref mut state, .. } = app.route {
                state.follow = !state.follow;
                if state.follow {
                    state.scroll = state.visible_count().saturating_sub(1);
                } else {
                    let (_, rows) = crossterm::terminal::size().unwrap_or((80, DEFAULT_TERMINAL_HEIGHT as u16));
                    let visible = (rows as usize).saturating_sub(LOG_CHROME_LINES);
                    let total = state.visible_count();
                    state.scroll = total.saturating_sub(visible);
                }
            }
        }
        Action::ToggleLogWrap => {
            if let Route::Logs { ref mut state, .. } = app.route {
                state.wrap = !state.wrap;
            }
        }
        Action::ToggleLogTimestamps => {
            if let Route::Logs { ref mut state, .. } = app.route {
                state.show_timestamps = !state.show_timestamps;
                app.ui.flash = Some(crate::app::FlashMessage::info(
                    if state.show_timestamps { "Timestamps: on" } else { "Timestamps: off" }
                ));
            }
        }
        Action::ClearLogs => {
            if let Route::Logs { ref mut state, .. } = app.route {
                state.clear();
            }
            app.ui.flash = Some(crate::app::FlashMessage::info("Logs cleared"));
        }
        Action::LogSince(ref since) => {
            let since_label = since.as_deref().unwrap_or("tail");
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Log range: {}", since_label)));
            if let Route::Logs { ref target, ref mut state, ref mut stream } = app.route {
                state.since = since.clone();
                let tail_lines = state.tail_lines;
                state.clear();
                state.follow = true;
                state.streaming = true;
                state.since = since.clone();
                let tail = if since.is_none() { Some(tail_lines) } else { None };
                let new_stream = data_source.stream_log_substream(crate::kube::protocol::LogInit {
                    pod: target.pod.clone(),
                    namespace: crate::kube::protocol::Namespace::from_row(&target.namespace),
                    container: target.container.clone(),
                    follow: true,
                    tail,
                    since: since.clone(),
                    previous: false,
                });
                state.generation = new_stream.generation;
                *stream = Some(new_stream);
            }
        }
        Action::SaveLogs => {
            if let Route::Logs { ref target, ref state, .. } = app.route {
                let safe = |s: &str| s.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect::<String>();
                let filename = format!(
                    "logs-{}-{}-{}.log",
                    safe(&target.pod), safe(target.container_label()),
                    chrono::Utc::now().format("%Y%m%d-%H%M%S"),
                );
                let content: String = state.lines.iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("\n");
                match crate::util::safe_write_temp(&filename, content.as_bytes()) {
                    Ok(path) => {
                        let count = content.lines().count();
                        app.ui.flash = Some(crate::app::FlashMessage::info(
                            format!("Saved {} lines to {}", count, path.display())
                        ));
                    }
                    Err(e) => {
                        app.ui.flash = Some(crate::app::FlashMessage::error(
                            format!("Save failed: {}", e)
                        ));
                    }
                }
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Drill-down navigation (ShowNode, UsedBy, JumpToOwner, NodeShell)
// ---------------------------------------------------------------------------

fn handle_drill(
    app: &mut App,
    action: crate::app::actions::Action,
    data_source: &mut ClientSession,
) -> ActionResult {
    use crate::app::actions::Action;

    match action {
        Action::ShowNode => {
            if app.current_capabilities().supports(crate::kube::protocol::OperationKind::ShowNode) {
                if let Some(row) = app.active_view_table().and_then(|t| t.selected_item()) {
                    let node = row.node.clone().unwrap_or_default();
                    if !node.is_empty() {
                        let sel = app.active_table_selected();
                        app.nav.save_selected(sel);
                        let change = app.nav.push(crate::app::nav::NavStep::new(
                            rid(BuiltInKind::Node),
                            Some(crate::app::nav::NavFilter::Grep(
                                crate::app::nav::CompiledGrep::new(regex::escape(&node)),
                            )),
                        ));
                        apply_nav_change(app, data_source, change);
                        app.reapply_nav_filters();
                    }
                }
            }
        }
        Action::UsedBy => {
            if let Some(info) = get_selected_resource_info(app) {
                let name = info.name.clone();
                let kind = info.resource.display_label().to_string();
                let sel = app.active_table_selected();
                app.nav.save_selected(sel);
                let change = app.nav.push(crate::app::nav::NavStep::new(
                    rid(BuiltInKind::Pod),
                    Some(crate::app::nav::NavFilter::Grep(
                        crate::app::nav::CompiledGrep::new(regex::escape(&name)),
                    )),
                ));
                apply_nav_change(app, data_source, change);
                app.reapply_nav_filters();
                app.ui.flash = Some(crate::app::FlashMessage::info(
                    format!("Pods referencing {}/{}", kind.to_lowercase(), name)
                ));
            }
        }
        Action::JumpToOwner => {
            if let Some(row) = app.active_view_table().and_then(|t| t.selected_item()) {
                if let Some(owner) = row.owner_refs.first() {
                    let owner_kind = owner.kind.clone();
                    let owner_name = owner.name.clone();
                    let owner_rid = crate::kube::protocol::ResourceId::from_alias(&owner_kind.to_lowercase())
                        .unwrap_or_else(|| {
                            crate::kube::protocol::ResourceId::CrdUnresolved(owner_kind.to_lowercase())
                        });
                    let sel = app.active_table_selected();
                    app.nav.save_selected(sel);
                    let change = app.nav.push(crate::app::nav::NavStep::new(
                        owner_rid,
                        Some(crate::app::nav::NavFilter::Grep(
                            crate::app::nav::CompiledGrep::new(regex::escape(&owner_name)),
                        )),
                    ));
                    apply_nav_change(app, data_source, change);
                    app.reapply_nav_filters();
                    app.ui.flash = Some(crate::app::FlashMessage::info(
                        format!("Owner: {}/{}", owner_kind.to_lowercase(), owner_name)
                    ));
                } else {
                    app.ui.flash = Some(crate::app::FlashMessage::warn("No owner found".to_string()));
                }
            }
        }
        Action::NodeShell => {
            if let Some(row) = app.data.tables
                .get(&rid(BuiltInKind::Node))
                .and_then(|t| t.selected_item())
            {
                let node = row.name.clone();
                return ActionResult::NodeShell { node };
            }
        }
        Action::OverlayCapability(ref cap_name) => {
            let plural = app.nav.resource_id().plural().to_owned();
            let overlay = crate::kube::overlay::overlay_for(&plural);
            let cap = overlay.and_then(|o| o.capabilities.get(cap_name));
            match cap {
                Some(crate::kube::overlay::OverlayCapability::Drill { target, column }) => {
                    let target_rid = crate::kube::protocol::ResourceId::from_alias(target)
                        .unwrap_or_else(|| crate::kube::protocol::ResourceId::CrdUnresolved(target.clone()));
                    let filter_value = app.active_view_table()
                        .and_then(|t| t.selected_item())
                        .and_then(|row| {
                            let desc = app.active_view_descriptor()?;
                            let col_idx = desc.headers.iter()
                                .position(|h| h.eq_ignore_ascii_case(column))?;
                            row.cells.get(col_idx).map(|c| c.to_string())
                        });
                    if let Some(value) = filter_value.filter(|v| !v.is_empty()) {
                        let sel = app.active_table_selected();
                        app.nav.save_selected(sel);
                        let change = app.nav.push(crate::app::nav::NavStep::new(
                            target_rid,
                            Some(crate::app::nav::NavFilter::Grep(
                                crate::app::nav::CompiledGrep::new(regex::escape(&value)),
                            )),
                        ));
                        apply_nav_change(app, data_source, change);
                        app.reapply_nav_filters();
                        app.ui.flash = Some(crate::app::FlashMessage::info(
                            format!("{} matching: {}", target, value)
                        ));
                    } else {
                        app.ui.flash = Some(crate::app::FlashMessage::warn(
                            format!("No value in column '{}' for '{}'", column, cap_name)
                        ));
                    }
                }
                None => {
                    app.ui.flash = Some(crate::app::FlashMessage::warn(
                        format!("Unknown overlay capability: {}", cap_name)
                    ));
                }
            }
        }
        _ => {}
    }
    ActionResult::None
}

// ---------------------------------------------------------------------------
// Clipboard / file I/O
// ---------------------------------------------------------------------------

fn handle_io(
    app: &mut App,
    action: crate::app::actions::Action,
    event_tx: &mpsc::Sender<AppEvent>,
    data_source: &mut ClientSession,
) {
    use crate::app::actions::Action;
    use crate::app::Route;

    match action {
        Action::Copy => {
            let (text, label) = match &app.route {
                Route::ContentView { ref state, .. } => {
                    if state.content.is_empty() {
                        (String::new(), String::new())
                    } else {
                        let lines = state.line_count();
                        (state.content.clone(), format!("Copied {} lines to clipboard", lines))
                    }
                }
                Route::Logs { ref state, .. } => {
                    if state.lines.is_empty() {
                        (String::new(), String::new())
                    } else {
                        let joined: String = state.lines.iter().cloned().collect::<Vec<_>>().join("\n");
                        let count = state.lines.len();
                        (joined, format!("Copied {} lines to clipboard", count))
                    }
                }
                _ => {
                    if let Some(info) = get_selected_resource_info(app) {
                        let label = format!("Copied: {}", info.name);
                        (info.name, label)
                    } else {
                        (String::new(), String::new())
                    }
                }
            };
            if !text.is_empty() {
                let tx = event_tx.clone();
                data_source.track_task(async move {
                    let ok = tokio::task::spawn_blocking(move || {
                        crate::util::try_copy_to_clipboard(&text)
                    }).await.unwrap_or(false);
                    if ok {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(label))).await;
                    } else {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::warn(
                            "No clipboard tool found (install xclip, xsel, wl-copy, or pbcopy)"
                        ))).await;
                    }
                });
            }
        }
        Action::SaveTable => {
            let safe = |s: &str| s.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect::<String>();
            let filename = format!(
                "{}-{}.txt",
                safe(app.nav.resource_id().short_label()),
                chrono::Utc::now().format("%Y%m%d-%H%M%S")
            );
            let content = build_table_dump(app);
            let tx = event_tx.clone();
            data_source.track_task(async move {
                let result = tokio::task::spawn_blocking(move || {
                    crate::util::safe_write_temp(&filename, content.as_bytes())
                }).await;
                match result {
                    Ok(Ok(path)) => {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(format!("Saved to {}", path.display())))).await;
                    }
                    Ok(Err(e)) => {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(format!("Save failed: {}", e)))).await;
                    }
                    Err(e) => {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(format!("Save failed: {}", e)))).await;
                    }
                }
            });
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Standalone helpers
// ---------------------------------------------------------------------------

fn handle_show_port_forwards(app: &mut App, data_source: &mut ClientSession) {
    use crate::app::Route;
    let pf_rid = crate::kube::protocol::ResourceId::Local(
        crate::kube::local::LocalResourceKind::PortForward,
    );
    let sel = app.active_table_selected();
    app.nav.save_selected(sel);
    app.route = Route::Resources;
    let change = app.nav.push(crate::app::nav::NavStep::new(pf_rid, None));
    apply_nav_change(app, data_source, change);
}

fn handle_refresh(app: &mut App, data_source: &mut ClientSession) {
    let rid = app.nav.resource_id().clone();
    let filter = app.nav.with_subscription_owner(|owner| {
        let f = owner.filter.as_ref().and_then(|f| f.to_subscription_filter());
        owner.stream = None;
        f
    });
    app.clear_resource(&rid);
    let stream = data_source.subscribe_stream_force(rid, app.kube.selected_ns.clone(), filter);
    app.nav.with_subscription_owner(|owner| owner.stream = Some(stream));
    app.ui.flash = Some(crate::app::FlashMessage::info("Refreshed"));
}

fn handle_show_aliases(app: &mut App) {
    use crate::app::Route;
    let mut content = String::from("Resource Aliases\n================\n\n");
    content.push_str(&format!("  {:<45} {}\n", "ALIAS", "RESOURCE"));
    content.push_str(&format!("  {:<45} {}\n", "-----", "--------"));
    for def in crate::kube::resource_defs::REGISTRY.all() {
        let aliases = def.aliases().join("/");
        content.push_str(&format!("  {:<45} {}\n", aliases, def.gvr().kind));
    }
    content.push_str("\n\nSpecial Commands\n================\n\n");
    content.push_str("  :q / :quit / :exit         Quit\n");
    content.push_str("  :help / :h / :?            Show help\n");
    content.push_str("  :ctx / :context            Context selector\n");
    content.push_str("  :ctx <name>                Switch context\n");
    content.push_str("  :ns <name>                 Switch namespace\n");
    content.push_str("  :alias / :aliases / :a     This view\n");
    content.push_str("\n\nKey Bindings\n============\n\n");
    content.push_str("  Ctrl-a                     Aliases view\n");
    content.push_str("  Ctrl-c                     Quit\n");
    content.push_str("  Ctrl-r                     Refresh\n");
    content.push_str("  Ctrl-e                     Toggle header\n");
    content.push_str("  Ctrl-s                     Save table to file\n");
    let mut state = crate::app::ContentViewState::default();
    state.set_content(content);
    app.navigate_to(Route::ContentView {
        kind: crate::app::ContentViewKind::Aliases,
        target: None,
        awaiting_response: false,
        state,
    });
}

// ===========================================================================
// Action handlers (merged from session_handlers.rs)
// ===========================================================================

/// Map a container name string from a row to the typed `LogContainer`.
/// Empty -> `Default` (let kubectl pick); a real container name -> `Named`.
fn log_container_from_str(name: &str) -> protocol::LogContainer {
    if name.is_empty() {
        protocol::LogContainer::Default
    } else {
        protocol::LogContainer::Named(name.to_string())
    }
}

pub(crate) fn handle_enter(
    app: &mut App,
    data_source: &mut ClientSession,
) -> ActionResult {
    use crate::app::Route;
    use crate::kube::protocol::ResourceId;

    // Handle context view Enter
    if matches!(app.route, Route::Contexts) {
        if let Some(ctx) = app.data.contexts.selected_item() {
            let ctx_name = ctx.name.clone();
            begin_context_switch(app, data_source, &ctx_name);
        }
        return ActionResult::None;
    }

    // Handle ContainerSelect: open logs or shell for the selected container.
    if let Route::ContainerSelect { ref target, selected, action } = app.route {
        let target = target.clone();
        let pod_ns_str = target.namespace.display().to_string();
        let container_name = {
            let table = if crate::app::nav::is_globally_stored(&target.resource) {
                app.data.tables.get(&target.resource)
            } else {
                app.nav.find_table_for_resource(&target.resource)
            };
            table
                .and_then(|t| t.items.iter().find(|p| {
                    p.name == target.name && p.namespace.as_deref() == target.namespace.as_option()
                }))
                .and_then(|p| p.containers.get(selected).map(|ci| ci.name.clone()))
        };

        let container_name = match container_name {
            Some(n) => n,
            None => {
                app.ui.flash = Some(crate::app::FlashMessage::error(
                    format!("Pod {}/{} no longer has a container at index {}", pod_ns_str, target.name, selected)
                ));
                app.route = app.route_stack.pop().unwrap_or(Route::Resources);
                return ActionResult::None;
            }
        };

        if matches!(action, crate::app::ContainerAction::Shell) {
            app.route = app.route_stack.pop().unwrap_or(Route::Resources);
            return ActionResult::Shell(crate::kube::session::ExecTarget {
                pod: target.name,
                namespace: pod_ns_str,
                container: container_name,
            });
        }

        let previous = matches!(action, crate::app::ContainerAction::PreviousLogs);

        let mut log_state = crate::app::LogState::new();
        log_state.follow = !previous;
        log_state.streaming = true;
        let tail = Some(log_state.tail_lines);

        let new_stream = data_source.stream_log_substream(crate::kube::protocol::LogInit {
            pod: target.name.clone(),
            namespace: target.namespace.clone(),
            container: log_container_from_str(&container_name),
            follow: !previous,
            tail,
            since: None,
            previous,
        });
        log_state.generation = new_stream.generation;

        app.navigate_to(Route::Logs {
            target: ContainerRef::new(
                target.name.clone(),
                pod_ns_str,
                log_container_from_str(&container_name),
            ),
            state: Box::new(log_state),
            stream: Some(new_stream),
        });
        return ActionResult::None;
    }

    // Handle Enter: read the row's drill_target and act on it.
    use crate::kube::resources::row::DrillTarget;
    let row_data = app.active_view_table().and_then(|t| t.selected_item()).cloned();

    let Some(row) = row_data else {
        handle_describe(app, data_source);
        return ActionResult::None;
    };

    match row.drill_target.clone() {
        Some(DrillTarget::SwitchNamespace(ns)) => {
            do_switch_namespace(app, data_source, ns);
        }
        Some(DrillTarget::BrowseCrd(crd_ref)) => {
            let kind_label = crd_ref.kind.clone();
            let sel = app.active_view_table().map(|t| t.selected()).unwrap_or(0);
            app.nav.save_selected(sel);
            let change = app.nav.push(crate::app::nav::NavStep::new(
                ResourceId::Crd(crd_ref),
                None,
            ));
            apply_nav_change(app, data_source, change);
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Browsing CRD: {}", kind_label)));
        }
        Some(DrillTarget::PodsByLabels { labels, breadcrumb }) => {
            drill_to_pods_by_labels(app, data_source, labels, &breadcrumb);
        }
        Some(DrillTarget::PodsByOwner { uid, kind, name }) => {
            drill_to_pods_by_owner(app, data_source, &uid, kind, &name);
        }
        Some(DrillTarget::PodsByField(selector)) => {
            let breadcrumb = selector.breadcrumb();
            let sel = app.active_view_table().map(|t| t.selected()).unwrap_or(0);
            app.nav.save_selected(sel);
            let change = app.nav.push(crate::app::nav::NavStep::new(
                rid(BuiltInKind::Pod),
                Some(crate::app::nav::NavFilter::Field(selector)),
            ));
            apply_nav_change(app, data_source, change);
            app.reapply_nav_filters();
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Pods filtered by {}", breadcrumb)));
        }
        Some(DrillTarget::PodsByNameGrep(name)) => {
            drill_to_pods_by_grep(app, data_source, &name);
        }
        Some(DrillTarget::JobsByOwner { uid, kind, name }) => {
            use crate::app::nav::{NavFilter, NavStep};
            let sel = app.active_view_table().map(|t| t.selected()).unwrap_or(0);
            app.nav.save_selected(sel);
            let kind_lower = crate::kube::resource_defs::REGISTRY.by_kind(kind).gvr().kind.to_lowercase();
            let change = app.nav.push(NavStep::new(
                rid(BuiltInKind::Job),
                Some(NavFilter::OwnerChain {
                    uid,
                    kind,
                    display_name: name.clone(),
                }),
            ));
            apply_nav_change(app, data_source, change);
            app.reapply_nav_filters();
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Jobs for {}/{}", kind_lower, name)));
        }
        None => {
            handle_describe(app, data_source);
        }
    }
    ActionResult::None
}

pub(crate) fn handle_describe(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    use crate::app::Route;
    if let Some(info) = get_selected_resource_info(app) {
        if let Some(cached) = app.kube.kubectl_cache.get(&info, crate::app::ContentKind::Describe) {
            let mut state = crate::app::ContentViewState::default();
            state.set_content(cached.to_string());
            app.navigate_to(Route::ContentView {
                kind: crate::app::ContentViewKind::Describe,
                target: Some(info),
                awaiting_response: false,
                state,
            });
            return;
        }

        ds_try!(app, data_source.describe(&info));
        app.navigate_to(Route::ContentView {
            kind: crate::app::ContentViewKind::Describe,
            target: Some(info),
            awaiting_response: true,
            state: crate::app::ContentViewState::default(),
        });
    }
}

pub(crate) fn handle_yaml(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    use crate::app::Route;
    if let Some(info) = get_selected_resource_info(app) {
        if let Some(cached) = app.kube.kubectl_cache.get(&info, crate::app::ContentKind::Yaml) {
            let mut state = crate::app::ContentViewState::default();
            state.set_content(cached.to_string());
            app.navigate_to(Route::ContentView {
                kind: crate::app::ContentViewKind::Yaml,
                target: Some(info),
                awaiting_response: false,
                state,
            });
            return;
        }

        ds_try!(app, data_source.yaml(&info));
        app.navigate_to(Route::ContentView {
            kind: crate::app::ContentViewKind::Yaml,
            target: Some(info),
            awaiting_response: true,
            state: crate::app::ContentViewState::default(),
        });
    }
}

pub(crate) fn handle_logs(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    open_logs(app, data_source, false);
}

pub(crate) fn handle_previous_logs(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    open_logs(app, data_source, true);
}

/// Core log-open flow shared by live and previous-logs actions.
fn open_logs(
    app: &mut App,
    data_source: &mut ClientSession,
    previous: bool,
) {
    use crate::app::Route;

    let Some(info) = get_selected_resource_info(app) else { return; };
    let name = info.name.clone();
    let namespace_typed = info.namespace.clone();
    let namespace_display = namespace_typed.display().to_string();

    let containers = app.active_view_table()
        .and_then(|t| t.selected_item())
        .map(|row| row.containers.clone())
        .unwrap_or_default();

    if containers.len() > 1 {
        app.navigate_to(Route::ContainerSelect {
            target: info.clone(),
            selected: 0,
            action: if previous {
                crate::app::ContainerAction::PreviousLogs
            } else {
                crate::app::ContainerAction::Logs
            },
        });
        return;
    }

    let (log_target, route_pod, route_container) = if !containers.is_empty() {
        let container = containers.first().map(|ci| ci.name.clone()).unwrap_or_default();
        (name.clone(), name.clone(), log_container_from_str(&container))
    } else {
        let target = info.kubectl_target();
        (target.clone(), target, protocol::LogContainer::All)
    };

    let mut log_state = crate::app::LogState::new();
    log_state.follow = !previous;
    log_state.streaming = true;
    let tail = Some(log_state.tail_lines);

    let new_stream = data_source.stream_log_substream(protocol::LogInit {
        pod: log_target.clone(),
        namespace: namespace_typed,
        container: route_container.clone(),
        follow: !previous,
        tail,
        since: None,
        previous,
    });
    log_state.generation = new_stream.generation;

    app.navigate_to(Route::Logs {
        target: ContainerRef::new(route_pod, namespace_display, route_container),
        state: Box::new(log_state),
        stream: Some(new_stream),
    });
}

/// Build a tab-separated text dump of the currently visible resource table.
pub(crate) fn build_table_dump(app: &App) -> String {
    let current_rid = app.nav.resource_id();
    if let Some(table) = app.active_view_table() {
        let skip_ns = !app.kube.selected_ns.is_all();
        let visible = app.active_view_descriptor()
            .map(|d| d.visible_columns(current_rid, app.ui.column_level, skip_ns))
            .unwrap_or_default();
        let visible_indices: Vec<usize> = visible.iter().map(|&(i, _)| i).collect();
        let headers: String = visible.iter().map(|&(_, name)| name).collect::<Vec<_>>().join("\t");
        let mut lines = vec![headers];
        for &i in table.filtered_indices() {
            if let Some(item) = table.items.get(i) {
                let row: String = visible_indices.iter()
                    .map(|&ci| item.cells.get(ci).map(|c| c.to_string()).unwrap_or_default())
                    .collect::<Vec<_>>()
                    .join("\t");
                lines.push(row);
            }
        }
        lines.join("\n")
    } else {
        String::new()
    }
}

/// Build a Scale `FormDialog` for the given target.
pub(crate) fn build_scale_form(target: ObjectRef, current_replicas: Option<&str>) -> crate::app::FormDialog {
    use crate::app::{FormDialog, FormFieldKind, FormFieldState};

    let title = format!("Scale: {}/{}", target.resource.display_label(), target.name);
    let subtitle = format!("namespace: {}", target.namespace.display());
    let default_value = current_replicas.unwrap_or("").to_string();
    FormDialog {
        submit: crate::app::FormSubmit::Scale,
        title,
        subtitle,
        target,
        fields: vec![FormFieldState {
            name: crate::kube::protocol::form_field_name::REPLICAS.into(),
            label: "Replicas".into(),
            kind: FormFieldKind::Number { min: 0, max: 1_000_000 },
            value: default_value,
        }],
        focused: 0,
    }
}

/// Build a port-forward `FormDialog` for the currently-selected row.
pub(crate) fn resolve_port_forward_dialog(app: &App) -> Option<crate::app::FormDialog> {
    use crate::app::{FormDialog, FormFieldKind, FormFieldState};
    use crate::kube::protocol::{Namespace, OperationKind};

    if !app.current_capabilities().supports(OperationKind::PortForward) {
        return None;
    }

    let rid_key = app.nav.resource_id().clone();
    let row = app.active_view_table()?.selected_item()?;

    let ports: Vec<u16> = row.pf_ports.clone();
    let first_port = ports.first().copied().unwrap_or(8080);

    let short = rid_key.short_label().to_lowercase();
    let title = format!("Port forward: {}/{}", short, row.name);
    let ns = row.namespace.clone().unwrap_or_default();
    let subtitle = if ns.is_empty() {
        String::new()
    } else {
        format!("namespace: {}", ns)
    };
    let target_ref = ObjectRef::new(rid_key, row.name.clone(), Namespace::from_row(ns.as_str()));

    let container_field = if ports.is_empty() {
        FormFieldState {
            name: crate::kube::protocol::form_field_name::CONTAINER_PORT.into(),
            label: "Container port".into(),
            kind: FormFieldKind::Port,
            value: first_port.to_string(),
        }
    } else {
        let options: Vec<crate::app::SelectOption> = ports
            .iter()
            .map(|p| crate::app::SelectOption::new(p.to_string(), p.to_string()))
            .collect();
        FormFieldState {
            name: crate::kube::protocol::form_field_name::CONTAINER_PORT.into(),
            label: "Container port".into(),
            kind: FormFieldKind::Select { options },
            value: "0".into(),
        }
    };

    let local_field = FormFieldState {
        name: crate::kube::protocol::form_field_name::LOCAL_PORT.into(),
        label: "Local port".into(),
        kind: FormFieldKind::Port,
        value: first_port.to_string(),
    };

    Some(FormDialog {
        submit: crate::app::FormSubmit::PortForward,
        title,
        subtitle,
        target: target_ref,
        fields: vec![container_field, local_field],
        focused: 0,
    })
}

pub(crate) fn get_selected_resource_info(app: &App) -> Option<ObjectRef> {
    use crate::kube::resources::KubeResource;
    use crate::kube::protocol::Namespace;

    let current_rid = app.nav.resource_id().clone();
    let table = app.active_view_table()?;
    let item = table.selected_item()?;
    Some(ObjectRef::new(
        current_rid,
        item.name().to_string(),
        Namespace::from_row(item.namespace()),
    ))
}

/// Get resource info for all marked items in the active table.
pub(crate) fn get_marked_resource_infos(app: &App) -> Vec<ObjectRef> {
    use crate::kube::resources::KubeResource;
    use crate::kube::protocol::Namespace;

    let current_rid = app.nav.resource_id().clone();
    let mut result = Vec::new();
    if let Some(table) = app.active_view_table() {
        for item in &table.items {
            let key = crate::kube::protocol::ObjectKey::new(item.namespace(), item.name());
            if table.marked.contains(&key) {
                result.push(ObjectRef::new(
                    current_rid.clone(),
                    item.name().to_string(),
                    Namespace::from_row(item.namespace()),
                ));
            }
        }
    }
    result
}

// ===========================================================================
// Navigation helpers (merged from session_nav.rs)
// ===========================================================================

/// Drill down to pods filtered by label selector.
pub(crate) fn drill_to_pods_by_labels(
    app: &mut App,
    data_source: &mut ClientSession,
    labels: std::collections::BTreeMap<String, String>,
    description: &str,
) {
    use crate::app::nav::{NavFilter, NavStep};

    app.nav.save_selected(app.active_table_selected());
    let change = app.nav.push(NavStep::new(
        rid(BuiltInKind::Pod),
        Some(NavFilter::Labels(labels)),
    ));
    apply_nav_change(app, data_source, change);
    app.reapply_nav_filters();
    app.ui.flash = Some(crate::app::FlashMessage::info(format!("Pods for {}", description)));
}

/// Drill down to pods filtered by name prefix (fallback when no selector_labels).
pub(crate) fn drill_to_pods_by_grep(
    app: &mut App,
    data_source: &mut ClientSession,
    name: &str,
) {
    use crate::app::nav::{CompiledGrep, NavFilter, NavStep};
    let filter = format!("{}-", regex::escape(name));
    app.nav.save_selected(app.active_table_selected());
    let change = app.nav.push(NavStep::new(
        rid(BuiltInKind::Pod),
        Some(NavFilter::Grep(CompiledGrep::new(filter))),
    ));
    apply_nav_change(app, data_source, change);
    app.reapply_nav_filters();
    app.ui.flash = Some(crate::app::FlashMessage::info(format!("Pods matching: {}", name)));
}

/// Drill down to pods owned by a resource (via ownerReferences chain).
pub(crate) fn drill_to_pods_by_owner(
    app: &mut App,
    data_source: &mut ClientSession,
    uid: &str,
    kind: BuiltInKind,
    name: &str,
) {
    use crate::app::nav::{NavFilter, NavStep};

    app.nav.save_selected(app.active_table_selected());
    let change = app.nav.push(NavStep::new(
        rid(BuiltInKind::Pod),
        Some(NavFilter::OwnerChain {
            uid: uid.to_string(),
            kind,
            display_name: name.to_string(),
        }),
    ));
    apply_nav_change(app, data_source, change);
    app.reapply_nav_filters();
    let kind_lower = crate::kube::resource_defs::REGISTRY.by_kind(kind).gvr().kind.to_lowercase();
    app.ui.flash = Some(crate::app::FlashMessage::info(format!(
        "Pods for {}/{}",
        kind_lower, name
    )));
}

/// Begin a context switch.
pub(crate) fn begin_context_switch(
    app: &mut App,
    _data_source: &mut ClientSession,
    ctx_name: &crate::kube::protocol::ContextName,
) {
    if !app.kube.context_switch.is_stable() {
        app.ui.flash = Some(crate::app::FlashMessage::error(
            "Context switch already in progress".to_string(),
        ));
        return;
    }

    app.kube.core_streams.clear();
    app.nav.current_mut().stream = None;
    app.kube.context = ctx_name.clone();
    app.kube.selected_ns = crate::kube::protocol::Namespace::All;
    app.kube.identity = app.data.contexts.items.iter()
        .find(|c| c.name == *ctx_name)
        .map(|ctx| ctx.identity.clone())
        .unwrap_or_default();
    let root = app.nav.root_resource_id().clone();
    let _change = app.nav.reset(root);
    *app.nav.filter_input_mut() = Default::default();
    app.kube.kubectl_cache.clear();
    app.route_stack.clear();
    app.route = crate::app::Route::Overview;
    app.ui.confirm_dialog = None;
    app.ui.form_dialog = None;
    app.ui.input_mode = InputMode::Normal;
    app.ui.deltas.clear();
    app.kube.pod_metrics.clear();
    app.kube.node_metrics.clear();

    app.kube.context_switch = crate::app::ContextSwitchState::Requested(ctx_name.clone());
    app.ui.flash = Some(crate::app::FlashMessage::info(format!(
        "Switching to context: {}...",
        ctx_name
    )));
}

/// Perform a namespace switch: update app state, clear data, restart watchers.
pub(crate) fn do_switch_namespace(
    app: &mut App,
    data_source: &mut ClientSession,
    ns: crate::kube::protocol::Namespace,
) {
    app.kube.selected_ns = ns.clone();

    if app.current_tab_is_cluster_scoped() {
        return;
    }

    app.route_stack.clear();
    app.route = crate::app::Route::Resources;
    app.ui.confirm_dialog = None;
    app.ui.form_dialog = None;
    app.ui.input_mode = InputMode::Normal;
    app.clear_namespaced_caches();
    app.kube.kubectl_cache.clear();
    app.ui.deltas.clear();

    let root_rid = app.nav.root_resource_id().clone();
    let _change = app.nav.reset(root_rid.clone());
    *app.nav.filter_input_mut() = Default::default();

    app.clear_resource(&root_rid);
    let stream = data_source.subscribe_stream(root_rid, ns.clone(), None);
    app.nav.current_mut().stream = Some(stream);

    app.ui.flash = Some(crate::app::FlashMessage::info(format!(
        "Switched to namespace: {}",
        ns.display()
    )));
}
