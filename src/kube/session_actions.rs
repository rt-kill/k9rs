use tokio::sync::mpsc;

use crate::app::{App, InputMode};
use crate::app::nav::rid;
use crate::event::AppEvent;
use crate::kube::client_session::ClientSession;
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
fn help_max_scroll() -> usize {
    let h = crossterm::terminal::size()
        .map(|(_, h)| h)
        .unwrap_or(DEFAULT_TERMINAL_HEIGHT as u16);
    crate::ui::widgets::HelpOverlay::max_scroll(h)
}

use crate::kube::session_nav::{
    do_switch_namespace, begin_context_switch,
};
use crate::kube::session_handlers::{
    handle_enter, handle_describe, handle_yaml, handle_logs,
    handle_previous_logs, build_table_dump, resolve_port_forward_dialog,
    build_scale_form, get_selected_resource_info, get_marked_resource_infos,
};

pub(crate) fn handle_action(
    app: &mut App,
    action: crate::app::actions::Action,
    event_tx: &mpsc::Sender<AppEvent>,
    data_source: &mut ClientSession,
) -> ActionResult {
    use crate::app::actions::Action;
    use crate::app::Route;

    match action {
        Action::Quit => {
            app.exit_reason = Some(crate::app::ExitReason::UserQuit);
            app.should_quit = true;
        }
        Action::Back => {
            // Pop the route stack — old route (with its state + stream) is dropped
            if let Some(route) = app.route_stack.pop() {
                app.route = route;
            }
        }
        Action::Help => {
            if matches!(app.route, Route::Help) {
                // Toggle: pressing ? while in help goes back
                if let Some(route) = app.route_stack.pop() {
                    app.route = route;
                } else {
                    app.route = Route::Resources;
                }
            } else {
                app.navigate_to(Route::Help);
            }
        }
        Action::NextTab => {
            if matches!(app.route, crate::app::Route::Overview) {
                app.route = crate::app::Route::Resources;
            }
            let new_rid = app.next_tab();
            let change = app.nav.reset(new_rid);
            *app.nav.filter_input_mut() = Default::default();
            apply_nav_change(app, data_source, change);
        }
        Action::PrevTab => {
            if matches!(app.route, crate::app::Route::Overview) {
                app.route = crate::app::Route::Resources;
            }
            let new_rid = app.prev_tab();
            let change = app.nav.reset(new_rid);
            *app.nav.filter_input_mut() = Default::default();
            apply_nav_change(app, data_source, change);
        }
        Action::NextItem => {
            match &mut app.route {
                Route::Yaml { ref mut state, .. } => {
                    let max = state.line_count().saturating_sub(1);
                    state.scroll = (state.scroll + 1).min(max);
                }
                Route::Describe { ref mut state, .. } | Route::Aliases { ref mut state, .. } => {
                    let max = state.line_count().saturating_sub(1);
                    state.scroll = (state.scroll + 1).min(max);
                }
                Route::ContainerSelect { ref target, ref mut selected, .. } => {
                    let container_count = app.data.unified.get(&target.resource)
                        .and_then(|t| t.items.iter().find(|p| {
                            p.name == target.name && p.namespace.as_deref() == target.namespace.as_option()
                        }))
                        .map(|p| p.containers.len())
                        .unwrap_or(0);
                    if container_count > 0 && *selected + 1 < container_count {
                        *selected += 1;
                    }
                }
                Route::Help => {
                    let max = help_max_scroll();
                    app.help_scroll = (app.help_scroll + 1).min(max);
                }
                _ => app.select_next(),
            }
        }
        Action::PrevItem => {
            match &mut app.route {
                Route::Yaml { ref mut state, .. } => state.scroll = state.scroll.saturating_sub(1),
                Route::Describe { ref mut state, .. } | Route::Aliases { ref mut state, .. } => state.scroll = state.scroll.saturating_sub(1),
                Route::ContainerSelect { ref mut selected, .. } => {
                    *selected = selected.saturating_sub(1);
                }
                Route::Help => {
                    app.help_scroll = app.help_scroll.saturating_sub(1);
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
                Route::Yaml { ref mut state, .. } => state.scroll = state.scroll.saturating_sub(PAGE_SCROLL_LINES),
                Route::Describe { ref mut state, .. } | Route::Aliases { ref mut state, .. } => state.scroll = state.scroll.saturating_sub(PAGE_SCROLL_LINES),
                Route::Help => {
                    app.help_scroll = app.help_scroll.saturating_sub(HELP_PAGE_SCROLL_LINES);
                }
                _ => app.page_up(),
            }
        }
        Action::PageDown => {
            match &mut app.route {
                Route::Logs { ref mut state, .. } => {
                    state.follow = false;
                    let total = state.visible_count();
                    let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CHROME_LINES);
                    let max = total.saturating_sub(visible);
                    state.scroll = (state.scroll + PAGE_SCROLL_LINES).min(max);
                }
                Route::Yaml { ref mut state, .. } => {
                    let max = state.line_count().saturating_sub(1);
                    state.scroll = (state.scroll + PAGE_SCROLL_LINES).min(max);
                }
                Route::Describe { ref mut state, .. } | Route::Aliases { ref mut state, .. } => {
                    let max = state.line_count().saturating_sub(1);
                    state.scroll = (state.scroll + PAGE_SCROLL_LINES).min(max);
                }
                Route::Help => {
                    let max = help_max_scroll();
                    app.help_scroll = (app.help_scroll + HELP_PAGE_SCROLL_LINES).min(max);
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
                Route::Yaml { ref mut state, .. } => state.scroll = 0,
                Route::Describe { ref mut state, .. } | Route::Aliases { ref mut state, .. } => state.scroll = 0,
                Route::Help => app.help_scroll = 0,
                _ => app.go_home(),
            }
        }
        Action::End => {
            match &mut app.route {
                Route::Logs { ref mut state, .. } => {
                    let total = state.visible_count();
                    let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CHROME_LINES);
                    state.scroll = total.saturating_sub(visible);
                    state.follow = true;
                }
                Route::Yaml { ref mut state, .. } => {
                    state.scroll = state.line_count().saturating_sub(1);
                }
                Route::Describe { ref mut state, .. } | Route::Aliases { ref mut state, .. } => {
                    state.scroll = state.line_count().saturating_sub(1);
                }
                Route::Help => {
                    // Use the same render-time clamp formula so PrevItem /
                    // PageUp from End immediately scrolls — using
                    // `total_lines() - 1` left help_scroll well above the
                    // rendered max, so the first ~visible_height keystrokes
                    // produced no visible movement.
                    app.help_scroll = help_max_scroll();
                }
                _ => app.go_end(),
            }
        }
        Action::Enter => {
            let result = handle_enter(app, data_source);
            if !matches!(result, ActionResult::None) {
                return result;
            }
        }
        Action::Describe => handle_describe(app, data_source),
        Action::Yaml => handle_yaml(app, data_source),
        Action::Logs => handle_logs(app, data_source),
        Action::Shell => {
            if app.current_capabilities().supports(crate::kube::protocol::OperationKind::Shell) {
                if let Some(row) = app.data.unified.get(&rid(BuiltInKind::Pod)).and_then(|t| t.selected_item()) {
                    // Refuse to shell if the row lacks a namespace string —
                    // mirrors the force-kill guard. Without this, an
                    // unwrap_or_default would pass `-n ""` to kubectl and
                    // shell into an arbitrary pod in the default namespace.
                    let Some(pod_ns) = row.namespace.clone() else {
                        app.flash = Some(crate::app::FlashMessage::error(
                            format!("Shell refused: pod/{} has no resolved namespace", row.name)
                        ));
                        return ActionResult::None;
                    };
                    if pod_ns.is_empty() {
                        app.flash = Some(crate::app::FlashMessage::error(
                            format!("Shell refused: pod/{} has empty namespace", row.name)
                        ));
                        return ActionResult::None;
                    }
                    let pod_name = row.name.clone();
                    let containers = &row.containers;
                    // Build the typed target once — used by both the
                    // multi-container picker route and the single-container
                    // direct exec.
                    let target = crate::kube::protocol::ObjectRef::new(
                        rid(BuiltInKind::Pod),
                        pod_name.clone(),
                        crate::kube::protocol::Namespace::from_row(&pod_ns),
                    );
                    if containers.len() > 1 {
                        // Multi-container pod: show container selector.
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
                app.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Delete {} {}s?", count, resource),
                    pending: crate::app::PendingAction::BatchDelete(marked),
                    yes_selected: false,
                });
            } else if let Some(info) = get_selected_resource_info(app) {
                app.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Delete {}/{}?", info.resource.display_label(), info.name),
                    pending: crate::app::PendingAction::Single { op: crate::app::SingleOp::Delete, target: info },
                    yes_selected: false,
                });
            }
        }
        Action::Edit => {
            if matches!(app.route, crate::app::Route::EditingResource { .. }) {
                app.flash = Some(crate::app::FlashMessage::warn("Edit already in progress".to_string()));
                return ActionResult::None;
            }
            if let Some(info) = get_selected_resource_info(app) {
                ds_try!(app, data_source.yaml(&info));
                app.navigate_to(crate::app::Route::EditingResource {
                    target: info,
                    state: crate::app::EditState::AwaitingYaml,
                });
            }
        }
        Action::Scale => {
            if let Some(info) = get_selected_resource_info(app) {
                app.form_dialog = Some(build_scale_form(info));
            }
        }
        Action::Filter(_) => {
            if matches!(app.route, crate::app::Route::Resources) {
                app.nav.filter_input_mut().active = true;
                app.nav.filter_input_mut().text.clear();
            } else if let crate::app::Route::Logs { ref mut state, .. } = app.route {
                state.start_filter();
            }
        }
        Action::ClearFilter => {
            // Handle log filter stack before NavStack pop
            if let crate::app::Route::Logs { ref mut state, .. } = app.route {
                if state.is_filtering() {
                    state.cancel_filter();
                } else if !state.filters.is_empty() {
                    state.pop_filter();
                }
                // Don't fall through to NavStack pop for log filter operations
            } else if let Some((popped, change)) = app.nav.pop() {
                apply_nav_change(app, data_source, change);
                if popped.resource != *app.nav.resource_id() {
                    // Resource changed (was a drill-down) — restore saved cursor position
                    let saved = app.nav.current().saved_selected;
                    app.select_in_active_table(saved);
                }
                app.reapply_nav_filters();
            } else {
                // At root with no filters — clear any stale filter state
                app.clear_filter();
            }
        }
        Action::ToggleLogFollow => {
            if let crate::app::Route::Logs { ref mut state, .. } = app.route {
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
            if let crate::app::Route::Logs { ref mut state, .. } = app.route {
                state.wrap = !state.wrap;
            }
        }
        Action::ToggleLogTimestamps => {
            if let crate::app::Route::Logs { ref mut state, .. } = app.route {
                state.show_timestamps = !state.show_timestamps;
                app.flash = Some(crate::app::FlashMessage::info(
                    if state.show_timestamps { "Timestamps: on" } else { "Timestamps: off" }
                ));
            }
        }
        Action::ClearLogs => {
            if let crate::app::Route::Logs { ref mut state, .. } = app.route {
                state.clear();
            }
            app.flash = Some(crate::app::FlashMessage::info("Logs cleared"));
        }
        Action::Restart => {
            let marked = get_marked_resource_infos(app);
            if !marked.is_empty() {
                let count = marked.len();
                let resource = marked[0].resource.display_label().to_string();
                app.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Restart {} {}s?", count, resource),
                    pending: crate::app::PendingAction::BatchRestart(marked),
                    yes_selected: false,
                });
            } else if let Some(info) = get_selected_resource_info(app) {
                app.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Restart {}/{}?", info.resource.display_label(), info.name),
                    pending: crate::app::PendingAction::Single { op: crate::app::SingleOp::Restart, target: info },
                    yes_selected: false,
                });
            }
        }
        Action::ForceKill => {
            let marked = get_marked_resource_infos(app);
            if !marked.is_empty() {
                let count = marked.len();
                app.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Force-kill {} pods?", count),
                    pending: crate::app::PendingAction::BatchForceKill(marked),
                    yes_selected: false,
                });
            } else if let Some(info) = get_selected_resource_info(app) {
                app.confirm_dialog = Some(crate::app::ConfirmDialog {
                    message: format!("Force-kill {}/{}?", info.resource.display_label(), info.name),
                    pending: crate::app::PendingAction::Single { op: crate::app::SingleOp::ForceKill, target: info },
                    yes_selected: false,
                });
            }
        }
        Action::ShowPortForwards => {
            // Navigate to the port-forward table as a drill-down so Esc returns
            // to the previous view. The server streams snapshots via the normal
            // Subscribe pipeline.
            let pf_rid = crate::kube::protocol::ResourceId::Local(
                crate::kube::local::LocalResourceKind::PortForward,
            );
            let sel = app.active_table_selected();
            app.nav.save_selected(sel);
            app.route = Route::Resources;
            let change = app.nav.push(crate::app::nav::NavStep::new(pf_rid, None));
            apply_nav_change(app, data_source, change);
        }
        Action::PortForward => {
            if let Some(dialog) = resolve_port_forward_dialog(app) {
                app.form_dialog = Some(dialog);
            } else {
                app.flash = Some(crate::app::FlashMessage::error(
                    "No target found for port-forward".to_string()
                ));
            }
        }
        Action::ToggleHeader => {
            app.show_header = !app.show_header;
        }
        Action::ScrollUp(n) => {
            if let crate::app::Route::Logs { ref mut state, .. } = app.route {
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
            if let crate::app::Route::Logs { ref mut state, .. } = app.route {
                if !state.follow {
                    let total = state.visible_count();
                    let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CHROME_LINES);
                    let max = total.saturating_sub(visible);
                    state.scroll = (state.scroll + n).min(max);
                }
            }
        }
        Action::SwitchNamespace(ns) => {
            do_switch_namespace(app, data_source, ns);
        }
        Action::SwitchContext(ctx) => {
            begin_context_switch(app, data_source, &ctx);
        }
        Action::Refresh => {
            // Refresh = drop the substream feeding the current view, clear
            // stale rows, and force a fresh watcher. Without `clear_resource`,
            // the old rows stay visible until the new snapshot lands — and if
            // the new watcher fails outright, the table keeps showing stale
            // data with no error indication.
            //
            // The substream is owned by an ancestor step when the current step
            // is a same-rid client-side filter (Grep, Fault) over a parent
            // that drilled in via Labels/Field/OwnerChain. Reading the current
            // step's filter would lose the parent's server-side filter and
            // open an unfiltered watcher alongside the still-alive parent.
            // Walk back to the actual owner.
            let rid = app.nav.resource_id().clone();
            // Snapshot the owner's server-side filter (if any) and drop
            // its stream. The stream handle closing is what forces the
            // daemon-side bridge to shut down — the new subscribe opens
            // a fresh substream on the owner step.
            let filter = app.nav.with_subscription_owner(|owner| {
                let f = owner.filter.as_ref().and_then(|f| f.to_subscription_filter());
                owner.stream = None;
                f
            });
            app.clear_resource(&rid);
            let stream = data_source.subscribe_stream_force(rid, app.selected_ns.clone(), filter);
            app.nav.with_subscription_owner(|owner| owner.stream = Some(stream));
            app.flash = Some(crate::app::FlashMessage::info("Refreshed"));
        }
        Action::Copy => {
            let (text, label) = match &app.route {
                Route::Yaml { ref state, .. } => {
                    if state.content.is_empty() {
                        (String::new(), String::new())
                    } else {
                        let lines = state.line_count();
                        (state.content.clone(), format!("Copied {} lines to clipboard", lines))
                    }
                }
                Route::Describe { ref state, .. } | Route::Aliases { ref state, .. } => {
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
        Action::Confirm => {
            if let Some(dialog) = app.confirm_dialog.take() {
                app.kubectl_cache.clear();
                match dialog.pending {
                    crate::app::PendingAction::Single { op: crate::app::SingleOp::Delete, ref target } => {
                        ds_try!(app, data_source.delete(target));
                    }
                    crate::app::PendingAction::Single { op: crate::app::SingleOp::Restart, ref target } => {
                        ds_try!(app, data_source.restart(target));
                    }
                    crate::app::PendingAction::Single { op: crate::app::SingleOp::ForceKill, ref target } => {
                        // Force-kill REQUIRES a concrete namespace. The
                        // daemon enforces this too via
                        // `reject_if_namespace_unresolved`, but we surface
                        // a friendlier message at the call site so the
                        // user sees the row name instead of "ObjectRef".
                        if target.namespace.as_option().is_none() {
                            app.flash = Some(crate::app::FlashMessage::error(
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
                        // Refuse the entire batch up-front if ANY target
                        // lacks a resolved namespace. The daemon also
                        // checks per-target, but the all-or-nothing policy
                        // is friendlier than partial completion.
                        if let Some(bad) = batch.iter().find(|t| t.namespace.as_option().is_none()) {
                            app.flash = Some(crate::app::FlashMessage::error(
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
        }
        Action::Cancel => {
            app.confirm_dialog = None;
        }
        Action::CommandMode => {
            app.input_mode = InputMode::Command { input: String::new(), history_index: None };
        }
        Action::ToggleDialogButton => {
            if let Some(ref mut dialog) = app.confirm_dialog {
                dialog.yes_selected = !dialog.yes_selected;
            }
        }
        Action::Sort(target) => {
            app.sort_by(target);
        }
        Action::ToggleSortDirection => {
            app.toggle_sort_direction();
        }
        Action::SearchStart => {
            match &mut app.route {
                Route::Yaml { ref mut state, .. } => {
                    state.search_input_active = true;
                    state.search_input.clear();
                }
                Route::Describe { ref mut state, .. } | Route::Aliases { ref mut state, .. } => {
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
            match &mut app.route {
                Route::Yaml { ref mut state, .. } => {
                    state.search = Some(term.clone());
                    state.update_search();
                }
                Route::Describe { ref mut state, .. } | Route::Aliases { ref mut state, .. } => {
                    state.search = Some(term.clone());
                    state.update_search();
                }
                _ => {}
            }
        }
        Action::SearchNext => {
            match &mut app.route {
                Route::Yaml { ref mut state, .. } => {
                    state.next_match(40);
                }
                Route::Describe { ref mut state, .. } | Route::Aliases { ref mut state, .. } => {
                    state.next_match(40);
                }
                Route::Logs { ref mut state, .. } => {
                    // Jump to next filtered line (n in vim search).
                    let current_scroll = state.scroll;
                    // Find the first filtered_index after current_scroll.
                    if let Some(&next_idx) = state.filtered_indices.iter()
                        .find(|&&idx| idx > current_scroll)
                    {
                        state.scroll = next_idx;
                        state.follow = false;
                    } else if let Some(&first) = state.filtered_indices.first() {
                        // Wrap around.
                        state.scroll = first;
                        state.follow = false;
                    }
                }
                _ => {}
            }
        }
        Action::SearchPrev => {
            match &mut app.route {
                Route::Yaml { ref mut state, .. } => {
                    state.prev_match(40);
                }
                Route::Describe { ref mut state, .. } | Route::Aliases { ref mut state, .. } => {
                    state.prev_match(40);
                }
                Route::Logs { ref mut state, .. } => {
                    // Jump to previous filtered line (N in vim search).
                    let current_scroll = state.scroll;
                    if let Some(&prev_idx) = state.filtered_indices.iter().rev()
                        .find(|&&idx| idx < current_scroll)
                    {
                        state.scroll = prev_idx;
                        state.follow = false;
                    } else if let Some(&last) = state.filtered_indices.last() {
                        // Wrap around.
                        state.scroll = last;
                        state.follow = false;
                    }
                }
                _ => {}
            }
        }
        Action::SearchClear => {
            match &mut app.route {
                Route::Yaml { ref mut state, .. } => {
                    state.clear_search();
                }
                Route::Describe { ref mut state, .. } | Route::Aliases { ref mut state, .. } => {
                    state.clear_search();
                }
                Route::Logs { .. } => {
                    // Log view uses stackable filters — cleared via Esc/pop_filter
                }
                _ => {}
            }
        }
        Action::PreviousLogs => {
            handle_previous_logs(app, data_source);
        }
        Action::ShowNode => {
            if app.current_capabilities().supports(crate::kube::protocol::OperationKind::ShowNode) {
                if let Some(row) = app.data.unified.get(&rid(BuiltInKind::Pod)).and_then(|t| t.selected_item()) {
                    let node = row.node.clone().unwrap_or_default();
                    if !node.is_empty() {
                        let sel = app.data.unified.get(&rid(BuiltInKind::Pod)).map(|t| t.selected).unwrap_or(0);
                        app.nav.save_selected(sel);
                        // Escape: node names contain `.` which is a regex
                        // metachar. Without escape the filter matches
                        // arbitrary nodes whose names happen to share the
                        // pattern shape.
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
        Action::NodeShell => {
            if let Some(row) = app.data.unified
                .get(&rid(BuiltInKind::Node))
                .and_then(|t| t.selected_item())
            {
                let node = row.name.clone();
                return ActionResult::NodeShell { node };
            }
        }
        Action::ToggleLastView => {
            if let Some(last) = app.nav.prev_root().cloned() {
                let change = app.nav.reset(last);
                *app.nav.filter_input_mut() = Default::default();
                apply_nav_change(app, data_source, change);
            }
        }
        Action::UsedBy => {
            // Reverse-reference lookup: drill into pods that reference
            // the selected resource by name. Works for Secrets, ConfigMaps,
            // PVCs, ServiceAccounts — any resource that pods mount/use.
            // The grep approach is a fast approximation; the pod's spec
            // contains the resource name in volume mounts, env refs, etc.
            if let Some(info) = get_selected_resource_info(app) {
                let name = info.name.clone();
                let kind = info.resource.display_label().to_string();
                let sel = app.active_table_selected();
                app.nav.save_selected(sel);
                // Escape regex metachars in resource names (dots, dashes,
                // brackets) — otherwise the filter silently matches the
                // wrong rows.
                let change = app.nav.push(crate::app::nav::NavStep::new(
                    rid(BuiltInKind::Pod),
                    Some(crate::app::nav::NavFilter::Grep(
                        crate::app::nav::CompiledGrep::new(regex::escape(&name)),
                    )),
                ));
                apply_nav_change(app, data_source, change);
                app.reapply_nav_filters();
                app.flash = Some(crate::app::FlashMessage::info(
                    format!("Pods referencing {}/{}", kind.to_lowercase(), name)
                ));
            }
        }
        Action::JumpToOwner => {
            // Walk up the ownerReferences chain. The selected row's
            // owner_refs carry the owner's kind/name/uid. We resolve the
            // kind to a ResourceId and drill into that resource filtered
            // by name (grep). If there's no owner, flash a message.
            let current_rid = app.nav.resource_id().clone();
            if let Some(row) = app.data.unified.get(&current_rid).and_then(|t| t.selected_item()) {
                if let Some(owner) = row.owner_refs.first() {
                    let owner_kind = owner.kind.clone();
                    let owner_name = owner.name.clone();
                    // Resolve owner kind → ResourceId. Built-ins hit the
                    // registry by alias; unknown kinds become a typed
                    // unresolved CRD ref via `CrdRef::unresolved`. The
                    // daemon's `api_resource_for` recognizes the placeholder
                    // and walks discovery to fill in group/version. The
                    // discovery loop matches against both `kind` and
                    // `kind+s`, so no client-side pluralization is needed.
                    let owner_rid = crate::kube::protocol::ResourceId::from_alias(&owner_kind.to_lowercase())
                        .unwrap_or_else(|| {
                            crate::kube::protocol::ResourceId::Crd(
                                crate::kube::protocol::CrdRef::unresolved(owner_kind.to_lowercase()),
                            )
                        });
                    let sel = app.active_table_selected();
                    app.nav.save_selected(sel);
                    // Escape: owner names from K8s contain `.` and `-`;
                    // unescaped grep matches wrong rows.
                    let change = app.nav.push(crate::app::nav::NavStep::new(
                        owner_rid,
                        Some(crate::app::nav::NavFilter::Grep(
                            crate::app::nav::CompiledGrep::new(regex::escape(&owner_name)),
                        )),
                    ));
                    apply_nav_change(app, data_source, change);
                    app.reapply_nav_filters();
                    app.flash = Some(crate::app::FlashMessage::info(
                        format!("Owner: {}/{}", owner_kind.to_lowercase(), owner_name)
                    ));
                } else {
                    app.flash = Some(crate::app::FlashMessage::warn("No owner found".to_string()));
                }
            }
        }
        Action::ToggleMark => {
            app.toggle_mark();
        }
        Action::SpanMark => {
            app.span_mark();
        }
        Action::ClearMarks => {
            app.clear_marks();
        }
        Action::DecodeSecret => {
            if let Some(info) = get_selected_resource_info(app) {
                let mut state = crate::app::ContentViewState::default();
                state.set_content(format!("Decoding secret {}/{}...", info.namespace, info.name));
                ds_try!(app, data_source.decode_secret(&info));
                app.navigate_to(crate::app::Route::Describe {
                    target: info,
                    awaiting_response: false,
                    state,
                });
            }
        }
        Action::TriggerCronJob => {
            if let Some(info) = get_selected_resource_info(app) {
                let name = info.name.clone();
                ds_try!(app, data_source.trigger_cronjob(&info));
                app.flash = Some(crate::app::FlashMessage::info(format!("Triggering CronJob: {}", name)));
            }
        }
        Action::SuspendCronJob => {
            if let Some(info) = get_selected_resource_info(app) {
                // Server reads current state and toggles — client doesn't need to know.
                ds_try!(app, data_source.toggle_suspend_cronjob(&info));
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
                // Use the blocking safe-write helper from a spawn_blocking
                // thread so the tokio runtime stays unblocked. The helper
                // does the O_CREAT|O_EXCL dance internally.
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
        Action::ShowAliases => {
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
            app.navigate_to(Route::Aliases { state });
        }
        Action::LogSince(ref since) => {
            let since_label = since.as_deref().unwrap_or("tail");
            app.flash = Some(crate::app::FlashMessage::info(format!("Log range: {}", since_label)));
            // Restart with new since/tail parameter. The old stream is
            // dropped when we replace it — RAII cleanup.
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
                // Replace the stream in the route — old stream drops (RAII).
                *stream = Some(new_stream);
            }
        }
        Action::ColLeft => {
            app.col_left();
        }
        Action::ColRight => {
            app.col_right();
        }
        Action::ToggleWide => {
            app.column_level = app.column_level.next();
            app.flash = Some(crate::app::FlashMessage::info(
                format!("Columns: {}", app.column_level.label())
            ));
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
                app.flash = Some(crate::app::FlashMessage::info("Fault filter ON"));
            } else {
                app.nav.pop_fault_filter();
                app.reapply_nav_filters();
                app.flash = Some(crate::app::FlashMessage::info("Fault filter OFF"));
            }
        }
        Action::FlashInfo(msg) => {
            app.flash = Some(crate::app::FlashMessage::info(msg));
        }
        Action::SaveLogs => {
            if let crate::app::Route::Logs { ref target, ref state, .. }
 = app.route
            {
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
                        app.flash = Some(crate::app::FlashMessage::info(
                            format!("Saved {} lines to {}", count, path.display())
                        ));
                    }
                    Err(e) => {
                        app.flash = Some(crate::app::FlashMessage::error(
                            format!("Save failed: {}", e)
                        ));
                    }
                }
            }
        }
    }
    ActionResult::None
}

