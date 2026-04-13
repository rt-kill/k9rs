use tokio::sync::mpsc;

use crate::app::{App, InputMode};
use crate::app::nav::rid;
use crate::event::AppEvent;
use crate::kube::client_session::ClientSession;
use crate::kube::session::{ds_try, ActionResult, apply_nav_change};

const PAGE_SCROLL_LINES: usize = 40;
const HELP_PAGE_SCROLL_LINES: usize = 10;
const DEFAULT_TERMINAL_HEIGHT: usize = 24;
const CHROME_LINES: usize = 5;
const LOG_CHROME_LINES: usize = 4;
const FAULT_FILTER_PATTERN: &str = "error|failed|crashloop|pending|imagepull|oom|evicted|init";

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
    log_stream: &mut Option<crate::kube::client_session::LogStream>,
) -> ActionResult {
    use crate::app::actions::Action;
    use crate::app::Route;

    match action {
        Action::Quit => {
            app.exit_reason = Some(crate::app::ExitReason::UserQuit);
            app.should_quit = true;
        }
        Action::Back => {
            // Cancel any active log stream — drop closes the substream.
            *log_stream = None;
            // Pop the route stack to go back — old route (with its state) is dropped
            if let Some(route) = app.route_stack.pop() {
                app.route = route;
            }
        }
        Action::Help => {
            if app.route == Route::Help {
                // Toggle: pressing ? while in help goes back
                if let Some(route) = app.route_stack.pop() {
                    app.route = route;
                } else {
                    app.route = Route::Resources;
                }
            } else {
                app.push_route(app.route.clone());
                app.route = Route::Help;
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
                Route::ContainerSelect { ref pod, ref namespace, ref mut selected, .. } => {
                    let container_count = app.data.unified.get(&rid("pods"))
                        .and_then(|t| t.items.iter().find(|p| p.name == *pod && p.namespace.as_deref() == Some(namespace.as_str())))
                        .map(|p| p.containers.len())
                        .unwrap_or(0);
                    if container_count > 0 && *selected + 1 < container_count {
                        *selected += 1;
                    }
                }
                Route::Help => {
                    app.help_scroll += 1;
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
                Route::Logs { ref mut state, .. } | Route::Shell { ref mut state, .. } => {
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
                Route::Logs { ref mut state, .. } | Route::Shell { ref mut state, .. } => {
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
                    app.help_scroll += HELP_PAGE_SCROLL_LINES;
                }
                _ => app.page_down(),
            }
        }
        Action::Home => {
            match &mut app.route {
                Route::Logs { ref mut state, .. } | Route::Shell { ref mut state, .. } => {
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
                Route::Logs { ref mut state, .. } | Route::Shell { ref mut state, .. } => {
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
                    app.help_scroll = usize::MAX; // clamped in render
                }
                _ => app.go_end(),
            }
        }
        Action::Enter => {
            let result = handle_enter(app, data_source, log_stream);
            if !matches!(result, ActionResult::None) {
                return result;
            }
        }
        Action::Describe => handle_describe(app, data_source),
        Action::Yaml => handle_yaml(app, data_source),
        Action::Logs => handle_logs(app, data_source, log_stream),
        Action::Shell => {
            if app.current_capabilities().supports(crate::kube::protocol::OperationKind::Shell) {
                if let Some(row) = app.data.unified.get(&rid("pods")).and_then(|t| t.selected_item()) {
                    let containers = &row.containers;
                    if containers.len() > 1 {
                        // Multi-container pod: show container selector
                        let pod_name = row.name.clone();
                        let pod_ns = row.namespace.clone().unwrap_or_default();
                        app.push_route(app.route.clone());
                        app.route = Route::ContainerSelect {
                            pod: pod_name,
                            namespace: pod_ns,
                            selected: 0,
                            for_shell: true,
                        };
                    } else {
                        let pod_name = row.name.clone();
                        let pod_ns = row.namespace.clone().unwrap_or_default();
                        let container = containers.first().map(|c| c.real_name.clone()).unwrap_or_default();
                        let context = app.context.clone();
                        return ActionResult::Shell {
                            pod: pod_name,
                            namespace: pod_ns,
                            container,
                            context,
                        };
                    }
                }
            }
        }
        Action::Delete => {
            // Local resources skip the confirm dialog (cheap to recreate).
            // The wire command (`SessionCommand::Delete`) is the same for
            // both local and K8s — the server routes by `is_local()`.
            if app.nav.resource_id().is_local() {
                if let Some(info) = get_selected_resource_info(app) {
                    let name = info.name.clone();
                    ds_try!(app, data_source.delete(&info));
                    app.flash = Some(crate::app::FlashMessage::info(
                        format!("Stopped {}", name)
                    ));
                }
            } else {
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
                        pending: crate::app::PendingAction::Single { action: Action::Delete, target: info },
                        yes_selected: false,
                    });
                }
            }
        }
        Action::Edit => {
            // Unified edit flow: fetch yaml → $EDITOR → apply.
            if let Some(info) = get_selected_resource_info(app) {
                ds_try!(app, data_source.yaml(&info));
                app.push_route(app.route.clone());
                app.route = crate::app::Route::EditingResource {
                    target: info,
                    state: crate::app::EditState::AwaitingYaml,
                };
            }
        }
        Action::Scale => {
            if let Some(info) = get_selected_resource_info(app) {
                app.form_dialog = Some(build_scale_form(info));
            }
        }
        Action::Filter(_) => {
            if app.route == crate::app::Route::Resources {
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
            if let crate::app::Route::Logs { ref mut state, .. } | crate::app::Route::Shell { ref mut state, .. } = app.route {
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
            if let crate::app::Route::Logs { ref mut state, .. } | crate::app::Route::Shell { ref mut state, .. } = app.route {
                state.wrap = !state.wrap;
            }
        }
        Action::ToggleLogTimestamps => {
            if let crate::app::Route::Logs { ref mut state, .. } | crate::app::Route::Shell { ref mut state, .. } = app.route {
                state.show_timestamps = !state.show_timestamps;
                app.flash = Some(crate::app::FlashMessage::info(
                    if state.show_timestamps { "Timestamps: on" } else { "Timestamps: off" }
                ));
            }
        }
        Action::ClearLogs => {
            if let crate::app::Route::Logs { ref mut state, .. } | crate::app::Route::Shell { ref mut state, .. } = app.route {
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
                    pending: crate::app::PendingAction::Single { action: Action::Restart, target: info },
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
                    pending: crate::app::PendingAction::Single { action: Action::ForceKill, target: info },
                    yes_selected: false,
                });
            }
        }
        Action::ShowPortForwards => {
            // Navigate to the port-forward table as a drill-down so Esc returns
            // to the previous view. The server streams snapshots via the normal
            // Subscribe pipeline.
            let pf_rid = crate::kube::protocol::ResourceId::from_alias("pf")
                .expect("pf alias should resolve");
            let sel = app.active_table_selected();
            app.nav.save_selected(sel);
            app.route = Route::Resources;
            let change = app.nav.push(crate::app::nav::NavStep {
                resource: pf_rid,
                filter: None,
                saved_selected: 0,
                filter_input: crate::app::nav::FilterInputState::default(),
                stream: None,
            });
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
        Action::ToggleFullFetch => {
            // No-op: full-fetch mode is not implemented in the daemon architecture.
        }
        Action::ScrollUp(n) => {
            if let crate::app::Route::Logs { ref mut state, .. } | crate::app::Route::Shell { ref mut state, .. } = app.route {
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
            if let crate::app::Route::Logs { ref mut state, .. } | crate::app::Route::Shell { ref mut state, .. } = app.route {
                if !state.follow {
                    let total = state.visible_count();
                    let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CHROME_LINES);
                    let max = total.saturating_sub(visible);
                    state.scroll = (state.scroll + n).min(max);
                }
            }
        }
        Action::SwitchNamespace(ns) => {
            do_switch_namespace(app, data_source, ns, log_stream);
        }
        Action::SwitchContext(ctx) => {
            begin_context_switch(app, data_source, &ctx, log_stream);
        }
        Action::Refresh => {
            // Refresh = drop the current substream and open a fresh one.
            let rid = app.nav.resource_id().clone();
            let filter = app.nav.current().filter.as_ref()
                .and_then(|f| f.to_subscription_filter());
            app.nav.current_mut().stream = None;
            let stream = data_source.subscribe_stream(rid, app.selected_ns.clone(), filter);
            app.nav.current_mut().stream = Some(stream);
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
                Route::Logs { ref state, .. } | Route::Shell { ref state, .. } => {
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
                tokio::spawn(async move {
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
                    crate::app::PendingAction::Single { action: Action::Delete, ref target } => {
                        ds_try!(app, data_source.delete(target));
                    }
                    crate::app::PendingAction::Single { action: Action::Restart, ref target } => {
                        ds_try!(app, data_source.restart(target));
                    }
                    crate::app::PendingAction::Single { action: Action::ForceKill, ref target } => {
                        let name = target.name.clone();
                        // `as_option().map(...)` so `Namespace::All` ends
                        // up as `None` and we omit the `-n` flag.
                        let namespace = target.namespace.as_option().map(|s| s.to_string());
                        let context = app.context.clone();
                        let tx = event_tx.clone();
                        tokio::spawn(async move {
                            let mut cmd = tokio::process::Command::new("kubectl");
                            cmd.arg("delete").arg("pod").arg(&name).arg("--force").arg("--grace-period=0");
                            if let Some(ns) = namespace.as_deref() { cmd.arg("-n").arg(ns); }
                            if !context.is_empty() { cmd.arg("--context").arg(&context); }
                            match cmd.output().await {
                                Ok(o) if o.status.success() => {
                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(format!("Force-killed pod/{}", name)))).await;
                                }
                                Ok(o) => {
                                    let err = String::from_utf8_lossy(&o.stderr);
                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(format!("Force-kill failed: {}", err)))).await;
                                }
                                Err(e) => {
                                    let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(format!("Failed: {}", e)))).await;
                                }
                            }
                        });
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
                        let count = batch.len();
                        app.clear_marks();
                        let context = app.context.clone();
                        let tx = event_tx.clone();
                        tokio::spawn(async move {
                            let mut ok_count = 0usize;
                            let mut last_err = String::new();
                            for item in &batch {
                                let mut cmd = tokio::process::Command::new("kubectl");
                                cmd.arg("delete").arg("pod").arg(&item.name).arg("--force").arg("--grace-period=0");
                                // Pull the typed Namespace's name out via
                                // `as_option`, NOT `display()`, so that
                                // `Namespace::All` becomes "no -n flag" rather
                                // than the literal string "all".
                                if let Some(ns) = item.namespace.as_option() {
                                    cmd.arg("-n").arg(ns);
                                }
                                if !context.is_empty() { cmd.arg("--context").arg(&context); }
                                match cmd.output().await {
                                    Ok(o) if o.status.success() => { ok_count += 1; }
                                    Ok(o) => { last_err = String::from_utf8_lossy(&o.stderr).to_string(); }
                                    Err(e) => { last_err = e.to_string(); }
                                }
                            }
                            if ok_count == count {
                                let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(
                                    format!("Force-killed {} pods", count)
                                ))).await;
                            } else {
                                let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(
                                    format!("Force-killed {}/{}, last error: {}", ok_count, count, last_err)
                                ))).await;
                            }
                        });
                    }
                    _ => {
                        app.flash = Some(crate::app::FlashMessage::info("Confirmed"));
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
        Action::Sort(col) => {
            app.sort_by(col);
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
                Route::Logs { ref mut state, .. } | Route::Shell { ref mut state, .. } => {
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
                Route::Logs { ref mut state, .. } | Route::Shell { ref mut state, .. } => {
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
            handle_previous_logs(app, data_source, log_stream);
        }
        Action::ShowNode => {
            if app.current_capabilities().supports(crate::kube::protocol::OperationKind::ShowNode) {
                if let Some(row) = app.data.unified.get(&rid("pods")).and_then(|t| t.selected_item()) {
                    let node = row.node.clone().unwrap_or_default();
                    if !node.is_empty() {
                        let sel = app.data.unified.get(&rid("pods")).map(|t| t.selected).unwrap_or(0);
                        app.nav.save_selected(sel);
                        let change = app.nav.push(crate::app::nav::NavStep {
                            resource: rid("nodes"),
                            filter: Some(crate::app::nav::NavFilter::Grep(node.clone())),
                            saved_selected: 0,
                            filter_input: crate::app::nav::FilterInputState::default(),
                stream: None,
                        });
                        apply_nav_change(app, data_source, change);
                        app.reapply_nav_filters();
                    }
                }
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
                let change = app.nav.push(crate::app::nav::NavStep {
                    resource: rid("pods"),
                    filter: Some(crate::app::nav::NavFilter::Grep(name.clone())),
                    saved_selected: 0,
                    filter_input: crate::app::nav::FilterInputState::default(),
                    stream: None,
                });
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
                    let owner_rid = crate::kube::protocol::ResourceId::from_alias(&owner_kind.to_lowercase())
                        .unwrap_or_else(|| {
                            // CRD or unknown kind — build a best-effort rid
                            crate::kube::protocol::ResourceId::new(
                                String::new(), String::new(),
                                owner_kind.clone(), format!("{}s", owner_kind.to_lowercase()),
                                crate::kube::protocol::ResourceScope::Namespaced,
                            )
                        });
                    let sel = app.active_table_selected();
                    app.nav.save_selected(sel);
                    let change = app.nav.push(crate::app::nav::NavStep {
                        resource: owner_rid,
                        filter: Some(crate::app::nav::NavFilter::Grep(owner_name.clone())),
                        saved_selected: 0,
                        filter_input: crate::app::nav::FilterInputState::default(),
                        stream: None,
                    });
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
                app.push_route(app.route.clone());
                let mut state = crate::app::ContentViewState::default();
                state.set_content(format!("Decoding secret {}/{}...", info.namespace, info.name));
                ds_try!(app, data_source.decode_secret(&info));
                app.route = crate::app::Route::Describe {
                    target: info,
                    awaiting_response: false,
                    state,
                };
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
            let filename = format!(
                "/tmp/k9rs-{}-{}.txt",
                app.nav.resource_id().short_label(),
                chrono::Utc::now().format("%Y%m%d-%H%M%S")
            );
            let content = build_table_dump(app);
            let fname = filename.clone();
            let tx = event_tx.clone();
            tokio::spawn(async move {
                match tokio::fs::write(&fname, &content).await {
                    Ok(_) => {
                        let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(format!("Saved to {}", fname)))).await;
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
            for meta in crate::kube::resource_types::RESOURCE_TYPES {
                let aliases = meta.aliases.join("/");
                content.push_str(&format!("  {:<45} {}\n", aliases, meta.kind));
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
            app.push_route(app.route.clone());
            app.route = Route::Aliases { state };
        }
        Action::LogSince(ref since) => {
            let since_label = since.as_deref().unwrap_or("tail");
            app.flash = Some(crate::app::FlashMessage::info(format!("Log range: {}", since_label)));
            // Cancel existing log task
            *log_stream = None;
            // Restart with new since/tail parameter
            if let Route::Logs { ref target, ref mut state } = app.route {
                state.since = since.clone();
                let tail_lines = state.tail_lines;
                state.clear();
                state.follow = true;
                state.streaming = true;
                state.since = since.clone();
                let tail = if since.is_none() { Some(tail_lines) } else { None };
                let pod = target.pod.clone();
                let namespace = target.namespace.clone();
                let container = target.container.clone();
                *log_stream = Some(data_source.stream_log_substream(
                    &pod,
                    &namespace,
                    &container,
                    true,  // follow
                    tail,
                    since.clone(),
                    false, // not previous
                ));
            }
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
                let change = app.nav.push(crate::app::nav::NavStep {
                    resource: app.nav.resource_id().clone(),
                    filter: Some(crate::app::nav::NavFilter::Grep(
                        FAULT_FILTER_PATTERN.to_string(),
                    )),
                    saved_selected: 0,
                    filter_input: crate::app::nav::FilterInputState::default(),
                stream: None,
                });
                apply_nav_change(app, data_source, change);
                app.reapply_nav_filters();
                app.flash = Some(crate::app::FlashMessage::info("Fault filter ON"));
            } else {
                // Remove the fault filter grep specifically (not just the top step)
                app.nav.pop_grep_containing("crashloop");
                app.reapply_nav_filters();
                app.flash = Some(crate::app::FlashMessage::info("Fault filter OFF"));
            }
        }
        Action::FlashInfo(msg) => {
            app.flash = Some(crate::app::FlashMessage::info(msg));
        }
        Action::SaveLogs => {
            if let crate::app::Route::Logs { ref target, ref state, .. }
                | crate::app::Route::Shell { ref target, ref state, .. } = app.route
            {
                let filename = format!(
                    "/tmp/k9rs-logs-{}-{}-{}.log",
                    target.pod, target.container,
                    chrono::Utc::now().format("%Y%m%d-%H%M%S"),
                );
                let content: String = state.lines.iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("\n");
                match std::fs::write(&filename, &content) {
                    Ok(()) => {
                        let count = content.lines().count();
                        app.flash = Some(crate::app::FlashMessage::info(
                            format!("Saved {} lines to {}", count, filename)
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

