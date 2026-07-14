use tokio::sync::mpsc;

use crate::app::{App, ContainerRef, InputMode};
use crate::app::nav::rid;
use crate::event::AppEvent;
use crate::kube::client_session::ClientSession;
use crate::kube::protocol::{self, ObjectRef};
use crate::kube::resource_def::BuiltInKind;
use crate::app::element::Element;
use crate::app::store::RowPredicate;
use crate::kube::session::{ds_try, ActionResult};

const HELP_PAGE_SCROLL_LINES: usize = 10;
const DEFAULT_TERMINAL_HEIGHT: usize = 24;
const LOG_CHROME_LINES: usize = 4;
const CONTENT_CHROME_LINES: usize = 3;

use crate::util::content_max_scroll;


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

    match action {
        // --- Lifecycle ---
        Action::Quit => {
            app.exit_reason = Some(crate::app::ExitReason::UserQuit);
            app.should_quit = true;
        }
        Action::Back => {
            // ONE undo path: close the overlay if any, else pop the top
            // element (reviving its data if the subscription died while
            // covered). At the root: no-op.
            if app.ui.overlay.is_some() {
                app.ui.overlay = None;
            } else if app.nav.pop().is_some() {
                app.nav.ensure_top_live(data_source);
            }
        }
        Action::Help => {
            if matches!(app.ui.overlay, Some(crate::app::Overlay::Help { .. })) {
                app.ui.overlay = None;
            } else {
                app.ui.overlay = Some(crate::app::Overlay::Help { scroll: 0 });
            }
        }

        // --- Tab switching ---
        Action::NextTab => {
            let new_rid = app.next_tab();
            let root = App::root_list_element(
                data_source, &app.kube.metrics, new_rid, app.kube.selected_ns.clone(),
            );
            app.nav.reset(root);
        }
        Action::PrevTab => {
            let new_rid = app.prev_tab();
            let root = App::root_list_element(
                data_source, &app.kube.metrics, new_rid, app.kube.selected_ns.clone(),
            );
            app.nav.reset(root);
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
            if let Some(crate::app::nav::RootSpec::Resource(last_rid)) = app.nav.prev_root().cloned() {
                let root = App::root_list_element(
                    data_source, &app.kube.metrics, last_rid, app.kube.selected_ns.clone(),
                );
                app.nav.reset(root);
            }
        }
        Action::ShowPortForwards => handle_show_port_forwards(app, data_source),
        Action::PortForward => {
            if !app.current_capabilities().supports(crate::kube::protocol::OperationKind::PortForward) {
                return ActionResult::None;
            }
            let schema = crate::kube::protocol::OperationKind::PortForward.form_schema()
                .expect("PortForward always has a form schema");
            let row = app.nav.top().selected_row();
            let headers = app.nav.top().headers_snapshot();
            if let Some(info) = get_selected_resource_info(app) {
                if let Some(dialog) = build_form_from_schema(schema, crate::kube::protocol::OperationKind::PortForward, info, row.as_ref(), &headers) {
                    app.ui.form_dialog = Some(dialog);
                } else {
                    app.ui.flash = Some(crate::app::FlashMessage::error(
                        "No target found for port-forward".to_string()
                    ));
                }
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
    use crate::app::element::Element;
    use crate::app::Overlay;

    // Overlays capture scroll keys first (help sheet, container picker).
    match (&mut app.ui.overlay, &action) {
        (Some(Overlay::Help { scroll }), a) => {
            let caps = app.nav.top().capabilities();
            let max = help_max_scroll(&caps);
            match a {
                Action::NextItem => *scroll = (*scroll + 1).min(max),
                Action::PrevItem => *scroll = scroll.saturating_sub(1),
                Action::PageUp => *scroll = scroll.saturating_sub(HELP_PAGE_SCROLL_LINES),
                Action::PageDown => *scroll = (*scroll + HELP_PAGE_SCROLL_LINES).min(max),
                Action::Home => *scroll = 0,
                Action::End => *scroll = max,
                _ => {}
            }
            return;
        }
        (Some(Overlay::ContainerSelect { containers, selected, .. }), a) => {
            match a {
                Action::NextItem => {
                    if !containers.is_empty() && *selected + 1 < containers.len() {
                        *selected += 1;
                    }
                }
                Action::PrevItem => *selected = selected.saturating_sub(1),
                _ => {}
            }
            return;
        }
        _ => {}
    }

    let page_lines = app.config.ui.page_scroll_lines;
    match app.nav.top_mut() {
        // Log views: scroll over the derived visible-line set.
        el @ (Element::LogSession(_) | Element::LogFilter(_)) => {
            let total = el.log_visible().map(|v| v.len()).unwrap_or(0);
            let visible = crossterm::terminal::size()
                .map(|(_, h)| h as usize)
                .unwrap_or(DEFAULT_TERMINAL_HEIGHT)
                .saturating_sub(LOG_CHROME_LINES);
            let max = total.saturating_sub(visible);
            let Some(view) = el.log_view_mut() else { return };
            match action {
                Action::PageUp => {
                    view.follow = false;
                    view.scroll = view.scroll.saturating_sub(page_lines);
                }
                Action::PageDown => {
                    view.follow = false;
                    view.scroll = (view.scroll + page_lines).min(max);
                }
                Action::Home => {
                    view.follow = false;
                    view.scroll = 0;
                }
                Action::End => {
                    view.scroll = max;
                    view.follow = true;
                }
                Action::ScrollUp(n) => {
                    if view.follow {
                        view.scroll = max;
                        view.follow = false;
                    }
                    view.scroll = view.scroll.saturating_sub(n);
                }
                Action::ScrollDown(n) => {
                    if !view.follow {
                        view.scroll = (view.scroll + n).min(max);
                    }
                }
                _ => {}
            }
        }
        // Content views: scroll over the cached line count.
        Element::ContentView(cv) => {
            let visible = crossterm::terminal::size()
                .map(|(_, h)| h as usize)
                .unwrap_or(DEFAULT_TERMINAL_HEIGHT)
                .saturating_sub(CONTENT_CHROME_LINES);
            let max = content_max_scroll(cv.state.line_count(), visible);
            match action {
                Action::NextItem => cv.state.scroll = (cv.state.scroll + 1).min(max),
                Action::PrevItem => cv.state.scroll = cv.state.scroll.saturating_sub(1),
                Action::PageUp => cv.state.scroll = cv.state.scroll.saturating_sub(page_lines),
                Action::PageDown => cv.state.scroll = (cv.state.scroll + page_lines).min(max),
                Action::Home => cv.state.scroll = 0,
                Action::End => cv.state.scroll = max,
                _ => {}
            }
        }
        // Table views (incl. the context picker via its element arms).
        _ => match action {
            Action::NextItem => app.select_next(),
            Action::PrevItem => app.select_prev(),
            Action::PageUp => app.page_up(),
            Action::PageDown => app.page_down(),
            Action::Home => app.go_home(),
            Action::End => app.go_end(),
            Action::ColLeft => app.col_left(),
            Action::ColRight => app.col_right(),
            _ => {}
        },
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

    // Derived views (container table, etc.) only support Shell — all other
    // mutating operations (delete, edit, scale, restart) don't apply to
    // derived rows. Shell is handled specially below (uses nav.source).
    if app.nav.top().derived_kind().is_some() && !matches!(action, Action::Shell) {
        return ActionResult::None;
    }

    match action {
        Action::Shell => {
            if app.current_capabilities().supports(crate::kube::protocol::OperationKind::Shell) {
                // Derived view (container table): shell directly into
                // the selected container; the element's origin IS the
                // parent pod.
                if app.nav.top().derived_kind().is_some() {
                    let origin = app.nav.top().origin().cloned();
                    let item = app.nav.top().selected_row();
                    if let (Some(origin), Some(item)) = (origin, item) {
                        // Only a Named namespace can be shelled into; `All` is
                        // structurally excluded (and a namespace literally named
                        // "all" is `Named("all")`, so it works correctly — the
                        // old `ns != "all"` magic-string guard wrongly rejected it).
                        if let crate::kube::protocol::Namespace::Named(ns) = &origin.namespace {
                            return ActionResult::Exec {
                                op: crate::kube::protocol::OperationKind::Shell,
                                target: crate::kube::session::ExecTarget::Pod {
                                    pod: origin.name.clone(),
                                    namespace: ns.clone(),
                                    container: item.name.clone(),
                                },
                            };
                        }
                    }
                    return ActionResult::None;
                }
                let Some(current_rid) = app.nav.resource_id().cloned() else { return ActionResult::None; };
                if let Some(row) = app.nav.top().selected_row() {
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
                        app.ui.confirm_dialog = None;
                        app.ui.form_dialog = None;
                        app.ui.overlay = Some(crate::app::Overlay::ContainerSelect {
                            target,
                            containers: containers.clone(),
                            selected: 0,
                            action: crate::app::ContainerAction::Shell,
                        });
                    } else {
                        let container = containers.first().map(|c| c.name.clone()).unwrap_or_default();
                        return ActionResult::Exec { op: crate::kube::protocol::OperationKind::Shell, target: crate::kube::session::ExecTarget::Pod {
                            pod: pod_name,
                            namespace: pod_ns,
                            container,
                        } };
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
            if matches!(app.ui.overlay, Some(crate::app::Overlay::Edit { .. })) {
                app.ui.flash = Some(crate::app::FlashMessage::warn("Edit already in progress".to_string()));
                return ActionResult::None;
            }
            if let Some(info) = get_selected_resource_info(app) {
                ds_try!(app, data_source.yaml(&info));
                app.ui.confirm_dialog = None;
                app.ui.form_dialog = None;
                app.ui.overlay = Some(crate::app::Overlay::Edit {
                    target: info,
                    state: crate::app::EditState::AwaitingYaml,
                });
            }
        }
        Action::Scale => {
            if let Some(info) = get_selected_resource_info(app) {
                let schema = crate::kube::protocol::OperationKind::Scale.form_schema()
                    .expect("Scale always has a form schema");
                let row = app.nav.top().selected_row();
                let headers = app.nav.top().headers_snapshot();
                if let Some(dialog) = build_form_from_schema(schema, crate::kube::protocol::OperationKind::Scale, info, row.as_ref(), &headers) {
                    app.ui.form_dialog = Some(dialog);
                }
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
                let el = crate::app::element::Element::ContentView(
                    crate::app::element::ContentView::new(
                        crate::app::element::ContentSpec::Describe(info),
                        state,
                        false,
                    ),
                );
                app.nav.push(el);
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

    match action {
        Action::Filter(_) => {
            if let Some(view) = app.nav.top_mut().log_view_mut() {
                view.draft = Some(String::new());
            } else if let Some(fi) = app.nav.top_mut().filter_input_mut() {
                fi.start();
            }
        }
        Action::ColumnFilter => {
            {
                // Data index — the predicate indexes the full `cells` array,
                // not the visible subset. Read from the element's own last
                // view (the same visible set the renderer used), so the
                // mapping cannot drift. The header rides along for the crumb.
                if let Some((col, header)) = app.nav.top().selected_data_col() {
                    if let Some(fi) = app.nav.top_mut().filter_input_mut() {
                        fi.start_column(col, header);
                    }
                }
            }
        }
        Action::ClearFilter => {
            // Esc, unified: cancel a draft → close the overlay → pop the
            // top element. First press pops; marks are never touched.
            if let Some(view) = app.nav.top_mut().log_view_mut() {
                if view.is_filtering() {
                    view.draft = None;
                    return;
                }
            }
            if app.ui.overlay.is_some() {
                app.ui.overlay = None;
            } else if app.nav.pop().is_some() {
                // Pop = drop: the popped element's stream RSTs, its
                // backward Arcs release. The revealed element still owns
                // its cursor, sort, and draft — nothing to restore. Only
                // its subscription may have died while covered; revive it.
                app.nav.ensure_top_live(data_source);
            }
            // At the root there is nothing to pop — Esc is a no-op.
        }
        Action::ToggleFaultFilter => {
            // Strict LIFO: Ctrl-Z pops the fault filter when it is the
            // TOP; a fault filter buried under later refinements can't be
            // spliced out from the middle (children's predicate chains
            // are value-copies — a splice would not update them anyway).
            if app.nav.top_is_fault() {
                app.nav.pop();
                app.nav.ensure_top_live(data_source);
                app.ui.flash = Some(crate::app::FlashMessage::info("Fault filter OFF"));
            } else if app.nav.any_fault() {
                app.ui.flash = Some(crate::app::FlashMessage::warn(
                    "Fault filter active below — Esc back to it",
                ));
            } else if let Ok(el) = Element::derive_filter(app.nav.top(), RowPredicate::Fault) {
                app.nav.push(el);
                app.ui.flash = Some(crate::app::FlashMessage::info("Fault filter ON"));
            }
        }
        Action::SearchStart => {
            match app.nav.top_mut() {
                crate::app::element::Element::ContentView(cv) => {
                    cv.state.search_input_active = true;
                    cv.state.search_input.clear();
                }
                el => {
                    if let Some(view) = el.log_view_mut() {
                        view.draft = Some(String::new());
                    }
                }
            }
        }
        Action::SearchExec(term) => {
            if let crate::app::element::Element::ContentView(cv) = app.nav.top_mut() {
                cv.state.search = Some(term.clone());
                cv.state.update_search();
            }
        }
        Action::SearchNext => {
            let el = app.nav.top_mut();
            if let crate::app::element::Element::ContentView(cv) = el {
                let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CONTENT_CHROME_LINES);
                cv.state.next_match(visible);
            } else if let Some(indices) = el.log_visible() {
                if let Some(view) = el.log_view_mut() {
                    let current_scroll = view.scroll;
                    if let Some(&next_idx) = indices.iter().find(|&&idx| idx > current_scroll) {
                        view.scroll = next_idx;
                        view.follow = false;
                    } else if let Some(&first) = indices.first() {
                        view.scroll = first;
                        view.follow = false;
                    }
                }
            }
        }
        Action::SearchPrev => {
            let el = app.nav.top_mut();
            if let crate::app::element::Element::ContentView(cv) = el {
                let visible = crossterm::terminal::size().map(|(_, h)| h as usize).unwrap_or(DEFAULT_TERMINAL_HEIGHT).saturating_sub(CONTENT_CHROME_LINES);
                cv.state.prev_match(visible);
            } else if let Some(indices) = el.log_visible() {
                if let Some(view) = el.log_view_mut() {
                    let current_scroll = view.scroll;
                    if let Some(&prev_idx) = indices.iter().rev().find(|&&idx| idx < current_scroll) {
                        view.scroll = prev_idx;
                        view.follow = false;
                    } else if let Some(&last) = indices.last() {
                        view.scroll = last;
                        view.follow = false;
                    }
                }
            }
        }
        Action::SearchClear => {
            if let crate::app::element::Element::ContentView(cv) = app.nav.top_mut() {
                cv.state.clear_search();
            }
            // Log views: committed filters are elements — cleared via Esc.
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
    use crate::app::element::Element;

    match action {
        Action::ToggleLogFollow => {
            let total = app.nav.top_mut().log_visible().map(|v| v.len()).unwrap_or(0);
            if let Some(view) = app.nav.top_mut().log_view_mut() {
                view.follow = !view.follow;
                if view.follow {
                    view.scroll = total.saturating_sub(1);
                } else {
                    let (_, rows) = crossterm::terminal::size().unwrap_or((80, DEFAULT_TERMINAL_HEIGHT as u16));
                    let visible = (rows as usize).saturating_sub(LOG_CHROME_LINES);
                    view.scroll = total.saturating_sub(visible);
                }
            }
        }
        Action::ToggleLogWrap => {
            if let Some(view) = app.nav.top_mut().log_view_mut() {
                view.wrap = !view.wrap;
            }
        }
        Action::ToggleLogTimestamps => {
            if let Some(view) = app.nav.top_mut().log_view_mut() {
                view.show_timestamps = !view.show_timestamps;
                let on = view.show_timestamps;
                app.ui.flash = Some(crate::app::FlashMessage::info(
                    if on { "Timestamps: on" } else { "Timestamps: off" }
                ));
            }
        }
        Action::ClearLogs => {
            // Clears the DATA (the session's line store) — every filter
            // element over it empties with it, honestly.
            if let Some(store) = app.nav.top().log_store() {
                store.clear();
                app.ui.flash = Some(crate::app::FlashMessage::info("Logs cleared"));
            }
        }
        Action::LogSince(ref since) => {
            // Range keys re-run the OWNING session's spec. Under a filter,
            // the owner is found by store pointer identity — the same
            // named-walk discipline as subscription revival.
            let Some(store) = app.nav.top().log_store().cloned() else { return };
            let since_label = since.as_deref().unwrap_or("tail");
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Log range: {}", since_label)));
            app.nav.with_log_session_of(&store, |session_el| {
                session_el.restart_range(data_source, since.clone());
            });
        }
        Action::SaveLogs => {
            let Element::LogSession(ref s) = app.nav.top() else {
                // Saving from a filter view saves what you see — the
                // filtered lines of the top element.
                save_visible_logs(app);
                return;
            };
            let target = &s.target;
            let safe = |x: &str| x.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect::<String>();
            let filename = format!(
                "logs-{}-{}-{}.log",
                safe(&target.pod), safe(target.container_label()),
                chrono::Utc::now().format("%Y%m%d-%H%M%S"),
            );
            let content: String = s.store().with_read(|i| {
                i.lines.iter()
                    .map(|l| crate::util::strip_ansi(&l.flat_text()))
                    .collect::<Vec<_>>()
                    .join("\n")
            });
            write_log_file(app, &filename, &content);
        }
        _ => {}
    }
}

/// Save the top log element's VISIBLE (filtered) lines.
fn save_visible_logs(app: &mut App) {
    let Some(indices) = app.nav.top_mut().log_visible() else { return };
    let Some(store) = app.nav.top().log_store() else { return };
    let content: String = store.with_read(|i| {
        indices.iter()
            .filter_map(|&idx| i.lines.get(idx))
            .map(|l| crate::util::strip_ansi(&l.flat_text()))
            .collect::<Vec<_>>()
            .join("\n")
    });
    let filename = format!("logs-filtered-{}.log", chrono::Utc::now().format("%Y%m%d-%H%M%S"));
    write_log_file(app, &filename, &content);
}

fn write_log_file(app: &mut App, filename: &str, content: &str) {
    match crate::util::safe_write_temp(filename, content.as_bytes()) {
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
                if let Some(row) = app.nav.top().selected_row() {
                    let node = row.node.clone().unwrap_or_default();
                    if !node.is_empty() {
                        // "The node with the ip of this pod": the row's node
                        // IS the whole definition — nodes are cluster-scoped,
                        // so nothing namespace-ish enters construction.
                        let selector = crate::app::nav::K8sFieldSelector::MetadataName(node.clone());
                        let el = app.list_element_from_top(
                            data_source,
                            rid(BuiltInKind::Node),
                            protocol::Namespace::All,
                            Some(protocol::SubscriptionFilter::Field(selector.to_wire())),
                            format!("Nodes/{}", node),
                        );
                        app.nav.push(el);
                    }
                }
            }
        }
        Action::UsedBy => {
            if let Some(info) = get_selected_resource_info(app) {
                let name = info.name.clone();
                let kind = info.resource.display_label().to_string();
                // A pods list (scoped by the selector at construction) plus
                // a grep refinement over it — two honest elements.
                let el = app.list_element_from_top(
                    data_source,
                    rid(BuiltInKind::Pod),
                    app.kube.selected_ns.clone(),
                    None,
                    "pods".to_string(),
                );
                app.nav.push(el);
                let predicate = RowPredicate::Grep(crate::app::nav::CompiledGrep::new(
                    regex::escape(&name),
                ));
                if let Ok(el) = Element::derive_filter(app.nav.top(), predicate) {
                    app.nav.push(el);
                }
                app.ui.flash = Some(crate::app::FlashMessage::info(
                    format!("Pods referencing {}/{}", kind.to_lowercase(), name)
                ));
            }
        }
        Action::JumpToOwner => {
            if let Some(row) = app.nav.top().selected_row() {
                if let Some(owner) = row.owner_refs.first() {
                    let owner_kind = owner.kind.clone();
                    let owner_name = owner.name.clone();
                    let owner_rid = crate::kube::protocol::ResourceId::from_alias(&owner_kind.to_lowercase())
                        .unwrap_or_else(|| {
                            crate::kube::protocol::ResourceId::CrdUnresolved(owner_kind.to_lowercase())
                        });
                    let selector = crate::app::nav::K8sFieldSelector::MetadataName(owner_name.clone());
                    let el = app.list_element_from_top(
                        data_source,
                        owner_rid.clone(),
                        app.kube.selected_ns.clone(),
                        Some(protocol::SubscriptionFilter::Field(selector.to_wire())),
                        format!("{}/{}", owner_rid.short_label(), owner_name),
                    );
                    app.nav.push(el);
                    app.ui.flash = Some(crate::app::FlashMessage::info(
                        format!("Owner: {}/{}", owner_kind.to_lowercase(), owner_name)
                    ));
                } else {
                    app.ui.flash = Some(crate::app::FlashMessage::warn("No owner found".to_string()));
                }
            }
        }
        Action::NodeShell => {
            if let Some(row) = app.nav.top().selected_row() {
                let node = row.name.clone();
                return ActionResult::Exec {
                    op: crate::kube::protocol::OperationKind::NodeShell,
                    target: crate::kube::session::ExecTarget::Node { node },
                };
            }
        }
        Action::OverlayCapability(ref cap_name) => {
            let Some(plural) = app.nav.top().rid().map(|r| r.plural().to_owned()) else {
                return ActionResult::None;
            };
            let overlay = crate::kube::overlay::overlay_for(&plural);
            let cap = overlay.and_then(|o| o.capabilities.get(cap_name));
            match cap {
                Some(crate::kube::overlay::OverlayCapability::Drill { target, column }) => {
                    let target_rid = crate::kube::protocol::ResourceId::from_alias(target)
                        .unwrap_or_else(|| crate::kube::protocol::ResourceId::CrdUnresolved(target.clone()));
                    let headers = app.nav.top().headers_snapshot();
                    let filter_value = app.nav.top().selected_row().and_then(|row| {
                        let col_idx = headers.iter().position(|h| h.eq_ignore_ascii_case(column))?;
                        row.cells.get(col_idx).map(|c| c.to_string())
                    });
                    if let Some(value) = filter_value.filter(|v| !v.is_empty()) {
                        let selector = crate::app::nav::K8sFieldSelector::MetadataName(value.clone());
                        let el = app.list_element_from_top(
                            data_source,
                            target_rid,
                            app.kube.selected_ns.clone(),
                            Some(protocol::SubscriptionFilter::Field(selector.to_wire())),
                            format!("{}/{}", target, value),
                        );
                        app.nav.push(el);
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

    match action {
        Action::Copy => {
            use crate::app::element::Element;
            let (text, label) = match app.nav.top_mut() {
                Element::ContentView(cv) => {
                    if cv.state.content.is_empty() {
                        (String::new(), String::new())
                    } else {
                        let lines = cv.state.line_count();
                        (cv.state.content.clone(), format!("Copied {} lines to clipboard", lines))
                    }
                }
                el @ (Element::LogSession(_) | Element::LogFilter(_)) => {
                    let indices = el.log_visible().unwrap_or_default();
                    let store = el.log_store().expect("log kinds have a store");
                    if indices.is_empty() {
                        (String::new(), String::new())
                    } else {
                        let joined: String = store.with_read(|i| {
                            indices.iter()
                                .filter_map(|&idx| i.lines.get(idx))
                                .map(|l| crate::util::strip_ansi(&l.flat_text()))
                                .collect::<Vec<_>>()
                                .join("\n")
                        });
                        let count = indices.len();
                        (joined, format!("Copied {} lines to clipboard", count))
                    }
                }
                Element::ContextList(c) => {
                    if c.table.items().is_empty() {
                        (String::new(), String::new())
                    } else {
                        let mut lines = vec!["CURRENT\tNAME\tCLUSTER".to_string()];
                        for ctx in c.table.items() {
                            let marker = if ctx.is_current { "*" } else { "" };
                            let cluster = if ctx.identity.cluster.is_empty() {
                                ctx.name.as_str()
                            } else {
                                &ctx.identity.cluster
                            };
                            lines.push(format!("{}\t{}\t{}", marker, ctx.name, cluster));
                        }
                        let count = c.table.items().len();
                        (lines.join("\n"), format!("Copied {} contexts to clipboard", count))
                    }
                }
                _ => {
                    let dump = build_table_dump(app);
                    if dump.is_empty() {
                        (String::new(), String::new())
                    } else {
                        let count = dump.lines().count().saturating_sub(1); // exclude header
                        (dump, format!("Copied {} rows to clipboard", count))
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
                safe(app.nav.top().title()),
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
    let pf_rid = crate::kube::protocol::ResourceId::Local(
        crate::kube::local::LocalResourceKind::PortForward,
    );
    let label = pf_rid.short_label().to_lowercase();
    let el = app.list_element_from_top(
        data_source, pf_rid, app.kube.selected_ns.clone(), None, label,
    );
    app.nav.push(el);
}

fn handle_refresh(app: &mut App, data_source: &mut ClientSession) {
    use crate::app::element::{ContentSpec, Element};

    // Inside a describe/yaml element: re-fetch ITS OWN target directly.
    if let Element::ContentView(cv) = app.nav.top_mut() {
        let result = match &cv.kind {
            ContentSpec::Describe(target) => Some(data_source.describe(target)),
            ContentSpec::Yaml(target) => Some(data_source.yaml(target)),
            ContentSpec::Aliases => None,
        };
        if let Some(result) = result {
            app.kube.kubectl_cache.clear();
            match result {
                Ok(()) => {
                    if let Element::ContentView(cv) = app.nav.top_mut() {
                        cv.state = crate::app::ContentViewState::default();
                        cv.awaiting_response = true;
                    }
                    app.ui.flash = Some(crate::app::FlashMessage::info("Refreshing..."));
                }
                Err(_) => {
                    app.ui.flash = Some(crate::app::FlashMessage::error("Failed to refresh"));
                }
            }
        }
        return;
    }

    // Derived views are client-side projections — no subscription to refresh.
    if app.nav.top().derived_kind().is_some() {
        app.ui.flash = Some(crate::app::FlashMessage::info("Pop back to refresh the parent view"));
        return;
    }
    app.kube.kubectl_cache.clear();
    // Re-run the owning element's OWN query spec with a forced watcher.
    // The old refresh re-subscribed with the AMBIENT namespace, silently
    // narrowing all-namespace drills — unrepresentable now.
    app.nav.refresh_top_query(data_source);
    app.ui.flash = Some(crate::app::FlashMessage::info("Refreshed"));
}

fn handle_show_aliases(app: &mut App) {
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
    app.nav.push(crate::app::element::Element::ContentView(
        crate::app::element::ContentView::new(crate::app::element::ContentSpec::Aliases, state, false),
    ));
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
    use crate::kube::protocol::ResourceId;

    // Derived view Enter: open logs for the selected container.
    if app.nav.top().derived_kind().is_some() {
        open_logs_from_derived(app, data_source, false);
        return ActionResult::None;
    }

    // Handle context-picker Enter: switch to the selected context.
    if let crate::app::element::Element::ContextList(c) = app.nav.top() {
        if let Some(ctx) = c.table.selected_item() {
            let ctx_name = ctx.name.clone();
            begin_context_switch(app, data_source, &ctx_name);
        }
        return ActionResult::None;
    }

    // Handle ContainerSelect overlay: open logs or shell for the pick.
    if let Some(crate::app::Overlay::ContainerSelect { ref target, ref containers, selected, action }) = app.ui.overlay {
        let target = target.clone();
        let pod_ns_str = target.namespace.display().to_string();
        // The dialog captured its container list at construction.
        let container_name = match containers.get(selected).map(|ci| ci.name.clone()) {
            Some(n) => n,
            None => {
                app.ui.flash = Some(crate::app::FlashMessage::error(
                    format!("Pod {}/{} no longer has a container at index {}", pod_ns_str, target.name, selected)
                ));
                app.ui.overlay = None;
                return ActionResult::None;
            }
        };
        app.ui.overlay = None;

        if matches!(action, crate::app::ContainerAction::Shell) {
            return ActionResult::Exec { op: crate::kube::protocol::OperationKind::Shell, target: crate::kube::session::ExecTarget::Pod {
                pod: target.name,
                namespace: pod_ns_str,
                container: container_name,
            } };
        }

        let previous = matches!(action, crate::app::ContainerAction::PreviousLogs);
        push_log_session(
            app,
            data_source,
            ContainerRef::new(
                target.name.clone(),
                pod_ns_str,
                log_container_from_str(&container_name),
            ),
            crate::kube::protocol::LogInit {
                pod: target.name.clone(),
                namespace: target.namespace.clone(),
                container: log_container_from_str(&container_name),
                follow: !previous,
                tail: Some(app.config.ui.logs.tail_lines),
                since: None,
                previous,
            },
        );
        return ActionResult::None;
    }

    // Handle Enter: read the row's drill_target and act on it.
    use crate::kube::resources::row::DrillTarget;
    let row_data = app.nav.top().selected_row();

    let Some(row) = row_data else {
        handle_describe(app, data_source);
        return ActionResult::None;
    };

    match row.drill_target.clone() {
        Some(DrillTarget::PodsInNamespace(ns)) => {
            drill_to_pods_in_namespace(app, data_source, ns);
        }
        Some(DrillTarget::BrowseCrd(crd_ref)) => {
            let kind_label = crd_ref.kind.clone();
            let crd_rid = ResourceId::Crd(crd_ref);
            let label = crd_rid.short_label().to_lowercase();
            let el = app.list_element_from_top(
                data_source, crd_rid, app.kube.selected_ns.clone(), None, label,
            );
            app.nav.push(el);
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Browsing CRD: {}", kind_label)));
        }
        Some(DrillTarget::PodsByLabels { labels, breadcrumb }) => {
            // Scope to the *source* object's namespace, not the parent view's —
            // from an all-namespaces parent these labels would otherwise match
            // pods cluster-wide.
            let source_ns = crate::kube::protocol::Namespace::from_row(row.namespace.as_deref().unwrap_or(""));
            drill_to_pods_by_labels(app, data_source, labels, source_ns, &breadcrumb);
        }
        Some(DrillTarget::PodsByOwner { uid, kind, name }) => {
            let source_ns = crate::kube::protocol::Namespace::from_row(row.namespace.as_deref().unwrap_or(""));
            drill_to_pods_by_owner(app, data_source, &uid, kind, &name, source_ns);
        }
        Some(DrillTarget::PodsByField(selector)) => {
            let breadcrumb = selector.breadcrumb();
            // The element's OWN scope is intrinsic to the drill: a node
            // hosts pods from EVERY namespace, so a spec.nodeName drill is
            // cluster-wide; name/phase drills keep the session scope as a
            // construction input. Because the element owns its columns,
            // the cluster-wide view shows NAMESPACE regardless of the
            // ambient selector.
            let ns = match &selector {
                crate::app::nav::K8sFieldSelector::SpecNodeName(_) => protocol::Namespace::All,
                _ => app.kube.selected_ns.clone(),
            };
            let el = app.list_element_from_top(
                data_source,
                rid(BuiltInKind::Pod),
                ns,
                Some(protocol::SubscriptionFilter::Field(selector.to_wire())),
                breadcrumb.clone(),
            );
            app.nav.push(el);
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Pods filtered by {}", breadcrumb)));
        }
        Some(DrillTarget::PodsByNameGrep(name)) => {
            drill_to_pods_by_grep(app, data_source, &name);
        }
        Some(DrillTarget::JobsByOwner { uid, kind, name }) => {
            let kind_str = crate::kube::resource_defs::REGISTRY.by_kind(kind).gvr().kind;
            let kind_lower = kind_str.to_lowercase();
            // Scope to the SOURCE object's namespace — its jobs live there.
            let source_ns = protocol::Namespace::from_row(row.namespace.as_deref().unwrap_or(""));
            let el = app.list_element_from_top(
                data_source,
                rid(BuiltInKind::Job),
                source_ns,
                Some(protocol::SubscriptionFilter::OwnerUid(uid)),
                format!("{}/{}", kind_str, name),
            );
            app.nav.push(el);
            app.ui.flash = Some(crate::app::FlashMessage::info(format!("Jobs for {}/{}", kind_lower, name)));
        }
        Some(DrillTarget::Derived(kind)) => {
            // "Containers on this pod": the row's identity + the top's
            // live source ARE the definition. The projection is LIVE — it
            // re-derives as the parent row changes and empties honestly
            // if the pod disappears.
            if let Ok(el) = Element::derive_projection(app.nav.top(), &row, kind) {
                app.nav.push(el);
            }
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
    use crate::app::element::{ContentSpec, ContentView, Element};
    if let Some(info) = get_selected_resource_info(app) {
        if let Some(lines) = app.kube.kubectl_cache.get_describe_lines(&info) {
            let mut state = crate::app::ContentViewState::default();
            state.set_describe_lines(lines);
            app.nav.push(Element::ContentView(ContentView::new(
                ContentSpec::Describe(info),
                state,
                false,
            )));
            return;
        }

        ds_try!(app, data_source.describe(&info));
        app.nav.push(Element::ContentView(ContentView::new(
            ContentSpec::Describe(info),
            crate::app::ContentViewState::default(),
            true,
        )));
    }
}

pub(crate) fn handle_yaml(
    app: &mut App,
    data_source: &mut ClientSession,
) {
    use crate::app::element::{ContentSpec, ContentView, Element};
    if let Some(info) = get_selected_resource_info(app) {
        if let Some(cached) = app.kube.kubectl_cache.get(&info, crate::app::ContentKind::Yaml) {
            let mut state = crate::app::ContentViewState::default();
            state.set_content(cached.to_string());
            app.nav.push(Element::ContentView(ContentView::new(
                ContentSpec::Yaml(info),
                state,
                false,
            )));
            return;
        }

        ds_try!(app, data_source.yaml(&info));
        app.nav.push(Element::ContentView(ContentView::new(
            ContentSpec::Yaml(info),
            crate::app::ContentViewState::default(),
            true,
        )));
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

/// Push a `LogSession` element for `spec` — "logs of this container" is
/// the element's whole self-definition.
fn push_log_session(
    app: &mut App,
    data_source: &mut ClientSession,
    target: ContainerRef,
    spec: protocol::LogInit,
) {
    let el = crate::app::element::Element::LogSession(Box::new(
        crate::app::element::LogSession::open(data_source, target, spec, &app.config.ui.logs),
    ));
    app.nav.push(el);
}

/// Core log-open flow shared by live and previous-logs actions.
fn open_logs(
    app: &mut App,
    data_source: &mut ClientSession,
    previous: bool,
) {
    // Derived view (e.g., container table): the selected row IS the
    // container, and the element's origin IS the parent pod.
    if app.nav.top().derived_kind().is_some() {
        open_logs_from_derived(app, data_source, previous);
        return;
    }

    let Some(info) = get_selected_resource_info(app) else { return; };
    let name = info.name.clone();
    let namespace_typed = info.namespace.clone();
    let namespace_display = namespace_typed.display().to_string();

    let containers = app.nav.top().selected_row()
        .map(|row| row.containers.clone())
        .unwrap_or_default();

    if containers.len() > 1 {
        app.ui.confirm_dialog = None;
        app.ui.form_dialog = None;
        app.ui.overlay = Some(crate::app::Overlay::ContainerSelect {
            target: info.clone(),
            containers: containers.clone(),
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

    push_log_session(
        app,
        data_source,
        ContainerRef::new(route_pod, namespace_display, route_container.clone()),
        protocol::LogInit {
            pod: log_target,
            namespace: namespace_typed,
            container: route_container,
            follow: !previous,
            tail: Some(app.config.ui.logs.tail_lines),
            since: None,
            previous,
        },
    );
}

/// Open logs from a derived view (e.g., container table). The selected
/// row's name is the container; the element's origin is the parent pod.
fn open_logs_from_derived(
    app: &mut App,
    data_source: &mut ClientSession,
    previous: bool,
) {
    let Some(origin) = app.nav.top().origin().cloned() else { return; };
    let Some(item) = app.nav.top().selected_row() else { return; };

    let container_name = item.name.clone();
    let pod_name = origin.name.clone();
    let namespace = origin.namespace.clone();
    let ns_display = namespace.display().to_string();
    let container = protocol::LogContainer::Named(container_name.clone());

    push_log_session(
        app,
        data_source,
        ContainerRef::new(pod_name.clone(), ns_display, container.clone()),
        protocol::LogInit {
            pod: pod_name,
            namespace,
            container,
            follow: !previous,
            tail: Some(app.config.ui.logs.tail_lines),
            since: None,
            previous,
        },
    );
}

/// Build a tab-separated text dump of the currently visible resource table.
/// Reads the element's own last-materialized view: same columns (the
/// element's OWN NAMESPACE decision — not the ambient selector), same
/// filter and sort, same effective (metrics-overlaid) cell strings the
/// user is looking at. What you copy is exactly what you see.
pub(crate) fn build_table_dump(app: &App) -> String {
    let Some(view) = app.nav.top().last_view() else { return String::new() };
    if view.rows.is_empty() && view.total_rows == 0 {
        return String::new();
    }
    let mut lines = vec![view.headers.join("\t")];
    for row in &view.rows {
        lines.push(row.join("\t"));
    }
    lines.join("\n")
}

/// Build a `FormDialog` from a declarative `FormSchema`.
///
/// Each `FormFieldSchema` in the schema maps to a `FormFieldState`:
/// - `Number { default_column }` reads the default from the row's cell
///   (splitting on '/' to get the denominator, e.g. "2/3" -> "3").
/// - `Port` uses the first port from `row.pf_ports`, or 8080.
/// - `DynamicSelect` builds a `Select` from `row.pf_ports` if non-empty,
///   otherwise falls back to a `Port` input.
///
/// Title is derived from `title_template` with `{{kind}}` and `{{name}}`
/// substitutions. Subtitle comes from the target namespace.
pub(crate) fn build_form_from_schema(
    schema: &crate::kube::protocol::FormSchema,
    op: crate::kube::protocol::OperationKind,
    target: ObjectRef,
    row: Option<&crate::kube::resources::row::ResourceRow>,
    headers: &[String],
) -> Option<crate::app::FormDialog> {
    use crate::app::{FormDialog, FormFieldKind, FormFieldState, FormSubmit};
    use crate::kube::protocol::{FormFieldSchemaKind, DynamicSelectFallback};

    let submit = FormSubmit::from_operation(op)?;

    let title = schema.title_template
        .replace("{{kind}}", &target.resource.short_label().to_lowercase())
        .replace("{{name}}", &target.name);

    let subtitle = match &target.namespace {
        crate::kube::protocol::Namespace::Named(ns) if !ns.is_empty() => {
            format!("namespace: {}", ns)
        }
        _ => String::new(),
    };

    let fields: Vec<FormFieldState> = schema.fields.iter().map(|field_schema| {
        match &field_schema.kind {
            FormFieldSchemaKind::Number { min, max, default_column } => {
                let default_value = default_column
                    .and_then(|col_name| {
                        let col_idx = headers.iter().position(|h| h.eq_ignore_ascii_case(col_name))?;
                        let r = row?;
                        let cell = r.cells.get(col_idx)?;
                        let s = cell.to_string();
                        let denom = s.split('/').nth(1)?.trim().to_string();
                        if denom.is_empty() { None } else { Some(denom) }
                    })
                    .unwrap_or_default();
                FormFieldState {
                    name: field_schema.name.into(),
                    label: field_schema.label.into(),
                    kind: FormFieldKind::Number { min: *min, max: *max },
                    value: default_value,
                }
            }
            FormFieldSchemaKind::Port => {
                let default_port = row
                    .and_then(|r| r.pf_ports.first().copied())
                    .unwrap_or(8080);
                FormFieldState {
                    name: field_schema.name.into(),
                    label: field_schema.label.into(),
                    kind: FormFieldKind::Port,
                    value: default_port.to_string(),
                }
            }
            FormFieldSchemaKind::DynamicSelect { fallback } => {
                let ports = row.map(|r| &r.pf_ports[..]).unwrap_or(&[]);
                if ports.is_empty() {
                    let default_port = match fallback {
                        DynamicSelectFallback::Port => 8080,
                    };
                    FormFieldState {
                        name: field_schema.name.into(),
                        label: field_schema.label.into(),
                        kind: FormFieldKind::Port,
                        value: default_port.to_string(),
                    }
                } else {
                    let options: Vec<crate::app::SelectOption> = ports
                        .iter()
                        .map(|p| crate::app::SelectOption::new(p.to_string(), p.to_string()))
                        .collect();
                    FormFieldState {
                        name: field_schema.name.into(),
                        label: field_schema.label.into(),
                        kind: FormFieldKind::Select { options, selected: 0 },
                        value: String::new(),
                    }
                }
            }
        }
    }).collect();

    Some(FormDialog {
        submit,
        title,
        subtitle,
        target,
        fields,
        focused: 0,
    })
}

pub(crate) fn get_selected_resource_info(app: &App) -> Option<ObjectRef> {
    use crate::kube::protocol::Namespace;

    let current_rid = app.nav.top().rid()?.clone();
    let row = app.nav.top().selected_row()?;
    Some(ObjectRef::new(
        current_rid,
        row.name.clone(),
        Namespace::from_row(row.namespace.as_deref().unwrap_or("")),
    ))
}

/// Resource refs for all marked rows on the top element's data. Marks are
/// IDENTITY-keyed on the shared store, so this is the marked set as data
/// — independent of the current refinement (matching the fused-table
/// behavior; the marked∩visible question belongs to the batch-ops
/// feature, not here).
pub(crate) fn get_marked_resource_infos(app: &App) -> Vec<ObjectRef> {
    use crate::kube::protocol::Namespace;

    let Some(current_rid) = app.nav.top().rid().cloned() else { return Vec::new(); };
    app.nav
        .top()
        .marked_keys()
        .into_iter()
        .map(|key| {
            ObjectRef::new(
                current_rid.clone(),
                key.name,
                Namespace::from_row(&key.namespace),
            )
        })
        .collect()
}

// ===========================================================================
// Navigation helpers (merged from session_nav.rs)
// ===========================================================================

/// Drill down to pods filtered by label selector. `namespace` is the
/// SOURCE object's namespace — intrinsic to the drill, carried on the
/// element's own query spec.
pub(crate) fn drill_to_pods_by_labels(
    app: &mut App,
    data_source: &mut ClientSession,
    labels: std::collections::BTreeMap<String, String>,
    namespace: crate::kube::protocol::Namespace,
    description: &str,
) {
    let el = app.list_element_from_top(
        data_source,
        rid(BuiltInKind::Pod),
        namespace,
        Some(protocol::SubscriptionFilter::Labels(labels)),
        description.to_string(),
    );
    app.nav.push(el);
    app.ui.flash = Some(crate::app::FlashMessage::info(format!("Pods for {}", description)));
}

/// Drill down to pods filtered by name prefix (fallback when no
/// selector_labels): a pods list element plus a grep refinement over it.
pub(crate) fn drill_to_pods_by_grep(
    app: &mut App,
    data_source: &mut ClientSession,
    name: &str,
) {
    use crate::app::nav::CompiledGrep;
    let el = app.list_element_from_top(
        data_source,
        rid(BuiltInKind::Pod),
        app.kube.selected_ns.clone(),
        None,
        "pods".to_string(),
    );
    app.nav.push(el);
    let predicate = RowPredicate::Grep(CompiledGrep::new(format!("{}-", regex::escape(name))));
    if let Ok(el) = Element::derive_filter(app.nav.top(), predicate) {
        app.nav.push(el);
    }
    app.ui.flash = Some(crate::app::FlashMessage::info(format!("Pods matching: {}", name)));
}

/// Drill down to pods owned by a resource (via ownerReferences chain).
/// `namespace` is the source object's namespace — intrinsic scope.
pub(crate) fn drill_to_pods_by_owner(
    app: &mut App,
    data_source: &mut ClientSession,
    uid: &str,
    kind: BuiltInKind,
    name: &str,
    namespace: crate::kube::protocol::Namespace,
) {
    let kind_str = crate::kube::resource_defs::REGISTRY.by_kind(kind).gvr().kind;
    let el = app.list_element_from_top(
        data_source,
        rid(BuiltInKind::Pod),
        namespace,
        Some(protocol::SubscriptionFilter::OwnerUid(uid.to_string())),
        format!("{}/{}", kind_str, name),
    );
    app.nav.push(el);
    let kind_lower = kind_str.to_lowercase();
    app.ui.flash = Some(crate::app::FlashMessage::info(format!(
        "Pods for {}/{}",
        kind_lower, name
    )));
}

/// Drill from a namespace row into the pods running in that namespace.
/// Entering a namespace also makes it the session's active scope (matching
/// k9s), so sibling tabs inherit it. The pushed Pod step carries no filter,
/// so `apply_nav_change` falls back to `selected_ns` for the fresh
/// subscription — scoping the pods to the namespace we just selected. Esc
/// pops back to the (cluster-scoped) namespace list, whose cached rows
/// survive the namespaced-cache clear below.
pub(crate) fn drill_to_pods_in_namespace(
    app: &mut App,
    data_source: &mut ClientSession,
    ns: crate::kube::protocol::Namespace,
) {
    // Entering a namespace also makes it the session's active SELECTOR
    // (k9s parity) — an explicit selector write, not something the
    // element reads back. The pushed element carries the namespace as
    // its OWN scope.
    app.kube.selected_ns = ns.clone();
    app.kube.kubectl_cache.clear();

    let el = app.list_element_from_top(
        data_source,
        rid(BuiltInKind::Pod),
        ns.clone(),
        None,
        "pods".to_string(),
    );
    app.nav.push(el);
    app.ui.flash = Some(crate::app::FlashMessage::info(format!(
        "Pods in namespace: {}",
        ns.display()
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

    // All core data belongs to the old cluster; elements drop with the
    // stack reset (their streams RST; the new root's stream binds to the
    // NEW session after the rebuild via the revive path).
    app.core.clear();
    app.kube.context = ctx_name.clone();
    app.kube.selected_ns = crate::kube::protocol::Namespace::All;
    app.kube.identity = app.data.contexts.items().iter()
        .find(|c| c.name == *ctx_name)
        .map(|ctx| ctx.identity.clone())
        .unwrap_or_default();
    // Land on the Overview root for the new context (matching the old
    // Route::Overview landing) — no resource watch opens until the user
    // navigates to one.
    app.nav.reset(crate::app::element::Element::Overview(crate::app::element::Overview));
    app.kube.kubectl_cache.clear();
    app.ui.confirm_dialog = None;
    app.ui.form_dialog = None;
    app.ui.overlay = None;
    app.ui.input_mode = InputMode::Normal;
    app.kube.metrics.clear();

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

    app.ui.confirm_dialog = None;
    app.ui.form_dialog = None;
    app.ui.overlay = None;
    app.ui.input_mode = InputMode::Normal;
    app.kube.kubectl_cache.clear();

    // Same root recipe, re-scoped: a fresh element with a fresh store —
    // the old namespace's rows drop with the old stack (no cache to
    // scrub, no tracker to reset).
    let root_rid = match app.nav.root_spec() {
        Some(crate::app::nav::RootSpec::Resource(r)) => r,
        None => rid(BuiltInKind::Pod),
    };
    let root = App::root_list_element(data_source, &app.kube.metrics, root_rid, ns.clone());
    app.nav.reset(root);

    app.ui.flash = Some(crate::app::FlashMessage::info(format!(
        "Switched to namespace: {}",
        ns.display()
    )));
}
