use std::time::Duration;

use anyhow::Result;
use crossterm::{
    cursor::SetCursorStyle,
    event::{Event as CtEvent, KeyCode},
    execute,
    terminal::{disable_raw_mode, LeaveAlternateScreen},
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::app::{App, InputMode};
use crate::kube::protocol::ResourceId;
use crate::event::AppEvent;
use crate::kube::client_session::ClientSession;

use crate::kube::session_actions::handle_action;
use crate::kube::session_events::apply_event;
use crate::kube::session_nav::{apply_context_switch_result, do_switch_namespace};
use crate::kube::session_commands::{
    build_shell_args, build_edit_args, run_interactive_local,
    parse_resource_command, parse_resource_filter_command,
    parse_crd_ns_command, parse_resource_ns_command,
};

/// Surface a data_source operation error as a flash message.
macro_rules! ds_try {
    ($app:expr, $expr:expr) => {
        if let Err(e) = $expr {
            $app.flash = Some(crate::app::FlashMessage::error(format!("Connection error: {}", e)));
        }
    };
}

// Make ds_try usable from sibling modules (session_actions, session_nav, etc.)
pub(crate) use ds_try;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum events to drain per iteration to prevent UI freezes during bursts.
const EVENT_DRAIN_CAP: usize = 200;
/// Maximum number of entries in the command history ring.
const COMMAND_HISTORY_LIMIT: usize = 50;
/// Scroll offset when jumping to a search match (lines of context above match).
const SEARCH_SCROLL_CONTEXT: usize = 10;

/// Result returned from `handle_action` to signal the main loop about actions
/// that require terminal access (suspend/resume TUI for interactive commands).
pub(crate) enum ActionResult {
    /// No special handling needed.
    None,
    /// Suspend the TUI and run `kubectl exec -it` into a pod shell.
    Shell {
        pod: String,
        namespace: String,
        container: String,
        context: String,
    },
    /// Suspend the TUI and run `kubectl edit` on a resource.
    Edit {
        resource: String,
        name: String,
        namespace: String,
        context: String,
    },
}

pub(crate) fn apply_nav_change(app: &mut App, data_source: &mut ClientSession, change: crate::app::nav::NavChange) {
    if let Some(ref old) = change.unsubscribe {
        data_source.unsubscribe_resource(old);
        // Free memory for the old table's row data, but keep the table entry
        // so re-subscribing shows "Loading..." correctly (has_data stays as-is
        // until the entry is removed or overwritten by clear_resource).
        if let Some(table) = app.data.unified.get_mut(old) {
            table.items.clear();
            table.filtered_indices.clear();
        }
    }
    if let Some(ref new) = change.subscribe {
        // Clear the target table so the view shows "Loading..." until fresh data arrives.
        app.clear_resource(new);
        data_source.subscribe_resource(new);
    }
}

pub async fn session_main(
    mut app: App,
    mut data_source: ClientSession,
    mut terminal: ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    event_tx: mpsc::Sender<AppEvent>,
    mut event_rx: mpsc::Receiver<AppEvent>,
    mut input_rx: mpsc::Receiver<CtEvent>,
    tick_rate: Duration,
    input_suspend: tokio::sync::watch::Sender<bool>,
    mut input_suspend_ack: mpsc::Receiver<()>,
) -> Result<Option<crate::app::ExitReason>> {
    // Track active log streaming task so we can cancel on Back
    let mut log_task: Option<JoinHandle<()>> = None;
    // Track active port-forward task so we can cancel it
    let mut port_forward_task: Option<JoinHandle<()>> = None;

    let mut tick_interval = tokio::time::interval(tick_rate);
    let mut last_tick = std::time::Instant::now();

    // Main event loop — only redraw when state changes
    let mut needs_redraw = true; // draw the first frame immediately
    loop {
        // Tick check BEFORE select — guarantees animation runs even during event floods.
        if last_tick.elapsed() >= tick_rate {
            if app.tick() {
                needs_redraw = true;
            }
            last_tick = std::time::Instant::now();
        }

        if needs_redraw {
            terminal.draw(|f| {
                crate::ui::draw(f, &mut app);
            })?;

            // Set cursor style based on input mode: bar for text input, block otherwise
            let route_has_text_input = match &app.route {
                crate::app::Route::Yaml { ref state, .. } => state.search_input_active,
                crate::app::Route::Describe { ref state, .. } | crate::app::Route::Aliases { ref state, .. } => state.search_input_active,
                crate::app::Route::Logs { ref state, .. } | crate::app::Route::Shell { ref state, .. } => state.is_filtering(),
                _ => false,
            };
            let in_input_mode = matches!(app.input_mode, InputMode::Command { .. }) || matches!(app.input_mode, InputMode::Scale { .. }) || app.port_forward_dialog.as_ref().map_or(false, |d| d.selected_field.is_text_input())
                || app.nav.filter_input().active
                || route_has_text_input;
            if in_input_mode {
                execute!(terminal.backend_mut(), SetCursorStyle::SteadyBar)?;
            } else {
                execute!(terminal.backend_mut(), SetCursorStyle::SteadyBlock)?;
            }

            needs_redraw = false;
        }

        tokio::select! {
            // biased: always check key events first so user input (like disabling
            // autoscroll) takes effect before processing queued data events.
            biased;

            Some(ct_event) = input_rx.recv() => {
                match ct_event {
                    CtEvent::Key(key) => {
                        // Capture input in command mode before normal dispatch
                        if matches!(app.input_mode, InputMode::Command { .. }) {
                            match key.code {
                                KeyCode::Esc => {
                                    app.input_mode = InputMode::Normal;
                                }
                                KeyCode::Enter => {
                                    let raw_cmd = if let InputMode::Command { ref input, .. } = app.input_mode { input.trim().to_string() } else { String::new() };
                                    let cmd = raw_cmd.to_lowercase();
                                    // Push to command history before clearing
                                    if !cmd.is_empty() {
                                        app.command_history.push(raw_cmd.clone());
                                        if app.command_history.len() > COMMAND_HISTORY_LIMIT {
                                            app.command_history.remove(0);
                                        }
                                    }
                                    app.input_mode = InputMode::Normal;
                                    // Handle command
                                    if cmd.is_empty() {
                                        // Empty command — just close the prompt
                                    } else if cmd == "q" || cmd == "quit" || cmd == "exit" || cmd == "q!" {
                                        app.exit_reason = Some(crate::app::ExitReason::UserQuit);
                                        app.should_quit = true;
                                    } else if cmd == "help" || cmd == "h" || cmd == "?" {
                                        app.push_route(app.route.clone());
                                        app.route = crate::app::Route::Help;
                                    } else if cmd == "alias" || cmd == "aliases" || cmd == "a" {
                                        // Show aliases view
                                        let action = crate::app::actions::Action::ShowAliases;
                                        handle_action(
                                            &mut app, action, &event_tx,
                                            &mut data_source,
                                            &mut log_task,
                                            &mut port_forward_task,
                                        );
                                    } else if cmd == "ctx" || cmd == "context" || cmd == "contexts" {
                                        app.push_route(app.route.clone());
                                        app.route = crate::app::Route::Contexts;
                                    } else if cmd.starts_with("ctx ") || cmd.starts_with("context ") {
                                        let ctx_name = if cmd.starts_with("ctx ") { &raw_cmd[4..] } else { &raw_cmd[8..] }.trim().to_string();
                                        crate::kube::session_nav::begin_context_switch(&mut app, &mut data_source, &ctx_name, &mut log_task, &mut port_forward_task);
                                    } else if cmd.starts_with("ns ") || cmd.starts_with("namespace ") {
                                        let ns = if cmd.starts_with("ns ") { &raw_cmd[3..] } else { &raw_cmd[10..] }.trim().to_string();
                                        do_switch_namespace(&mut app, &mut data_source, crate::kube::protocol::Namespace::from(ns.as_str()), &mut log_task);
                                    } else if let Some(resource_rid) = parse_resource_command(&cmd) {
                                        app.route = crate::app::Route::Resources;
                                        let change = app.nav.reset(resource_rid);
                                        *app.nav.filter_input_mut() = Default::default();
                                        apply_nav_change(&mut app, &mut data_source, change);
                                    } else if let Some(parsed) = parse_resource_ns_command(&cmd) {
                                        app.route = crate::app::Route::Resources;
                                        let change = app.nav.reset(parsed.rid.clone());
                                        *app.nav.filter_input_mut() = Default::default();
                                        if crate::kube::protocol::Namespace::from(parsed.argument.as_str()) != app.selected_ns {
                                            do_switch_namespace(&mut app, &mut data_source, crate::kube::protocol::Namespace::from(parsed.argument.as_str()), &mut log_task);
                                        }
                                        apply_nav_change(&mut app, &mut data_source, change);
                                        app.flash = Some(crate::app::FlashMessage::info(format!(
                                            "{}({})", parsed.rid.short_label(), parsed.argument
                                        )));
                                    } else if let Some(parsed) = parse_resource_filter_command(&cmd) {
                                        app.route = crate::app::Route::Resources;
                                        let change = app.nav.reset(parsed.rid.clone());
                                        apply_nav_change(&mut app, &mut data_source, change);
                                        let change = app.nav.push(crate::app::nav::NavStep {
                                            resource: parsed.rid,
                                            filter: Some(crate::app::nav::NavFilter::Grep(parsed.argument.clone())),
                                            saved_selected: 0,
                                            filter_input: crate::app::nav::FilterInputState::default(),
                                        });
                                        apply_nav_change(&mut app, &mut data_source, change);
                                        *app.nav.filter_input_mut() = Default::default();
                                        app.reapply_nav_filters();
                                    } else if let Some(parsed) = parse_crd_ns_command(&cmd, &app) {
                                        // :clickhouseinstallation prod — CRD + namespace
                                        let crd = parsed.crd;
                                        app.route = crate::app::Route::Resources;
                                        if crate::kube::protocol::Namespace::from(parsed.namespace.as_str()) != app.selected_ns {
                                            do_switch_namespace(&mut app, &mut data_source, crate::kube::protocol::Namespace::from(parsed.namespace.as_str()), &mut log_task);
                                        }
                                        let crd_rid = ResourceId::new(
                                            crd.group.clone(), crd.version.clone(),
                                            crd.kind.clone(), crd.plural.clone(),
                                            crd.scope,
                                        );
                                        let change = app.nav.reset(crd_rid);
                                        apply_nav_change(&mut app, &mut data_source, change);
                                        app.flash = Some(crate::app::FlashMessage::info(
                                            format!("Browsing CRD: {}({})", crd.kind, parsed.namespace)
                                        ));
                                    } else if cmd == "overview" || cmd == "home" {
                                        app.route = crate::app::Route::Overview;
                                    } else {
                                        // Not a built-in resource — try as a CRD.
                                        // If known, use the discovered metadata. If unknown,
                                        // trust the user and let the server discover it.
                                        let crd_info = app.find_crd_by_name(&cmd);
                                        let (group, version, kind, plural, scope) = if let Some(crd) = &crd_info {
                                            (crd.group.clone(), crd.version.clone(), crd.kind.clone(), crd.plural.clone(),
                                             crd.scope)
                                        } else {
                                            // Unknown CRD — use the command as the plural name.
                                            // The server will discover the real group/version via API discovery.
                                            (String::new(), String::new(), cmd.clone(), cmd.clone(),
                                             crate::kube::protocol::ResourceScope::Namespaced)
                                        };
                                        app.route = crate::app::Route::Resources;
                                        let crd_rid = ResourceId::new(
                                            group.clone(), version.clone(),
                                            kind.clone(), plural.clone(), scope,
                                        );
                                        let change = app.nav.reset(crd_rid);
                                        apply_nav_change(&mut app, &mut data_source, change);
                                        app.flash = Some(crate::app::FlashMessage::info(
                                            format!("Browsing: {}", kind)
                                        ));
                                    }
                                }
                                KeyCode::Tab => {
                                    app.accept_completion();
                                }
                                KeyCode::Up => {
                                    if !app.command_history.is_empty() {
                                        if let InputMode::Command { ref mut input, ref mut history_index } = app.input_mode {
                                            let idx = match *history_index {
                                                None => app.command_history.len() - 1,
                                                Some(i) => i.saturating_sub(1),
                                            };
                                            *history_index = Some(idx);
                                            *input = app.command_history[idx].clone();
                                        }
                                    }
                                }
                                KeyCode::Down => {
                                    if let InputMode::Command { ref mut input, ref mut history_index } = app.input_mode {
                                        if let Some(idx) = *history_index {
                                            if idx + 1 < app.command_history.len() {
                                                *history_index = Some(idx + 1);
                                                *input = app.command_history[idx + 1].clone();
                                            } else {
                                                *history_index = None;
                                                input.clear();
                                            }
                                        }
                                    }
                                }
                                KeyCode::Backspace => {
                                    if let InputMode::Command { ref mut input, .. } = app.input_mode {
                                        input.pop();
                                    }
                                }
                                KeyCode::Char(c) => {
                                    if let InputMode::Command { ref mut input, .. } = app.input_mode {
                                        input.push(c);
                                    }
                                }
                                _ => {}
                            }
                            needs_redraw = true;
                            continue; // skip normal key handling
                        }

                        // Scale mode: capture numeric input for replica count
                        if matches!(app.input_mode, InputMode::Scale { .. }) {
                            match key.code {
                                KeyCode::Esc => {
                                    app.input_mode = InputMode::Normal;
                                }
                                KeyCode::Enter => {
                                    let extracted = if let InputMode::Scale { ref input, ref target } = app.input_mode {
                                        Some((input.trim().to_string(), target.clone()))
                                    } else { None };
                                    app.input_mode = InputMode::Normal;
                                    if let Some((replica_str, target)) = extracted {
                                        if let Ok(replicas) = replica_str.parse::<u32>() {
                                            app.kubectl_cache.clear();
                                            ds_try!(app, data_source.scale(
                                                target.resource.display_label(),
                                                &target.name,
                                                target.namespace.display(),
                                                replicas,
                                            ));
                                        } else {
                                            app.flash = Some(crate::app::FlashMessage::warn(
                                                format!("Invalid replica count: {}", replica_str)
                                            ));
                                        }
                                    }
                                }
                                KeyCode::Backspace => {
                                    if let InputMode::Scale { ref mut input, .. } = app.input_mode {
                                        input.pop();
                                    }
                                }
                                KeyCode::Char(c) if c.is_ascii_digit() => {
                                    if let InputMode::Scale { ref mut input, .. } = app.input_mode {
                                        input.push(c);
                                    }
                                }
                                _ => {} // Ignore non-digit input in scale mode
                            }
                            needs_redraw = true;
                            continue;
                        }

                        // Port-forward dialog: capture input
                        if let Some(ref mut pf_dialog) = app.port_forward_dialog {
                            match key.code {
                                KeyCode::Esc => {
                                    app.port_forward_dialog = None;
                                }
                                KeyCode::Tab | KeyCode::Down => {
                                    pf_dialog.selected_field = pf_dialog.selected_field.next();
                                }
                                KeyCode::BackTab | KeyCode::Up => {
                                    pf_dialog.selected_field = pf_dialog.selected_field.prev();
                                }
                                KeyCode::Enter => {
                                    if pf_dialog.selected_field == crate::app::PortForwardField::Cancel {
                                        // Cancel
                                        app.port_forward_dialog = None;
                                    } else {
                                        // OK — execute port-forward
                                        let local = pf_dialog.local_port.trim().to_string();
                                        let remote = pf_dialog.container_port.trim().to_string();
                                        let kubectl_target = pf_dialog.target.clone();
                                        let target_ns = pf_dialog.namespace.clone();
                                        app.port_forward_dialog = None;

                                        if local.parse::<u16>().is_ok() && remote.parse::<u16>().is_ok() {
                                            let ports_str = format!("{}:{}", local, remote);
                                            let context = app.context.clone();
                                            let tx = event_tx.clone();
                                            let target = kubectl_target.clone();
                                            if let Some(h) = port_forward_task.take() { h.abort(); }
                                            port_forward_task = Some(tokio::spawn(async move {
                                                let mut cmd = tokio::process::Command::new("kubectl");
                                                cmd.arg("port-forward").arg(&target).arg(&ports_str);
                                                if !target_ns.is_empty() { cmd.arg("-n").arg(&target_ns); }
                                                if !context.is_empty() { cmd.arg("--context").arg(&context); }
                                                match cmd.kill_on_drop(true).spawn() {
                                                    Ok(mut child) => {
                                                        match child.wait().await {
                                                            Ok(_) => { let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::info(format!("Port-forward ended for {}", target)))).await; }
                                                            Err(e) => { let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(format!("Port-forward failed: {}", e)))).await; }
                                                        }
                                                    }
                                                    Err(e) => { let _ = tx.send(AppEvent::Flash(crate::app::FlashMessage::error(format!("Port-forward failed: {}", e)))).await; }
                                                }
                                            }));
                                            app.flash = Some(crate::app::FlashMessage::info(
                                                format!("Port-forwarding {} on {}:{}", kubectl_target, local, remote)
                                            ));
                                        } else {
                                            app.flash = Some(crate::app::FlashMessage::warn(
                                                "Invalid port numbers".to_string()
                                            ));
                                        }
                                    }
                                }
                                KeyCode::Backspace => {
                                    match pf_dialog.selected_field {
                                        crate::app::PortForwardField::LocalPort => { pf_dialog.local_port.pop(); }
                                        crate::app::PortForwardField::ContainerPort => { pf_dialog.container_port.pop(); }
                                        _ => {}
                                    }
                                }
                                KeyCode::Char(c) if c.is_ascii_digit() => {
                                    match pf_dialog.selected_field {
                                        crate::app::PortForwardField::LocalPort => { pf_dialog.local_port.push(c); }
                                        crate::app::PortForwardField::ContainerPort => { pf_dialog.container_port.push(c); }
                                        _ => {}
                                    }
                                }
                                _ => {}
                            }
                            needs_redraw = true;
                            continue;
                        }

                        // Capture input in filter mode before normal dispatch
                        if app.nav.filter_input().active {
                            match key.code {
                                KeyCode::Esc => {
                                    // Discard uncommitted text, reapply existing nav filters
                                    app.nav.filter_input_mut().text.clear();
                                    app.nav.filter_input_mut().active = false;
                                    app.reapply_nav_filters();
                                }
                                KeyCode::Enter => {
                                    // Commit: push grep onto nav stack
                                    let text = std::mem::take(&mut app.nav.filter_input_mut().text);
                                    app.nav.filter_input_mut().active = false;
                                    if !text.is_empty() {
                                        let change = app.nav.push(crate::app::nav::NavStep {
                                            resource: app.nav.resource_id().clone(),
                                            filter: Some(crate::app::nav::NavFilter::Grep(text)),
                                            saved_selected: 0,
                                            filter_input: crate::app::nav::FilterInputState::default(),
                                        });
                                        apply_nav_change(&mut app, &mut data_source, change);
                                    }
                                    app.reapply_nav_filters();
                                }
                                KeyCode::Backspace => {
                                    app.nav.filter_input_mut().text.pop();
                                    // Live preview — use lightweight table filter, not full nav reapply
                                    let text = app.nav.filter_input().text.clone();
                                    let rid = app.nav.resource_id().clone();
                                    if let Some(table) = app.data.unified.get_mut(&rid) {
                                        table.filter_text = text;
                                        table.rebuild_filter();
                                    }
                                }
                                KeyCode::Char(c) => {
                                    app.nav.filter_input_mut().text.push(c);
                                    let text = app.nav.filter_input().text.clone();
                                    let rid = app.nav.resource_id().clone();
                                    if let Some(table) = app.data.unified.get_mut(&rid) {
                                        table.filter_text = text;
                                        table.rebuild_filter();
                                    }
                                }
                                _ => {}
                            }
                            needs_redraw = true;
                            continue;
                        }

                        // Log filter input capture — must come before the
                        // yaml/describe search handler so it intercepts keys
                        // while the user is typing a filter pattern.
                        if let crate::app::Route::Logs { ref mut state, .. } = app.route {
                            if state.is_filtering() {
                                match key.code {
                                    KeyCode::Esc => { state.cancel_filter(); }
                                    KeyCode::Enter => { state.commit_filter(); }
                                    KeyCode::Backspace => {
                                        let mut text = state.draft_filter.clone().unwrap_or_default();
                                        text.pop();
                                        state.update_draft(text);
                                    }
                                    KeyCode::Char(c) => {
                                        let mut text = state.draft_filter.clone().unwrap_or_default();
                                        text.push(c);
                                        state.update_draft(text);
                                    }
                                    _ => {}
                                }
                                needs_redraw = true;
                                continue;
                            }
                        }

                        // Search input capture for yaml and describe views.
                        use crate::app::SearchInputResult;
                        let search_handled = match app.route {
                            crate::app::Route::Yaml { ref mut state, .. } if state.search_input_active => {
                                match crate::app::handle_search_key(true, &mut state.search_input, key.code) {
                                    SearchInputResult::Cancelled => { state.search_input_active = false; state.clear_search(); true }
                                    SearchInputResult::Committed(term) => {
                                        state.search_input_active = false;
                                        if term.is_empty() { state.clear_search(); }
                                        else {
                                            state.search = Some(term);
                                            state.update_search();
                                            if let Some(&t) = state.search_matches.first() {
                                                state.current_match = 0;
                                                state.scroll = t.saturating_sub(SEARCH_SCROLL_CONTEXT);
                                            }
                                        }
                                        true
                                    }
                                    SearchInputResult::Updated => true,
                                    SearchInputResult::Ignored => false,
                                }
                            }
                            crate::app::Route::Describe { ref mut state, .. } | crate::app::Route::Aliases { ref mut state, .. } if state.search_input_active => {
                                match crate::app::handle_search_key(true, &mut state.search_input, key.code) {
                                    SearchInputResult::Cancelled => { state.search_input_active = false; state.clear_search(); true }
                                    SearchInputResult::Committed(term) => {
                                        state.search_input_active = false;
                                        if term.is_empty() { state.clear_search(); }
                                        else {
                                            state.search = Some(term);
                                            state.update_search();
                                            if let Some(&t) = state.search_matches.first() {
                                                state.current_match = 0;
                                                state.scroll = t.saturating_sub(SEARCH_SCROLL_CONTEXT);
                                            }
                                        }
                                        true
                                    }
                                    SearchInputResult::Updated => true,
                                    SearchInputResult::Ignored => false,
                                }
                            }
                            _ => false,
                        };
                        if search_handled {
                            needs_redraw = true;
                            continue;
                        }

                        needs_redraw = true;
                        if let Some(action) = crate::event::handler::handle_key_event(&app, key) {
                            if app.read_only && action.is_mutating() {
                                app.flash = Some(crate::app::FlashMessage::info("Read-only mode".to_string()));
                                continue;
                            }
                            let result = handle_action(
                                &mut app,
                                action,
                                &event_tx,
                                &mut data_source,
                                &mut log_task,
                                &mut port_forward_task,
                            );
                            match result {
                                ActionResult::Shell { pod, namespace, container, context } => {
                                    let args = build_shell_args(&pod, &namespace, &container, &context);
                                    run_interactive_local(
                                        &mut terminal, &mut app, "kubectl", &args, &input_suspend, &mut input_suspend_ack,
                                    ).await?;
                                }
                                ActionResult::Edit { resource, name, namespace, context } => {
                                    let args = build_edit_args(&resource, &name, &namespace, &context);
                                    app.flash = Some(crate::app::FlashMessage::info(
                                        format!("Applying edit: {}/{}...", resource, name)
                                    ));
                                    run_interactive_local(
                                        &mut terminal, &mut app, "kubectl", &args, &input_suspend, &mut input_suspend_ack,
                                    ).await?;
                                    // Clear cache after edit (resource may have changed)
                                    app.kubectl_cache.clear();
                                    app.flash = Some(crate::app::FlashMessage::info(
                                        format!("Edit complete: {}/{}", resource, name)
                                    ));
                                }
                                ActionResult::None => {}
                            }
                        }
                    }
                    CtEvent::Mouse(_mouse) => {
                        // TODO: mouse handling
                    }
                    CtEvent::Resize(_, _) => {
                        needs_redraw = true;
                    }
                    _ => {}
                }
            }
            Some(event) = event_rx.recv() => {
                if let AppEvent::ContextSwitchResult { context, result } = event {
                    apply_context_switch_result(
                        &mut app, &mut data_source,
                        &context, result,
                    );
                    app.pod_metrics.clear();
                    app.node_metrics.clear();
                } else {
                    apply_event(&mut app, event);
                }
                // Drain pending events before redrawing to batch log lines,
                // but cap at EVENT_DRAIN_CAP to prevent UI freezes during massive bursts.
                let mut drained = 0;
                while drained < EVENT_DRAIN_CAP {
                    match event_rx.try_recv() {
                        Ok(event) => {
                            if let AppEvent::ContextSwitchResult { context, result } = event {
                                apply_context_switch_result(
                                    &mut app, &mut data_source,
                                    &context, result,
                                );
                                app.pod_metrics.clear();
                                app.node_metrics.clear();
                            } else {
                                apply_event(&mut app, event);
                            }
                            drained += 1;
                        }
                        Err(_) => break,
                    }
                }
                needs_redraw = true;
            }
            _ = tick_interval.tick() => {
                if app.tick() {
                    needs_redraw = true;
                }
                // Check if the log streaming task has finished
                if let Some(ref handle) = log_task {
                    if handle.is_finished() {
                        log_task.take();
                        if let crate::app::Route::Logs { ref mut state, .. } | crate::app::Route::Shell { ref mut state, .. } = app.route {
                            state.streaming = false;
                        }
                        needs_redraw = true;
                    }
                }
            }
        }

        if app.should_quit {
            break;
        }
    }

    // Cleanup
    if let Some(handle) = log_task.take() {
        handle.abort();
    }
    if let Some(handle) = port_forward_task.take() {
        handle.abort();
    }
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        SetCursorStyle::DefaultUserShape,
        LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;

    // Return the exit reason so main.rs can print a message after terminal restore.
    Ok(app.exit_reason)
}
