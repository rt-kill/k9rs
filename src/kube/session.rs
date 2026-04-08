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
use crate::event::AppEvent;
use crate::kube::client_session::ClientSession;

use crate::kube::session_actions::handle_action;
use crate::kube::session_events::apply_event;
use crate::kube::session_nav::apply_context_switch_result;
use crate::kube::session_commands::{
    build_shell_args, build_edit_args, run_interactive_local,
    handle_command_key, handle_scale_key, handle_filter_key,
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
                        // Input mode handlers — each returns true if the key was consumed.
                        if handle_command_key(&mut app, key, &mut data_source, &mut log_task, &mut port_forward_task, &event_tx) {
                            needs_redraw = true;
                            continue;
                        }
                        if handle_scale_key(&mut app, key, &mut data_source) {
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
                                        app.port_forward_dialog = None;
                                    } else {
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

                        if handle_filter_key(&mut app, key, &mut data_source) {
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
