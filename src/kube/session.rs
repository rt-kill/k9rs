use std::time::Duration;

use anyhow::Result;
use crossterm::{
    cursor::SetCursorStyle,
    event::{Event as CtEvent, KeyCode},
    execute,
    terminal::{disable_raw_mode, LeaveAlternateScreen},
};
use tokio::sync::mpsc;

use crate::app::{App, InputMode};
use crate::event::AppEvent;
use crate::kube::client_session::ClientSession;

use crate::kube::session_actions::handle_action;
use crate::kube::session_events::apply_event;
use crate::kube::session_commands::{
    build_shell_args, run_interactive_local,
    handle_command_key, handle_form_dialog_key, handle_filter_key,
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
///
/// Note: edit no longer lives here. The unified edit flow is driven by the
/// `Route::EditingResource` state machine — `apply_resource_update` writes
/// the YAML to a temp file when the server's response arrives, and the main
/// loop polls for `EditState::EditorReady` at the top of each iteration.
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
}

/// Auto-subscriptions that the TUI opens once the daemon connection is ready.
/// These are core resources that the TUI always needs (namespace list for
/// the namespace picker, node list for ShowNode drill, etc.). Each gets its
/// own yamux substream — the daemon creates a watcher per substream.
///
/// Stored on App so they stay alive for the session's lifetime. Dropped on
/// context switch (new session = new substreams).
pub(crate) fn open_core_subscriptions(app: &mut App, data_source: &ClientSession) {
    use crate::kube::protocol::ResourceId;

    let core = ["namespaces", "nodes"];
    for alias in &core {
        if let Some(rid) = ResourceId::from_alias(alias) {
            // Core resources are cluster-scoped — always All.
            let stream = data_source.subscribe_stream(
                rid,
                crate::kube::protocol::Namespace::All,
                None,
            );
            app.core_streams.push(stream);
        }
    }
}

fn dispatch_app_event(app: &mut App, data_source: &mut ClientSession, event: AppEvent) {
    match event {
        event @ AppEvent::ConnectionEstablished { .. } => {
            // Apply the event first (populates context/cluster/user/namespaces).
            apply_event(app, event);
            // Context switch complete — allow new switches.
            app.context_switch_pending = false;
            // Open core resource substreams now that the connection is ready.
            // The user lands on Overview (the default route) with only the
            // mandatory subscriptions (namespaces, nodes). They navigate to
            // a resource view explicitly — no automatic pods(all) subscribe.
            open_core_subscriptions(app, data_source);
        }
        other => apply_event(app, other),
    }
}

pub(crate) fn apply_nav_change(app: &mut App, data_source: &mut ClientSession, change: crate::app::nav::NavChange) {
    // Unsubscribe: dropping the old step's stream is all that's needed.
    // The yamux substream RSTs → daemon bridge exits → watcher enters
    // grace period. No control-stream command required.

    if let Some(ref new) = change.subscribe {
        // Clear the target table so the view shows fresh "Loading…" until
        // the first snapshot arrives on the new substream. This prevents
        // stale rows from a previous subscription (different filter, same
        // rid) from bleeding into the new view. The substream's bridge
        // sends its first snapshot within milliseconds (the daemon's
        // watcher has data cached), so the flash is negligible.
        app.clear_resource(new);
        // Open a fresh subscription substream via yamux. The bridge task
        // inside subscribe_stream reads StreamEvents from the substream
        // and forwards them as AppEvents into the main event channel.
        let stream = data_source.subscribe_stream(
            new.clone(),
            app.selected_ns.clone(),
            change.subscription_filter.clone(),
        );
        app.nav.current_mut().stream = Some(stream);
    }
}

pub async fn session_main(
    mut app: App,
    mut data_source: ClientSession,
    mut terminal: ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    mut event_tx: mpsc::Sender<AppEvent>,
    mut event_rx: mpsc::Receiver<AppEvent>,
    mut input_rx: mpsc::Receiver<CtEvent>,
    tick_rate: Duration,
    input_suspend: tokio::sync::watch::Sender<bool>,
    mut input_suspend_ack: mpsc::Receiver<()>,
) -> Result<Option<crate::app::ExitReason>> {
    // Track active log streaming substream so we can cancel on Back.
    // Drop = close substream = daemon kills kubectl = log stream ended.
    let mut log_stream: Option<crate::kube::client_session::LogStream> = None;

    let mut tick_interval = tokio::time::interval(tick_rate);
    let mut last_tick = std::time::Instant::now();

    // Main event loop — only redraw when state changes
    let mut needs_redraw = true; // draw the first frame immediately
    loop {
        // Context switch: one socket = one context = one session.
        //
        // Ownership guarantee: we create a NEW event channel for the new
        // session. Dropping the old `event_rx` makes it impossible for
        // stale bridge tasks (which hold clones of the old `event_tx`) to
        // deliver events — their `send()` returns `Err` and they exit.
        // The new session's bridge tasks use the new `event_tx`, and only
        // those events reach our new `event_rx`. Cross-session data bleed
        // is structurally impossible.
        if let Some(new_ctx) = app.pending_context_switch.take() {
            let no_daemon = data_source.is_no_daemon();
            // 1. Drop the old session — closes the socket, aborts bridge tasks.
            drop(data_source);
            // 2. Drop the old event_rx — any buffered stale events are discarded.
            //    Old bridge tasks that haven't been aborted yet will fail on send.
            drop(event_rx);
            // 3. Clear all data AFTER dropping the session so no race exists
            //    between clear and stale bridge sends.
            app.clear_data();
            // 4. New channel for the new session — total isolation.
            let (new_tx, new_rx) = mpsc::channel::<AppEvent>(256);
            event_tx = new_tx;
            event_rx = new_rx;
            // 5. Create a fresh session with the new channel.
            let new_params = crate::kube::client_session::ConnectionParams {
                context: Some(new_ctx),
                namespace: app.selected_ns.as_option().map(|s| s.to_string()),
                readonly: app.read_only,
                no_daemon,
            };
            data_source = ClientSession::new(new_params, event_tx.clone());
            // ConnectionEstablished will fire when the new session is ready,
            // which triggers open_core_subscriptions + re-subscribe.
            needs_redraw = true;
        }

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
            let in_input_mode = matches!(app.input_mode, InputMode::Command { .. })
                || app.form_dialog.as_ref().map_or(false, |d| {
                    d.fields.get(d.focused).map_or(false, |f| f.is_text_input())
                })
                || app.nav.filter_input().active
                || route_has_text_input;
            if in_input_mode {
                execute!(terminal.backend_mut(), SetCursorStyle::SteadyBar)?;
            } else {
                execute!(terminal.backend_mut(), SetCursorStyle::SteadyBlock)?;
            }

            needs_redraw = false;
        }

        // Unified edit flow, stage 2: if the YAML for an in-flight edit
        // arrived since the last loop iteration, the event handler put us
        // into `EditState::EditorReady { temp_path }`. Take the path out,
        // transition to `Applying`, suspend the TUI, run `$EDITOR`, read
        // the result back, and send `Apply { target, yaml }`. The response
        // (a `CommandResult` flash) will pop the edit route. Failures
        // along the way pop the route immediately and flash the error.
        if let crate::app::Route::EditingResource { ref target, ref state } = app.route {
            if let crate::app::EditState::EditorReady { ref temp_path } = state {
                let target = target.clone();
                let temp_path = temp_path.clone();
                // Mark the route as Applying *before* the suspend so any
                // CommandResult that arrives during the editor session
                // (which can't happen — the loop is parked — but for
                // future-proofing) lands in the right state.
                app.route = crate::app::Route::EditingResource {
                    target: target.clone(),
                    state: crate::app::EditState::Applying,
                };

                let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".into());
                let path_arg = temp_path.to_string_lossy().to_string();
                let args = vec![path_arg];
                run_interactive_local(
                    &mut terminal, &mut app, &editor, &args, &input_suspend, &mut input_suspend_ack,
                ).await?;

                // Read the edited file back, send Apply, then delete the temp.
                let read_result = std::fs::read_to_string(&temp_path);
                let _ = std::fs::remove_file(&temp_path);
                match read_result {
                    Ok(yaml) => {
                        if let Err(e) = data_source.apply(&target, yaml) {
                            app.flash = Some(crate::app::FlashMessage::error(
                                format!("Apply failed: {}", e)
                            ));
                            app.pop_route();
                        }
                    }
                    Err(e) => {
                        app.flash = Some(crate::app::FlashMessage::error(
                            format!("Failed to read edited file: {}", e)
                        ));
                        app.pop_route();
                    }
                }
                needs_redraw = true;
                continue;
            }
        }

        tokio::select! {
            // biased: always check key events first so user input (like disabling
            // autoscroll) takes effect before processing queued data events.
            biased;

            Some(ct_event) = input_rx.recv() => {
                match ct_event {
                    CtEvent::Key(key) => {
                        // Input mode handlers — each returns true if the key was consumed.
                        if handle_command_key(&mut app, key, &mut data_source, &mut log_stream, &event_tx) {
                            needs_redraw = true;
                            continue;
                        }
                        // Generic form dialog (Scale, PortForward, …) — one
                        // handler for every operation that needs user input.
                        if handle_form_dialog_key(&mut app, key, &mut data_source) {
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
                                &mut log_stream,
                            );
                            match result {
                                ActionResult::Shell { pod, namespace, container, context } => {
                                    let args = build_shell_args(&pod, &namespace, &container, &context);
                                    run_interactive_local(
                                        &mut terminal, &mut app, "kubectl", &args, &input_suspend, &mut input_suspend_ack,
                                    ).await?;
                                }
                                ActionResult::None => {}
                            }
                        }
                    }
                    // Pure-keyboard TUI by design — mouse events are
                    // intentionally dropped (see ~/.config/claude memory).
                    CtEvent::Resize(_, _) => {
                        needs_redraw = true;
                    }
                    _ => {}
                }
            }
            Some(event) = event_rx.recv() => {
                dispatch_app_event(&mut app, &mut data_source, event);
                // Drain pending events before redrawing to batch log lines,
                // but cap at EVENT_DRAIN_CAP to prevent UI freezes during massive bursts.
                let mut drained = 0;
                while drained < EVENT_DRAIN_CAP {
                    match event_rx.try_recv() {
                        Ok(event) => {
                            dispatch_app_event(&mut app, &mut data_source, event);
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
                // Log stream end is signalled by the bridge task via
                // AppEvent::LogStreamEnded — no polling needed here.
            }
        }

        if app.should_quit {
            break;
        }
    }

    // Cleanup — dropping log_stream closes the substream.
    drop(log_stream);
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
