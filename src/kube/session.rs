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
    run_interactive_local,
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
    Shell(ExecTarget),
    /// Suspend the TUI and run `kubectl debug node/<name> -it` via the
    /// daemon's PTY exec substream.
    NodeShell { node: String },
}

/// Identifies a pod + container for an interactive exec session via
/// `kubectl exec`. Uses SPDY protocol which works universally, unlike
/// kube-rs's WebSocket-only exec which fails on many proxied clusters.
#[derive(Debug, Clone)]
pub(crate) struct ExecTarget {
    pub pod: String,
    pub namespace: String,
    pub container: String,
}

/// Auto-subscriptions that the TUI opens once the daemon connection is ready.
/// These are core resources that the TUI always needs (namespace list for
/// the namespace picker, node list for ShowNode drill, etc.). Each gets its
/// own yamux substream — the daemon creates a watcher per substream.
///
/// Stored on App so they stay alive for the session's lifetime. Dropped on
/// context switch (new session = new substreams).
/// Convert a crossterm key event to raw terminal bytes for forwarding
/// to a PTY. Handles printable chars, control keys, arrows, etc.
fn crossterm_key_to_bytes(key: crossterm::event::KeyEvent) -> Vec<u8> {
    use crossterm::event::{KeyCode, KeyModifiers};

    // Alt wraps the inner key bytes with an ESC prefix.
    if key.modifiers.contains(KeyModifiers::ALT) {
        let inner = crossterm_key_to_bytes(crossterm::event::KeyEvent::new(
            key.code,
            key.modifiers.difference(KeyModifiers::ALT),
        ));
        if !inner.is_empty() {
            let mut v = vec![0x1b];
            v.extend(inner);
            return v;
        }
    }

    // Shift modifier for navigation keys: CSI 1;2 <final>.
    if key.modifiers.contains(KeyModifiers::SHIFT) {
        let shifted = match key.code {
            KeyCode::Up => Some(b'A'),
            KeyCode::Down => Some(b'B'),
            KeyCode::Right => Some(b'C'),
            KeyCode::Left => Some(b'D'),
            KeyCode::Home => Some(b'H'),
            KeyCode::End => Some(b'F'),
            _ => None,
        };
        if let Some(ch) = shifted {
            return vec![0x1b, b'[', b'1', b';', b'2', ch];
        }
    }

    match key.code {
        KeyCode::Char(c) => {
            if key.modifiers.contains(KeyModifiers::CONTROL) {
                // Ctrl+A..Z → 0x01..0x1a
                if c.is_ascii_alphabetic() {
                    let byte = (c.to_ascii_lowercase() as u8) - b'a' + 1;
                    return vec![byte];
                }
                // Ctrl+Space → NUL, Ctrl+[ → ESC, etc.
                return match c {
                    ' ' | '2' => vec![0x00],  // Ctrl+Space / Ctrl+2
                    '[' | '3' => vec![0x1b],  // Ctrl+[
                    '\\' | '4' => vec![0x1c], // Ctrl+\
                    ']' | '5' => vec![0x1d],  // Ctrl+]
                    '^' | '6' => vec![0x1e],  // Ctrl+^
                    '_' | '7' | '/' => vec![0x1f], // Ctrl+_
                    _ => vec![],
                };
            }
            let mut buf = [0u8; 4];
            let s = c.encode_utf8(&mut buf);
            s.as_bytes().to_vec()
        }
        KeyCode::Esc => vec![0x1b],
        KeyCode::Enter => vec![b'\r'],
        KeyCode::Backspace => vec![0x7f],
        KeyCode::Tab => vec![b'\t'],
        KeyCode::BackTab => vec![0x1b, b'[', b'Z'],
        KeyCode::Up => vec![0x1b, b'[', b'A'],
        KeyCode::Down => vec![0x1b, b'[', b'B'],
        KeyCode::Right => vec![0x1b, b'[', b'C'],
        KeyCode::Left => vec![0x1b, b'[', b'D'],
        KeyCode::Home => vec![0x1b, b'[', b'H'],
        KeyCode::End => vec![0x1b, b'[', b'F'],
        KeyCode::PageUp => vec![0x1b, b'[', b'5', b'~'],
        KeyCode::PageDown => vec![0x1b, b'[', b'6', b'~'],
        KeyCode::Insert => vec![0x1b, b'[', b'2', b'~'],
        KeyCode::Delete => vec![0x1b, b'[', b'3', b'~'],
        KeyCode::F(n) => match n {
            1 => vec![0x1b, b'O', b'P'],
            2 => vec![0x1b, b'O', b'Q'],
            3 => vec![0x1b, b'O', b'R'],
            4 => vec![0x1b, b'O', b'S'],
            5..=12 => {
                let codes = [b"15", b"17", b"18", b"19", b"20", b"21", b"23", b"24"];
                let idx = (n - 5) as usize;
                if idx < codes.len() {
                    let mut v = vec![0x1b, b'['];
                    v.extend_from_slice(codes[idx]);
                    v.push(b'~');
                    v
                } else { vec![] }
            }
            _ => vec![],
        },
        _ => vec![],
    }
}

/// Open an exec session and navigate to Route::Shell. The session renders
/// inside the TUI — no suspend, no alternate screen exit. The daemon spawns
/// kubectl in a PTY; bytes flow over yamux and are parsed by vt100 into a
/// screen buffer that the TUI renders each frame.
pub(crate) async fn open_exec_route(
    app: &mut App,
    data_source: &ClientSession,
    exec_init: crate::kube::protocol::ExecInit,
    title: String,
    event_tx: &tokio::sync::mpsc::Sender<crate::event::AppEvent>,
) -> Result<()> {
    let stream = data_source.open_exec_stream(exec_init.clone()).await?;
    let parser = vt100::Parser::new(exec_init.term_height, exec_init.term_width, 0);

    // Split: writer stays in Route for keystrokes, reader goes to bridge task.
    let crate::kube::client_session::ExecStream { reader, writer } = stream;
    let tx = event_tx.clone();
    let bridge = tokio::spawn(async move {
        let panic_tx = tx.clone();
        let result = std::panic::AssertUnwindSafe(async {
        let mut reader = reader;
        loop {
            match crate::kube::protocol::read_bincode::<_, crate::kube::protocol::ExecFrame>(&mut reader).await {
                Ok(crate::kube::protocol::ExecFrame::Data(bytes)) => {
                    if tx.send(crate::event::AppEvent::ExecData(bytes)).await.is_err() { break; }
                }
                Ok(crate::kube::protocol::ExecFrame::Resize { .. }) => {}
                Err(_) => {
                    let _ = tx.send(crate::event::AppEvent::ExecEnded).await;
                    break;
                }
            }
        }
        });
        if let Err(_panic) = futures::FutureExt::catch_unwind(result).await {
            let _ = panic_tx.send(crate::event::AppEvent::ExecEnded).await;
            let _ = panic_tx.send(crate::event::AppEvent::Flash(
                crate::app::FlashMessage::error("exec bridge panicked".to_string()),
            )).await;
        }
    });

    app.navigate_to(crate::app::Route::Shell(Box::new(crate::app::ShellState {
        title,
        parser,
        writer: Some(writer),
        _bridge: Some(bridge.abort_handle()),
        connected: false,
    })));
    Ok(())
}

/// Strip leading `#` comment lines from edited YAML. We prepend server
/// error messages as `# ...` on re-open so the user can see what went
/// wrong. These must be stripped before diffing or sending to the server.
/// Only strips the contiguous comment block at the top — comments in the
/// middle of the YAML are the user's and are left alone.
/// Strip leading comment lines that WE prepended (error re-open).
/// Only strips lines starting with `# k9rs:` or blank lines in the
/// leading block. User's own `#` comments in the YAML are preserved.
fn strip_leading_comments(s: &str) -> &str {
    let mut pos = 0;
    for line in s.lines() {
        if line.starts_with("# k9rs:") || line == "#" || line.trim().is_empty() {
            pos += line.len();
            if s[pos..].starts_with("\r\n") {
                pos += 2;
            } else if s[pos..].starts_with('\n') {
                pos += 1;
            }
        } else {
            break;
        }
    }
    &s[pos.min(s.len())..]
}

/// RAII guard for a suspended TUI. Creation suspends the input bridge and
/// leaves the alternate screen. Drop restores both — whether the caller
/// returns normally, errors, or panics. This makes it impossible to leave
/// the TUI in a suspended state: you hold a `SuspendGuard` while doing
/// interactive work, and the guard's drop handles cleanup.
///
/// Follows the same permit pattern as `SubscriptionStream` (drop closes
/// substream), `LogStream` (drop aborts bridge), and `LiveQuery` (drop
/// aborts watcher): the resource's lifetime IS the guard's lifetime.
pub(crate) struct SuspendGuard<'a, B: ratatui::backend::Backend + std::io::Write> {
    terminal: &'a mut ratatui::Terminal<B>,
    input_suspend: &'a tokio::sync::watch::Sender<bool>,
}

impl<'a, B: ratatui::backend::Backend + std::io::Write> SuspendGuard<'a, B> {
    /// Suspend the TUI: stop the input bridge, leave alternate screen.
    /// Returns a guard that resumes on drop.
    pub(crate) async fn new(
        terminal: &'a mut ratatui::Terminal<B>,
        input_suspend: &'a tokio::sync::watch::Sender<bool>,
        input_suspend_ack: &mut mpsc::Receiver<()>,
    ) -> Result<Self> {
        let _ = input_suspend.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(1), input_suspend_ack.recv()).await;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        Ok(Self { terminal, input_suspend })
    }

    /// Access the terminal (e.g., to disable raw mode for subprocess shells).
    pub(crate) fn terminal_mut(&mut self) -> &mut ratatui::Terminal<B> {
        self.terminal
    }
}

impl<B: ratatui::backend::Backend + std::io::Write> Drop for SuspendGuard<'_, B> {
    fn drop(&mut self) {
        let _ = execute!(self.terminal.backend_mut(), crossterm::terminal::EnterAlternateScreen);
        let _ = self.terminal.clear();
        let _ = self.input_suspend.send(false);
    }
}


pub(crate) fn open_core_subscriptions(app: &mut App, data_source: &ClientSession) {
    for def in crate::kube::resource_defs::REGISTRY.all() {
        if def.is_core() {
            let rid = def.resource_id();
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
            // Transition: InFlight → Stable. The new session is live; a
            // subsequent `begin_context_switch` is now allowed.
            app.context_switch.mark_stable();
            // Open core resource substreams now that the connection is ready.
            // The user lands on Overview (the default route) with only the
            // mandatory subscriptions (namespaces, nodes). They navigate to
            // a resource view explicitly — no automatic pods(all) subscribe.
            open_core_subscriptions(app, data_source);
        }
        other => apply_event(app, other),
    }
}

/// Capture a keystroke into the log filter input draft (`/`-prompt within
/// the logs view). Returns `true` if the key was consumed. Mirrors the
/// shape of `handle_command_key` / `handle_form_dialog_key` /
/// `handle_filter_key` — each input modality has its own free function so
/// the main loop just calls them in priority order.
fn handle_log_filter_key(app: &mut App, key: crossterm::event::KeyEvent) -> bool {
    let crate::app::Route::Logs { ref mut state, .. } = app.route else {
        return false;
    };
    if !state.is_filtering() {
        return false;
    }
    match key.code {
        KeyCode::Esc => state.cancel_filter(),
        KeyCode::Enter => state.commit_filter(),
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
    true
}

/// Capture a keystroke into the YAML / Describe / Aliases search input.
/// Both content-view routes share the same `ContentViewState`, so the
/// match arms diverge only on which route variant we're inside —
/// extracted so the main loop doesn't carry the same 20 lines twice.
fn handle_content_search_key(app: &mut App, key: crossterm::event::KeyEvent) -> bool {
    use crate::app::SearchInputResult;
    let state = match app.route {
        crate::app::Route::Yaml { ref mut state, .. } if state.search_input_active => state,
        crate::app::Route::Describe { ref mut state, .. }
        | crate::app::Route::Aliases { ref mut state, .. }
            if state.search_input_active =>
        {
            state
        }
        _ => return false,
    };
    match crate::app::handle_search_key(&mut state.search_input, key.code) {
        SearchInputResult::Cancelled => {
            state.search_input_active = false;
            state.clear_search();
            true
        }
        SearchInputResult::Committed(term) => {
            state.search_input_active = false;
            if term.is_empty() {
                state.clear_search();
            } else {
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
    }
}

pub(crate) fn apply_nav_change(app: &mut App, data_source: &mut ClientSession, change: crate::app::nav::NavChange) {
    // Unsubscribe: dropping the old step's stream is all that's needed.
    // The yamux substream RSTs → daemon bridge exits → watcher enters
    // grace period. No control-stream command required.

    if let Some(ref new) = change.subscribe {
        // For non-globally-stored resources, create the table on the
        // current nav step so snapshot data lands there instead of the
        // global store.
        if !crate::app::nav::is_globally_stored(new) {
            let step = app.nav.current_mut();
            if step.table.is_none() {
                step.table = Some(crate::app::StatefulTable::new());
            }
        } else if change.subscription_filter.is_some() {
            // Globally-stored resources with a filter: clear the global
            // table so stale rows from a DIFFERENT filter can't be shown.
            app.clear_resource(new);
        }
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
    // Log stream is owned by Route::Logs — no separate variable needed.

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
        //
        // `take_requested()` atomically transitions the state from
        // `Requested(name)` to `InFlight`, so a second `begin_context_switch`
        // call fired between this line and `mark_stable()` (in
        // `dispatch_app_event` on `ConnectionEstablished`) will be rejected
        // by the `is_stable()` check.
        if let Some(new_ctx) = app.context_switch.take_requested() {
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
                namespace: app.selected_ns.clone(),
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
                crate::app::Route::Logs { ref state, .. } => state.is_filtering(),
                _ => false,
            };
            let in_input_mode = matches!(app.input_mode, InputMode::Command { .. })
                || app.form_dialog.as_ref().is_some_and(|d| {
                    d.fields.get(d.focused).is_some_and(|f| f.is_text_input())
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
        // into `EditState::EditorReady { temp_path, original }`. Run the
        // editor, validate the result, and send Apply. On server error,
        // the CommandResult handler loops back to EditorReady (re-open).
        // Check if we're in EditorReady before taking the route out.
        let is_editor_ready = matches!(
            app.route,
            crate::app::Route::EditingResource {
                state: crate::app::EditState::EditorReady { .. }, ..
            }
        );
        if is_editor_ready {
            // Take the route out so we own TempFile (move, not borrow).
            // TempFile::Drop deletes the file, so on abort paths we just
            // let it drop. On the apply path we move it to Applying.
            let old_route = std::mem::replace(&mut app.route, crate::app::Route::Resources);
            let (target, temp_file, original) = match old_route {
                crate::app::Route::EditingResource {
                    target,
                    state: crate::app::EditState::EditorReady { temp_file, original },
                } => (target, temp_file, original),
                _ => unreachable!(),
            };

            let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".into());
            let path_arg = temp_file.path().to_string_lossy().to_string();
            let args = vec![path_arg];
            let exit_status = match run_interactive_local(
                &mut terminal, &mut app, &editor, &args,
                crate::kube::session_commands::InteractiveKind::Editor,
                &input_suspend, &mut input_suspend_ack,
            ).await {
                Ok(s) => s,
                Err(e) => {
                    // TempFile drops here → deletes the file.
                    app.flash = Some(crate::app::FlashMessage::error(
                        format!("Editor failed: {}", e)
                    ));
                    needs_redraw = true;
                    continue;
                }
            };

            // Editor non-zero exit (vim `:cq`) → abort. TempFile drops.
            if exit_status.is_some_and(|s| !s.success()) {
                app.flash = Some(crate::app::FlashMessage::info(
                    "Edit aborted.".to_string()
                ));
                needs_redraw = true;
                continue;
            }

            // Read the edited file back.
            let edited = match std::fs::read_to_string(temp_file.path()) {
                Ok(s) => s,
                Err(e) => {
                    app.flash = Some(crate::app::FlashMessage::error(
                        format!("Failed to read edited file: {}", e)
                    ));
                    needs_redraw = true;
                    continue;
                }
            };

            let stripped = strip_leading_comments(&edited);

            // Unchanged → abort. TempFile drops.
            if stripped.trim() == original.trim() {
                app.flash = Some(crate::app::FlashMessage::info(
                    "Edit cancelled, no changes made.".to_string()
                ));
                needs_redraw = true;
                continue;
            }

            // Empty → abort. TempFile drops.
            if stripped.trim().is_empty() {
                app.flash = Some(crate::app::FlashMessage::info(
                    "Edit cancelled, file is empty.".to_string()
                ));
                needs_redraw = true;
                continue;
            }

            // Send Apply. Move TempFile to Applying state — keeps file alive
            // for re-open on error. TempFile only drops when edit completes.
            let target_clone = target.clone();
            app.route = crate::app::Route::EditingResource {
                target,
                state: crate::app::EditState::Applying {
                    temp_file,
                    original,
                },
            };
            if let Err(e) = data_source.apply(&target_clone, stripped.to_string()) {
                app.flash = Some(crate::app::FlashMessage::error(
                    format!("Apply failed: {}", e)
                ));
                app.pop_route();
            }
            needs_redraw = true;
            continue;
        }

        tokio::select! {
            // biased: always check key events first so user input (like disabling
            // autoscroll) takes effect before processing queued data events.
            biased;

            Some(ct_event) = input_rx.recv() => {
                match ct_event {
                    CtEvent::Key(key) => {
                        // Shell route: forward ALL keys as raw bytes to the
                        // daemon's PTY. Shell exits only when the remote
                        // process exits (ExecEnded event), not on Esc.
                        if let crate::app::Route::Shell(ref mut shell) = app.route {
                            if let Some(ref mut w) = shell.writer {
                                let bytes = crossterm_key_to_bytes(key);
                                if !bytes.is_empty() {
                                    let frame = crate::kube::protocol::ExecFrame::Data(bytes);
                                    if crate::kube::protocol::write_bincode(w, &frame).await.is_err() {
                                        app.pop_route();
                                        app.flash = Some(crate::app::FlashMessage::error(
                                            "Shell disconnected".to_string()
                                        ));
                                        needs_redraw = true;
                                        continue;
                                    }
                                }
                            }
                            needs_redraw = true;
                            continue;
                        }

                        // Input mode handlers — each returns true if the key was consumed.
                        if handle_command_key(&mut app, key, &mut data_source, &event_tx) {
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
                        if handle_log_filter_key(&mut app, key) {
                            needs_redraw = true;
                            continue;
                        }

                        // Search input capture for yaml and describe views.
                        if handle_content_search_key(&mut app, key) {
                            needs_redraw = true;
                            continue;
                        }

                        needs_redraw = true;
                        if let Some(action) = crate::event::handler::handle_key_event(&app, key) {
                            // Client-side UX shortcut: flash "Read-only mode"
                            // immediately without a wire round-trip. The server
                            // is the real security boundary and rejects every
                            // mutating command in readonly mode via its own
                            // `reject_if_readonly` — if `is_mutating()` ever
                            // drifts from the server's classification, the
                            // server still refuses. This is polish, not a
                            // second source of truth.
                            if app.read_only && action.is_mutating() {
                                app.flash = Some(crate::app::FlashMessage::info("Read-only mode".to_string()));
                                continue;
                            }
                            let result = handle_action(
                                &mut app,
                                action,
                                &event_tx,
                                &mut data_source,
                            );
                            match result {
                                ActionResult::Shell(target) => {
                                    let (tw, th) = crossterm::terminal::size().unwrap_or((80, 24));
                                    let mut kubectl_args = vec![
                                        "exec".to_string(), "-it".to_string(),
                                        "-n".to_string(), target.namespace.clone(),
                                        target.pod.clone(),
                                    ];
                                    if !target.container.is_empty() {
                                        kubectl_args.push("-c".to_string());
                                        kubectl_args.push(target.container.clone());
                                    }
                                    kubectl_args.push("--".to_string());
                                    kubectl_args.push("sh".to_string());
                                    let title = format!("{}/{}", target.pod, target.container);
                                    let exec_init = crate::kube::protocol::ExecInit {
                                        kubectl_args,
                                        term_width: tw,
                                        term_height: th,
                                    };
                                    if let Err(e) = open_exec_route(
                                        &mut app, &data_source, exec_init, title, &event_tx,
                                    ).await {
                                        app.flash = Some(crate::app::FlashMessage::error(
                                            format!("Shell failed: {}", e)
                                        ));
                                    }
                                }
                                ActionResult::NodeShell { node, .. } => {
                                    let (tw, th) = crossterm::terminal::size().unwrap_or((80, 24));
                                    let kubectl_args = vec![
                                        "debug".to_string(),
                                        format!("node/{}", node),
                                        "-it".to_string(),
                                        "--image=busybox".to_string(),
                                    ];
                                    let title = format!("node/{}", node);
                                    let exec_init = crate::kube::protocol::ExecInit {
                                        kubectl_args,
                                        term_width: tw,
                                        term_height: th,
                                    };
                                    if let Err(e) = open_exec_route(
                                        &mut app, &data_source, exec_init, title, &event_tx,
                                    ).await {
                                        app.flash = Some(crate::app::FlashMessage::error(
                                            format!("Node shell failed: {}", e)
                                        ));
                                    }
                                }
                                ActionResult::None => {}
                            }
                        }
                    }
                    // Pure-keyboard TUI by design — mouse events are
                    // intentionally dropped (see ~/.config/claude memory).
                    CtEvent::Resize(w, h) => {
                        // Forward resize to daemon's PTY when in shell view.
                        if let crate::app::Route::Shell(ref mut shell) = app.route {
                            // Update local parser first so the render matches
                            // even if the daemon write fails.
                            shell.parser.set_size(h, w);
                            if let Some(ref mut wr) = shell.writer {
                                let frame = crate::kube::protocol::ExecFrame::Resize { width: w, height: h };
                                let _ = crate::kube::protocol::write_bincode(wr, &frame).await;
                            }
                        }
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
