use std::os::unix::io::AsRawFd;
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

struct RawFd(std::os::unix::io::RawFd);
impl AsRawFd for RawFd {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd { self.0 }
}

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
            $app.ui.flash = Some(crate::app::FlashMessage::error(format!("Connection error: {}", e)));
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
// (SEARCH_SCROLL_CONTEXT moved to app.config.ui.search_context_lines)

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
    /// Launch an interactive exec session. The `op` identifies the operation
    /// (Shell, NodeShell) whose `exec_template()` defines the kubectl args.
    Exec {
        op: crate::kube::protocol::OperationKind,
        target: ExecTarget,
    },
}

/// Target for an interactive exec session. Two disjoint shapes — a pod
/// shell needs `(pod, namespace, container)` while a node debug shell
/// needs only the node name. The enum makes this distinction structural
/// instead of encoding "not applicable" as empty strings.
#[derive(Debug, Clone)]
pub(crate) enum ExecTarget {
    /// `kubectl exec -it -n <ns> <pod> -c <container> -- sh`
    Pod {
        pod: String,
        namespace: String,
        container: String,
    },
    /// `kubectl debug node/<node> -it --image=busybox`
    Node {
        node: String,
    },
}

/// Open an exec session and navigate to Route::Shell. The daemon spawns
/// kubectl in a PTY and proxies bytes over yamux. During the Connecting
/// phase the TUI shows a loading bar and the user has full navigation
/// control. Once the first ExecData arrives (Connected), the session
/// loop suspends the TUI and enters a raw byte bridge — stdin→daemon,
/// daemon→stdout — with 100% terminal fidelity and no vt100 parsing.
pub(crate) async fn open_exec_route(
    app: &mut App,
    data_source: &ClientSession,
    exec_init: crate::kube::protocol::ExecInit,
    title: String,
    event_tx: &tokio::sync::mpsc::Sender<crate::event::AppEvent>,
) -> Result<()> {
    let stream = data_source.open_exec_stream(exec_init.clone(), event_tx.clone()).await?;

    app.navigate_to(crate::app::Route::Shell(Box::new(crate::app::ShellState {
        title,
        stream: Some(stream),
        connect_state: crate::app::ShellConnectState::Connecting,
        pending_output: Vec::new(),
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


/// Run the editor flow when the route is `EditState::EditorReady`.
/// Returns `true` if the editor was launched (the caller should `continue`
/// the main loop regardless of outcome — success, abort, or error).
async fn run_editor_flow(
    app: &mut App,
    terminal: &mut ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    data_source: &mut ClientSession,
    input_suspend: &tokio::sync::watch::Sender<bool>,
    input_suspend_ack: &mut mpsc::Receiver<()>,
) -> bool {
    let is_editor_ready = matches!(
        app.route,
        crate::app::Route::EditingResource {
            state: crate::app::EditState::EditorReady { .. }, ..
        }
    );
    if !is_editor_ready {
        return false;
    }

    // Take the route out so we own TempFile (move, not borrow).
    let old_route = std::mem::replace(&mut app.route, crate::app::Route::Resources);
    let (target, temp_file, original) = match old_route {
        crate::app::Route::EditingResource {
            target,
            state: crate::app::EditState::EditorReady { temp_file, original },
        } => (target, temp_file, original),
        _ => unreachable!(),
    };

    // Record file mtime BEFORE launching the editor. If the user
    // exits without saving (:q), the mtime won't change → abort.
    // This correctly handles the retry-after-failure case: we rewrite
    // the file with error comments, then open the editor. :q = mtime
    // unchanged = abort. :wq = mtime newer = apply.
    let pre_edit_mtime = std::fs::metadata(temp_file.path())
        .and_then(|m| m.modified())
        .ok();

    let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".into());
    let path_arg = temp_file.path().to_string_lossy().to_string();
    let args = vec![path_arg];
    let exit_status = match run_interactive_local(
        terminal, app, &editor, &args,
        crate::kube::session_commands::InteractiveKind::Editor,
        input_suspend, input_suspend_ack,
    ).await {
        Ok(s) => s,
        Err(e) => {
            app.ui.flash = Some(crate::app::FlashMessage::error(
                format!("Editor failed: {}", e)
            ));
            return true;
        }
    };

    // Editor non-zero exit (vim `:cq`) -> abort. TempFile drops.
    if exit_status.is_some_and(|s| !s.success()) {
        app.ui.flash = Some(crate::app::FlashMessage::info(
            "Edit aborted.".to_string()
        ));
        return true;
    }

    // Check if the file was modified by comparing mtime before/after the
    // editor. :q (no save) = mtime unchanged = abort. :wq = mtime newer = apply.
    // Handles retry-after-failure: we rewrote the file with error comments
    // before reopening. :q means "I give up", not "retry same content."
    let post_edit_mtime = std::fs::metadata(temp_file.path())
        .and_then(|m| m.modified())
        .ok();
    if pre_edit_mtime == post_edit_mtime {
        app.ui.flash = Some(crate::app::FlashMessage::info(
            "Edit cancelled (file not saved).".to_string()
        ));
        return true;
    }

    // Read the edited file back.
    let edited = match std::fs::read_to_string(temp_file.path()) {
        Ok(s) => s,
        Err(e) => {
            app.ui.flash = Some(crate::app::FlashMessage::error(
                format!("Failed to read edited file: {}", e)
            ));
            return true;
        }
    };

    let stripped = strip_leading_comments(&edited);

    // Unchanged from original → abort (first edit, no changes made).
    if stripped.trim() == original.trim() {
        app.ui.flash = Some(crate::app::FlashMessage::info(
            "Edit cancelled, no changes made.".to_string()
        ));
        return true;
    }

    // Empty -> abort.
    if stripped.trim().is_empty() {
        app.ui.flash = Some(crate::app::FlashMessage::info(
            "Edit cancelled, file is empty.".to_string()
        ));
        return true;
    }

    // Send Apply. Move TempFile to Applying state.
    let target_clone = target.clone();
    app.route = crate::app::Route::EditingResource {
        target,
        state: crate::app::EditState::Applying {
            temp_file,
            original,
        },
    };
    if let Err(e) = data_source.apply(&target_clone, stripped.to_string()) {
        app.ui.flash = Some(crate::app::FlashMessage::error(
            format!("Apply failed: {}", e)
        ));
        app.pop_route();
    }
    true
}

/// Suspend the TUI and run a raw byte bridge between stdin/stdout and
/// the daemon's PTY stream. Called when `Route::Shell` transitions to
/// `ShellConnectState::Connected`. On return the TUI is restored and the
/// shell route is popped.
///
/// During the bridge:
/// - Raw stdin bytes → `ExecFrame::Data` → daemon → PTY
/// - `AppEvent::ExecData` → raw stdout (no vt100 parsing)
/// - SIGWINCH → `ExecFrame::Resize` → daemon (PTY ioctl)
///
/// The bridge exits when `ExecEnded` arrives, the yamux stream errors,
/// or stdin closes. Terminal state is hard-reset (`\x1bc`) before the
/// TUI resumes, so nothing the shell did can leak into the restored UI.
async fn run_shell_bridge(
    app: &mut App,
    terminal: &mut ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    event_rx: &mut mpsc::Receiver<AppEvent>,
    input_suspend: &tokio::sync::watch::Sender<bool>,
    input_suspend_ack: &mut mpsc::Receiver<()>,
    input_rx: &mut mpsc::Receiver<crossterm::event::Event>,
) {
    use tokio::io::AsyncWriteExt;

    // Extract the exec stream and pending output from ShellState.
    let (mut stream, pending) = match &mut app.route {
        crate::app::Route::Shell(ref mut shell) => {
            let s = shell.stream.take();
            let p = std::mem::take(&mut shell.pending_output);
            (s, p)
        }
        _ => return,
    };
    let Some(ref mut stream) = stream else { return };
    let writer = &mut stream.writer;

    // Send resize with full terminal dimensions (no TUI borders during bridge).
    let (tw, th) = crossterm::terminal::size().unwrap_or((80, 24));
    let _ = crate::kube::protocol::write_bincode(
        writer,
        &crate::kube::protocol::ExecFrame::Resize { width: tw, height: th },
    ).await;

    // Suspend TUI: stop the input bridge (releases stdin), leave alternate
    // screen. Raw mode stays on — we need raw byte passthrough.
    let _ = input_suspend.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(1), input_suspend_ack.recv()).await;
    let _ = execute!(terminal.backend_mut(), crossterm::terminal::LeaveAlternateScreen);

    // Clear the main screen so the user's local shell prompt (visible
    // underneath the alternate screen) doesn't bleed through.
    let mut stdout = tokio::io::stdout();
    let _ = stdout.write_all(b"\x1b[2J\x1b[H").await;
    let _ = stdout.flush().await;

    // Print a small banner so the shell entry feels intentional.
    let _ = stdout.write_all(b"\x1b[36m\
  _     ___\r\n\
 | | __/ _ \\ _ __ ___\r\n\
 | |/ / (_) | '__/ __|\r\n\
 |   < \\__, | |  \\__ \\\r\n\
 |_|\\_\\  /_/|_|  |___/\r\n\
\x1b[0m\r\n").await;
    let _ = stdout.flush().await;

    // Flush any bytes received during the Connecting→Connected transition
    // (typically the initial shell prompt).
    if !pending.is_empty() {
        let _ = stdout.write_all(&pending).await;
        let _ = stdout.flush().await;
    }

    // Non-blocking stdin via AsyncFd — no background thread, clean drop.
    // tokio::io::stdin() spawns a blocking thread that outlives drop and
    // races with crossterm's EventStream for fd 0 on resume.
    let stdin_fd = std::io::stdin().as_raw_fd();
    let old_flags = unsafe { libc::fcntl(stdin_fd, libc::F_GETFL) };
    unsafe { libc::fcntl(stdin_fd, libc::F_SETFL, old_flags | libc::O_NONBLOCK) };
    let async_stdin = tokio::io::unix::AsyncFd::new(RawFd(stdin_fd))
        .expect("AsyncFd registration");

    let mut buf = [0u8; 4096];
    let mut sigwinch = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::window_change(),
    ).expect("SIGWINCH handler");

    loop {
        tokio::select! {
            biased;
            readable = async_stdin.readable() => {
                let Ok(mut guard) = readable else { break };
                match guard.try_io(|fd| {
                    let n = unsafe {
                        libc::read(fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len())
                    };
                    if n < 0 { Err(std::io::Error::last_os_error()) } else { Ok(n as usize) }
                }) {
                    Ok(Ok(0)) | Ok(Err(_)) => break,
                    Ok(Ok(n)) => {
                        let frame = crate::kube::protocol::ExecFrame::Data(buf[..n].to_vec());
                        if crate::kube::protocol::write_bincode(writer, &frame).await.is_err() {
                            break;
                        }
                    }
                    Err(_would_block) => continue,
                }
            }
            event = event_rx.recv() => {
                match event {
                    Some(AppEvent::ExecData(bytes)) => {
                        let _ = stdout.write_all(&bytes).await;
                        let _ = stdout.flush().await;
                    }
                    Some(AppEvent::ExecEnded) | None => break,
                    Some(_) => {}
                }
            }
            _ = sigwinch.recv() => {
                if let Ok((w, h)) = crossterm::terminal::size() {
                    let frame = crate::kube::protocol::ExecFrame::Resize { width: w, height: h };
                    let _ = crate::kube::protocol::write_bincode(writer, &frame).await;
                }
            }
        }
    }

    // Clean up stdin: deregister from epoll, restore blocking mode.
    drop(async_stdin);
    unsafe { libc::fcntl(stdin_fd, libc::F_SETFL, old_flags) };

    // Reset terminal modes the container shell may have changed.
    // Synchronous writes — no tokio Blocking threads in teardown.
    {
        use std::io::Write;
        let mut out = std::io::stdout();
        let _ = out.write_all(concat!(
            "\x1b[m",        // SGR 0: reset colors/bold/underline
            "\x1b[r",        // DECSTBM: reset scroll region
            "\x1b[?1000l",   // disable mouse tracking
            "\x1b[?1006l",   // disable SGR mouse extension
            "\x1b[?2004l",   // disable bracketed paste
            "\x1b(B",        // select US-ASCII charset for G0
            "\x1b[2J\x1b[H", // clear screen + cursor home
        ).as_bytes());
        let _ = out.flush();
    }

    // Restore TUI state.
    let _ = crossterm::terminal::enable_raw_mode();
    let _ = execute!(
        terminal.backend_mut(),
        crossterm::terminal::EnterAlternateScreen,
        crossterm::cursor::Hide,
    );
    let _ = terminal.clear();
    while input_rx.try_recv().is_ok() {}
    let _ = input_suspend.send(false);

    // Pop the shell route and flash a message.
    app.pop_route();
    app.ui.flash = Some(crate::app::FlashMessage::info("Shell session ended".to_string()));
}

/// Handle an `ActionResult` that requires opening an exec session.
/// Build ExecInit from an exec template and target. Resolves placeholders
/// from the target fields. Template-driven — no hardcoded kubectl args.
fn build_exec_init(
    template: &crate::kube::protocol::ExecTemplate,
    target: &ExecTarget,
) -> (crate::kube::protocol::ExecInit, String) {
    use crate::kube::protocol::ExecArg;
    let (tw, th) = crossterm::terminal::size().unwrap_or((80, 24));
    let mut args = Vec::new();
    for arg in template.args {
        match arg {
            ExecArg::Literal(s) => args.push(s.to_string()),
            ExecArg::Placeholder(p) => {
                if let Some(val) = resolve_placeholder(p, target) {
                    args.push(val);
                }
            }
            ExecArg::ConditionalPair(lit, p) => {
                if let Some(val) = resolve_placeholder(p, target) {
                    if !val.is_empty() {
                        args.push(lit.to_string());
                        args.push(val);
                    }
                }
            }
        }
    }
    let title = match target {
        ExecTarget::Pod { pod, container, .. } => template.title_template
            .replace("{{pod}}", pod)
            .replace("{{container}}", container),
        ExecTarget::Node { node } => template.title_template
            .replace("{{node}}", node),
    };
    (crate::kube::protocol::ExecInit { kubectl_args: args, term_width: tw, term_height: th }, title)
}

/// Resolve a placeholder against the exec target. Returns `None` if the
/// placeholder doesn't apply to this target variant (e.g., `Namespace`
/// on a `Node` target) — the caller skips it instead of inserting an
/// empty string.
fn resolve_placeholder(p: &crate::kube::protocol::ExecPlaceholder, target: &ExecTarget) -> Option<String> {
    use crate::kube::protocol::ExecPlaceholder;
    match (p, target) {
        (ExecPlaceholder::Namespace, ExecTarget::Pod { namespace, .. }) => Some(namespace.clone()),
        (ExecPlaceholder::PodName, ExecTarget::Pod { pod, .. }) => Some(pod.clone()),
        (ExecPlaceholder::Container, ExecTarget::Pod { container, .. }) => Some(container.clone()),
        (ExecPlaceholder::NodeName, ExecTarget::Node { node }) => Some(format!("node/{}", node)),
        (ExecPlaceholder::NodeDebugPodName, ExecTarget::Node { node }) => {
            let sanitized: String = node.chars()
                .map(|c| if c.is_ascii_alphanumeric() || c == '-' { c.to_ascii_lowercase() } else { '-' })
                .collect();
            let name = format!("node-dbg-{}", sanitized);
            let name = if name.len() > 63 { &name[..63] } else { &name };
            Some(name.trim_end_matches('-').to_string())
        }
        // Placeholder doesn't apply to this target variant.
        _ => None,
    }
}

async fn handle_action_result(
    result: ActionResult,
    app: &mut App,
    data_source: &ClientSession,
    event_tx: &mpsc::Sender<AppEvent>,
) {
    match result {
        ActionResult::Exec { op, target } => {
            let Some(template) = op.exec_template() else {
                app.ui.flash = Some(crate::app::FlashMessage::error(
                    format!("No exec template for {:?}", op)
                ));
                return;
            };
            let (exec_init, title) = build_exec_init(template, &target);
            let label = op.descriptor().label;
            if let Err(e) = open_exec_route(app, data_source, exec_init, title, event_tx).await {
                app.ui.flash = Some(crate::app::FlashMessage::error(
                    format!("{} failed: {}", label, e)
                ));
            }
        }
        ActionResult::None => {}
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
            app.kube.core_streams.push(stream);
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
            app.kube.context_switch.mark_stable();
            // Open core resource substreams now that the connection is ready.
            // The user lands on Overview (the default route) with only the
            // mandatory subscriptions (namespaces, nodes). They navigate to
            // a resource view explicitly — no automatic pods(all) subscribe.
            open_core_subscriptions(app, data_source);
        }
        other => apply_event(app, other),
    }
}

/// Unified input mode handler. Checks all active input modes in priority
/// order and returns true if the key was consumed. This keeps the main
/// loop's key handling path linear:
///   Shell → input modes → normal key dispatch → action handler
fn try_handle_input_mode(
    app: &mut App,
    key: crossterm::event::KeyEvent,
    data_source: &mut ClientSession,
    event_tx: &mpsc::Sender<AppEvent>,
) -> bool {
    // 1. Command mode (`:` prompt)
    if handle_command_key(app, key, data_source, event_tx) {
        return true;
    }
    // 2. Form dialog (Scale, PortForward, etc.)
    if handle_form_dialog_key(app, key, data_source) {
        return true;
    }
    // 3. Resource filter input (`/` in resource view)
    if handle_filter_key(app, key, data_source) {
        return true;
    }
    // 4. Log filter input (`/` in log view)
    if handle_log_filter_key(app, key) {
        return true;
    }
    // 5. Content search input (`/` in yaml/describe view)
    if handle_content_search_key(app, key) {
        return true;
    }
    false
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
        crate::app::Route::ContentView { ref mut state, .. } if state.search_input_active => state,
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
                    state.scroll = t.saturating_sub(app.config.ui.search_context_lines);
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
        if !crate::app::nav::is_globally_stored(&crate::app::view::ViewId::Resource(new.clone())) {
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
            app.kube.selected_ns.clone(),
            change.subscription_filter.clone(),
        );
        app.nav.current_mut().stream = Some(stream);
    }
}

/// Channels for the input bridge: keypress delivery and suspend/resume.
/// Bundles the three related channels so `session_main` stays under the
/// arg limit.
#[derive(Debug)]
pub struct InputChannels {
    pub input_rx: mpsc::Receiver<CtEvent>,
    pub suspend_tx: tokio::sync::watch::Sender<bool>,
    pub suspend_ack_rx: mpsc::Receiver<()>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube::protocol::ExecPlaceholder;

    fn pod_target() -> ExecTarget {
        ExecTarget::Pod {
            pod: "my-pod".into(),
            namespace: "default".into(),
            container: "nginx".into(),
        }
    }

    fn node_target() -> ExecTarget {
        ExecTarget::Node { node: "worker-1".into() }
    }

    #[test]
    fn resolve_namespace_on_pod() {
        let result = resolve_placeholder(&ExecPlaceholder::Namespace, &pod_target());
        assert_eq!(result, Some("default".into()));
    }

    #[test]
    fn resolve_namespace_on_node_returns_none() {
        let result = resolve_placeholder(&ExecPlaceholder::Namespace, &node_target());
        assert_eq!(result, None);
    }

    #[test]
    fn resolve_node_name_on_node() {
        let result = resolve_placeholder(&ExecPlaceholder::NodeName, &node_target());
        assert_eq!(result, Some("node/worker-1".into()));
    }

    #[test]
    fn resolve_pod_name_on_node_returns_none() {
        let result = resolve_placeholder(&ExecPlaceholder::PodName, &node_target());
        assert_eq!(result, None);
    }

    #[test]
    fn resolve_container_on_pod_with_empty_container() {
        let target = ExecTarget::Pod {
            pod: "p".into(),
            namespace: "ns".into(),
            container: String::new(),
        };
        let result = resolve_placeholder(&ExecPlaceholder::Container, &target);
        assert_eq!(result, Some(String::new()), "empty container is valid — ConditionalPair skips it");
    }
}

pub async fn session_main(
    mut app: App,
    mut data_source: ClientSession,
    mut terminal: ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    mut event_tx: mpsc::Sender<AppEvent>,
    mut event_rx: mpsc::Receiver<AppEvent>,
    tick_rate: Duration,
    mut input: InputChannels,
) -> Result<Option<crate::app::ExitReason>> {
    // Track active log streaming substream so we can cancel on Back.
    // Drop = close substream = daemon kills kubectl = log stream ended.
    // Log stream is owned by Route::Logs — no separate variable needed.

    let mut tick_interval = tokio::time::interval(tick_rate);
    let mut last_tick = std::time::Instant::now();

    // Listen for SIGTERM so `pkill k9rs` triggers a clean shutdown instead
    // of killing the process without running TerminalGuard::drop. The
    // handler sets should_quit, and the normal exit path restores the
    // terminal (disable raw mode, leave alternate screen, show cursor).
    let mut sigterm = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::terminate(),
    ).expect("failed to install SIGTERM handler");

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
        let rebuild_ctx = if let Some(new_ctx) = app.kube.context_switch.take_requested() {
            Some(Some(new_ctx))
        } else if app.reconnect_requested {
            Some(if app.kube.context.is_empty() { None } else { Some(app.kube.context.clone()) })
        } else {
            None
        };
        if let Some(ctx) = rebuild_ctx {
            app.reconnect_requested = false;
            let no_daemon = data_source.is_no_daemon();
            drop(data_source);
            drop(event_rx);
            app.clear_data();
            let (new_tx, new_rx) = mpsc::channel::<AppEvent>(256);
            event_tx = new_tx;
            event_rx = new_rx;
            data_source = ClientSession::new(
                crate::kube::client_session::ConnectionParams {
                    context: ctx,
                    namespace: app.kube.selected_ns.clone(),
                    readonly: app.read_only,
                    no_daemon,
                },
                event_tx.clone(),
            );
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
                crate::app::Route::ContentView { ref state, .. } => state.search_input_active,
                crate::app::Route::Logs { ref state, .. } => state.is_filtering(),
                _ => false,
            };
            let in_input_mode = matches!(app.ui.input_mode, InputMode::Command { .. })
                || app.ui.form_dialog.as_ref().is_some_and(|d| {
                    d.fields.get(d.focused).is_some_and(|field| field.is_text_input())
                })
                || app.nav.filter_input().active()
                || route_has_text_input;
            if in_input_mode {
                execute!(terminal.backend_mut(), SetCursorStyle::SteadyBar)?;
            } else {
                execute!(terminal.backend_mut(), SetCursorStyle::SteadyBlock)?;
            }

            needs_redraw = false;
        }

        // Unified edit flow: if route is EditorReady, run the editor.
        if run_editor_flow(
            &mut app, &mut terminal, &mut data_source,
            &input.suspend_tx, &mut input.suspend_ack_rx,
        ).await {
            needs_redraw = true;
            continue;
        }

        // Shell bridge: when the daemon confirms the shell is connected,
        // suspend the TUI and enter a raw byte bridge. stdin→daemon,
        // daemon→stdout, no parsing. Restores TUI on exit.
        if let crate::app::Route::Shell(ref shell) = app.route {
            if shell.connect_state == crate::app::ShellConnectState::Connected {
                run_shell_bridge(
                    &mut app, &mut terminal, &mut event_rx,
                    &input.suspend_tx, &mut input.suspend_ack_rx,
                    &mut input.input_rx,
                ).await;
                needs_redraw = true;
                continue;
            }
        }

        tokio::select! {
            // biased: always check key events first so user input (like disabling
            // autoscroll) takes effect before processing queued data events.
            biased;

            Some(ct_event) = input.input_rx.recv() => {
                match ct_event {
                    CtEvent::Key(key) => {
                        // Input modes get priority over normal key handling.
                        // Each mode handler returns true if the key was consumed.
                        if try_handle_input_mode(
                            &mut app, key, &mut data_source, &event_tx,
                        ) {
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
                                app.ui.flash = Some(crate::app::FlashMessage::info("Read-only mode".to_string()));
                                continue;
                            }
                            let result = handle_action(
                                &mut app,
                                action,
                                &event_tx,
                                &mut data_source,
                            );
                            handle_action_result(
                                result, &mut app, &data_source, &event_tx,
                            ).await;
                        }
                    }
                    // Pure-keyboard TUI by design — mouse events are
                    // intentionally dropped (see ~/.config/claude memory).
                    CtEvent::Resize(_w, _h) => {
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
                // Gap detection for log initial load: if we drained all
                // pending events and the log view is still in initial_load,
                // the initial tail batch is complete — snap to bottom.
                if let crate::app::Route::Logs { ref mut state, .. } = app.route {
                    if state.initial_load && !state.lines().is_empty() {
                        state.initial_load = false;
                    }
                }
                needs_redraw = true;
            }
            _ = tick_interval.tick() => {
                if app.tick() {
                    needs_redraw = true;
                }
            }
            _ = sigterm.recv() => {
                app.exit_reason = Some(crate::app::ExitReason::UserQuit);
                app.should_quit = true;
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
