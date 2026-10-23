//! The TUI's session layer: the event loop, the per-key action dispatch and
//! the command bar.
//!
//! These files lived under `kube/` and are not Kubernetes access — they are
//! written against `&mut App` and `session::mod` calls `ui::draw` outright.
//! That misfiling was the whole of the `kube -> app` cycle: 306 of its 330
//! references were here.
//!
//! What is left here all needs the runtime: a tokio task, the client socket,
//! a pty, a spawned editor. The apply path for daemon events was here too and
//! needed none of them, so it went to [`k9rs_app::event`] — see that
//! module's doc for why.

pub mod actions;
pub mod commands;

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

use k9rs_app::App;
use k9rs_app::event::{apply_event, AppEvent};
use k9rs_app::client::ClientSession;
use k9rs_kube::repaint::Repaint;

struct RawFd(std::os::unix::io::RawFd);
impl AsRawFd for RawFd {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd { self.0 }
}

use crate::actions::handle_action;
use crate::commands::{
    run_interactive_local,
    handle_command_key, handle_form_dialog_key, handle_filter_key,
};

/// Surface a data_source operation error as a flash message.
macro_rules! ds_try {
    ($app:expr, $expr:expr) => {
        if let Err(e) = $expr {
            $app.ui.flash = Some(k9rs_app::FlashMessage::error(format!("Connection error: {}", e)));
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

/// Capacity of the App event channel. Used for both the initial channel
/// (in `main`) and every reconnect/context-switch rebuild, so
/// buffer-dependent behavior (backpressure onset, drain batching) is
/// identical before and after the first reconnect.
pub const EVENT_CHANNEL_CAP: usize = 500;
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
        op: k9rs_proto::protocol::OperationKind,
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

/// Open an exec session as the Shell OVERLAY. The daemon spawns kubectl
/// in a PTY and proxies bytes over yamux. During the Connecting phase the
/// TUI shows a loading bar over the current view (which is never
/// displaced); once the first ExecData arrives (Connected), the session
/// loop suspends the TUI and enters a raw byte bridge — stdin→daemon,
/// daemon→stdout — with 100% terminal fidelity and no vt100 parsing.
pub(crate) async fn open_exec_overlay(
    app: &mut App,
    data_source: &ClientSession,
    exec_init: k9rs_proto::protocol::ExecInit,
    title: String,
    event_tx: &tokio::sync::mpsc::Sender<k9rs_app::event::AppEvent>,
) -> Result<()> {
    let stream = data_source.open_exec_stream(exec_init.clone(), event_tx.clone()).await?;

    // ONE call: a modal is one field, so opening or clearing it displaces
    // whatever was there (see `Modal`). This was written twice — a fossil of
    // the four-slot era, when every opener had to clear the other three.
    app.ui.close_modal();
    app.ui.open(k9rs_app::Modal::Takeover(k9rs_app::TakeoverOp::Shell(Box::new(k9rs_app::ShellState {
        title,
        stream: Some(stream),
        connect_state: k9rs_app::ShellConnectState::Connecting,
        pending_output: Vec::new(),
    }))));
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

/// The signals that mean "shut down", as ONE future.
///
/// Two `select!` arms doing the same thing is how one of them ends up with a
/// different body; the shutdown decision is the same whichever signal
/// arrives, so it is written once. See the installation site for why SIGINT
/// is here and SIGHUP is not.
struct ShutdownSignal {
    term: tokio::signal::unix::Signal,
    int: tokio::signal::unix::Signal,
}

impl ShutdownSignal {
    /// Resolves when either signal arrives.
    async fn recv(&mut self) {
        tokio::select! {
            _ = self.term.recv() => {}
            _ = self.int.recv() => {}
        }
    }
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
    /// Drained on restore to discard stale terminal responses provoked by the
    /// alt-screen / raw-mode transition (see [`drain_stale_input`]).
    input_rx: &'a mut mpsc::Receiver<CtEvent>,
    /// Set once [`SuspendGuard::restore`] has run; makes the `Drop` fallback a no-op.
    restored: bool,
}

impl<'a, B: ratatui::backend::Backend + std::io::Write> SuspendGuard<'a, B> {
    /// Suspend the TUI: stop the input bridge, leave alternate screen.
    /// Returns a guard that resumes on `restore()` (or `Drop` as a fallback).
    pub(crate) async fn new(
        terminal: &'a mut ratatui::Terminal<B>,
        input_suspend: &'a tokio::sync::watch::Sender<bool>,
        input_suspend_ack: &mut mpsc::Receiver<()>,
        input_rx: &'a mut mpsc::Receiver<CtEvent>,
    ) -> Result<Self> {
        // Drain any STALE ack first: if a previous suspend timed out (bridge
        // wedged >1s) and the bridge acked late, that ack sits in the cap-1
        // channel and would satisfy THIS wait instantly — proceeding while
        // the bridge is still reading stdin, racing keystrokes between the
        // subprocess and the TUI. An empty channel makes the recv below wait
        // for an ack to *this* request.
        while input_suspend_ack.try_recv().is_ok() {}
        let _ = input_suspend.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(1), input_suspend_ack.recv()).await;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        // RAW MODE IS PART OF THE SUSPENSION, so the guard owns it. It used
        // to be two bare calls around the subprocess in
        // `run_interactive_local`: `disable_raw_mode()` after `new`, and
        // `enable_raw_mode()?` before `restore`. A failure at the second one
        // returned through `?`, `Drop` put the alternate screen back, and the
        // TUI carried on painting in COOKED mode — every keystroke
        // line-buffered and echoed, nothing responding until Enter, and no
        // key that could fix it. The guard's doc claimed it made a suspended
        // state impossible to leave behind; that was true of the screen and
        // false of the mode, and the mode is the half the user notices.
        let _ = crossterm::terminal::disable_raw_mode();
        Ok(Self { terminal, input_suspend, input_rx, restored: false })
    }

    /// Access the terminal (e.g., to disable raw mode for subprocess shells).
    pub(crate) fn terminal_mut(&mut self) -> &mut ratatui::Terminal<B> {
        self.terminal
    }

    /// Re-enter the alternate screen and resume the input bridge. Shared by
    /// the async `restore` (normal exit) and `Drop` (panic / early-return
    /// fallback).
    fn restore_screen(&mut self) {
        // Raw mode first: it is what makes the restored screen usable, and
        // it must be re-entered on EVERY exit — `restore()` and `Drop` alike.
        let _ = crossterm::terminal::enable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), crossterm::terminal::EnterAlternateScreen);
        let _ = self.terminal.clear();
        let _ = self.input_suspend.send(false);
    }

    /// Normal exit path: restore the screen, then drain the stale terminal
    /// responses the transition provokes over a short settling window. They
    /// arrive a few ms after the bridge resumes and re-reads stdin, so a
    /// single synchronous drain races them. Real type-ahead caught in the
    /// window is returned for the session loop to replay. Consuming `self`
    /// runs `Drop` immediately afterwards, but the `restored` flag makes it
    /// a no-op.
    pub(crate) async fn restore(mut self) -> Vec<CtEvent> {
        self.restored = true;
        self.restore_screen();
        drain_stale_input(self.input_rx).await
    }
}

impl<B: ratatui::backend::Backend + std::io::Write> Drop for SuspendGuard<'_, B> {
    fn drop(&mut self) {
        if self.restored {
            return; // `restore()` already restored the screen and drained.
        }
        // Reached only on an early return / error / panic before `restore()`.
        // `Drop` can't await, so the settling drain is unavailable — do a
        // best-effort one-shot drain. The deterministic protection against a
        // surviving stray key is the non-sticky confirm dialog (handler.rs).
        self.restore_screen();
        while self.input_rx.try_recv().is_ok() {}
    }
}

/// Drain stale terminal input provoked by an alt-screen / raw-mode transition.
/// When the TUI suspends for a subprocess (editor) or the shell bridge, the
/// terminal emits response sequences (cursor-position / device-status reports)
/// in reply to the mode changes; crossterm shreds the ones it can't parse
/// whole into key events, and a stray 'r' would pop the restart dialog. The
/// responses can land a few milliseconds after the screen is restored — the
/// input bridge has to wake and re-read stdin first — so one synchronous drain
/// races them; settle over a few short passes instead.
///
/// CONTENT-aware, not a blanket drop: only events matching the shredded-
/// response shape (see `is_resume_debris`) are discarded. Real type-ahead
/// keyed during the ~45ms window is KEPT — returned in arrival order for the
/// caller to replay — honoring the input arm's "keys are never dropped"
/// contract, which the old drain-everything version quietly violated.
async fn drain_stale_input(input_rx: &mut mpsc::Receiver<CtEvent>) -> Vec<CtEvent> {
    /// Longest the window can run. Reached only while the terminal is still
    /// talking; a quiet one exits after the first couple of passes.
    const MAX_PASSES: usize = 8;
    const PASS: Duration = Duration::from_millis(15);

    let mut kept = Vec::new();
    let mut state = DebrisState::Idle;
    let mut quiet = 0;
    for pass in 0..MAX_PASSES {
        let mut saw_any = false;
        while let Ok(ev) = input_rx.try_recv() {
            saw_any = true;
            state = debris_step(state, ev, &mut kept);
        }
        quiet = if saw_any { 0 } else { quiet + 1 };
        // Stop once the terminal has gone quiet AND we are not holding half a
        // sequence. Cutting the window mid-response is what let the tail of a
        // reply through as keystrokes; waiting out the full window every time
        // would delay every resume for the sake of the slow case.
        if quiet >= 2 && matches!(state, DebrisState::Idle) {
            break;
        }
        if pass + 1 < MAX_PASSES {
            tokio::time::sleep(PASS).await;
        }
    }
    // A trailing unconfirmed Esc is a REAL keypress: debris Esc is always
    // followed by its sequence body well inside the settling window.
    if let DebrisState::PendingEsc(esc) = state {
        kept.push(esc);
    }
    kept
}

/// Shred-shape state for the settling window, carried across the drain's
/// passes (one response can straddle two stdin reads).
enum DebrisState {
    Idle,
    /// Saw a bare Esc — debris only if a sequence introducer follows; the
    /// event is held, not judged, until the next event decides.
    PendingEsc(CtEvent),
    /// Inside a confirmed `Esc [` / `Esc O` shred — body until a final byte.
    InSeq,
    /// Inside an `Esc ]` (OSC) / `Esc P` (DCS) / `Esc ^` (PM) / `Esc _` (APC)
    /// STRING. Unlike CSI/SS3 these do not end at a final letter — they run to
    /// a `BEL` or an `ST`, and their bodies contain arbitrary text. `consumed`
    /// bounds them so a malformed one cannot swallow the rest of the window.
    InString { consumed: usize },
    /// Saw an `Esc` inside a string: `\` completes the `ST`, anything else
    /// means the string was malformed and the event is judged afresh.
    StringEsc,
}

/// Body characters tolerated inside an unterminated OSC/DCS string before the
/// sieve gives up and starts keeping events again. An OSC 11 colour reply is
/// ~25; real typing never reaches this inside one settling window.
const STRING_BODY_CAP: usize = 128;

/// The four introducers that begin a STRING rather than a control sequence.
fn is_string_introducer(c: char) -> bool {
    matches!(c, ']' | 'P' | '^' | '_')
}

/// One classifier step. A response sequence crossterm can't parse whole
/// shreds into an introducer and a body, in one of two grammars:
///
/// - **Control sequences** — `Esc [` (CSI) / `Esc O` (SS3) — run until a final
///   letter: the `R` of a cursor report, the `c` of a DA1 reply, or the
///   spurious `r` that used to pop the restart dialog.
/// - **Strings** — `Esc ]` (OSC) / `Esc P` (DCS) / `Esc ^` / `Esc _` — have
///   arbitrary bodies and run until a `BEL` or an `ST`.
///
/// Real type-ahead never takes either full shape: crossterm parses KNOWN
/// sequences (arrows, F-keys) into proper key events, plain typing arrives
/// as ordinary chars, and a real Esc press is followed by something other
/// than an introducer — so the held Esc is released as real the moment the
/// shape breaks.
///
/// The strings arm exists because this comment used to say the opposite —
/// that OSC/DCS replies were "never provoked by our transitions" and could
/// therefore be left to leak. That was true of OUR transitions and false of
/// the subprocess's: editors query the terminal's background color on startup,
/// and a reply landing after the editor exits is ours to deal with. A claim
/// about what the world does is not a fact about the code.
///
/// The residual trade is now only timing: a reply arriving after the settling
/// window closes still leaks. `TextKey` bounds the damage (a leaked Ctrl chord
/// can no longer type), but a leaked `:` still opens the command bar.
fn debris_step(state: DebrisState, ev: CtEvent, kept: &mut Vec<CtEvent>) -> DebrisState {
    use crossterm::event::{KeyCode, KeyModifiers};
    let (code, mods) = match &ev {
        CtEvent::Key(k) => (Some(k.code), k.modifiers),
        // Focus/resize churn from the mode transition — never replayed
        // (the resume paint re-observes the real size), never state-changing.
        _ => (None, KeyModifiers::NONE),
    };
    // crossterm folds `Esc <char>` into ONE Alt-modified event when both bytes
    // land in the same read, and yields them separately when they don't — so
    // an introducer can arrive either way and both shapes have to be caught.
    let alt_char = |c: char| {
        matches!(code, Some(KeyCode::Char(got)) if got == c) && mods.contains(KeyModifiers::ALT)
    };
    match state {
        DebrisState::Idle => match code {
            Some(KeyCode::Esc) => DebrisState::PendingEsc(ev),
            Some(KeyCode::Char(c))
                if mods.contains(KeyModifiers::ALT) && is_string_introducer(c) =>
            {
                DebrisState::InString { consumed: 0 }
            }
            None => DebrisState::Idle,
            Some(_) => {
                kept.push(ev);
                DebrisState::Idle
            }
        },
        DebrisState::PendingEsc(esc) => match code {
            // Introducer confirms the shred: drop the Esc and consume on.
            Some(KeyCode::Char('[' | 'O')) => DebrisState::InSeq,
            Some(KeyCode::Char(c)) if is_string_introducer(c) => {
                DebrisState::InString { consumed: 0 }
            }
            // Anything else: the Esc was a real keypress after all.
            _ => {
                kept.push(esc);
                debris_step(DebrisState::Idle, ev, kept)
            }
        },
        DebrisState::InString { consumed } => {
            // `BEL` terminates an OSC, and crossterm delivers 0x07 as Ctrl-G.
            let bel = matches!(code, Some(KeyCode::Char('g')))
                && mods.contains(KeyModifiers::CONTROL);
            if bel || alt_char('\\') {
                return DebrisState::Idle;
            }
            match code {
                Some(KeyCode::Esc) => DebrisState::StringEsc,
                _ if consumed >= STRING_BODY_CAP => debris_step(DebrisState::Idle, ev, kept),
                // Anything else is body text — OSC payloads are arbitrary
                // (`11;rgb:2f2f/3434/3f3f`), so there is nothing to validate.
                _ => DebrisState::InString { consumed: consumed + 1 },
            }
        }
        DebrisState::StringEsc => match code {
            Some(KeyCode::Char('\\')) => DebrisState::Idle,
            _ => debris_step(DebrisState::Idle, ev, kept),
        },
        DebrisState::InSeq => match code {
            Some(KeyCode::Char(c))
                if c.is_ascii_digit() || matches!(c, ';' | '?' | ':' | '<' | '=' | '>') =>
            {
                DebrisState::InSeq
            }
            Some(KeyCode::Char(c)) if c.is_ascii_alphabetic() || c == '~' => {
                DebrisState::Idle // final byte — sequence complete
            }
            None => DebrisState::InSeq,
            // Malformed tail — stop consuming, judge this event afresh.
            _ => debris_step(DebrisState::Idle, ev, kept),
        },
    }
}

/// Outcome of handling one key inside the input-drain loop.
enum KeyDrain {
    /// Keep draining any further keys batched into this turn.
    Continue,
    /// A terminal-suspending (`Exec`) or quit transition happened — stop
    /// draining so the outer loop's editor/shell gates run and it repaints.
    Stop,
}

/// Handle exactly ONE key through the full mode → gate → readonly → action
/// pipeline (the path the input arm used to inline). Painting is the caller's
/// job — done ONCE after the whole batch drains, so an SSH-batched burst of
/// keystrokes no longer costs one blocking remote repaint apiece. Returns
/// whether the drain loop should keep going.
async fn handle_one_key(
    app: &mut App,
    key: crossterm::event::KeyEvent,
    data_source: &mut ClientSession,
    event_tx: &mpsc::Sender<AppEvent>,
    repaint: &mut k9rs_kube::repaint::Repaint,
) -> KeyDrain {
    // Terminals that report key RELEASES (kitty keyboard protocol, Windows
    // console) would double-fire every binding without this guard. Repeats
    // pass — holding `j` must keep scrolling.
    if key.kind == crossterm::event::KeyEventKind::Release {
        return KeyDrain::Continue;
    }
    // Input modes get priority over normal key handling.
    if try_handle_input_mode(app, key, data_source, event_tx) {
        repaint.on_input();
        return KeyDrain::Continue;
    }

    repaint.on_input();
    let Some(action) = k9rs_app::handler::handle_key_event(app, key) else {
        return KeyDrain::Continue;
    };
    // Select-mode gate: THE single choke point — every key→Action route
    // funnels through here, transforming batch-capable ops into their Batch*
    // variants and killing single-target actions while marks are active. Runs
    // BEFORE the readonly check so readonly sees the batch variant.
    let action = match k9rs_app::select_gate::gate_action(app, action) {
        k9rs_app::select_gate::Gated::Pass(a) => a,
        k9rs_app::select_gate::Gated::Blocked(msg) => {
            app.ui.flash = Some(k9rs_app::FlashMessage::warn(msg.to_string()));
            return KeyDrain::Continue;
        }
    };
    // Client-side UX shortcut: flash "Read-only mode" without a wire round-
    // trip. The server is the real boundary and rejects mutations itself.
    if app.read_only && action.is_mutating() {
        app.ui.flash = Some(k9rs_app::FlashMessage::info("Read-only mode".to_string()));
        return KeyDrain::Continue;
    }
    let result = handle_action(app, action, event_tx, data_source);
    let suspends = matches!(result, ActionResult::Exec { .. });
    handle_action_result(result, app, data_source, event_tx).await;
    if suspends || app.should_quit {
        KeyDrain::Stop
    } else {
        KeyDrain::Continue
    }
}


/// Run the editor flow when the route is `EditState::EditorReady`.
/// Returns `true` if the editor was launched (the caller should `continue`
/// the main loop regardless of outcome — success, abort, or error).
#[allow(clippy::too_many_arguments)]
async fn run_editor_flow(
    app: &mut App,
    event_rx: &mut mpsc::Receiver<AppEvent>,
    terminal: &mut ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    data_source: &mut ClientSession,
    input_suspend: &tokio::sync::watch::Sender<bool>,
    input_suspend_ack: &mut mpsc::Receiver<()>,
    input_rx: &mut mpsc::Receiver<CtEvent>,
    replay: &mut std::collections::VecDeque<CtEvent>,
) -> bool {
    let is_editor_ready = matches!(
        app.ui.takeover(),
        Some(k9rs_app::TakeoverOp::Edit {
            state: k9rs_app::EditState::EditorReady { .. }, ..
        })
    );
    if !is_editor_ready {
        return false;
    }

    // Take the overlay out so we own TempFile (move, not borrow).
    let (target, temp_file, original) = match app.ui.take_modal().into_takeover() {
        Some(k9rs_app::TakeoverOp::Edit {
            target,
            state: k9rs_app::EditState::EditorReady { temp_file, original },
        }) => (target, temp_file, original),
        _ => unreachable!("gated by is_editor_ready"),
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
        app, event_rx,
        terminal, &editor, &args,
        crate::commands::InteractiveKind::Editor,
        input_suspend, input_suspend_ack, input_rx, replay,
    ).await {
        Ok(s) => s,
        Err(e) => {
            app.ui.flash = Some(k9rs_app::FlashMessage::error(
                format!("Editor failed: {}", e)
            ));
            return true;
        }
    };

    // Editor non-zero exit (vim `:cq`) -> abort. TempFile drops.
    if exit_status.is_some_and(|s| !s.success()) {
        app.ui.flash = Some(k9rs_app::FlashMessage::info(
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
        app.ui.flash = Some(k9rs_app::FlashMessage::info(
            "Edit cancelled (file not saved).".to_string()
        ));
        return true;
    }

    // Read the edited file back.
    let edited = match std::fs::read_to_string(temp_file.path()) {
        Ok(s) => s,
        Err(e) => {
            app.ui.flash = Some(k9rs_app::FlashMessage::error(
                format!("Failed to read edited file: {}", e)
            ));
            return true;
        }
    };

    let stripped = strip_leading_comments(&edited);

    // Unchanged from original → abort (first edit, no changes made).
    if stripped.trim() == original.trim() {
        app.ui.flash = Some(k9rs_app::FlashMessage::info(
            "Edit cancelled, no changes made.".to_string()
        ));
        return true;
    }

    // Empty -> abort.
    if stripped.trim().is_empty() {
        app.ui.flash = Some(k9rs_app::FlashMessage::info(
            "Edit cancelled, file is empty.".to_string()
        ));
        return true;
    }

    // Send Apply. Move TempFile to the Applying state.
    let target_clone = target.clone();
    app.ui.open(k9rs_app::Modal::Takeover(k9rs_app::TakeoverOp::Edit {
        target,
        state: k9rs_app::EditState::Applying {
            temp_file,
            original,
        },
    }));
    if let Err(e) = data_source.apply(&target_clone, stripped.to_string()) {
        app.ui.flash = Some(k9rs_app::FlashMessage::error(
            format!("Apply failed: {}", e)
        ));
        app.ui.close_modal();
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
    replay: &mut std::collections::VecDeque<CtEvent>,
) {
    use tokio::io::AsyncWriteExt;

    // Extract the exec stream and pending output from the Shell overlay.
    let (mut stream, pending) = match app.ui.takeover_mut() {
        Some(k9rs_app::TakeoverOp::Shell(ref mut shell)) => {
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
    let _ = k9rs_proto::protocol::write_bincode(
        writer,
        &k9rs_proto::protocol::ExecFrame::Resize { width: tw, height: th },
    ).await;

    // Suspend TUI: stop the input bridge (releases stdin), leave alternate
    // screen. Raw mode stays on — we need raw byte passthrough. Stale-ack
    // drain first, same rationale as `SuspendGuard::new`.
    while input_suspend_ack.try_recv().is_ok() {}
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
                        let frame = k9rs_proto::protocol::ExecFrame::Data(buf[..n].to_vec());
                        if k9rs_proto::protocol::write_bincode(writer, &frame).await.is_err() {
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
                    // Everything else (Store deltas, log lines, OpResults,
                    // and critically DaemonDisconnected) must still be
                    // APPLIED, not dropped — this bridge owns event_rx for
                    // the whole shell session, so swallowing here would
                    // leave stores diverged (missed deltas are never
                    // re-sent) and could eat a disconnect, stranding the
                    // TUI in a no-reconnect limbo. apply_event touches only
                    // app state and stores (no terminal I/O) and runs on
                    // this same task; ExecData/ExecEnded are already peeled
                    // off above.
                    Some(other) => apply_event(app, other),
                }
            }
            _ = sigwinch.recv() => {
                if let Ok((w, h)) = crossterm::terminal::size() {
                    let frame = k9rs_proto::protocol::ExecFrame::Resize { width: w, height: h };
                    let _ = k9rs_proto::protocol::write_bincode(writer, &frame).await;
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
    // Resume the input bridge first, then drain the stale terminal responses
    // the transition provokes — they arrive *after* resume, once the bridge
    // re-reads stdin, so the old pre-resume one-shot drain caught nothing.
    // Real type-ahead caught in the window comes back for the loop to replay.
    let _ = input_suspend.send(false);
    replay.extend(drain_stale_input(input_rx).await);

    // Clear the shell overlay (the view underneath was never displaced).
    app.ui.close_modal();
    app.ui.flash = Some(k9rs_app::FlashMessage::info("Shell session ended".to_string()));
}

/// Handle an `ActionResult` that requires opening an exec session.
/// Build ExecInit from an exec template and target. Resolves placeholders
/// from the target fields. Template-driven — no hardcoded kubectl args.
fn build_exec_init(
    template: &k9rs_proto::protocol::ExecTemplate,
    target: &ExecTarget,
) -> (k9rs_proto::protocol::ExecInit, String) {
    use k9rs_proto::protocol::ExecArg;
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
    (k9rs_proto::protocol::ExecInit { kubectl_args: args, term_width: tw, term_height: th }, title)
}

/// Resolve a placeholder against the exec target. Returns `None` if the
/// placeholder doesn't apply to this target variant (e.g., `Namespace`
/// on a `Node` target) — the caller skips it instead of inserting an
/// empty string.
fn resolve_placeholder(p: &k9rs_proto::protocol::ExecPlaceholder, target: &ExecTarget) -> Option<String> {
    use k9rs_proto::protocol::ExecPlaceholder;
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
                app.ui.flash = Some(k9rs_app::FlashMessage::error(
                    format!("No exec template for {:?}", op)
                ));
                return;
            };
            let (exec_init, title) = build_exec_init(template, &target);
            let label = op.descriptor().label;
            if let Err(e) = open_exec_overlay(app, data_source, exec_init, title, event_tx).await {
                app.ui.flash = Some(k9rs_app::FlashMessage::error(
                    format!("{} failed: {}", label, e)
                ));
            }
        }
        ActionResult::None => {}
    }
}

fn dispatch_app_event(app: &mut App, data_source: &mut ClientSession, event: AppEvent) {
    match event {
        event @ AppEvent::ConnectionEstablished { .. } => {
            // Was a context SWITCH resolving here (vs a plain reconnect)? Read
            // it BEFORE `settle()` clears the state. ONLY `InFlight` counts:
            // `Requested` means the switch's session hasn't been BUILT yet
            // (the main loop takes it at the top of its next turn), so an
            // establishment arriving in that state belongs to a prior attempt
            // — treating it as the switch landing would destroy the user's
            // nav stack over a coincidental reconnect, and settling would
            // silently discard their queued switch. (`target()` deliberately
            // spans both states; this edge needs the narrower question.)
            // A switch that reaches this arm IN FLIGHT succeeded: drop the
            // OLD context's core data and namespace selection now that the
            // new connection is confirmed. (The nav stack was already reset
            // to home by `begin_context_switch` — a switch is a root-level
            // change and shouldn't leave you staring at the old cluster's
            // view while the new one connects. Only the CHROME teardown
            // waits, because clearing it early would blank data the fallback
            // still needs if the switch fails.) Done BEFORE `apply_event` so
            // `switch_context` doesn't wipe the namespaces the event would
            // seed. (`Ready.namespaces` is currently always empty — the seed
            // is a live path only if the daemon starts populating it.) A reconnect (was_switch == false, incl. the recovered
            // fallback after a FAILED switch, which already settled to
            // Stable) leaves core in place and revives streams below.
            let switch_target = match &app.kube.context_switch {
                k9rs_app::ContextSwitchState::InFlight(t) => Some(t.clone()),
                _ => None,
            };
            let was_switch = switch_target.is_some();
            if let Some(target) = switch_target {
                // `app.kube.context` is still the OUTGOING context here —
                // `apply_event` below overwrites it — so this is the one
                // moment both names are in hand.
                app.core.switch_context(app.kube.context.clone(), &target);
                app.kube.selected_ns = k9rs_proto::protocol::Namespace::All;
            }
            // Apply the event (populates context/cluster/user, seeds the new
            // namespaces, flips `link` → Live).
            apply_event(app, event);
            // Resolve the switch: InFlight → Stable (the success edge). A
            // subsequent `begin_context_switch` is now allowed. Gated on
            // `was_switch` so a queued-but-not-yet-taken `Requested` can
            // never be settled away by an unrelated establishment.
            if was_switch {
                app.kube.context_switch.settle();
            }
            // Open the always-on core subscriptions (namespaces, nodes) into
            // the app-level core stores now that the connection is ready.
            app.core.open_streams(data_source);
            // Revive the subscription feeding the TOP element's data if its
            // bridge died during the rebuild (reconnect, or the recovered
            // fallback after a failed switch) — re-subscribed with its OWN
            // stored spec; its next Baseline replaces rows in place. On a fresh
            // switch the top is now Overview (no stream), so this is a no-op.
            app.nav.ensure_top_live(data_source);
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
    // A Ctrl/Alt chord is never text. Guarded ONCE, here, by building the
    // `TextKey` the sinks below require — see [`k9rs_app::TextKey`] for why
    // this is a type rather than five copies of an `if`.
    let Some(key) = k9rs_app::TextKey::new(key) else {
        return false;
    };
    // 1. Command mode (`:` prompt)
    if handle_command_key(app, key, data_source, event_tx) {
        return true;
    }
    // 2. Form dialog (Scale, PortForward, etc.)
    if handle_form_dialog_key(app, key, data_source) {
        return true;
    }
    // 3. Resource filter input (`/` in resource view)
    if handle_filter_key(app, key) {
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

    // Fall-through guard, for a modal that none of the sinks above owns AND
    // that `handler::handle_key_event` has no branch for. Such a modal would
    // capture NOTHING: keys sail past and keep driving the table underneath
    // it, while Esc pops the nav stack out from under the thing on screen.
    //
    // This used to be a blanket `is_modal()`, which is true for every modal
    // — including the ones `handle_key_event` handles perfectly well. It
    // therefore swallowed their keys BEFORE their own handler could run, and
    // help, the container picker, a delete confirmation and a hanging shell
    // connect all lost their Esc. The predicate has to name who handles
    // what, and be exhaustive so the next modal cannot skip the question.
    app.ui.modal_without_key_handler()
}

/// Capture a keystroke into the log filter input draft (`/`-prompt within
/// the logs view). Returns `true` if the key was consumed. Mirrors the
/// shape of `handle_command_key` / `handle_form_dialog_key` /
/// `handle_filter_key` — each input modality has its own free function so
/// the main loop just calls them in priority order.
fn handle_log_filter_key(app: &mut App, key: k9rs_app::TextKey) -> bool {
    if !app.nav.top().log_view().is_some_and(|v| v.is_filtering()) {
        return false;
    }
    match key.code() {
        KeyCode::Esc => {
            if let Some(view) = app.nav.top_mut().log_view_mut() {
                view.draft = None;
            }
        }
        KeyCode::Enter => {
            // Commit = one LogFilter element narrowing the top's line
            // output; Esc later pops filters one at a time.
            let draft = app
                .nav
                .top_mut()
                .log_view_mut()
                .and_then(|v| v.draft.take())
                .map(|mut d| d.take())
                .filter(|t| !t.is_empty());
            if let Some(text) = draft {
                // `!pat` excludes — the sense comes off the front before the
                // pattern is compiled, so the vim translator never sees it.
                let (sense, pattern) = k9rs_app::element::sense_of(&text);
                if !pattern.is_empty() {
                    if let Ok(el) = k9rs_app::element::Element::derive_log_filter(
                        app.nav.top(),
                        k9rs_app::nav::CompiledGrep::new(pattern),
                        sense,
                    ) {
                        app.nav.push(el);
                    }
                }
            }
        }
        // Editing goes through the ONE text input, so the caret works here
        // exactly as it does in the command prompt and the table filter.
        code => {
            if let Some(Some(d)) = app.nav.top_mut().log_view_mut().map(|v| v.draft.as_mut()) {
                match code {
                    KeyCode::Backspace => d.backspace(),
                    KeyCode::Delete => d.delete(),
                    KeyCode::Left => d.left(),
                    KeyCode::Right => d.right(),
                    KeyCode::Home => d.home(),
                    KeyCode::End => d.end(),
                    KeyCode::Char(c) => d.insert(c),
                    _ => {}
                }
            }
        }
    }
    true
}

/// Capture a keystroke into the YAML / Describe / Aliases search input.
/// Both content-view routes share the same `ContentViewState`, so the
/// match arms diverge only on which route variant we're inside —
/// extracted so the main loop doesn't carry the same 20 lines twice.
fn handle_content_search_key(app: &mut App, key: k9rs_app::TextKey) -> bool {
    use k9rs_app::SearchInputResult;
    let search_context_lines = app.config.ui.search_context_lines;
    let state = match app.nav.top_mut() {
        k9rs_app::element::Element::ContentView(cv) if cv.state.search_input_active => {
            &mut cv.state
        }
        _ => return false,
    };
    match k9rs_app::handle_search_key(&mut state.search_input, key) {
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
                    state.viewport.scroll_to(t.saturating_sub(search_context_lines));
                }
            }
            true
        }
        SearchInputResult::Updated => true,
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


pub async fn session_main(
    mut app: App,
    mut data_source: ClientSession,
    mut terminal: ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    mut event_tx: mpsc::Sender<AppEvent>,
    mut event_rx: mpsc::Receiver<AppEvent>,
    tick_rate: Duration,
    mut input: InputChannels,
) -> Result<Option<k9rs_app::ExitReason>> {
    // Track active log streaming substream so we can cancel on Back.
    // Drop = close substream = daemon kills kubectl = log stream ended.
    // Log stream is owned by Route::Logs — no separate variable needed.

    let mut tick_interval = tokio::time::interval(tick_rate);
    let mut last_tick = std::time::Instant::now();

    // Listen for SIGTERM *and* SIGINT so `pkill k9rs` / `kill -INT` trigger a
    // clean shutdown instead of killing the process without running
    // TerminalGuard::drop. The handler sets should_quit, and the normal exit
    // path restores the terminal (disable raw mode, leave alternate screen,
    // show cursor).
    //
    // SIGINT matters because Ctrl-C is NOT a signal here: raw mode clears
    // `ISIG`, so Ctrl-C arrives as byte 0x03 and is handled as a KEY (see
    // `handler::handle_global_keys`). Every way out of the TUI therefore rode
    // the key path, and when that path is not being serviced there was no
    // escape hatch at all — the daemon installed both signals, the client
    // only one. This arm does not make terminal Ctrl-C work while the key
    // path is wedged (nothing can: the terminal never sends a signal), but it
    // makes `kill -INT` exit gracefully from another shell, with the terminal
    // restored.
    //
    // SIGHUP is deliberately left at its default (terminate): it means the
    // terminal is already gone, so there is nothing left to restore, and
    // trying to redraw into a hung-up pty only turns a clean death into an
    // error exit.
    let mut shutdown_signal = {
        use tokio::signal::unix::{signal, SignalKind};
        let term = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        let int = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        ShutdownSignal { term, int }
    };

    // Main event loop. Input paints immediately; data-driven paints
    // coalesce to the frame budget (`tick_rate`) so a busy cluster's stream
    // of updates collapses into ≤1 frame per budget instead of
    // one-paint-per-event — the difference between a trickle and a firehose
    // of terminal bytes over SSH. See `Repaint`.
    let mut repaint = Repaint::Now; // draw the first frame immediately
    // Animation clock: the loading spinner's own cadence, independent of the
    // data-repaint budget above. `Some(deadline)` while a spinner is on screen
    // (re-armed after each paint that drew one); `None` when nothing animates.
    let mut next_anim: Option<std::time::Instant> = None;
    // Real type-ahead the suspend-resume settling drain kept instead of
    // dropping (see `drain_stale_input`). Processed at the top of the loop,
    // AFTER the editor/shell gates, through the exact same per-key pipeline
    // as live input — order preserved, keys never dropped.
    let mut replay: std::collections::VecDeque<CtEvent> = std::collections::VecDeque::new();
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
        // `Requested(name)` to `InFlight(name)`, so a second
        // `begin_context_switch` call fired between this line and `settle()`
        // (on `ConnectionEstablished` success or `ConnectionFailed` for the
        // target) will be rejected by the `is_stable()` check.
        // A reconnect may be BACKED OFF (a prior reconnect attempt failed);
        // defer the rebuild until its deadline so a down daemon isn't
        // hammered. Context switches are user-initiated and always run now.
        let reconnect_due = app.conn.rebuild_due(std::time::Instant::now());
        let rebuild_ctx = if let Some(new_ctx) = app.kube.context_switch.take_requested() {
            // A fresh switch request (Requested → InFlight): connect to it.
            Some(Some(new_ctx))
        } else if reconnect_due {
            // A reconnect fired. If a switch is STILL in flight (InFlight — its
            // target was already taken above on a prior turn), keep aiming at
            // the SWITCH target, not the previous context: a mid-switch daemon
            // blip must not silently abandon the switch and strand the user on
            // the old context. Otherwise reconnect to the last-confirmed context.
            let target = app
                .kube
                .context_switch
                .target()
                .cloned()
                .or_else(|| app.kube.context.clone());
            Some(target)
        } else {
            None
        };
        if let Some(ctx) = rebuild_ctx {
            // The old live session is being dropped and replaced (reconnect
            // OR context switch): consume the reconnect plan and mark the
            // link Connecting so the render gate shows the connecting
            // screen for the whole gap, not stale rows — this is the single
            // choke point every session replacement passes through. Cleared
            // to Live only on ConnectionEstablished.
            app.conn.begin_rebuild();
            // Stop the old per-subscription bridges retrying against the mux
            // we're about to drop — they'd otherwise burn a backoff loop for
            // the whole gap. Revived on ConnectionEstablished (top) / on pop
            // (covered), or dropped by a switch's nav reset. BOTH stream
            // owners: nav elements AND the always-on core subscriptions
            // (replaced wholesale by `open_streams` on ConnectionEstablished).
            app.nav.abort_all_streams();
            app.core.abort_streams();
            let no_daemon = data_source.is_no_daemon();
            drop(data_source);
            drop(event_rx);
            // Stores are KEPT: rows stay visible through the reconnect,
            // and per-store flash-hash continuity means rows that changed
            // during the gap flash after the recovery baseline. Dead
            // stream handles are inert (`is_alive` = false); revival
            // happens on ConnectionEstablished (top) and on pop (covered).
            // Metrics are stale for the new session either way.
            app.kube.metrics.clear();
            app.kube.kubectl_cache.clear();
            // Overlays / dialogs awaiting a response from the OLD session
            // would hang forever (their Yaml/OpResult/ExecEnded died with
            // the dropped channel) — clear them, the same way a context
            // switch does. Content fetches in flight against the old
            // session can never complete either: mark every such view
            // Failed, WHEREVER it sits in the stack (the old top-only
            // reset left covered views wedged in an eternal spinner), so
            // it renders an honest error with the retry hint.
            // ONE call, not three — see the note in `session::open_shell`.
            app.ui.close_modal();
            app.nav.for_each_content_view(|cv| {
                if cv.phase == k9rs_app::element::ContentPhase::Fetching {
                    cv.phase = k9rs_app::element::ContentPhase::Failed(
                        "fetch interrupted by reconnect".to_string(),
                    );
                }
            });
            let (new_tx, new_rx) = mpsc::channel::<AppEvent>(EVENT_CHANNEL_CAP);
            event_tx = new_tx;
            event_rx = new_rx;
            data_source = ClientSession::new(
                k9rs_app::client::ConnectionParams {
                    context: ctx,
                    namespace: app.kube.selected_ns.clone(),
                    readonly: app.read_only,
                    no_daemon,
                },
                event_tx.clone(),
            );
            repaint.on_input();
        }

        // Tick BEFORE select — guarantees animation advances (and any paint it
        // triggers fires) even during an event flood that starves the in-select
        // tick arm. A tick-driven change is data, so it coalesces.
        if last_tick.elapsed() >= tick_rate {
            if app.tick() {
                repaint.on_data(std::time::Instant::now(), tick_rate);
            }
            last_tick = std::time::Instant::now();
        }

        // Animation frame due? Advance the spinner's phase and force a paint at
        // its own cadence (bypassing the data-coalescing budget). Runs before
        // the paint gate so a due frame renders THIS turn; re-armed after the
        // paint iff a spinner is still on screen (see the gate below).
        if next_anim.is_some_and(|t| std::time::Instant::now() >= t) {
            app.ui.anim.advance();
            repaint.on_animation();
            next_anim = None;
        }

        // Top-of-loop paint gate: runs every turn, so a coalesced frame fires
        // as soon as its deadline passes even while the biased arms below keep
        // winning during a flood.
        if repaint.due(std::time::Instant::now()) {
            // Clear the per-frame liveness flag; the spinner primitive
            // (`Anim::bar`) sets it again iff a spinner draws this frame.
            app.ui.anim.begin_frame();
            terminal.draw(|f| {
                k9rs_ui::draw(f, &mut app);
            })?;

            // Set cursor style based on input mode: bar for text input, block otherwise
            let view_has_text_input = match app.nav.top() {
                k9rs_app::element::Element::ContentView(c) => c.state.search_input_active,
                el => el.log_view().is_some_and(|v| v.is_filtering()),
            };
            let in_input_mode = app.ui.command_input().is_some()
                || app.nav.top().form_dialog().is_some_and(|d| {
                    d.focused_field().is_some_and(|field| field.is_text_input())
                })
                || app.nav.top().filter_input().active()
                || view_has_text_input;
            if in_input_mode {
                execute!(terminal.backend_mut(), SetCursorStyle::SteadyBar)?;
            } else {
                execute!(terminal.backend_mut(), SetCursorStyle::SteadyBlock)?;
            }

            repaint.painted();

            // The spinner declared its own liveness by drawing this frame. Keep
            // the animation clock running while one is on screen; stop it
            // otherwise. Nothing here enumerates loading states — the draw is
            // the single source of truth, so a new spinner surface can't forget
            // to animate.
            if app.ui.anim.is_animating() {
                next_anim.get_or_insert_with(|| {
                    std::time::Instant::now() + k9rs_app::anim::FRAME_INTERVAL
                });
            } else {
                next_anim = None;
            }
        }

        // Unified edit flow: if route is EditorReady, run the editor.
        if run_editor_flow(
            &mut app, &mut event_rx, &mut terminal, &mut data_source,
            &input.suspend_tx, &mut input.suspend_ack_rx, &mut input.input_rx,
            &mut replay,
        ).await {
            // The stale-input drain now lives in the suspend guard's
            // `restore()` (with a settling window), so no ad-hoc drain here.
            repaint.on_input();
            continue;
        }

        // Shell bridge: when the daemon confirms the shell is connected,
        // suspend the TUI and enter a raw byte bridge. stdin→daemon,
        // daemon→stdout, no parsing. Restores TUI on exit.
        if let Some(k9rs_app::TakeoverOp::Shell(ref shell)) = app.ui.takeover() {
            if shell.connect_state == k9rs_app::ShellConnectState::Connected {
                run_shell_bridge(
                    &mut app, &mut terminal, &mut event_rx,
                    &input.suspend_tx, &mut input.suspend_ack_rx,
                    &mut input.input_rx, &mut replay,
                ).await;
                repaint.on_input();
                continue;
            }
        }

        // Replay type-ahead kept by a settling drain, through the same
        // pipeline live keys use. Placed AFTER the gates above so a replayed
        // key that opens the editor/shell has its gate run before the next
        // key routes; on such a transition (`Stop`) the remainder stays
        // queued and resumes here next iteration. Any replay activity
        // `continue`s the loop: the paint gate at the top already ran this
        // iteration, so falling into the select would sit on the dirty frame.
        let mut replayed_any = false;
        while let Some(ev) = replay.pop_front() {
            match ev {
                CtEvent::Key(k) => {
                    replayed_any = true;
                    if matches!(
                        handle_one_key(&mut app, k, &mut data_source, &event_tx, &mut repaint)
                            .await,
                        KeyDrain::Stop
                    ) {
                        break;
                    }
                }
                CtEvent::Resize(_, _) => {
                    replayed_any = true;
                    repaint.on_input();
                }
                _ => {}
            }
        }
        if replayed_any {
            // Mirror the bottom-of-loop quit check: a replayed `q` must not
            // park in the select waiting for an unrelated event to notice it.
            if app.should_quit {
                break;
            }
            continue;
        }

        // Snapshot the next wake deadline before the select so the timer arm
        // below borrows nothing from `repaint`: the earliest of the coalesced-
        // paint deadline and the next animation frame. `None` (idle, or `Now`
        // which the top-of-loop gate already paints, and nothing animating) →
        // the arm parks forever and the loop blocks on real events.
        let wake = match (repaint.wake_at(), next_anim) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        tokio::select! {
            // biased: always check key events first so user input (like disabling
            // autoscroll) takes effect before processing queued data events.
            biased;

            ct_event = input.input_rx.recv() => {
                // Binding the whole `Option` is load-bearing. As
                // `Some(ct_event) = ..`, a `None` — the input bridge gone,
                // so no key can ever arrive again — made the pattern fail to
                // match, which DISABLES the arm: the loop went on ticking and
                // painting a perfectly healthy-looking TUI that ignored every
                // key, Ctrl-C included (which is a key, not a signal, once
                // raw mode clears ISIG). A freeze with no diagnosis and no
                // way out. Named, it becomes an exit with a reason.
                let Some(ct_event) = ct_event else {
                    app.exit_reason = Some(k9rs_app::ExitReason::Error(
                        "terminal input stopped and could not be re-established — exiting rather than ignoring every key".to_string(),
                    ));
                    app.should_quit = true;
                    // `break`, not `continue`: `continue` would skip the
                    // quit check below and re-enter the select, where
                    // `recv()` resolves `None` again at once — a hot loop.
                    break;
                };
                match ct_event {
                    CtEvent::Key(key) => {
                        // Handle this key, then DRAIN the rest of an SSH-batched
                        // burst in the SAME turn, painting ONCE after (the top-of-
                        // loop gate). Processing one key per blocking remote paint
                        // is what made fast typing feel sluggish over SSH. Stop
                        // early on an Exec/quit transition so the outer editor/shell
                        // gates run; cap the batch so a paste can't monopolize the
                        // loop — undrained keys stay queued for the next turn and
                        // are never dropped (the channel keeps them).
                        let mut drain = handle_one_key(
                            &mut app, key, &mut data_source, &event_tx, &mut repaint,
                        ).await;
                        let mut drained = 0;
                        while matches!(drain, KeyDrain::Continue) && drained < EVENT_DRAIN_CAP {
                            match input.input_rx.try_recv() {
                                Ok(CtEvent::Key(k)) => {
                                    drain = handle_one_key(
                                        &mut app, k, &mut data_source, &event_tx, &mut repaint,
                                    ).await;
                                }
                                Ok(CtEvent::Resize(_, _)) => repaint.on_input(),
                                Ok(_) => {}
                                Err(_) => break,
                            }
                            drained += 1;
                        }
                    }
                    // Pure-keyboard TUI by design — mouse events are
                    // intentionally dropped (see ~/.config/claude memory).
                    CtEvent::Resize(_w, _h) => {
                        repaint.on_input();
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
                // Initial tail-fetch completion: a gap in line delivery means
                // the batch is done, so release the auto-scroll suppression
                // that keeps the view from racing while the tail streams in.
                //
                // This deliberately does NOT require the store to have lines.
                // It used to, and a stream that produced none — a silent pod,
                // or `1m` on something that last logged an hour ago — left
                // `initial_load` true FOREVER, which suppressed autoscroll
                // while the indicator cheerfully said "AutoScroll: On". With
                // no lines there is nothing to race, so there is nothing to
                // suppress.
                // EVERY log view on the stack, not just the top — see
                // `NavStack::for_each_log_view`.
                app.nav.for_each_log_view(|view| view.initial_load = false);
                // Covered filters collect too, or they come back with a hole.
                app.nav.keep_log_filters_current();
                // A range fetch that has the oldest N it asked for stops
                // there rather than streaming the rest at us to discard.
                if app.nav.stop_completed_head_fetches() {
                    app.ui.flash = Some(k9rs_app::FlashMessage::info(
                        "Reached the line limit for this range — showing the oldest".to_string(),
                    ));
                }
                repaint.on_data(std::time::Instant::now(), tick_rate);
            }
            // Fire a pending coalesced paint once events go quiet. Under a
            // flood this arm is starved by the biased arms above, but the
            // top-of-loop `repaint.due(..)` gate paints regardless — so
            // coalescing holds both under load and precisely when idle.
            () = async {
                match wake {
                    Some(t) => tokio::time::sleep_until(t.into()).await,
                    None => std::future::pending().await,
                }
            } => {}
            _ = tick_interval.tick() => {
                if app.tick() {
                    repaint.on_data(std::time::Instant::now(), tick_rate);
                }
            }
            () = shutdown_signal.recv() => {
                app.exit_reason = Some(k9rs_app::ExitReason::UserQuit);
                app.should_quit = true;
            }
        }

        if app.should_quit {
            break;
        }
    }

    // Teardown is BEST-EFFORT, and `TerminalGuard::drop` is the backstop that
    // retries it. These used to be `?`: when the terminal had already gone
    // away — which is one of the reasons we exit — every call failed with
    // EIO/ENXIO and the error propagated INSTEAD of `app.exit_reason`, so the
    // user got "Input/output error" where the diagnosis should have been.
    // Failing to restore a terminal that no longer exists is not the news.
    let _ = disable_raw_mode();
    let _ = execute!(
        terminal.backend_mut(),
        SetCursorStyle::DefaultUserShape,
        LeaveAlternateScreen
    );
    let _ = terminal.show_cursor();

    // Return the exit reason so main.rs can print a message after terminal restore.
    Ok(app.exit_reason)
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
