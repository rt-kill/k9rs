use anyhow::Result;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use tokio::sync::mpsc;

use crate::app::App;
use crate::kube::protocol::ResourceId;

/// Build `kubectl exec -it` args for shelling into a container.
/// Uses `sh -c "command -v bash && exec bash || exec sh"` (same as k9s)
/// so it works regardless of where bash/sh are installed.
pub(crate) fn build_shell_args(pod: &str, namespace: &str, container: &str, context: &str) -> Vec<String> {
    let mut args = vec![
        "exec".to_string(),
        "-it".to_string(),
        pod.to_string(),
        "-n".to_string(),
        namespace.to_string(),
        "-c".to_string(),
        container.to_string(),
    ];
    if !context.is_empty() {
        args.push("--context".to_string());
        args.push(context.to_string());
    }
    args.push("--".to_string());
    args.push("sh".to_string());
    args.push("-c".to_string());
    args.push("command -v bash >/dev/null && exec bash || exec sh".to_string());
    args
}

/// Build `kubectl edit` args for editing a resource.
pub(crate) fn build_edit_args(resource: &str, name: &str, namespace: &str, context: &str) -> Vec<String> {
    let mut args = vec![
        "edit".to_string(),
        resource.to_string(),
        name.to_string(),
    ];
    if !namespace.is_empty() {
        args.push("-n".to_string());
        args.push(namespace.to_string());
    }
    if !context.is_empty() {
        args.push("--context".to_string());
        args.push(context.to_string());
    }
    args
}

/// Suspend the TUI and run an interactive command directly (with bash->sh fallback for shell).
pub(crate) async fn run_interactive_local(
    terminal: &mut ratatui::Terminal<impl ratatui::backend::Backend + std::io::Write>,
    app: &mut App,
    command: &str,
    args: &[String],
    input_suspend: &tokio::sync::watch::Sender<bool>,
    input_suspend_ack: &mut mpsc::Receiver<()>,
) -> Result<()> {
    // Suspend the EventStream bridge and wait for it to actually stop reading.
    let _ = input_suspend.send(true);
    // Wait for the bridge to ack (with timeout to avoid hanging if bridge died).
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        input_suspend_ack.recv(),
    ).await;

    // Leave the TUI cleanly: disable raw mode, show cursor, leave alt screen.
    disable_raw_mode()?;
    crossterm::execute!(
        terminal.backend_mut(),
        crossterm::cursor::Show,
        crossterm::cursor::SetCursorStyle::DefaultUserShape,
        LeaveAlternateScreen,
    )?;

    let is_shell = args.first().map_or(false, |a| a == "exec");

    if is_shell {
        let pod = args.get(2).map(|s| s.as_str()).unwrap_or("?");
        let container = args.iter()
            .position(|a| a == "-c")
            .and_then(|i| args.get(i + 1))
            .map(|s| s.as_str())
            .unwrap_or("?");
        let msg = format!("{}/{}", pod, container);
        // Clear screen and show a centered connecting box.
        let (cols, rows) = crossterm::terminal::size().unwrap_or((80, 24));
        print!("\x1b[2J\x1b[H");
        let box_w = msg.len() + 6;
        let x = (cols as usize).saturating_sub(box_w) / 2;
        let y = (rows as usize) / 2 - 1;
        let pad = " ".repeat(x);
        let top = format!("{}┌{}┐", pad, "─".repeat(box_w));
        let mid = format!("{}│  {}  │", pad, msg);
        let status = format!("{}│  {}  │", pad, "Connecting...");
        let bot = format!("{}└{}┘", pad, "─".repeat(box_w));
        for _ in 0..y { println!(); }
        println!("{}", top);
        println!("{}", mid);
        println!("{}", status);
        println!("{}", bot);
    }

    let mut cmd = std::process::Command::new(command);
    cmd.args(args);
    let status = cmd.status();

    // Report shell failures (the smart shell command already tries bash then sh).
    if is_shell {
        if status.is_err() || status.as_ref().map_or(false, |s| !s.success()) {
            app.flash = Some(crate::app::FlashMessage::error(
                "Shell failed — no shell available in container".to_string(),
            ));
        }
    }

    // Resume TUI — restore raw mode and alternate screen.
    enable_raw_mode()?;
    crossterm::execute!(
        terminal.backend_mut(),
        EnterAlternateScreen,
        crossterm::cursor::Hide,
    )?;
    terminal.clear()?;

    // Resume the EventStream bridge.
    let _ = input_suspend.send(false);

    Ok(())
}

/// A parsed command that targets a resource tab with a filter or namespace.
pub(crate) struct ParsedResourceCommand {
    pub(crate) rid: ResourceId,
    pub(crate) argument: String,
}

/// Parse commands like "deploy /nginx" -> resource + filter text.
pub(crate) fn parse_resource_filter_command(cmd: &str) -> Option<ParsedResourceCommand> {
    let (resource_part, filter_part) = if let Some(slash_pos) = cmd.find('/') {
        let r = cmd[..slash_pos].trim();
        let f = cmd[slash_pos + 1..].trim();
        (r, f)
    } else {
        return None;
    };
    if filter_part.is_empty() {
        return None;
    }
    let resource_rid = parse_resource_command(resource_part)?;
    Some(ParsedResourceCommand { rid: resource_rid, argument: filter_part.to_string() })
}

/// A parsed CRD command with namespace.
pub(crate) struct ParsedCrdCommand {
    pub(crate) crd: crate::app::CrdInfo,
    pub(crate) namespace: String,
}

/// Parse a CRD command with optional namespace: "clickhouseinstallation prod"
pub(crate) fn parse_crd_ns_command(cmd: &str, app: &App) -> Option<ParsedCrdCommand> {
    let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return None;
    }
    let crd_part = parts[0].trim();
    let ns = parts[1].trim();
    if ns.is_empty() {
        return None;
    }
    let crd = app.find_crd_by_name(crd_part)?;
    if crd.scope == crate::kube::protocol::ResourceScope::Cluster {
        return None;
    }
    Some(ParsedCrdCommand { crd, namespace: ns.to_string() })
}

pub(crate) fn parse_resource_ns_command(cmd: &str) -> Option<ParsedResourceCommand> {
    let parts: Vec<&str> = cmd.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return None;
    }
    let resource = parts[0].trim();
    let ns = parts[1].trim();
    if ns.is_empty() {
        return None;
    }
    let resource_rid = parse_resource_command(resource)?;
    if resource_rid.is_cluster_scoped() {
        None
    } else {
        Some(ParsedResourceCommand { rid: resource_rid, argument: ns.to_string() })
    }
}

pub(crate) fn parse_resource_command(cmd: &str) -> Option<ResourceId> {
    ResourceId::from_alias(cmd)
}

// ---------------------------------------------------------------------------
// Input mode handlers — extracted from the main event loop
// ---------------------------------------------------------------------------

use crossterm::event::{KeyCode, KeyEvent};
use tokio::task::JoinHandle;

use crate::app::InputMode;
use crate::kube::client_session::ClientSession;
use crate::kube::session::{ds_try, apply_nav_change};
use crate::kube::session_nav::do_switch_namespace;

/// Maximum command history entries.
const COMMAND_HISTORY_LIMIT: usize = 50;

/// Handle a keystroke while in `:command` mode. Returns true if consumed.
pub(crate) fn handle_command_key(
    app: &mut App,
    key: KeyEvent,
    data_source: &mut ClientSession,
    log_task: &mut Option<JoinHandle<()>>,
    event_tx: &tokio::sync::mpsc::Sender<crate::event::AppEvent>,
) -> bool {
    if !matches!(app.input_mode, InputMode::Command { .. }) {
        return false;
    }
    match key.code {
        KeyCode::Esc => {
            app.input_mode = InputMode::Normal;
        }
        KeyCode::Enter => {
            let raw_cmd = if let InputMode::Command { ref input, .. } = app.input_mode {
                input.trim().to_string()
            } else {
                String::new()
            };
            let cmd = raw_cmd.to_lowercase();
            if !cmd.is_empty() {
                app.command_history.push(raw_cmd.clone());
                if app.command_history.len() > COMMAND_HISTORY_LIMIT {
                    app.command_history.remove(0);
                }
            }
            app.input_mode = InputMode::Normal;
            handle_command_submit(app, &raw_cmd, &cmd, data_source, log_task, event_tx);
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
    true
}

/// Dispatch a submitted `:command` string.
fn handle_command_submit(
    app: &mut App,
    raw_cmd: &str,
    cmd: &str,
    data_source: &mut ClientSession,
    log_task: &mut Option<JoinHandle<()>>,
    event_tx: &tokio::sync::mpsc::Sender<crate::event::AppEvent>,
) {
    use crate::kube::session_actions::handle_action;
    use crate::kube::session_nav::begin_context_switch;

    if cmd.is_empty() {
        return;
    }

    if matches!(cmd, "q" | "quit" | "exit" | "q!") {
        app.exit_reason = Some(crate::app::ExitReason::UserQuit);
        app.should_quit = true;
    } else if matches!(cmd, "help" | "h" | "?") {
        app.push_route(app.route.clone());
        app.route = crate::app::Route::Help;
    } else if matches!(cmd, "alias" | "aliases" | "a") {
        handle_action(
            app, crate::app::actions::Action::ShowAliases, event_tx,
            data_source, log_task,
        );
    } else if matches!(cmd, "ctx" | "context" | "contexts") {
        app.push_route(app.route.clone());
        app.route = crate::app::Route::Contexts;
    } else if cmd.starts_with("ctx ") || cmd.starts_with("context ") {
        let ctx_name = if cmd.starts_with("ctx ") { &raw_cmd[4..] } else { &raw_cmd[8..] }.trim().to_string();
        begin_context_switch(app, data_source, &ctx_name, log_task);
    } else if cmd.starts_with("ns ") || cmd.starts_with("namespace ") {
        let ns = if cmd.starts_with("ns ") { &raw_cmd[3..] } else { &raw_cmd[10..] }.trim().to_string();
        do_switch_namespace(app, data_source, crate::kube::protocol::Namespace::from(ns.as_str()), log_task);
    } else if let Some(resource_rid) = parse_resource_command(cmd) {
        app.route = crate::app::Route::Resources;
        if resource_rid.is_cluster_scoped() && !app.selected_ns.is_all() {
            do_switch_namespace(app, data_source, crate::kube::protocol::Namespace::All, log_task);
        }
        let change = app.nav.reset(resource_rid);
        *app.nav.filter_input_mut() = Default::default();
        apply_nav_change(app, data_source, change);
    } else if let Some(parsed) = parse_resource_ns_command(cmd) {
        app.route = crate::app::Route::Resources;
        let change = app.nav.reset(parsed.rid.clone());
        *app.nav.filter_input_mut() = Default::default();
        if crate::kube::protocol::Namespace::from(parsed.argument.as_str()) != app.selected_ns {
            do_switch_namespace(app, data_source, crate::kube::protocol::Namespace::from(parsed.argument.as_str()), log_task);
        }
        apply_nav_change(app, data_source, change);
        app.flash = Some(crate::app::FlashMessage::info(format!(
            "{}({})", parsed.rid.short_label(), parsed.argument
        )));
    } else if let Some(parsed) = parse_resource_filter_command(cmd) {
        app.route = crate::app::Route::Resources;
        let change = app.nav.reset(parsed.rid.clone());
        apply_nav_change(app, data_source, change);
        let change = app.nav.push(crate::app::nav::NavStep {
            resource: parsed.rid,
            filter: Some(crate::app::nav::NavFilter::Grep(parsed.argument.clone())),
            saved_selected: 0,
            filter_input: crate::app::nav::FilterInputState::default(),
        });
        apply_nav_change(app, data_source, change);
        *app.nav.filter_input_mut() = Default::default();
        app.reapply_nav_filters();
    } else if let Some(parsed) = parse_crd_ns_command(cmd, app) {
        let crd = parsed.crd;
        app.route = crate::app::Route::Resources;
        if crate::kube::protocol::Namespace::from(parsed.namespace.as_str()) != app.selected_ns {
            do_switch_namespace(app, data_source, crate::kube::protocol::Namespace::from(parsed.namespace.as_str()), log_task);
        }
        let crd_rid = ResourceId::new(
            crd.group.clone(), crd.version.clone(),
            crd.kind.clone(), crd.plural.clone(), crd.scope,
        );
        let change = app.nav.reset(crd_rid);
        apply_nav_change(app, data_source, change);
        app.flash = Some(crate::app::FlashMessage::info(
            format!("Browsing CRD: {}({})", crd.kind, parsed.namespace)
        ));
    } else if matches!(cmd, "overview" | "home") {
        app.route = crate::app::Route::Overview;
    } else {
        // Unknown — try as CRD.
        let crd_info = app.find_crd_by_name(cmd);
        let (group, version, kind, plural, scope) = if let Some(crd) = &crd_info {
            (crd.group.clone(), crd.version.clone(), crd.kind.clone(), crd.plural.clone(), crd.scope)
        } else {
            (String::new(), String::new(), cmd.to_string(), cmd.to_string(),
             crate::kube::protocol::ResourceScope::Namespaced)
        };
        app.route = crate::app::Route::Resources;
        if scope == crate::kube::protocol::ResourceScope::Cluster && !app.selected_ns.is_all() {
            do_switch_namespace(app, data_source, crate::kube::protocol::Namespace::All, log_task);
        }
        let crd_rid = ResourceId::new(group, version, kind.clone(), plural, scope);
        let change = app.nav.reset(crd_rid);
        apply_nav_change(app, data_source, change);
        app.flash = Some(crate::app::FlashMessage::info(format!("Browsing: {}", kind)));
    }
}

/// Handle a keystroke while a generic form dialog is open. Returns true if
/// consumed. Replaces the old `handle_scale_key` and the inline port-forward
/// dialog block in `session.rs` — both flowed into one place because the
/// dialog is now schema-driven and doesn't need per-operation input logic.
///
/// Key bindings:
///   - Esc                       → cancel (close without dispatching)
///   - Tab / Down                → focus next field (or OK button)
///   - BackTab / Up              → focus previous field
///   - Enter                     → submit (collect values, dispatch typed command)
///   - Left / Right              → cycle Select field options
///   - Backspace                 → delete last char from text/number/port field
///   - Char                      → append to text field; digit-only for number/port
pub(crate) fn handle_form_dialog_key(
    app: &mut App,
    key: KeyEvent,
    data_source: &mut ClientSession,
) -> bool {
    use crate::app::FormFieldKind;
    if app.form_dialog.is_none() {
        return false;
    }
    match key.code {
        KeyCode::Esc => {
            app.form_dialog = None;
        }
        KeyCode::Tab | KeyCode::Down => {
            if let Some(ref mut d) = app.form_dialog {
                d.focus_next();
            }
        }
        KeyCode::BackTab | KeyCode::Up => {
            if let Some(ref mut d) = app.form_dialog {
                d.focus_prev();
            }
        }
        KeyCode::Enter => {
            // Take the dialog out so the dispatcher can consume it without
            // borrowing app twice.
            if let Some(dialog) = app.form_dialog.take() {
                dispatch_form_submit(app, data_source, dialog);
            }
        }
        KeyCode::Left | KeyCode::Right => {
            if let Some(ref mut d) = app.form_dialog {
                if let Some(field) = d.current_field_mut() {
                    if let FormFieldKind::Select { ref options } = field.kind {
                        if !options.is_empty() {
                            let n = options.len();
                            let cur: usize = field.value.parse().unwrap_or(0);
                            let new = if matches!(key.code, KeyCode::Left) {
                                (cur + n - 1) % n
                            } else {
                                (cur + 1) % n
                            };
                            field.value = new.to_string();
                        }
                    }
                }
            }
        }
        KeyCode::Backspace => {
            if let Some(ref mut d) = app.form_dialog {
                if let Some(field) = d.current_field_mut() {
                    if field.is_text_input() {
                        field.value.pop();
                    }
                }
            }
        }
        KeyCode::Char(c) => {
            if let Some(ref mut d) = app.form_dialog {
                if let Some(field) = d.current_field_mut() {
                    let accept = match field.kind {
                        FormFieldKind::Text { max_len } => {
                            max_len.map_or(true, |m| field.value.chars().count() < m)
                        }
                        FormFieldKind::Number { .. } | FormFieldKind::Port => c.is_ascii_digit(),
                        FormFieldKind::Select { .. } => false,
                    };
                    if accept {
                        field.value.push(c);
                    }
                }
            }
        }
        _ => {}
    }
    true
}

/// Dispatch a submitted form dialog to the appropriate typed
/// `SessionCommand`. The schema told the user what to type; this function
/// converts the collected values into the strongly-typed wire payload.
/// Centralized so adding a new operation kind only adds an arm here.
fn dispatch_form_submit(
    app: &mut App,
    data_source: &mut ClientSession,
    dialog: crate::app::FormDialog,
) {
    use crate::app::{FormDialog, FormFieldKind};
    use crate::kube::protocol::OperationKind;

    let FormDialog { kind, target, fields, .. } = dialog;
    match kind {
        OperationKind::Scale => {
            let replicas_str = fields
                .iter()
                .find(|f| f.name == "replicas")
                .map(|f| f.value.trim().to_string())
                .unwrap_or_default();
            match replicas_str.parse::<u32>() {
                Ok(replicas) => {
                    app.kubectl_cache.clear();
                    ds_try!(app, data_source.scale(&target, replicas));
                    app.flash = Some(crate::app::FlashMessage::info(
                        format!("Scaling to {} replicas", replicas)
                    ));
                }
                Err(_) => {
                    app.flash = Some(crate::app::FlashMessage::warn(
                        format!("Invalid replica count: {}", replicas_str)
                    ));
                }
            }
        }
        OperationKind::PortForward => {
            // container_port may be Select (value = option index → port string)
            // or Port (value = port string directly). Handle both.
            let container_port = fields
                .iter()
                .find(|f| f.name == "container_port")
                .and_then(|f| match &f.kind {
                    FormFieldKind::Select { options } => {
                        let idx: usize = f.value.parse().ok()?;
                        options.get(idx).and_then(|(v, _)| v.parse::<u16>().ok())
                    }
                    _ => f.value.trim().parse::<u16>().ok(),
                });
            let local_port = fields
                .iter()
                .find(|f| f.name == "local_port")
                .and_then(|f| f.value.trim().parse::<u16>().ok());
            match (container_port, local_port) {
                (Some(cp), Some(lp)) => {
                    ds_try!(app, data_source.port_forward(&target, lp, cp));
                    app.flash = Some(crate::app::FlashMessage::info(
                        format!("Port-forwarding {}:{} → {}", lp, cp, target.name)
                    ));
                }
                _ => {
                    app.flash = Some(crate::app::FlashMessage::warn(
                        "Invalid port numbers".to_string()
                    ));
                }
            }
        }
        // Other operations either have no input (handled directly without a
        // form) or aren't yet wired up to use the form dialog. Surface a
        // flash so we notice if a stray submit ever lands here.
        other => {
            app.flash = Some(crate::app::FlashMessage::warn(
                format!("No form dispatcher for operation {:?}", other)
            ));
        }
    }
}

/// Handle a keystroke while the nav filter bar is active. Returns true if consumed.
pub(crate) fn handle_filter_key(
    app: &mut App,
    key: KeyEvent,
    data_source: &mut ClientSession,
) -> bool {
    if !app.nav.filter_input().active {
        return false;
    }
    match key.code {
        KeyCode::Esc => {
            app.nav.filter_input_mut().text.clear();
            app.nav.filter_input_mut().active = false;
            app.reapply_nav_filters();
        }
        KeyCode::Enter => {
            let text = std::mem::take(&mut app.nav.filter_input_mut().text);
            app.nav.filter_input_mut().active = false;
            if !text.is_empty() {
                let change = app.nav.push(crate::app::nav::NavStep {
                    resource: app.nav.resource_id().clone(),
                    filter: Some(crate::app::nav::NavFilter::Grep(text)),
                    saved_selected: 0,
                    filter_input: crate::app::nav::FilterInputState::default(),
                });
                apply_nav_change(app, data_source, change);
            }
            app.reapply_nav_filters();
        }
        KeyCode::Backspace | KeyCode::Char(_) => {
            match key.code {
                KeyCode::Backspace => { app.nav.filter_input_mut().text.pop(); }
                KeyCode::Char(c) => { app.nav.filter_input_mut().text.push(c); }
                _ => {}
            }
            // The nav layer is the only source of truth for filter text;
            // re-derive the visible row set from the updated input. This
            // also refreshes the marked-visible cache.
            app.reapply_nav_filters();
        }
        _ => {}
    }
    true
}
