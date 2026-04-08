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
