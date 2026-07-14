pub mod app;
pub mod cli;
pub mod event;
pub mod kube;
pub mod ui;
pub mod util;

use std::io;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::EventStream,
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::StreamExt;
use ratatui::prelude::*;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

use crate::app::App;
use crate::event::AppEvent;

#[derive(Parser, Debug)]
#[command(name = "k9rs", version, about = "A fast Kubernetes TUI")]
#[command(args_conflicts_with_subcommands = true)]
struct Cli {
    /// Kubernetes context to use
    #[arg(long)]
    context: Option<String>,

    /// Namespace to select on startup
    #[arg(short, long)]
    namespace: Option<String>,

    /// UI tick rate in milliseconds
    #[arg(long, default_value = "100", value_parser = clap::value_parser!(u64).range(10..))]
    tick_rate: u64,

    /// Log file path
    #[arg(long)]
    log_file: Option<String>,

    /// Start on a specific resource view (legacy; prefer positional nav path)
    #[arg(short, long)]
    command: Option<String>,

    /// Run without the cache daemon (local watchers, file cache only)
    #[arg(long)]
    no_daemon: bool,

    /// Read-only mode: disables all destructive actions (delete, edit, scale, restart, shell)
    #[arg(long)]
    readonly: bool,

    /// Navigation path: resource [filter] [resource [filter]] ...
    ///
    /// Each arg that matches a known resource (pods, deploy, svc, ...) starts
    /// a new nav level. Unrecognized args become grep filters for the current
    /// level. Examples:
    ///   k9rs pods                  # view pods
    ///   k9rs pods nginx            # pods grepped to "nginx"
    ///   k9rs deploy my-app pods    # deploy > /my-app > pods
    nav_path: Vec<String>,

    #[command(subcommand)]
    subcmd: Option<crate::cli::Command>,
}

/// RAII guard that restores the terminal on drop.
struct TerminalGuard;

impl TerminalGuard {
    fn new() -> Self { Self }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(
            io::stdout(),
            crossterm::cursor::Show,
            crossterm::cursor::SetCursorStyle::DefaultUserShape,
            LeaveAlternateScreen
        );
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Subcommand dispatch: daemon, ctl, get, contexts
    if let Some(subcmd) = cli.subcmd {
        return crate::cli::dispatch(subcmd).await;
    }

    // Setup logging
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    if let Some(ref log_file) = cli.log_file {
        let file = std::fs::File::create(log_file)?;
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(file)
            .with_ansi(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(io::sink)
            .init();
    }

    // Initialize app state. context/cluster/user start empty — the UI renders
    // "connecting…" placeholders until the connection manager publishes the
    // resolved values via `AppEvent::KubeconfigLoaded` (fast, from disk) and
    // then `AppEvent::ConnectionEstablished` (authoritative, from the daemon).
    let namespace = crate::kube::protocol::Namespace::from_user_command(
        cli.namespace.as_deref().unwrap_or("all"),
    );
    let cli_context: Option<crate::kube::protocol::ContextName> = cli.context.clone().map(Into::into);

    // Parse startup navigation from positional args or legacy -c flag.
    let startup_segments = parse_startup_segments(&cli);

    let (event_tx, event_rx) = mpsc::channel::<AppEvent>(500);

    use crate::kube::client_session::ClientSession;

    // Construct the data source FIRST — `ClientSession::new` is
    // non-blocking (it spawns a background manager that does the
    // kubeconfig read, socket connect, and handshake), and the App's
    // root nav element opens its subscription against it. Bridge tasks
    // inside `subscribe_stream` await the MuxHandle, so subscribes fire
    // as soon as the connection is up.
    let data_source = ClientSession::new(
        crate::kube::client_session::ConnectionParams {
            context: cli_context.clone(),
            namespace: namespace.clone(),
            readonly: cli.readonly,
            no_daemon: cli.no_daemon,
        },
        event_tx.clone(),
    );

    let mut app = App::new(crate::kube::protocol::ContextName::default(), namespace, &data_source);
    if cli.readonly {
        app.read_only = true;
    }

    // -----------------------------------------------------------------------
    // Enter the TUI immediately. NO blocking I/O happens before this point.
    // Kubeconfig read, daemon check, and connection all run in background
    // tasks and stream results back via `startup_rx`.
    // -----------------------------------------------------------------------

    // Install panic hook that restores the terminal.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), crossterm::cursor::Show, crossterm::cursor::SetCursorStyle::DefaultUserShape, LeaveAlternateScreen);
        original_hook(info);
    }));

    enable_raw_mode()?;
    let _terminal_guard = TerminalGuard::new();

    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, crossterm::cursor::Hide)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    // Draw first frame immediately so the TUI appears instantly.
    terminal.draw(|f| crate::ui::draw(f, &mut app))?;

    // Apply startup navigation (positional args or -c). Each element
    // opens its own subscription at construction; core stores are wired
    // on ConnectionEstablished.
    apply_startup_nav(&mut app, &data_source, &startup_segments);

    // Spawn the input bridge so keypresses flow into `session_main` from
    // frame zero — the user can type `:`, navigate, scroll, etc. immediately,
    // and any daemon-bound commands queue up until the connection completes.
    let (input_tx, input_rx) =
        mpsc::channel::<crossterm::event::Event>(100);
    let (suspend_tx, mut suspend_rx) = tokio::sync::watch::channel(false);
    let (suspend_ack_tx, suspend_ack_rx) = tokio::sync::mpsc::channel::<()>(1);
    let input_bridge = tokio::spawn(async move {
        let mut event_stream = EventStream::new();
        let mut suspended = false;
        loop {
            if suspended {
                if suspend_rx.changed().await.is_err() { break; }
                if !*suspend_rx.borrow() {
                    suspended = false;
                    event_stream = EventStream::new();
                }
                continue;
            }
            tokio::select! {
                biased;
                _ = suspend_rx.changed() => {
                    if *suspend_rx.borrow() {
                        suspended = true;
                        let _ = suspend_ack_tx.send(()).await;
                    }
                }
                event = event_stream.next() => {
                    match event {
                        Some(Ok(ev)) => {
                            if input_tx.send(ev).await.is_err() {
                                break;
                            }
                        }
                        Some(Err(_)) => continue,
                        None => break,
                    }
                }
            }
        }
    });

    let tick_rate = Duration::from_millis(cli.tick_rate);

    // Run the TUI event loop.
    // TerminalGuard ensures cleanup even if this returns Err.
    let exit_reason = crate::kube::session::session_main(
        app,
        data_source,
        terminal,
        event_tx,
        event_rx,
        tick_rate,
        crate::kube::session::InputChannels {
            input_rx,
            suspend_tx,
            suspend_ack_rx,
        },
    )
    .await?;

    // Abort the input bridge so it doesn't outlive session_main. It
    // usually exits on its own when `input_tx.send()` fails (rx dropped),
    // but if it's parked inside `event_stream.next()` it can linger
    // holding the crossterm EventStream after raw mode has been disabled.
    input_bridge.abort();

    // TerminalGuard drops here, restoring the terminal.
    // Print exit message AFTER terminal is restored.
    drop(_terminal_guard);

    match exit_reason {
        Some(crate::app::ExitReason::DaemonDisconnected) => {
            eprintln!("k9rs: lost connection to daemon");
            std::process::exit(1);
        }
        Some(crate::app::ExitReason::Error(msg)) => {
            eprintln!("k9rs: {}", msg);
            std::process::exit(1);
        }
        Some(crate::app::ExitReason::UserQuit) | None => {
            // Normal exit — no message needed.
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Startup navigation — positional args → nav stack
// ---------------------------------------------------------------------------

use crate::kube::protocol::ResourceId;

enum StartupSegment {
    Resource(ResourceId),
    Filter(String),
}

fn parse_startup_segments(cli: &Cli) -> Vec<StartupSegment> {
    if !cli.nav_path.is_empty() {
        cli.nav_path.iter().map(|arg| {
            match ResourceId::from_alias(arg) {
                Some(rid) => StartupSegment::Resource(rid),
                None => StartupSegment::Filter(arg.clone()),
            }
        }).collect()
    } else if let Some(ref cmd) = cli.command {
        match ResourceId::from_alias(cmd) {
            Some(rid) => vec![StartupSegment::Resource(rid)],
            None => vec![],
        }
    } else {
        vec![]
    }
}

fn apply_startup_nav(
    app: &mut App,
    data_source: &crate::kube::client_session::ClientSession,
    segments: &[StartupSegment],
) {
    if segments.is_empty() {
        return;
    }

    // First segment must be a resource.
    let first_rid = match &segments[0] {
        StartupSegment::Resource(rid) => rid.clone(),
        StartupSegment::Filter(text) => {
            app.ui.flash = Some(crate::app::FlashMessage::warn(
                format!("Unknown resource: {}", text),
            ));
            return;
        }
    };

    let root = App::root_list_element(
        data_source,
        &app.kube.metrics,
        first_rid,
        app.kube.selected_ns.clone(),
    );
    app.nav.reset(root);

    for seg in &segments[1..] {
        match seg {
            StartupSegment::Filter(pattern) => {
                let predicate = crate::app::store::RowPredicate::Grep(
                    crate::app::nav::CompiledGrep::new(pattern),
                );
                if let Ok(el) =
                    crate::app::element::Element::derive_filter(app.nav.top(), predicate)
                {
                    app.nav.push(el);
                }
            }
            StartupSegment::Resource(rid) => {
                let label = rid.short_label().to_lowercase();
                let el = app.list_element_from_top(
                    data_source,
                    rid.clone(),
                    app.kube.selected_ns.clone(),
                    None,
                    label,
                );
                app.nav.push(el);
            }
        }
    }
}
