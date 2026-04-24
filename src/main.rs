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

    /// Start on a specific resource view
    #[arg(short, long)]
    command: Option<String>,

    /// Run without the cache daemon (local watchers, file cache only)
    #[arg(long)]
    no_daemon: bool,

    /// Read-only mode: disables all destructive actions (delete, edit, scale, restart, shell)
    #[arg(long)]
    readonly: bool,

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
    let namespace = cli.namespace.unwrap_or_else(|| "all".to_string());
    let cli_context: Option<crate::kube::protocol::ContextName> = cli.context.clone().map(Into::into);
    let mut app = App::new(crate::kube::protocol::ContextName::default(), namespace);
    if cli.readonly {
        app.read_only = true;
    }

    // Parse initial command/resource
    if let Some(ref cmd) = cli.command {
        if let Some(tab) = crate::kube::session_commands::parse_resource_command(cmd) {
            app.nav.reset(tab);
            app.route = crate::app::Route::Resources;
        }
    }

    let (event_tx, event_rx) = mpsc::channel::<AppEvent>(500);

    use crate::kube::client_session::ClientSession;

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

    // Construct the data source. This is non-blocking — `ClientSession::new`
    // returns immediately and spawns its own background manager that does the
    // (single) kubeconfig read, daemon socket connect, Init/Ready handshake,
    // and then brings up the reader/writer loops. Commands sent on
    // `data_source` queue locally on an unbounded channel and are flushed as
    // soon as the writer task spawns. Lifecycle events arrive on `event_tx`:
    //   - `AppEvent::KubeconfigLoaded`     (fast: contexts panel populated)
    //   - `AppEvent::ConnectionEstablished` (handshake complete: do initial subscribe)
    //   - `AppEvent::ConnectionFailed`     (fatal: TUI exits with error)
    let data_source = ClientSession::new(
        crate::kube::client_session::ConnectionParams {
            context: cli_context.clone(),
            namespace: app.kube.selected_ns.clone(),
            readonly: cli.readonly,
            no_daemon: cli.no_daemon,
        },
        event_tx.clone(),
    );

    // If `--command` put us straight into a resource view, open a
    // subscription substream immediately. The bridge task inside
    // subscribe_stream awaits the MuxHandle (which becomes available
    // after the connection handshake), so the subscribe fires as soon
    // as the connection is up. Core resources (namespaces, nodes) are
    // auto-subscribed by the client via `open_core_subscriptions` on
    // `ConnectionEstablished`, so we skip them here to avoid opening
    // a duplicate substream for the same table.
    if matches!(app.route, crate::app::Route::Resources) {
        let initial_rid = app.nav.resource_id().clone();
        let is_core = initial_rid.built_in_kind()
            .map(|k| crate::kube::resource_defs::REGISTRY.by_kind(k).is_core())
            .unwrap_or(false);
        if !is_core {
            let filter = app.nav.current().filter.as_ref()
                .and_then(|f| f.to_subscription_filter());
            let stream = data_source.subscribe_stream(initial_rid, app.kube.selected_ns.clone(), filter);
            app.nav.current_mut().stream = Some(stream);
        }
    }

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
                        drop(event_stream);
                        let _ = suspend_ack_tx.send(()).await;
                        event_stream = EventStream::new();
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
        input_rx,
        tick_rate,
        suspend_tx,
        suspend_ack_rx,
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
