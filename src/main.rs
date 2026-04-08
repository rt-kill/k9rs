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

    // Read kubeconfig ONCE for all early init
    let kubeconfig = ::kube::config::Kubeconfig::read().ok();
    let contexts: Vec<String> = kubeconfig.as_ref()
        .map(|kc| kc.contexts.iter().map(|c| c.name.clone()).collect())
        .unwrap_or_default();
    let current_context = cli.context.clone().or_else(|| {
        kubeconfig.as_ref().and_then(|kc| kc.current_context.clone())
    }).unwrap_or_else(|| "in-cluster".to_string());

    // Initialize app state
    let namespace = cli.namespace.unwrap_or_else(|| "all".to_string());
    let mut app = App::new(
        current_context.clone(),
        contexts.clone(),
        namespace,
    );
    if cli.readonly {
        app.read_only = true;
    }

    // Populate contexts table from kubeconfig
    if let Some(ref kc) = kubeconfig {
        let mut ctx_map = std::collections::HashMap::new();
        for named_ctx in &kc.contexts {
            if let Some(ref ctx) = named_ctx.context {
                ctx_map.insert(named_ctx.name.as_str(), (ctx.cluster.as_str(), ctx.user.as_deref().unwrap_or("")));
            }
        }
        let kube_contexts: Vec<crate::app::KubeContext> = contexts.iter().map(|name: &String| {
            let (cluster, user) = ctx_map.get(name.as_str()).unwrap_or(&("", ""));
            crate::app::KubeContext {
                name: name.clone(),
                cluster: cluster.to_string(),
                user: user.to_string(),
                is_current: name == &current_context,
            }
        }).collect();
        if let Some(&(cluster, user)) = ctx_map.get(current_context.as_str()) {
            app.cluster = cluster.to_string();
            app.user = user.to_string();
        }
        app.data.contexts.set_items(kube_contexts);
    }

    // Parse initial command/resource
    if let Some(ref cmd) = cli.command {
        if let Some(tab) = crate::kube::session_commands::parse_resource_command(cmd) {
            app.nav.reset(tab);
            app.route = crate::app::Route::Resources;
        }
    }

    // -----------------------------------------------------------------------
    // Connect to daemon BEFORE entering the TUI.
    // If connection fails, print the error on the normal terminal and exit
    // cleanly — no raw mode, no alternate screen, no terminal corruption.
    // -----------------------------------------------------------------------

    let (event_tx, event_rx) = mpsc::channel::<AppEvent>(500);

    use crate::kube::client_session::ClientSession;

    // In daemon mode: quick socket check BEFORE entering the TUI.
    // This fails fast if the daemon isn't running, without touching the terminal.
    // The full session handshake happens after the TUI is up.
    if !cli.no_daemon {
        let socket = crate::kube::daemon::socket_path();
        if let Err(e) = tokio::net::UnixStream::connect(&socket).await {
            eprintln!(
                "Daemon not running or connection failed: {}\n\n\
                 Start it with:  k9rs daemon\n\
                 Or run locally:  k9rs --no-daemon\n",
                e
            );
            std::process::exit(1);
        }
        // Socket is reachable — we'll do the full handshake after TUI starts.
    }

    // Connection happens AFTER TUI starts — the user sees "Connecting..." immediately.
    // Both daemon and no-daemon modes use the same deferred pattern.

    // -----------------------------------------------------------------------
    // Connection succeeded — NOW enter the TUI.
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

    app.flash = Some(crate::app::FlashMessage::info("Connecting to cluster..."));
    // Draw first frame immediately so the TUI appears instantly.
    terminal.draw(|f| crate::ui::draw(f, &mut app))?;

    // Spawn connection as a background task.
    // The user sees "Connecting..." and can already interact (quit, help, etc.).
    let connect_event_tx = event_tx.clone();
    let connect_context = current_context.clone();
    let connect_ns = app.selected_ns.clone();
    let connect_readonly = cli.readonly;
    let connect_no_daemon = cli.no_daemon;
    let (connect_tx, mut connect_rx) = tokio::sync::oneshot::channel::<
        anyhow::Result<(ClientSession, crate::kube::client_session::SessionReadyInfo)>
    >();
    tokio::spawn(async move {
        let result = if connect_no_daemon {
            ClientSession::connect_local(
                Some(connect_context.as_str()),
                connect_ns.as_option(),
                connect_readonly,
                connect_event_tx,
            ).await
        } else {
            ClientSession::connect(
                Some(connect_context.as_str()),
                connect_ns.as_option(),
                connect_readonly,
                connect_event_tx,
            ).await
        };
        let _ = connect_tx.send(result);
    });

    // Enter the event loop immediately — connection completes in the background.
    // We pass connect_rx so session_main can pick up the ClientSession when ready.
    // For now, create a placeholder that buffers commands until connected.
    // Actually — we need the ClientSession before entering session_main.
    // Simplest correct approach: poll connect_rx in a tight loop with terminal redraws.
    let mut data_source;
    loop {
        terminal.draw(|f| crate::ui::draw(f, &mut app))?;
        match connect_rx.try_recv() {
            Ok(Ok((cs, info))) => {
                app.context = info.context.clone();
                app.cluster = info.cluster.clone();
                app.user = info.user.clone();
                if !info.namespaces.is_empty() {
                    let ns_rows = crate::kube::cache::cached_namespaces_to_rows(&info.namespaces);
                    let table = app.data.unified.entry(crate::app::nav::rid("namespaces"))
                        .or_insert_with(crate::app::StatefulTable::new);
                    table.set_items(ns_rows);
                }
                app.flash = None;
                data_source = cs;
                break;
            }
            Ok(Err(e)) => {
                drop(terminal);
                drop(_terminal_guard);
                eprintln!("Failed to connect: {}", e);
                std::process::exit(1);
            }
            Err(_) => {
                // Not ready yet — sleep briefly then redraw (spinner animation)
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    // Subscribe to initial resource via apply_nav_change — the single subscription entry point.
    if app.route == crate::app::Route::Resources {
        let initial_rid = app.nav.resource_id().clone();
        let plural = initial_rid.plural.as_str();
        if plural != "namespaces" && plural != "nodes" {
            let change = crate::app::nav::NavChange {
                unsubscribe: None,
                subscribe: Some(initial_rid),
            };
            crate::kube::session::apply_nav_change(&mut app, &mut data_source, change);
        }
    }

    let tick_rate = Duration::from_millis(cli.tick_rate);

    // Bridge crossterm EventStream into an mpsc channel.
    let (input_tx, input_rx) =
        mpsc::channel::<crossterm::event::Event>(100);
    let (suspend_tx, mut suspend_rx) = tokio::sync::watch::channel(false);
    let (suspend_ack_tx, suspend_ack_rx) = tokio::sync::mpsc::channel::<()>(1);
    tokio::spawn(async move {
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
