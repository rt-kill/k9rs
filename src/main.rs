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

    // Read kubeconfig ONCE for all early init (contexts list, cluster/user info)
    let kubeconfig = ::kube::config::Kubeconfig::read().ok();
    let contexts: Vec<String> = kubeconfig.as_ref()
        .map(|kc| kc.contexts.iter().map(|c| c.name.clone()).collect())
        .unwrap_or_default();
    let current_context = cli.context.clone().or_else(|| {
        kubeconfig.as_ref().and_then(|kc| kc.current_context.clone())
    }).unwrap_or_else(|| "in-cluster".to_string());

    // Initialize app state and show TUI IMMEDIATELY — data loads in background
    let namespace = cli.namespace.unwrap_or_else(|| "all".to_string());
    let mut app = App::new(
        current_context.clone(),
        contexts.clone(),
        namespace,
    );
    if cli.readonly {
        app.read_only = true;
    }

    // Populate contexts table from the single kubeconfig read
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
        // Also set cluster/user on app from the same read
        if let Some(&(cluster, user)) = ctx_map.get(current_context.as_str()) {
            app.cluster = cluster.to_string();
            app.user = user.to_string();
        }
        app.data.contexts.set_items(kube_contexts);
    }

    // Pre-populate namespaces and CRDs from disk cache for instant autocomplete.
    // The watcher will deliver fresh data shortly after connection, but this
    // ensures completion works even before the first snapshot arrives.
    if let Some(cached) = crate::kube::cache::load_cache(&current_context) {
        if !cached.namespaces.is_empty() {
            let ns_items = crate::kube::cache::cached_namespaces_to_domain(&cached.namespaces);
            app.data.namespaces.set_items(ns_items);
        }
        if !cached.crds.is_empty() {
            app.discovered_crds = crate::kube::cache::cached_crds_to_domain(&cached.crds);
        }
    }

    // Parse initial command/resource — bypasses Overview, goes straight to Resources
    if let Some(ref cmd) = cli.command {
        if let Some(tab) = crate::kube::session::parse_resource_command(cmd) {
            app.resource_tab = tab;
            app.nav.reset(tab);
            app.route = crate::app::Route::Resources;
        }
    }

    // Install a panic hook that restores the terminal before printing the panic.
    // Without this, a panic leaves raw mode on and the shell is garbled.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), crossterm::cursor::SetCursorStyle::DefaultUserShape, LeaveAlternateScreen);
        original_hook(info);
    }));

    // Setup terminal FIRST — show TUI before any network IO
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    // Create event channel
    let (event_tx, event_rx) = mpsc::channel::<AppEvent>(500);

    app.flash = Some(crate::app::FlashMessage::info("Connecting to cluster..."));
    // Render one frame so the user sees the TUI immediately
    terminal.draw(|f| crate::ui::draw(f, &mut app))?;

    use crate::kube::client_session::ClientSession;

    // Both modes now use ClientSession — daemon connects via Unix socket,
    // --no-daemon spawns a local ServerSession over an in-memory duplex.
    let connect_result = if !cli.no_daemon {
        // ---- Daemon mode: connect via Unix socket ----
        ClientSession::connect(
            Some(current_context.as_str()),
            Some(app.selected_ns.as_str()),
            cli.readonly,
            event_tx.clone(),
        ).await
    } else {
        // ---- Local mode (--no-daemon): in-memory duplex + local ServerSession ----
        ClientSession::connect_local(
            Some(current_context.as_str()),
            Some(app.selected_ns.as_str()),
            cli.readonly,
            event_tx.clone(),
        ).await
    };

    let data_source = match connect_result {
        Ok((cs, info)) => {
            // Apply server-provided info to app state.
            app.context = info.context.clone();
            app.cluster = info.cluster.clone();
            app.user = info.user.clone();
            if !info.namespaces.is_empty() {
                let ns_items = crate::kube::cache::cached_namespaces_to_domain(&info.namespaces);
                app.data.namespaces.set_items(ns_items);
            }
            // Subscribe to the initial tab — but only if we're going straight
            // to Resources (CLI --command flag). In Overview mode, defer all
            // subscriptions until the user navigates to a resource view.
            let mut ds = cs;
            if app.route == crate::app::Route::Resources {
                let initial_rt = crate::kube::client_session::tab_resource_type(app.resource_tab);
                if initial_rt != "namespaces" && initial_rt != "nodes" {
                    ds.switch_tab(app.resource_tab).await;
                }
            }
            app.flash = None;
            ds
        }
        Err(e) => {
            // Clean up terminal before printing error
            disable_raw_mode()?;
            execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
            if !cli.no_daemon {
                eprintln!(
                    "Daemon not running or connection failed: {}\n\n\
                     Start it with:  k9rs daemon\n\
                     Or run locally:  k9rs --no-daemon\n",
                    e
                );
            } else {
                eprintln!(
                    "Error: Unable to connect to Kubernetes cluster.\n\n\
                     {}\n\n\
                     Make sure:\n  \
                     - Your kubeconfig is valid (~/.kube/config)\n  \
                     - The cluster is reachable\n  \
                     - Your credentials haven't expired\n\n\
                     Tip: Run 'kubectl cluster-info' to verify connectivity.",
                    e
                );
            }
            std::process::exit(1);
        }
    };

    let tick_rate = Duration::from_millis(cli.tick_rate);

    // Bridge crossterm EventStream into an mpsc channel so session_main
    // receives events through a uniform channel interface.
    let (input_tx, input_rx) =
        mpsc::channel::<crossterm::event::Event>(100);
    tokio::spawn(async move {
        let mut event_stream = EventStream::new();
        while let Some(Ok(ev)) = event_stream.next().await {
            if input_tx.send(ev).await.is_err() {
                break;
            }
        }
    });

    // Run the TUI event loop (all logic lives in session.rs)
    let result = crate::kube::session::session_main(
        app,
        data_source,
        terminal,
        event_tx,
        event_rx,
        input_rx,
        tick_rate,
    )
    .await;

    // Ensure the terminal is always restored, even if session_main returned Err.
    // (session_main also restores on success, but this catches error paths.)
    let _ = disable_raw_mode();
    let _ = execute!(io::stdout(), crossterm::cursor::SetCursorStyle::DefaultUserShape, LeaveAlternateScreen);

    result
}
