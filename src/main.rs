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
    /// Color scheme: auto (detect the terminal background), dark, or light.
    /// Overrides `ui.theme` from the config file for this run.
    #[arg(long, value_name = "MODE")]
    theme: Option<crate::ui::theme::ThemeMode>,

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

    // Subcommand dispatch: daemon, ctl, contexts
    if let Some(subcmd) = cli.subcmd {
        return crate::cli::dispatch(subcmd).await;
    }

    // Setup logging
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    if let Some(ref log_file) = cli.log_file {
        // 0600: the log can contain resource data (and, at debug level,
        // command bodies) — it must not be world-readable via a lax umask.
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(log_file)?;
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
    // `--context ""` is a typo, not a request: reject it loudly here rather
    // than letting a nameless context reach the daemon.
    let cli_context: Option<crate::kube::protocol::ContextName> =
        match cli.context.as_deref().map(crate::kube::protocol::ContextName::try_from).transpose() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("k9rs: --context: {e}");
                std::process::exit(2);
            }
        };

    // Parse startup navigation from positional args or legacy -c flag.
    let startup_segments = parse_startup_segments(&cli);

    let (event_tx, event_rx) =
        mpsc::channel::<AppEvent>(crate::kube::session::EVENT_CHANNEL_CAP);

    use crate::kube::client_session::ClientSession;

    // Load + validate the user config BEFORE any TUI setup: a config
    // error prints to a normal, visible stderr and exits. The strict
    // validation (unknown fields, malformed/reserved key chords) is
    // only worth anything if its rejection reaches the user — a silent
    // fallback to defaults would quietly drop settings like readOnly.
    let mut config = match App::load_config() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("k9rs: config error: {e}");
            std::process::exit(2);
        }
    };
    // `--theme` wins over `ui.theme` for this run.
    if let Some(mode) = cli.theme {
        config.ui.theme = mode;
    }
    // Resolve to a concrete palette HERE, at the edge. `auto` asks the
    // terminal for its background colour over `/dev/tty`, which must happen
    // before raw mode and the alternate screen (both below) so the reply
    // can't surface as a phantom keypress — and outside `App::new`, which
    // stays free of I/O so the test constructor doesn't touch a terminal.
    let appearance = config.ui.theme.resolve();

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

    // `None`: no context is confirmed until the daemon says so (or, with
    // nothing to connect to, until the user picks one in the contexts view).
    let mut app = App::new(appearance, None, namespace, &data_source, config);
    if cli.readonly {
        app.read_only = true;
    }

    // Overlays are the one user-facing extension point, and until now every
    // complaint they had went to `warn!` — i.e. to `io::sink` unless the user
    // passed `--log-file`. A typo'd column, an unknown capability, a bad
    // jsonpath and a YAML parse error all looked identical to "working". Say
    // the first one out loud; the count tells them there are more to fix.
    let overlay_problems = crate::kube::overlay::overlay_diagnostics();
    if let Some(first) = overlay_problems.first() {
        let more = overlay_problems.len() - 1;
        app.ui.flash = Some(crate::app::FlashMessage::warn(if more == 0 {
            first.clone()
        } else {
            format!("{first} (+{more} more overlay problem{})", if more == 1 { "" } else { "s" })
        }));
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
    // Reclaims `/tmp/k9rs-<pid>/` on the way out, including the `?` paths —
    // the same reason `TerminalGuard` exists one line up. Removes the
    // directory only when empty, so anything the user asked us to save stays.
    let _temp_dir_guard = crate::util::TempDirGuard;

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
        /// Why the stream is DROPPED on suspend rather than merely unpolled.
        ///
        /// crossterm's `EventStream` arms a helper thread that sits in
        /// `poll_internal(None, ..)` — an indefinite blocking read of fd 0 —
        /// and that thread is shut down ONLY by `EventStream::drop`. Dropping
        /// the `Next` future does nothing to it. So an "unpolled" stream is
        /// still an active reader of stdin, and while `$EDITOR` or a shell
        /// owns the terminal, the first keystroke aimed at them could be
        /// swallowed by crossterm instead — then buffered, then replayed into
        /// the TUI as type-ahead once the bridge resumed. A `:` typed in vim
        /// could open the command bar after the editor exited.
        ///
        /// `Drop` is the supported release: crossterm's own note explains that
        /// it both sets the shutdown flag AND wakes the blocked
        /// `poll_internal`, so the thread returns without consuming a byte.
        ///
        /// Recreating on resume loses nothing. The internal event reader is
        /// global (`lock_internal_event_reader`), so anything already parsed
        /// survives the handover, and anything typed during the window is
        /// still sitting unread in the kernel's tty buffer — which is exactly
        /// where input for a suspended TUI belongs.
        enum Wake {
            Suspend(Result<(), tokio::sync::watch::error::RecvError>),
            Input(Option<Result<crossterm::event::Event, std::io::Error>>),
        }

        let mut event_stream = Some(EventStream::new());
        loop {
            let Some(stream) = event_stream.as_mut() else {
                // Suspended: no stream, so nothing is reading stdin. Wait for
                // the resume edge and build a fresh one.
                if suspend_rx.changed().await.is_err() {
                    break;
                }
                if !*suspend_rx.borrow() {
                    event_stream = Some(EventStream::new());
                }
                continue;
            };

            let wake = tokio::select! {
                biased;
                res = suspend_rx.changed() => Wake::Suspend(res),
                ev = stream.next() => Wake::Input(ev),
            };

            match wake {
                // A closed watch resolves Ready(Err) forever. Ignoring it
                // meant `biased` re-polled the same always-ready branch and
                // the bridge span on CPU for as long as it lived.
                Wake::Suspend(Err(_)) => break,
                Wake::Suspend(Ok(())) => {
                    if *suspend_rx.borrow() {
                        // Release stdin BEFORE acking: the ack is what the
                        // suspend guard takes as "the terminal is yours".
                        event_stream = None;
                        let _ = suspend_ack_tx.send(()).await;
                    }
                }
                Wake::Input(Some(Ok(ev))) => {
                    if input_tx.send(ev).await.is_err() {
                        break;
                    }
                }
                Wake::Input(Some(Err(_))) => continue,
                Wake::Input(None) => break,
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
                &app.core,
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
