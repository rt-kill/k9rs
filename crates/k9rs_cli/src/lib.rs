//! The non-TUI entry points: `k9rs daemon`, `k9rs ctl …`, `k9rs contexts`.
//!
//! A sibling of the TUI, not a layer under it: these three subcommands talk
//! to the daemon (or to the kubeconfig) and never build an `App`, so this
//! crate depends on `k9rs_daemon` and `k9rs_proto` and knows nothing about
//! `app`, `ui` or `session`. `main.rs` dispatches to whichever side the
//! command line asked for.

pub(crate) mod contexts;
pub(crate) mod ctl;

use anyhow::Result;
use clap::Subcommand;

/// How `ctl` prints its answer.
///
/// A typed `ValueEnum` rather than a `String` compared against `"json"`.
/// With the string, `--output jsom` parsed fine, `output == "json"` was
/// false, and you got human output and exit 0 — a typo in a script read as
/// success. clap refuses an unknown value here and names the ones that
/// exist, which is what asking for a format should cost.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    /// Aligned columns, for reading.
    Human,
    /// One JSON document, for scripts.
    Json,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the cache daemon (foreground, stays alive until killed)
    Daemon,

    /// Daemon management commands
    Ctl {
        /// Output format.
        #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
        output: OutputFormat,
        #[command(subcommand)]
        cmd: ctl::CtlCommand,
    },

    /// List cached contexts from disk
    Contexts,
}

pub async fn dispatch(cmd: Command) -> Result<()> {
    match cmd {
        Command::Daemon => {
            // Set up logging to stderr for the daemon process.
            let filter = tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_writer(std::io::stderr)
                .init();
            // What loading the config had to say, now that there is
            // somewhere to say it. `main` loads it before any subscriber
            // exists — deliberately, so one site serves every entry point —
            // so the complaint is carried rather than logged, and this is
            // the daemon's turn to report it. It went nowhere at all
            // before: not even `--log-file` caught it.
            for problem in k9rs_kube::daemon_config::load_problems() {
                tracing::warn!("{problem}");
            }
            let result = k9rs_daemon::daemon::run_daemon().await;
            // `ctl stop` must actually stop the process. Under `#[tokio::main]`
            // the runtime's Drop waits for every started blocking closure, and
            // one of those is the exec credential plugin — a `waitpid` kube-rs
            // owns, on a child no `Drop` of ours can kill. A plugin waiting on
            // a browser therefore kept the daemon alive AFTER it had unlinked
            // its socket and logged "daemon stopped", so the next `k9rs`
            // started a second daemon beside the zombie. Exit on our own terms
            // instead of waiting for a thread that is never coming back.
            if let Err(e) = &result {
                tracing::error!("daemon exited with error: {e}");
            }
            // Flush tracing before the process ends.
            std::io::Write::flush(&mut std::io::stderr()).ok();
            std::process::exit(if result.is_ok() { 0 } else { 1 });
        }
        Command::Ctl { output, cmd } => {
            ctl::run(cmd, output == OutputFormat::Json).await
        }
        Command::Contexts => {
            contexts::run()
        }
    }
}

#[cfg(test)]
#[path = "tests/lib.rs"]
mod tests;
