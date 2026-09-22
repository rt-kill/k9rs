pub mod contexts;
pub mod ctl;

use anyhow::Result;
use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the cache daemon (foreground, stays alive until killed)
    Daemon,

    /// Daemon management commands
    Ctl {
        /// Output format: "human" (default) or "json"
        #[arg(long, default_value = "human")]
        output: String,
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
            let result = crate::kube::daemon::run_daemon().await;
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
            ctl::run(cmd, output == "json").await
        }
        Command::Contexts => {
            contexts::run()
        }
    }
}
