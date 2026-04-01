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
            crate::kube::daemon::run_daemon().await
        }
        Command::Ctl { cmd } => {
            ctl::run(cmd).await
        }
        Command::Contexts => {
            contexts::run()
        }
    }
}
