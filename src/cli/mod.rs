pub mod contexts;
pub mod ctl;
pub mod get;

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

    /// Query cached resources from the daemon
    Get {
        /// Resource type (e.g. pods, deploy, svc)
        resource: String,

        /// Context to query (defaults to current kubeconfig context)
        #[arg(long)]
        context: Option<String>,

        /// Namespace to query (defaults to "all")
        #[arg(short, long)]
        namespace: Option<String>,

        /// Output format: table, json, yaml
        #[arg(short, long, default_value = "table")]
        output: String,
    },

    /// List cached contexts from disk
    Contexts,
}

pub async fn dispatch(cmd: Command) -> Result<()> {
    match cmd {
        Command::Daemon => {
            crate::kube::daemon::run_daemon().await
        }
        Command::Ctl { cmd } => {
            ctl::run(cmd).await
        }
        Command::Get { resource, context, namespace, output } => {
            get::run(&resource, context.as_deref(), namespace.as_deref(), &output).await
        }
        Command::Contexts => {
            contexts::run().await
        }
    }
}
