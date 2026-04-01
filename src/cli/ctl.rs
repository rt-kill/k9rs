use std::time::Instant;

use anyhow::{bail, Result};
use clap::Subcommand;

use crate::kube::daemon::DaemonClient;
use crate::kube::protocol::{Request, Response};

#[derive(Subcommand, Debug)]
pub enum CtlCommand {
    /// Show daemon status (pid, uptime)
    Status,

    /// Ping the daemon and measure round-trip time
    Ping,

    /// Stop the daemon gracefully
    Stop,

    /// Clear cached data
    Clear {
        /// Context to clear (clears all if omitted)
        context: Option<String>,

        /// Also remove disk cache files
        #[arg(long)]
        disk: bool,
    },
}

pub async fn run(cmd: CtlCommand) -> Result<()> {
    match cmd {
        CtlCommand::Status => cmd_status().await,
        CtlCommand::Ping => cmd_ping().await,
        CtlCommand::Stop => cmd_stop().await,
        CtlCommand::Clear { context, disk } => cmd_clear(context, disk).await,
    }
}

async fn connect_or_bail() -> Result<DaemonClient> {
    match DaemonClient::connect().await {
        Some(dc) => Ok(dc),
        None => {
            bail!(
                "Daemon not running (socket: {:?})",
                crate::kube::daemon::socket_path()
            );
        }
    }
}

async fn cmd_status() -> Result<()> {
    let mut dc = connect_or_bail().await?;
    let resp = dc.request(&Request::Status).await;
    match resp {
        Some(Response::Status(status)) => {
            let uptime = format_duration(status.uptime_secs);
            println!("Daemon:     running (pid {})", status.pid);
            println!("Socket:     {}", status.socket_path);
            println!("Uptime:     {}", uptime);
        }
        _ => {
            println!("Daemon: not running");
            println!(
                "Socket: {:?} (not found)",
                crate::kube::daemon::socket_path()
            );
        }
    }
    Ok(())
}

async fn cmd_ping() -> Result<()> {
    let mut dc = connect_or_bail().await?;
    let start = Instant::now();
    let resp = dc.request(&Request::Ping).await;
    let elapsed = start.elapsed();
    match resp {
        Some(Response::Ok) => {
            println!(
                "Daemon is alive at {:?} ({}ms)",
                crate::kube::daemon::socket_path(),
                elapsed.as_millis()
            );
        }
        _ => {
            bail!("Daemon connected but not responding");
        }
    }
    Ok(())
}

async fn cmd_stop() -> Result<()> {
    let mut dc = connect_or_bail().await?;
    let resp = dc.request(&Request::Shutdown).await;
    match resp {
        Some(Response::Ok) => println!("Daemon shutting down"),
        _ => bail!("Failed to send shutdown request"),
    }
    Ok(())
}

async fn cmd_clear(context: Option<String>, disk: bool) -> Result<()> {
    let mut dc = connect_or_bail().await?;
    let resp = dc
        .request(&Request::Clear {
            context: context.clone(),
        })
        .await;
    match resp {
        Some(Response::Ok) => {
            match &context {
                Some(ctx) => println!("Cleared cache for context '{}'", ctx),
                None => println!("Cleared all cache entries"),
            }
        }
        _ => {
            bail!("Failed to clear cache");
        }
    }

    if disk {
        if let Some(cache_dir) = crate::kube::cache::cache_dir() {
            match &context {
                Some(ctx) => {
                    if let Some(path) = crate::kube::cache::cache_path(ctx) {
                        if path.exists() {
                            std::fs::remove_file(&path)?;
                            println!("Removed disk cache: {}", path.display());
                        }
                    }
                }
                None => {
                    if let Ok(entries) = std::fs::read_dir(&cache_dir) {
                        let mut count = 0;
                        for entry in entries.flatten() {
                            if entry.path().extension().map_or(false, |x| x == "json") {
                                std::fs::remove_file(entry.path())?;
                                count += 1;
                            }
                        }
                        println!("Removed {} disk cache files", count);
                    }
                }
            }
        }
    }
    Ok(())
}

fn format_duration(secs: u64) -> String {
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let mins = (secs % 3600) / 60;
    let s = secs % 60;
    if days > 0 {
        format!("{}d {}h {}m", days, hours, mins)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, mins, s)
    } else if mins > 0 {
        format!("{}m {}s", mins, s)
    } else {
        format!("{}s", s)
    }
}
