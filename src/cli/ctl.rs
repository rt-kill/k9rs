use std::time::Instant;

use anyhow::{bail, Result};
use clap::Subcommand;

use crate::kube::daemon::DaemonClient;
use crate::kube::protocol::{SessionCommand, SessionEvent};

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
    },
}

pub async fn run(cmd: CtlCommand, json: bool) -> Result<()> {
    match cmd {
        CtlCommand::Status => cmd_status(json).await,
        CtlCommand::Ping => cmd_ping(json).await,
        CtlCommand::Stop => cmd_stop(json).await,
        CtlCommand::Clear { context } => cmd_clear(context, json).await,
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

async fn cmd_status(json: bool) -> Result<()> {
    let mut dc = connect_or_bail().await?;
    let resp = dc.request(&SessionCommand::Status).await;
    match resp {
        Some(SessionEvent::DaemonStatus(status)) => {
            if json {
                println!("{}", serde_json::json!({
                    "status": "running",
                    "pid": status.pid,
                    "socket": status.socket_path,
                    "uptime_secs": status.uptime_secs,
                }));
            } else {
                let uptime = format_duration(status.uptime_secs);
                println!("Daemon:     running (pid {})", status.pid);
                println!("Socket:     {}", status.socket_path);
                println!("Uptime:     {}", uptime);
            }
        }
        _ => {
            if json {
                println!("{}", serde_json::json!({"status": "not_running"}));
            } else {
                println!("Daemon: not running");
                println!("Socket: {:?} (not found)", crate::kube::daemon::socket_path());
            }
        }
    }
    Ok(())
}

async fn cmd_ping(json: bool) -> Result<()> {
    let mut dc = connect_or_bail().await?;
    let start = Instant::now();
    let resp = dc.request(&SessionCommand::Ping).await;
    let elapsed = start.elapsed();
    match resp {
        Some(SessionEvent::CommandResult { ok: true, .. }) => {
            if json {
                println!("{}", serde_json::json!({"alive": true, "latency_ms": elapsed.as_millis() as u64}));
            } else {
                println!("Daemon is alive at {:?} ({}ms)", crate::kube::daemon::socket_path(), elapsed.as_millis());
            }
        }
        _ => bail!("Daemon connected but not responding"),
    }
    Ok(())
}

async fn cmd_stop(json: bool) -> Result<()> {
    let mut dc = connect_or_bail().await?;
    let resp = dc.request(&SessionCommand::Shutdown).await;
    match resp {
        Some(SessionEvent::CommandResult { ok: true, .. }) => {
            if json {
                println!("{}", serde_json::json!({"ok": true, "message": "shutting down"}));
            } else {
                println!("Daemon shutting down");
            }
        }
        _ => bail!("Failed to send shutdown request"),
    }
    Ok(())
}

async fn cmd_clear(context: Option<String>, json: bool) -> Result<()> {
    let mut dc = connect_or_bail().await?;
    let resp = dc.request(&SessionCommand::Clear { context: context.clone() }).await;
    match resp {
        Some(SessionEvent::CommandResult { ok: true, message }) => {
            if json {
                println!("{}", serde_json::json!({"ok": true, "message": message}));
            } else {
                println!("{}", message);
            }
        }
        _ => bail!("Failed to clear cache"),
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
