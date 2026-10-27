use std::time::Instant;

use anyhow::{bail, Result};
use clap::Subcommand;

use k9rs_daemon::daemon::DaemonClient;
use k9rs_proto::protocol::{ContextName, SessionCommand, SessionEvent};

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
        context: Option<ContextName>,
    },
}

pub(crate) async fn run(cmd: CtlCommand, json: bool) -> Result<()> {
    match cmd {
        CtlCommand::Status => cmd_status(json).await,
        CtlCommand::Ping => cmd_ping(json).await,
        CtlCommand::Stop => cmd_stop(json).await,
        CtlCommand::Clear { context } => cmd_clear(context, json).await,
    }
}

async fn connect_or_bail() -> Result<DaemonClient> {
    // Connect errors are already actionable ("daemon not running", or a
    // version mismatch from the v10 management handshake naming both
    // versions and suggesting a daemon restart).
    DaemonClient::connect().await
}

async fn cmd_status(json: bool) -> Result<()> {
    let mut dc = connect_or_bail().await?;
    let resp = dc.request(&SessionCommand::Status).await;
    match resp {
        Some(SessionEvent::DaemonStatus(status)) => {
            let c = &status.caches;
            if json {
                // `serde_json` at the OUTPUT boundary only — the payload
                // arrived typed and is serialized here because the caller
                // asked for JSON, which is exactly what the boundary rule
                // permits.
                println!("{}", serde_json::json!({
                    "status": "running",
                    "pid": status.pid,
                    "socket": status.socket_path,
                    "uptime_secs": status.uptime_secs,
                    "caches": c,
                }));
            } else {
                let uptime = format_duration(status.uptime_secs);
                println!("Daemon:     running (pid {})", status.pid);
                println!("Socket:     {}", status.socket_path);
                println!("Uptime:     {}", uptime);

                let forwards: usize = c.contexts.iter().map(|x| x.port_forwards).sum();
                println!(
                    "Holding:    {} watcher(s), {} context(s), {} port-forward(s)",
                    c.watchers.len(),
                    c.contexts.len(),
                    forwards,
                );
                println!(
                    "Caches:     {} discovery, {} metrics poller(s), {} discovery poller(s)",
                    c.discovery_entries, c.metrics_pollers, c.discovery_pollers,
                );

                if !c.contexts.is_empty() {
                    println!("\nContexts:");
                    // Width from the DATA, not a guess. Fixed `{:<24}`/`{:<40}`
                    // looked fine against short fixtures and fell apart on the
                    // first real cluster: `unsafe-crawling-cluster-production`
                    // is 34 characters and an EKS endpoint is 64, so every
                    // column shifted by a different amount per row and the
                    // table stopped being a table.
                    let name_w = c
                        .contexts
                        .iter()
                        .map(|x| x.context.as_str().len())
                        .max()
                        .unwrap_or(0);
                    let url_w = c.contexts.iter().map(|x| x.server_url.len()).max().unwrap_or(0);
                    for ctx in &c.contexts {
                        println!(
                            "  {:<name_w$}  {:<url_w$}  {} pf, {} holder(s)",
                            ctx.context.as_str(),
                            ctx.server_url,
                            ctx.port_forwards,
                            ctx.holders,
                        );
                    }
                }
                if !c.watchers.is_empty() {
                    println!("\nWatchers:");
                    // Same rule — and no truncation, because a context name
                    // clipped to `unsafe-crawling-cluster…` is exactly the
                    // information you came here for.
                    let w_ctx = c
                        .watchers
                        .iter()
                        .map(|w| w.context.as_str().len())
                        .max()
                        .unwrap_or(0);
                    let w_res = c.watchers.iter().map(|w| w.resource.len()).max().unwrap_or(0);
                    let w_ns = c.watchers.iter().map(|w| w.namespace.len()).max().unwrap_or(0);
                    for w in &c.watchers {
                        let filter = w.filter.as_deref().unwrap_or("-");
                        println!(
                            "  {:<w_ctx$}  {:<w_res$}  {:<w_ns$}  {:<10} {} holder(s)",
                            w.context.as_str(),
                            w.resource,
                            w.namespace,
                            filter,
                            w.holders,
                        );
                    }
                }
            }
        }
        _ => {
            if json {
                println!("{}", serde_json::json!({"status": "not_running"}));
            } else {
                println!("Daemon: not running");
                println!("Socket: {:?} (not found)", k9rs_daemon::daemon::socket_path());
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
        Some(SessionEvent::CommandResult(Ok(_))) => {
            if json {
                println!("{}", serde_json::json!({"alive": true, "latency_ms": elapsed.as_millis() as u64}));
            } else {
                println!("Daemon is alive at {:?} ({}ms)", k9rs_daemon::daemon::socket_path(), elapsed.as_millis());
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
        Some(SessionEvent::CommandResult(Ok(_))) => {
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

async fn cmd_clear(context: Option<ContextName>, json: bool) -> Result<()> {
    let mut dc = connect_or_bail().await?;
    let resp = dc.request(&SessionCommand::Clear { context: context.clone() }).await;
    match resp {
        Some(SessionEvent::CommandResult(Ok(message))) => {
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
