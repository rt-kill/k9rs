use std::borrow::Cow;

use anyhow::{bail, Result};

use crate::kube::daemon::DaemonClient;
use crate::kube::resources::KubeResource;

/// Cached resource types and the labels used as daemon cache keys.
const CACHED_TYPES: &[(&str, &[&str])] = &[
    ("Pods", &["po", "pod", "pods"]),
    ("Deploy", &["dp", "deploy", "deployment", "deployments"]),
    ("Svc", &["svc", "service", "services"]),
    ("Nodes", &["no", "node", "nodes"]),
    ("STS", &["sts", "statefulset", "statefulsets"]),
    ("DS", &["ds", "daemonset", "daemonsets"]),
    ("Jobs", &["job", "jobs"]),
    ("CronJobs", &["cj", "cronjob", "cronjobs"]),
];

pub async fn run(
    resource: &str,
    context: Option<&str>,
    namespace: Option<&str>,
    output: &str,
) -> Result<()> {
    let resource_lower = resource.to_lowercase();

    // Resolve user input to the daemon cache key (e.g. "pods" -> "Pods")
    let cache_key = CACHED_TYPES
        .iter()
        .find(|(_key, aliases)| aliases.contains(&resource_lower.as_str()))
        .map(|(key, _)| *key);

    let cache_key = match cache_key {
        Some(k) => k,
        None => {
            let all_aliases: Vec<&str> = CACHED_TYPES
                .iter()
                .flat_map(|(_, aliases)| aliases.iter().copied())
                .collect();
            bail!(
                "Unknown or uncached resource type: '{}'\nCached types: {}",
                resource,
                all_aliases.join(", ")
            );
        }
    };

    // Determine context: user flag, or read from kubeconfig
    let kube_context = match context {
        Some(c) => c.to_string(),
        None => {
            let kc = ::kube::config::Kubeconfig::read()
                .map_err(|e| anyhow::anyhow!("Failed to read kubeconfig: {}", e))?;
            kc.current_context
                .unwrap_or_else(|| "default".to_string())
        }
    };

    // Build the compound cache key (context@cluster) the same way the TUI does.
    // We read the kubeconfig to find the cluster name for this context.
    let cluster = resolve_cluster(&kube_context);
    let daemon_ctx = crate::kube::cache::cache_key(&kube_context, &cluster);

    let ns = namespace.unwrap_or("all");

    let mut dc = match DaemonClient::connect().await {
        Some(dc) => dc,
        None => {
            bail!(
                "Daemon not running (socket: {:?})",
                crate::kube::daemon::socket_path()
            );
        }
    };

    let data = dc.get_resources(&daemon_ctx, ns, cache_key).await;

    match data {
        Some(json_str) => {
            format_output(cache_key, &json_str, output)?;
        }
        None => {
            println!(
                "No cached {} data for context='{}' namespace='{}'",
                resource, kube_context, ns
            );
            println!("Hint: Start a TUI session to populate the cache, or check the context/namespace.");
        }
    }

    Ok(())
}

/// Resolve a kubeconfig context name to its cluster name.
fn resolve_cluster(context: &str) -> String {
    if let Ok(kc) = ::kube::config::Kubeconfig::read() {
        for named_ctx in &kc.contexts {
            if named_ctx.name == context {
                if let Some(ref ctx) = named_ctx.context {
                    return ctx.cluster.clone();
                }
            }
        }
    }
    String::new()
}

fn format_output(cache_key: &str, json_str: &str, output: &str) -> Result<()> {
    use crate::kube::resources::{
        cronjobs::KubeCronJob, daemonsets::KubeDaemonSet, deployments::KubeDeployment,
        jobs::KubeJob, nodes::KubeNode, pods::KubePod, services::KubeService,
        statefulsets::KubeStatefulSet,
    };

    match output {
        "json" => {
            // Pretty-print the raw JSON
            let val: serde_json::Value = serde_json::from_str(json_str)?;
            println!("{}", serde_json::to_string_pretty(&val)?);
        }
        "yaml" => {
            let val: serde_json::Value = serde_json::from_str(json_str)?;
            println!("{}", serde_yaml::to_string(&val)?);
        }
        "table" => {
            match cache_key {
                "Pods" => print_table::<KubePod>(json_str)?,
                "Deploy" => print_table::<KubeDeployment>(json_str)?,
                "Svc" => print_table::<KubeService>(json_str)?,
                "Nodes" => print_table::<KubeNode>(json_str)?,
                "STS" => print_table::<KubeStatefulSet>(json_str)?,
                "DS" => print_table::<KubeDaemonSet>(json_str)?,
                "Jobs" => print_table::<KubeJob>(json_str)?,
                "CronJobs" => print_table::<KubeCronJob>(json_str)?,
                _ => {
                    // Fallback: just dump the JSON
                    let val: serde_json::Value = serde_json::from_str(json_str)?;
                    println!("{}", serde_json::to_string_pretty(&val)?);
                }
            }
        }
        other => {
            anyhow::bail!("Unknown output format: '{}'. Supported: table, json, yaml", other);
        }
    }
    Ok(())
}

fn print_table<T: KubeResource + serde::de::DeserializeOwned>(json_str: &str) -> Result<()> {
    let items: Vec<T> = serde_json::from_str(json_str)?;
    if items.is_empty() {
        println!("No resources found");
        return Ok(());
    }

    let headers = T::headers();

    // Calculate column widths: max of header width and data width
    let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
    let rows: Vec<Vec<Cow<'_, str>>> = items.iter().map(|item| item.row()).collect();
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }

    // Cap column widths at a reasonable max to avoid absurdly wide output
    for w in &mut widths {
        if *w > 60 {
            *w = 60;
        }
    }

    // Print header
    let header_line: Vec<String> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| format!("{:<width$}", h, width = widths[i]))
        .collect();
    println!("{}", header_line.join("  "));

    // Print rows
    for row in &rows {
        let line: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, cell)| {
                let w = widths.get(i).copied().unwrap_or(20);
                let s: &str = cell;
                if s.len() > w {
                    format!("{:.width$}", s, width = w)
                } else {
                    format!("{:<width$}", s, width = w)
                }
            })
            .collect();
        println!("{}", line.join("  "));
    }

    println!("\n{} items", items.len());
    Ok(())
}
