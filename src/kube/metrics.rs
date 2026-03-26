use std::collections::HashMap;
use std::time::Duration;

use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use kube::Api;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::event::AppEvent;

pub fn spawn_metrics_poller(
    client: ::kube::Client,
    tx: mpsc::Sender<AppEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Small initial delay to let core watchers get data first
        tokio::time::sleep(Duration::from_secs(2)).await;

        loop {
            // Fetch pod metrics
            let pod_ar = ApiResource::from_gvk_with_plural(
                &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics"),
                "pods",
            );
            let pod_api: Api<DynamicObject> = Api::all_with(client.clone(), &pod_ar);
            if let Ok(list) = pod_api.list(&ListParams::default()).await {
                let mut metrics: HashMap<(String, String), (String, String)> = HashMap::new();
                for item in &list.items {
                    let ns = item.metadata.namespace.clone().unwrap_or_default();
                    let name = item.metadata.name.clone().unwrap_or_default();
                    let (cpu, mem) = parse_pod_metrics_usage(&item.data);
                    metrics.insert((ns, name), (cpu, mem));
                }
                let _ = tx.send(AppEvent::PodMetrics(metrics)).await;
            }

            // Fetch node metrics
            let node_ar = ApiResource::from_gvk_with_plural(
                &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "NodeMetrics"),
                "nodes",
            );
            let node_api: Api<DynamicObject> = Api::all_with(client.clone(), &node_ar);
            if let Ok(list) = node_api.list(&ListParams::default()).await {
                let mut metrics: HashMap<String, (String, String)> = HashMap::new();
                for item in &list.items {
                    let name = item.metadata.name.clone().unwrap_or_default();
                    let (cpu, mem) = parse_node_metrics_usage(&item.data);
                    metrics.insert(name, (cpu, mem));
                }
                let _ = tx.send(AppEvent::NodeMetrics(metrics)).await;
            }

            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    })
}

/// Parse the `usage` field from a PodMetrics DynamicObject.
/// PodMetrics has `containers: [{name, usage: {cpu, memory}}]`.
/// Sums CPU and memory across all containers.
pub fn parse_pod_metrics_usage(data: &serde_json::Value) -> (String, String) {
    let containers = match data.get("containers").and_then(|c| c.as_array()) {
        Some(c) => c,
        None => return ("n/a".to_string(), "n/a".to_string()),
    };

    let mut total_cpu_nano: u64 = 0;
    let mut total_mem_bytes: u64 = 0;

    for container in containers {
        if let Some(usage) = container.get("usage") {
            if let Some(cpu_str) = usage.get("cpu").and_then(|v| v.as_str()) {
                total_cpu_nano += parse_cpu_to_nano(cpu_str);
            }
            if let Some(mem_str) = usage.get("memory").and_then(|v| v.as_str()) {
                total_mem_bytes += parse_mem_to_bytes(mem_str);
            }
        }
    }

    let cpu = crate::util::format_cpu(&format!("{}n", total_cpu_nano));
    let mem = crate::util::format_mem(&total_mem_bytes.to_string());
    (cpu, mem)
}

/// Parse the `usage` field from a NodeMetrics DynamicObject.
/// NodeMetrics has `usage: {cpu, memory}` directly.
pub fn parse_node_metrics_usage(data: &serde_json::Value) -> (String, String) {
    let usage = match data.get("usage") {
        Some(u) => u,
        None => return ("n/a".to_string(), "n/a".to_string()),
    };

    let cpu = usage
        .get("cpu")
        .and_then(|v| v.as_str())
        .map(|s| crate::util::format_cpu(s))
        .unwrap_or_else(|| "n/a".to_string());

    let mem = usage
        .get("memory")
        .and_then(|v| v.as_str())
        .map(|s| crate::util::format_mem(s))
        .unwrap_or_else(|| "n/a".to_string());

    (cpu, mem)
}

/// Parse a CPU string (e.g. "250000000n", "500m", "2") to nanocores.
pub fn parse_cpu_to_nano(s: &str) -> u64 {
    let s = s.trim();
    if let Some(nano_str) = s.strip_suffix('n') {
        nano_str.parse::<u64>().unwrap_or(0)
    } else if let Some(milli_str) = s.strip_suffix('m') {
        milli_str.parse::<u64>().unwrap_or(0) * 1_000_000
    } else if let Ok(cores) = s.parse::<f64>() {
        (cores * 1_000_000_000.0) as u64
    } else {
        0
    }
}

/// Parse a memory string (e.g. "131072Ki", "256Mi", "1073741824") to bytes.
pub fn parse_mem_to_bytes(s: &str) -> u64 {
    let s = s.trim();
    if let Some(val) = s.strip_suffix("Ti") {
        val.parse::<u64>().unwrap_or(0) * 1024 * 1024 * 1024 * 1024
    } else if let Some(val) = s.strip_suffix("Gi") {
        val.parse::<u64>().unwrap_or(0) * 1024 * 1024 * 1024
    } else if let Some(val) = s.strip_suffix("Mi") {
        val.parse::<u64>().unwrap_or(0) * 1024 * 1024
    } else if let Some(val) = s.strip_suffix("Ki") {
        val.parse::<u64>().unwrap_or(0) * 1024
    } else {
        s.parse::<u64>().unwrap_or(0)
    }
}
