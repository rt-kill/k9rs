use serde::Deserialize;

// ---------------------------------------------------------------------------
// Typed metrics structs (deserialized from DynamicObject.data)
// ---------------------------------------------------------------------------

/// Pod metrics from the metrics.k8s.io API.
#[derive(Deserialize)]
struct PodMetricsData {
    #[serde(default)]
    containers: Vec<ContainerMetrics>,
}

#[derive(Deserialize)]
struct ContainerMetrics {
    #[serde(default)]
    usage: ResourceUsage,
}

/// Node metrics from the metrics.k8s.io API.
#[derive(Deserialize)]
struct NodeMetricsData {
    #[serde(default)]
    usage: ResourceUsage,
}

#[derive(Deserialize, Default)]
struct ResourceUsage {
    #[serde(default)]
    cpu: Option<String>,
    #[serde(default)]
    memory: Option<String>,
}

// ---------------------------------------------------------------------------
// Conversion from DynamicObject.data → typed metrics
// ---------------------------------------------------------------------------

impl From<&serde_json::Value> for PodMetricsData {
    fn from(data: &serde_json::Value) -> Self {
        serde_json::from_value(data.clone()).unwrap_or(PodMetricsData { containers: vec![] })
    }
}

impl From<&serde_json::Value> for NodeMetricsData {
    fn from(data: &serde_json::Value) -> Self {
        serde_json::from_value(data.clone()).unwrap_or(NodeMetricsData {
            usage: ResourceUsage::default(),
        })
    }
}

// ---------------------------------------------------------------------------
// Public API — typed access, no raw JSON navigation
// ---------------------------------------------------------------------------

/// Parse pod metrics from a DynamicObject's data field.
/// Sums CPU and memory across all containers.
pub fn parse_pod_metrics_usage(data: &serde_json::Value) -> crate::kube::protocol::MetricsUsage {
    let metrics = PodMetricsData::from(data);

    let mut total_cpu_nano: u64 = 0;
    let mut total_mem_bytes: u64 = 0;

    for container in &metrics.containers {
        if let Some(ref cpu_str) = container.usage.cpu {
            total_cpu_nano += parse_cpu_to_nano(cpu_str);
        }
        if let Some(ref mem_str) = container.usage.memory {
            total_mem_bytes += parse_mem_to_bytes(mem_str);
        }
    }

    let cpu = crate::util::format_cpu(&format!("{}n", total_cpu_nano));
    let mem = crate::util::format_mem(&total_mem_bytes.to_string());
    crate::kube::protocol::MetricsUsage { cpu, mem }
}

/// Parse node metrics from a DynamicObject's data field.
pub fn parse_node_metrics_usage(data: &serde_json::Value) -> crate::kube::protocol::MetricsUsage {
    let metrics = NodeMetricsData::from(data);

    let cpu = metrics.usage.cpu
        .as_deref()
        .map(|s| crate::util::format_cpu(s))
        .unwrap_or_else(|| "n/a".to_string());

    let mem = metrics.usage.memory
        .as_deref()
        .map(|s| crate::util::format_mem(s))
        .unwrap_or_else(|| "n/a".to_string());

    crate::kube::protocol::MetricsUsage { cpu, mem }
}

// ---------------------------------------------------------------------------
// Unit parsing helpers
// ---------------------------------------------------------------------------

fn parse_cpu_to_nano(s: &str) -> u64 {
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

fn parse_mem_to_bytes(s: &str) -> u64 {
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
