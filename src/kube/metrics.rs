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
/// Sums CPU and memory across all containers. All arithmetic is saturating
/// so a malformed metrics payload (`9999999999999Ti`) cannot panic in
/// debug or wrap silently in release.
pub fn parse_pod_metrics_usage(data: &serde_json::Value) -> crate::kube::protocol::MetricsUsage {
    let metrics = PodMetricsData::from(data);

    let mut total_cpu_nano: u64 = 0;
    let mut total_mem_bytes: u64 = 0;

    for container in &metrics.containers {
        if let Some(ref cpu_str) = container.usage.cpu {
            total_cpu_nano = total_cpu_nano.saturating_add(parse_cpu_to_nano(cpu_str));
        }
        if let Some(ref mem_str) = container.usage.memory {
            total_mem_bytes = total_mem_bytes.saturating_add(parse_mem_to_bytes(mem_str));
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
        .map(crate::util::format_cpu)
        .unwrap_or_else(|| "n/a".to_string());

    let mem = metrics.usage.memory
        .as_deref()
        .map(crate::util::format_mem)
        .unwrap_or_else(|| "n/a".to_string());

    crate::kube::protocol::MetricsUsage { cpu, mem }
}

// ---------------------------------------------------------------------------
// Unit parsing helpers
// ---------------------------------------------------------------------------

fn parse_cpu_to_nano(s: &str) -> u64 {
    let s = s.trim();
    // K8s Quantity (for CPU) accepts:
    //   "500n"  → nanocores
    //   "500u"  → microcores (n * 1000)
    //   "500m"  → millicores (n * 1_000_000)
    //   "0.5"   → cores       (n * 1_000_000_000)
    // Older code missed the "u" suffix and silently rendered 0.
    if let Some(nano_str) = s.strip_suffix('n') {
        nano_str.parse::<u64>().unwrap_or(0)
    } else if let Some(micro_str) = s.strip_suffix('u') {
        micro_str.parse::<u64>().unwrap_or(0).saturating_mul(1_000)
    } else if let Some(milli_str) = s.strip_suffix('m') {
        milli_str.parse::<u64>().unwrap_or(0).saturating_mul(1_000_000)
    } else if let Ok(cores) = s.parse::<f64>() {
        // f64 → u64 saturating cast: out-of-range values clamp to u64::MAX
        // (or 0 for negative). Avoids the panic-in-debug / wrap-in-release
        // behavior of the older `as u64` form.
        if cores.is_finite() && cores >= 0.0 {
            (cores * 1_000_000_000.0).min(u64::MAX as f64) as u64
        } else {
            0
        }
    } else {
        0
    }
}

fn parse_mem_to_bytes(s: &str) -> u64 {
    // K8s Quantity permits BOTH binary (Ki/Mi/Gi/Ti, base-1024) AND
    // decimal (K/M/G/T, base-1000) suffixes. Older code only handled the
    // binary ones, so decimal payloads fell through and silently rendered
    // 0. Saturating multiply throughout — malformed giant values clamp to
    // u64::MAX rather than wrapping.
    let s = s.trim();

    // Two-char binary suffixes first (otherwise "Ti" would match the
    // one-char "T" branch and consume only the leading 'T').
    if let Some(val) = s.strip_suffix("Ti") {
        return val.parse::<u64>().unwrap_or(0).saturating_mul(1024u64.pow(4));
    }
    if let Some(val) = s.strip_suffix("Gi") {
        return val.parse::<u64>().unwrap_or(0).saturating_mul(1024u64.pow(3));
    }
    if let Some(val) = s.strip_suffix("Mi") {
        return val.parse::<u64>().unwrap_or(0).saturating_mul(1024u64.pow(2));
    }
    if let Some(val) = s.strip_suffix("Ki") {
        return val.parse::<u64>().unwrap_or(0).saturating_mul(1024);
    }
    // One-char decimal suffixes per the K8s Quantity spec
    // (E, P, T, G, M, k — note the *lowercase* k for kilo). Some real-world
    // configs use uppercase 'K' anyway; accept both to avoid silently
    // dropping a value to "0 bytes" on a typo.
    if let Some(val) = s.strip_suffix('T') {
        return val.parse::<u64>().unwrap_or(0).saturating_mul(1_000_000_000_000);
    }
    if let Some(val) = s.strip_suffix('G') {
        return val.parse::<u64>().unwrap_or(0).saturating_mul(1_000_000_000);
    }
    if let Some(val) = s.strip_suffix('M') {
        return val.parse::<u64>().unwrap_or(0).saturating_mul(1_000_000);
    }
    if let Some(val) = s.strip_suffix('k').or_else(|| s.strip_suffix('K')) {
        return val.parse::<u64>().unwrap_or(0).saturating_mul(1_000);
    }
    // No suffix → raw bytes.
    s.parse::<u64>().unwrap_or(0)
}
