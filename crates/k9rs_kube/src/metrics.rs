use std::collections::HashMap;

use serde::Deserialize;

use k9rs_proto::protocol::{MetricsUsage, NodeName, ObjectKey};

/// A single poll's pod + node usage, fanned out to every session watching a
/// cluster via the shared metrics poller (see [`crate::shared_poller`]).
/// On a failed sub-fetch the prior map is kept intact (last-known), so a
/// transient metrics-server blip never blanks the display.
#[derive(Clone, Default)]
pub struct MetricsSnapshot {
    pub pods: HashMap<ObjectKey, MetricsUsage>,
    pub nodes: HashMap<NodeName, MetricsUsage>,
}

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
        // `&Value` is itself a `Deserializer`, so cast at the JSON boundary by
        // borrow-deserializing — no `data.clone()` of the raw payload.
        PodMetricsData::deserialize(data).unwrap_or(PodMetricsData { containers: vec![] })
    }
}

impl From<&serde_json::Value> for NodeMetricsData {
    fn from(data: &serde_json::Value) -> Self {
        NodeMetricsData::deserialize(data).unwrap_or(NodeMetricsData {
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
pub fn parse_pod_metrics_usage(data: &serde_json::Value) -> k9rs_proto::protocol::MetricsUsage {
    let metrics = PodMetricsData::from(data);

    // `None` until a container reports: a pod whose `containers` list is
    // missing or empty has not been MEASURED, which is a different
    // statement from "measured at zero". Summing into a `u64` started at 0
    // could not tell them apart, so an unmeasured pod rendered `0m`/`0Mi`
    // — indistinguishable from a genuinely idle one — while the node
    // accessor twelve lines down rendered `n/a` for the same situation.
    let mut cpu_nano: Option<u64> = None;
    let mut mem_bytes: Option<u64> = None;

    for container in &metrics.containers {
        if let Some(ref cpu_str) = container.usage.cpu {
            let n = parse_cpu_to_nano(cpu_str);
            cpu_nano = Some(cpu_nano.unwrap_or(0).saturating_add(n));
        }
        if let Some(ref mem_str) = container.usage.memory {
            let n = parse_mem_to_bytes(mem_str);
            mem_bytes = Some(mem_bytes.unwrap_or(0).saturating_add(n));
        }
    }

    usage(cpu_nano, mem_bytes)
}

/// Parse node metrics from a DynamicObject's data field.
pub fn parse_node_metrics_usage(data: &serde_json::Value) -> k9rs_proto::protocol::MetricsUsage {
    let metrics = NodeMetricsData::from(data);
    usage(
        metrics.usage.cpu.as_deref().map(parse_cpu_to_nano),
        metrics.usage.memory.as_deref().map(parse_mem_to_bytes),
    )
}

/// One rendering of a measurement, present or absent.
///
/// "Absent" had two spellings in two adjacent functions over the same
/// `MetricsUsage` shape: `n/a` for a node with no `usage`, and a formatted
/// ZERO for a pod with no containers. A table column that says `0m` for "we
/// do not know" is worse than one that says nothing — it is a number, and
/// it sorts like one.
///
/// The numeric fields stay `0` when absent, which is what the sort and the
/// percentage columns already expect; it is the DISPLAY that distinguishes.
fn usage(
    cpu_nano: Option<u64>,
    mem_bytes: Option<u64>,
) -> k9rs_proto::protocol::MetricsUsage {
    const ABSENT: &str = "n/a";
    k9rs_proto::protocol::MetricsUsage {
        cpu: cpu_nano
            .map(|n| k9rs_util::units::format_cpu(&format!("{n}n")))
            .unwrap_or_else(|| ABSENT.to_string()),
        mem: mem_bytes
            .map(|n| k9rs_util::units::format_mem(&n.to_string()))
            .unwrap_or_else(|| ABSENT.to_string()),
        cpu_milli: cpu_nano.unwrap_or(0) / 1_000_000,
        mem_bytes: mem_bytes.unwrap_or(0),
    }
}

// ---------------------------------------------------------------------------
// Unit parsing helpers
// ---------------------------------------------------------------------------

/// A CPU [K8s Quantity] in nanocores.
///
/// Every branch goes through `scale_quantity` (private), which parses
/// `f64`. The suffix branches used to `parse::<u64>()`, so a FRACTIONAL
/// quantity —
/// legal, and written by hand in real manifests — silently rendered 0:
/// `1.5m` was 0 millicores, not 1.5. That is the same defect
/// `scale_quantity`'s own doc records fixing in the memory sibling, left in
/// place here; only the no-suffix branch ever accepted a fraction.
///
/// The decimal SI `k` is accepted for the same reason the memory parser
/// accepts both `k` and `K`: it is in the Quantity grammar, a ResourceQuota
/// on a large cluster can carry it, and the alternative is a silent 0.
///
/// [K8s Quantity]: https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/quantity/
pub fn parse_cpu_to_nano(s: &str) -> u64 {
    const NANO: u64 = 1;
    const MICRO: u64 = 1_000;
    const MILLI: u64 = 1_000_000;
    const CORE: u64 = 1_000_000_000;

    let s = s.trim();
    // Longest-first is not a concern here: the four suffixes are one char
    // each and distinct, and `k`/`K` cannot prefix-collide with them.
    for (suffix, mult) in [("n", NANO), ("u", MICRO), ("m", MILLI), ("k", CORE * 1_000), ("K", CORE * 1_000)] {
        if let Some(val) = s.strip_suffix(suffix) {
            return scale_quantity(val, mult);
        }
    }
    // No suffix: whole cores.
    scale_quantity(s, CORE)
}

/// Scale the (possibly fractional) numeric part of a memory quantity by `mult`.
/// `1.5Gi` → 1.5 × 1024³ (the old `u64`-only parse rendered such values as 0);
/// a malformed value clamps to 0, and `as u64` saturates an out-of-range
/// product to `u64::MAX` instead of wrapping.
fn scale_quantity(val: &str, mult: u64) -> u64 {
    (val.trim().parse::<f64>().unwrap_or(0.0).max(0.0) * mult as f64) as u64
}

pub fn parse_mem_to_bytes(s: &str) -> u64 {
    // K8s Quantity permits BOTH binary (Ki/Mi/Gi/Ti, base-1024) AND
    // decimal (K/M/G/T, base-1000) suffixes. Older code only handled the
    // binary ones, so decimal payloads fell through and silently rendered
    // 0. Every branch goes through `scale_quantity`, whose saturating
    // float→int cast clamps a malformed giant value to `u64::MAX` rather
    // than wrapping. (This said "saturating multiply throughout"; there is
    // no `saturating_mul` here — the product is `f64` and the CAST is what
    // saturates, which is what `scale_quantity`'s own doc says.)
    let s = s.trim();

    // Two-char binary suffixes first (otherwise "Ti" would match the
    // one-char "T" branch and consume only the leading 'T').
    if let Some(val) = s.strip_suffix("Ti") {
        return scale_quantity(val, 1024u64.pow(4));
    }
    if let Some(val) = s.strip_suffix("Gi") {
        return scale_quantity(val, 1024u64.pow(3));
    }
    if let Some(val) = s.strip_suffix("Mi") {
        return scale_quantity(val, 1024u64.pow(2));
    }
    if let Some(val) = s.strip_suffix("Ki") {
        return scale_quantity(val, 1024);
    }
    // Pi/Ei really do turn up on large clusters — a ResourceQuota's computed
    // `status.used` is canonicalised into whichever suffix fits, so a
    // petabyte-scale storage quota reports `1Pi`. These were named in the
    // comment below and not implemented, which meant they parsed as 0.
    if let Some(val) = s.strip_suffix("Pi") {
        return scale_quantity(val, 1024u64.pow(5));
    }
    if let Some(val) = s.strip_suffix("Ei") {
        return scale_quantity(val, 1024u64.pow(6));
    }
    // One-char decimal suffixes per the K8s Quantity spec
    // (E, P, T, G, M, k — note the *lowercase* k for kilo). Some real-world
    // configs use uppercase 'K' anyway; accept both to avoid silently
    // dropping a value to "0 bytes" on a typo.
    if let Some(val) = s.strip_suffix('E') {
        return scale_quantity(val, 1_000_000_000_000_000_000);
    }
    if let Some(val) = s.strip_suffix('P') {
        return scale_quantity(val, 1_000_000_000_000_000);
    }
    if let Some(val) = s.strip_suffix('T') {
        return scale_quantity(val, 1_000_000_000_000);
    }
    if let Some(val) = s.strip_suffix('G') {
        return scale_quantity(val, 1_000_000_000);
    }
    if let Some(val) = s.strip_suffix('M') {
        return scale_quantity(val, 1_000_000);
    }
    if let Some(val) = s.strip_suffix('k').or_else(|| s.strip_suffix('K')) {
        return scale_quantity(val, 1_000);
    }
    // No suffix → raw bytes.
    s.parse::<u64>().unwrap_or(0)
}

#[cfg(test)]
#[path = "tests/metrics.rs"]
mod tests;
