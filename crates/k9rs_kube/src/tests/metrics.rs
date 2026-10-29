use super::*;

#[test]
fn parse_mem_handles_integers_and_units() {
    assert_eq!(parse_mem_to_bytes("0"), 0);
    assert_eq!(parse_mem_to_bytes("512"), 512);
    assert_eq!(parse_mem_to_bytes("1Ki"), 1024);
    assert_eq!(parse_mem_to_bytes("2Mi"), 2 * 1024 * 1024);
    assert_eq!(parse_mem_to_bytes("1Gi"), 1024u64.pow(3));
    assert_eq!(parse_mem_to_bytes("1M"), 1_000_000); // decimal SI
    assert_eq!(parse_mem_to_bytes("nonsense"), 0);
}

#[test]
fn parse_mem_handles_fractional_quantities() {
    // The old u64-only parse rendered these as 0.
    assert_eq!(parse_mem_to_bytes("1.5Gi"), 1024u64.pow(3) * 3 / 2);
    assert_eq!(parse_mem_to_bytes("0.5Mi"), 1024 * 1024 / 2);
}

// ---------------------------------------------------------------------------
// CPU quantities — the sibling that had no tests at all
// ---------------------------------------------------------------------------

/// The suffixes, at their documented scales.
#[test]
fn parse_cpu_handles_every_suffix() {
    assert_eq!(parse_cpu_to_nano("500n"), 500);
    assert_eq!(parse_cpu_to_nano("500u"), 500_000);
    assert_eq!(parse_cpu_to_nano("500m"), 500_000_000);
    assert_eq!(parse_cpu_to_nano("2"), 2_000_000_000, "no suffix is whole cores");
    assert_eq!(parse_cpu_to_nano("0"), 0);
    // Decimal SI `k`, in the Quantity grammar and reachable through a
    // ResourceQuota on a large cluster. Both cases, as the memory parser
    // accepts both.
    assert_eq!(parse_cpu_to_nano("1k"), 1_000 * 1_000_000_000);
    assert_eq!(parse_cpu_to_nano("1K"), 1_000 * 1_000_000_000);
}

/// THE defect: a FRACTIONAL suffixed quantity rendered 0.
///
/// The three suffix branches parsed `u64`, so `1.5m` — legal, and written by
/// hand in real manifests — was zero millicores. The callers are
/// user-authored YAML, not just metrics-server output: pod requests and
/// limits, node allocatable and capacity, resourcequotas. Only the
/// no-suffix branch ever accepted a fraction, and its sibling
/// `parse_mem_to_bytes` had this exact fix recorded in its own doc.
#[test]
fn parse_cpu_handles_fractional_quantities() {
    assert_eq!(parse_cpu_to_nano("1.5m"), 1_500_000);
    assert_eq!(parse_cpu_to_nano("0.5"), 500_000_000);
    assert_eq!(parse_cpu_to_nano("2.5u"), 2_500);
    assert_eq!(parse_cpu_to_nano("1.5n"), 1, "sub-nano truncates, it does not vanish");
}

/// Malformed and hostile input clamps rather than panicking or wrapping.
#[test]
fn parse_cpu_refuses_nonsense_without_wrapping() {
    assert_eq!(parse_cpu_to_nano("nonsense"), 0);
    assert_eq!(parse_cpu_to_nano(""), 0);
    assert_eq!(parse_cpu_to_nano("m"), 0);
    assert_eq!(parse_cpu_to_nano("-5"), 0, "negative is not a quantity");
    assert_eq!(parse_cpu_to_nano("-5m"), 0);
    // Far past u64 — the cast saturates where `as u64` on an f64 product
    // used to be the hazard.
    assert_eq!(parse_cpu_to_nano("1e30"), u64::MAX);
    assert_eq!(parse_cpu_to_nano("  500m  "), 500_000_000, "and it trims");
}

// ---------------------------------------------------------------------------
// Memory: the suffixes whose own doc says they used to parse as 0
// ---------------------------------------------------------------------------

/// `Pi`/`Ei` and the one-char decimal set. The comment above them records
/// that they were "named in the comment below and not implemented, which
/// meant they parsed as 0" — and then nothing tested them.
#[test]
fn parse_mem_handles_the_large_and_decimal_suffixes() {
    assert_eq!(parse_mem_to_bytes("1Ti"), 1024u64.pow(4));
    assert_eq!(parse_mem_to_bytes("1Pi"), 1024u64.pow(5));
    assert_eq!(parse_mem_to_bytes("1Ei"), 1024u64.pow(6));
    assert_eq!(parse_mem_to_bytes("1k"), 1_000, "lowercase k is the spec's kilo");
    assert_eq!(parse_mem_to_bytes("1K"), 1_000, "…and uppercase is accepted anyway");
    assert_eq!(parse_mem_to_bytes("1G"), 1_000_000_000);
    assert_eq!(parse_mem_to_bytes("1T"), 1_000_000_000_000);
    assert_eq!(parse_mem_to_bytes("1P"), 1_000_000_000_000_000);
    assert_eq!(parse_mem_to_bytes("1E"), 1_000_000_000_000_000_000);
}

/// A two-char binary suffix must not be consumed by its one-char sibling —
/// `Ti` is 1024⁴, not 'T' with a stray `i`.
#[test]
fn parse_mem_prefers_the_binary_suffix() {
    assert_ne!(parse_mem_to_bytes("1Ti"), parse_mem_to_bytes("1T"));
    assert_ne!(parse_mem_to_bytes("1Ki"), parse_mem_to_bytes("1K"));
    assert_eq!(parse_mem_to_bytes("-1Gi"), 0, "negative clamps");
}

// ---------------------------------------------------------------------------
// The two public entry points, which had no tests either
// ---------------------------------------------------------------------------

/// A pod's usage is the SUM over its containers, in both units.
#[test]
fn pod_metrics_sum_every_container() {
    let data = serde_json::json!({
        "containers": [
            { "name": "web", "usage": { "cpu": "1.5m", "memory": "64Mi" } },
            { "name": "sidecar", "usage": { "cpu": "500u", "memory": "0.5Mi" } },
        ]
    });
    let usage = parse_pod_metrics_usage(&data);
    assert_eq!(
        usage.cpu_milli, 2,
        "1.5m + 0.5m = 2m — and the fractional half was 0 before the parser fix",
    );
    assert_eq!(usage.mem_bytes, 64 * 1024 * 1024 + 1024 * 1024 / 2);
}

/// A pod with no containers reads as UNMEASURED, not as idle.
///
/// It rendered a formatted zero — `0m` / `0Mi` — indistinguishable from a
/// pod that really is using nothing, while the node accessor twelve lines
/// away rendered `n/a` for the same situation. A table column that says `0m`
/// for "we do not know" is worse than one that says nothing: it is a number,
/// and it sorts like one.
#[test]
fn a_pod_with_no_containers_reads_as_unmeasured() {
    let usage = parse_pod_metrics_usage(&serde_json::json!({}));
    assert_eq!(usage.cpu, "n/a");
    assert_eq!(usage.mem, "n/a");
    // The numeric fields stay 0 — the sort and the percentage columns
    // already expect that; it is the DISPLAY that distinguishes.
    assert_eq!(usage.cpu_milli, 0);
    assert_eq!(usage.mem_bytes, 0);
}

/// …and a pod that really reports zero still reads as a measurement.
#[test]
fn a_pod_reporting_zero_is_not_unmeasured() {
    let usage = parse_pod_metrics_usage(&serde_json::json!({
        "containers": [{ "name": "web", "usage": { "cpu": "0", "memory": "0" } }]
    }));
    assert_ne!(usage.cpu, "n/a", "it was measured, and the answer is zero");
    assert_ne!(usage.mem, "n/a");
    assert_eq!(usage.cpu_milli, 0);
}

/// A node with no `usage` object reads the same way — ONE spelling of
/// absent across both accessors.
#[test]
fn a_node_with_no_usage_reads_the_same_as_an_unmeasured_pod() {
    let node = parse_node_metrics_usage(&serde_json::json!({}));
    let pod = parse_pod_metrics_usage(&serde_json::json!({}));
    assert_eq!(node.cpu, pod.cpu);
    assert_eq!(node.mem, pod.mem);
    assert_eq!(node.cpu, "n/a");
}

/// A HALF-measured pod says so per axis: a container reporting cpu and not
/// memory is measured in one and unmeasured in the other.
#[test]
fn a_half_measured_pod_is_absent_only_where_it_is_absent() {
    let usage = parse_pod_metrics_usage(&serde_json::json!({
        "containers": [{ "name": "web", "usage": { "cpu": "500m" } }]
    }));
    assert_ne!(usage.cpu, "n/a");
    assert_eq!(usage.cpu_milli, 500);
    assert_eq!(usage.mem, "n/a", "memory was never reported");
}

/// A node's usage comes from one `usage` object, not a container list.
#[test]
fn node_metrics_read_the_single_usage_object() {
    let data = serde_json::json!({ "usage": { "cpu": "1500m", "memory": "2Gi" } });
    let usage = parse_node_metrics_usage(&data);
    assert_eq!(usage.cpu_milli, 1_500);
    assert_eq!(usage.mem_bytes, 2 * 1024u64.pow(3));
}
