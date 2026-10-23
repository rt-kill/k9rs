//! Unit tests for `live_query_dynamic` (mirrored out of the source file).

// ---------------------------------------------------------------------------
// json_path_tests (mirrored out of live_query_dynamic.rs)
// ---------------------------------------------------------------------------

use super::resolve_json_path;
use serde_json::json;

#[test]
fn plain_dot_path() {
    let obj = json!({ "spec": { "phase": "Running" } });
    assert_eq!(resolve_json_path(&obj, ".spec.phase"), "Running");
}

#[test]
fn escaped_dot_label_key() {
    // CRD printer columns (e.g. Karpenter NodeClaim) reference label keys
    // with escaped dots: `.metadata.labels.karpenter\.sh/capacity-type`.
    let obj = json!({ "metadata": { "labels": {
        "karpenter.sh/capacity-type": "spot",
        "node.kubernetes.io/instance-type": "r8i-flex.8xlarge",
    }}});
    assert_eq!(
        resolve_json_path(&obj, r".metadata.labels.karpenter\.sh/capacity-type"),
        "spot",
    );
    assert_eq!(
        resolve_json_path(&obj, r".metadata.labels.node\.kubernetes\.io/instance-type"),
        "r8i-flex.8xlarge",
    );
}

#[test]
fn missing_segment_returns_empty() {
    let obj = json!({ "spec": { "phase": "Running" } });
    assert_eq!(resolve_json_path(&obj, ".spec.does.not.exist"), "");
}

#[test]
fn number_and_bool_stringify() {
    let obj = json!({ "spec": { "replicas": 3, "paused": true } });
    assert_eq!(resolve_json_path(&obj, ".spec.replicas"), "3");
    assert_eq!(resolve_json_path(&obj, ".spec.paused"), "true");
}

#[test]
fn condition_filter_eq_match() {
    // cert-manager Certificate shape
    let obj = json!({
        "status": {
            "conditions": [
                { "type": "Issuing", "status": "False" },
                { "type": "Ready",   "status": "True"  },
            ]
        }
    });
    assert_eq!(
        resolve_json_path(&obj, ".status.conditions[?(@.type=='Ready')].status"),
        "True"
    );
}

#[test]
fn condition_filter_ne() {
    let obj = json!({
        "status": {
            "conditions": [
                { "type": "Ready", "status": "True" },
                { "type": "Degraded", "status": "False" },
            ]
        }
    });
    // First element whose type is NOT "Ready"
    assert_eq!(
        resolve_json_path(&obj, ".status.conditions[?(@.type!='Ready')].status"),
        "False"
    );
}

#[test]
fn filter_accepts_double_quoted_literal() {
    let obj = json!({
        "status": { "conditions": [{ "type": "Ready", "status": "True" }] }
    });
    assert_eq!(
        resolve_json_path(&obj, r#".status.conditions[?(@.type=="Ready")].status"#),
        "True"
    );
}

#[test]
fn filter_with_no_match_returns_empty() {
    let obj = json!({
        "status": { "conditions": [{ "type": "Issuing", "status": "False" }] }
    });
    assert_eq!(
        resolve_json_path(&obj, ".status.conditions[?(@.type=='Ready')].status"),
        ""
    );
}

#[test]
fn unsupported_expression_returns_empty() {
    let obj = json!({
        "status": { "conditions": [{ "type": "Ready", "status": "True" }] }
    });
    // Conjunctions are not supported — collapse to empty rather than
    // returning a silently-wrong field.
    assert_eq!(
        resolve_json_path(&obj, ".status.conditions[?(@.type=='Ready' && @.status=='True')].status"),
        ""
    );
}

#[test]
fn filter_on_non_array_returns_empty() {
    let obj = json!({ "status": { "conditions": { "type": "Ready" } } });
    assert_eq!(
        resolve_json_path(&obj, ".status.conditions[?(@.type=='Ready')].status"),
        ""
    );
}
