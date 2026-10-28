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

/// Whitespace around the operator is not a different query.
///
/// The literal was sliced with `expr[trimmed_key.len() + 2..]` — the trimmed
/// key's length indexed into the UNTRIMMED expression — so a space before
/// the operator shifted the slice by that much and
/// `@.type == 'Ready'` compared against the string `"= 'Ready"`. Nothing
/// matched, nothing complained, and the column rendered blank. Every test in
/// this file used the tight spelling, which is the only reason it survived;
/// a CRD's `additionalPrinterColumns` writes whichever it likes, and the
/// spaced form is the one kubectl's own documentation shows.
#[test]
fn a_condition_filter_ignores_whitespace_around_its_operator() {
    let obj = json!({
        "status": {
            "conditions": [
                { "type": "Issuing", "status": "False" },
                { "type": "Ready",   "status": "True"  },
            ]
        }
    });
    for spelling in [
        ".status.conditions[?(@.type=='Ready')].status",
        ".status.conditions[?(@.type == 'Ready')].status",
        ".status.conditions[?(@.type ==  'Ready')].status",
        ".status.conditions[?( @.type == 'Ready' )].status",
        r#".status.conditions[?(@.type == "Ready")].status"#,
    ] {
        assert_eq!(resolve_json_path(&obj, spelling), "True", "spelling: {spelling}");
    }

    // `!=` takes the same path and had the same defect.
    for spelling in [
        ".status.conditions[?(@.type!='Ready')].status",
        ".status.conditions[?(@.type != 'Ready')].status",
    ] {
        assert_eq!(resolve_json_path(&obj, spelling), "False", "spelling: {spelling}");
    }
}

/// Every form that resolves to `""` for every object is NAMED at ingest.
///
/// A user's overlay `jsonpath:` went into the column plan with no parse, no
/// validation and no log. If it used syntax this resolver does not
/// understand, the header appeared, every cell was blank for ever, and
/// nothing anywhere said why — indistinguishable from a field that is
/// genuinely unset.
///
/// Paired with `resolve_json_path` on a real object, so the two cannot
/// disagree about what "unsupported" means: anything the validator rejects
/// must actually come back empty, and anything it accepts must not be
/// rejected for a form the resolver handles.
#[test]
fn an_unsupported_jsonpath_is_named_rather_than_silently_empty() {
    use super::unsupported_json_path;

    let obj = json!({
        "metadata": { "labels": { "app.kubernetes.io/name": "api" } },
        "status": { "conditions": [{ "type": "Ready", "status": "True" }] },
        "spec": { "replicas": 3 }
    });

    let rejected = [
        // The alternate label spelling many CRDs emit.
        ".metadata.labels['app.kubernetes.io/name']",
        // Compound filters — the existing `unsupported_expression_returns_empty`
        // test pins that these resolve empty.
        ".status.conditions[?(@.type=='Ready' && @.status=='True')].status",
        ".status.conditions[?(@.type=='Ready' || @.type=='Available')].status",
        ".spec.containers[*].name",
        ".status.conditions[?(@.type=='Ready'].status",
        ".status.conditions[?(type=='Ready')].status",
        ".status.conditions[?(@.observedGeneration > 1)].status",
    ];
    for path in rejected {
        let why = unsupported_json_path(path)
            .unwrap_or_else(|| panic!("{path} should be reported as unsupported"));
        assert!(!why.is_empty(), "the reason is said out loud: {path}");
        assert_eq!(
            resolve_json_path(&obj, path),
            "",
            "{path} is reported unsupported, so it had better actually be empty",
        );
    }

    // And the forms it does handle are not slandered.
    let accepted = [
        ".metadata.labels.app\\.kubernetes\\.io/name",
        ".spec.replicas",
        ".status.conditions[?(@.type=='Ready')].status",
        ".status.conditions[?(@.type == 'Ready')].status",
        ".status.conditions[?(@.type!='Ready')].status",
    ];
    for path in accepted {
        assert_eq!(
            unsupported_json_path(path),
            None,
            "{path} is a form this resolver handles",
        );
    }
    // Two of those really do resolve, so "accepted" is not vacuous.
    assert_eq!(resolve_json_path(&obj, ".spec.replicas"), "3");
    assert_eq!(
        resolve_json_path(&obj, ".status.conditions[?(@.type=='Ready')].status"),
        "True",
    );
}
