//! The capability gate's decision, without a session to run it in.
//!
//! `require_capability` needs a live `ServerSession` — an authenticated kube
//! client and an event channel — which a unit test must not have, so the rule
//! it enforces had no test of any kind. That is how `handle_port_forward`
//! came to be the one mutating handler with no capability check at all,
//! while the gate's own doc claimed five callers it did not have.

use super::*;

fn obj(kind: k9rs_proto::resource_def::BuiltInKind, name: &str) -> protocol::ObjectRef {
    protocol::ObjectRef {
        name: name.to_string(),
        namespace: protocol::Namespace::Named("ns".to_string()),
        resource: kind.to_resource_id(),
    }
}

/// A kind that declares the operation passes; one that does not is refused
/// BY NAME, before anything reaches the API server.
#[test]
fn the_gate_admits_only_kinds_that_declare_the_operation() {
    use k9rs_proto::protocol::OperationKind;
    use k9rs_proto::resource_def::BuiltInKind;

    // Port-forward: the operation whose handler had no gate.
    let pod = obj(BuiltInKind::Pod, "api-0");
    assert!(
        capability_check(&pod, &OperationKind::PortForward, "Port-forward").is_ok(),
        "a pod forwards ports",
    );

    let cm = obj(BuiltInKind::ConfigMap, "settings");
    let err = capability_check(&cm, &OperationKind::PortForward, "Port-forward")
        .expect_err("a configmap has no ports to forward");
    assert!(
        err.contains("Port-forward") && err.contains("configmaps"),
        "the refusal names the action and the kind: {err:?}",
    );

    // And the same rule for the two operations that always had the gate, so
    // a future consolidation cannot quietly change one of the three.
    assert!(capability_check(&obj(BuiltInKind::Deployment, "api"), &OperationKind::Scale, "Scale").is_ok());
    assert!(capability_check(&cm, &OperationKind::Scale, "Scale").is_err());
    assert!(capability_check(
        &obj(BuiltInKind::CronJob, "nightly"),
        &OperationKind::ToggleSuspendCronJob,
        "Suspend",
    )
    .is_ok());
}

/// A resource the daemon cannot type at all is refused rather than guessed
/// at — a CRD or an overlay-declared kind has no `BuiltInKind`, so there are
/// no declared operations to check against.
#[test]
fn the_gate_refuses_a_kind_it_cannot_type() {
    use k9rs_proto::protocol::{OperationKind, ResourceId};
    let crd_target = protocol::ObjectRef {
        name: "my-cert".to_string(),
        namespace: protocol::Namespace::Named("ns".to_string()),
        resource: ResourceId::CrdUnresolved("certificates".to_string()),
    };
    let err = capability_check(&crd_target, &OperationKind::PortForward, "Port-forward")
        .expect_err("an untyped kind declares nothing");
    assert!(err.contains("certificates"), "the refusal names it: {err:?}");
}

// ---------------------------------------------------------------------------
// The namespace rule — one decision, three answer shapes
// ---------------------------------------------------------------------------

use super::namespace_for_op;

fn cluster_scoped(name: &str) -> protocol::ObjectRef {
    protocol::ObjectRef {
        name: name.to_string(),
        namespace: protocol::Namespace::All,
        resource: k9rs_proto::resource_def::BuiltInKind::Node.to_resource_id(),
    }
}

fn unresolved(kind: k9rs_proto::resource_def::BuiltInKind, name: &str) -> protocol::ObjectRef {
    protocol::ObjectRef {
        name: name.to_string(),
        namespace: protocol::Namespace::All,
        resource: kind.to_resource_id(),
    }
}

/// A namespaced target with a real namespace yields it.
#[test]
fn a_resolved_namespace_is_handed_back() {
    let target = obj(k9rs_proto::resource_def::BuiltInKind::Secret, "db-creds");
    assert_eq!(namespace_for_op(&target, "Decode"), Ok("ns"));
}

/// `Namespace::All` on a NAMESPACED target is the data-corruption case: the
/// row arrived without a namespace. Running anyway would mutate whatever
/// lives under that name in the default namespace.
#[test]
fn an_unresolved_namespace_is_refused_by_name() {
    let target = unresolved(k9rs_proto::resource_def::BuiltInKind::Secret, "db-creds");
    let err = namespace_for_op(&target, "Decode").expect_err("must refuse");
    assert_eq!(err, "Decode refused: secrets/db-creds has no resolved namespace");
}

/// A cluster-scoped target is refused with its OWN reason. "No resolved
/// namespace" would be true and misleading — a Node legitimately has none.
#[test]
fn a_cluster_scoped_target_is_refused_for_being_cluster_scoped() {
    let err = namespace_for_op(&cluster_scoped("node-1"), "Force-kill")
        .expect_err("must refuse");
    assert_eq!(err, "Force-kill refused: nodes/node-1 is cluster-scoped");
}

/// The action name rides into the message, so one rule can serve handlers
/// that each name themselves — this is what made four copies of the sentence
/// look necessary.
#[test]
fn the_action_names_itself_in_the_refusal() {
    let target = unresolved(k9rs_proto::resource_def::BuiltInKind::Deployment, "api");
    for action in ["Apply", "Delete", "Scale", "Restart", "Decode", "Trigger"] {
        let err = namespace_for_op(&target, action).expect_err("must refuse");
        assert!(err.starts_with(action), "{err:?} must name {action}");
    }
}

// ---------------------------------------------------------------------------
// Decode: every refusal is the same shape
// ---------------------------------------------------------------------------

use super::{plan_decode, DecodePlan};

/// THE property. A `DecodeSecret` opens a content view on the client whose
/// only exit is a `DescribeResult` for its target, so every refusal must be
/// text for that panel — not an `OpResult`, which the panel is not waiting
/// for and will never see.
///
/// The namespace refusal is the one that had drifted: it went through the
/// general `OpResult` gate, so a secret whose row arrived without a
/// namespace left the panel on "Loading…" for ever while a flash went by
/// somewhere else.
#[test]
fn every_decode_refusal_is_panel_text() {
    use k9rs_proto::resource_def::BuiltInKind;
    let refusals = [
        // Not a built-in at all: a CRD row.
        protocol::ObjectRef {
            name: "thing".to_string(),
            namespace: protocol::Namespace::Named("ns".to_string()),
            resource: protocol::ResourceId::CrdUnresolved("widgets".to_string()),
        },
        // A built-in that does not declare the operation.
        obj(BuiltInKind::Pod, "api-7d9f"),
        // Secret-like, but the row lost its namespace.
        protocol::ObjectRef {
            name: "db-creds".to_string(),
            namespace: protocol::Namespace::All,
            resource: BuiltInKind::Secret.to_resource_id(),
        },
    ];
    for target in refusals {
        match plan_decode(&target) {
            DecodePlan::Refused(text) => assert!(
                !text.is_empty(),
                "{target:?}: a refusal must say something",
            ),
            DecodePlan::Fetch { namespace } => panic!(
                "{target:?} must be refused, planned a fetch in {namespace:?}",
            ),
        }
    }
}

/// And the one case that should run, runs — with the namespace it was given.
#[test]
fn a_decodable_secret_plans_a_fetch() {
    let target = obj(k9rs_proto::resource_def::BuiltInKind::Secret, "db-creds");
    match plan_decode(&target) {
        DecodePlan::Fetch { namespace } => assert_eq!(namespace, "ns"),
        DecodePlan::Refused(text) => panic!("a secret in a namespace must decode, got {text:?}"),
    }
}

// ---------------------------------------------------------------------------
// The pure bodies the mutating handlers send
// ---------------------------------------------------------------------------

use super::{build_trigger_job, decode_secret_to_text, toggle_cron_suspend};

fn cron(suspend: Option<bool>) -> k8s_openapi::api::batch::v1::CronJob {
    use k8s_openapi::api::batch::v1::{CronJob, CronJobSpec, JobTemplateSpec};
    CronJob {
        metadata: kube::api::ObjectMeta {
            name: Some("nightly".to_string()),
            namespace: Some("prod".to_string()),
            ..Default::default()
        },
        spec: Some(CronJobSpec {
            schedule: "0 2 * * *".to_string(),
            suspend,
            job_template: JobTemplateSpec::default(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Suspend toggles, and ABSENT reads as not-suspended — the K8s default.
/// Reading absent as `true` would make the first press un-suspend a running
/// CronJob, i.e. do nothing, which looks like a dead key.
#[test]
fn toggling_suspend_treats_absent_as_running() {
    let (state, patch) = toggle_cron_suspend(&cron(None));
    assert!(state, "an unsuspended CronJob suspends");
    assert_eq!(
        serde_json::to_value(&patch).expect("serialize"),
        serde_json::json!({"spec": {"suspend": true}}),
    );

    let (state, patch) = toggle_cron_suspend(&cron(Some(true)));
    assert!(!state, "a suspended CronJob resumes");
    assert_eq!(
        serde_json::to_value(&patch).expect("serialize"),
        serde_json::json!({"spec": {"suspend": false}}),
    );

    let (state, _) = toggle_cron_suspend(&cron(Some(false)));
    assert!(state, "an explicitly-unsuspended CronJob suspends");
}

/// The patch names ONLY `spec.suspend`. A merge patch carrying anything else
/// would claim ownership of fields a GitOps controller owns — the reason
/// these small mutations are merge patches with no field manager at all.
#[test]
fn the_suspend_patch_touches_nothing_else() {
    let (_, patch) = toggle_cron_suspend(&cron(None));
    let body = serde_json::to_value(&patch).expect("serialize");
    let spec = body.get("spec").and_then(|s| s.as_object()).expect("a spec object");
    assert_eq!(body.as_object().expect("an object").len(), 1, "one top-level key: {body}");
    assert_eq!(spec.len(), 1, "one spec key: {body}");
    assert!(spec.contains_key("suspend"));
}

/// A manual trigger's Job name is unique WITHIN a second: two triggers in
/// the same second used to collide on a 409, because the stamp was epoch
/// seconds alone.
#[test]
fn two_triggers_in_one_second_get_different_job_names() {
    let c = cron(None);
    let a = build_trigger_job(&c, "prod");
    let b = build_trigger_job(&c, "prod");
    let (a_name, b_name) = (
        a.metadata.name.expect("a name"),
        b.metadata.name.expect("a name"),
    );
    assert_ne!(a_name, b_name, "the subsecond stamp is what makes these differ");
    assert!(a_name.starts_with("nightly-manual-"), "got {a_name}");
    assert!(
        a_name.len() <= 253,
        "a K8s name is at most 253 chars, got {}: {a_name}",
        a_name.len(),
    );
    assert_eq!(b.metadata.namespace.as_deref(), Some("prod"), "and lands where asked");
}

/// A CronJob with no spec still yields a Job rather than nothing — the
/// trigger is reported as whatever the apiserver then says about an empty
/// spec, not as a silent no-op here.
#[test]
fn a_cronjob_without_a_spec_still_builds_a_job() {
    let bare = k8s_openapi::api::batch::v1::CronJob {
        metadata: kube::api::ObjectMeta {
            name: Some("empty".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    let job = build_trigger_job(&bare, "ns");
    assert!(job.metadata.name.expect("a name").starts_with("empty-manual-"));
    assert!(job.spec.is_some(), "an empty spec, not an absent one");
}

/// Both secret sources render, and `data` is shown DECODED — it is base64 on
/// the wire from K8s, and the whole point of the view is to read it.
#[test]
fn decoding_a_secret_renders_both_sources() {
    use k8s_openapi::api::core::v1::Secret;
    use k8s_openapi::ByteString;
    let secret = Secret {
        metadata: kube::api::ObjectMeta {
            name: Some("db-creds".to_string()),
            namespace: Some("prod".to_string()),
            ..Default::default()
        },
        data: Some(std::collections::BTreeMap::from([(
            "password".to_string(),
            ByteString(b"hunter2".to_vec()),
        )])),
        string_data: Some(std::collections::BTreeMap::from([(
            "plain".to_string(),
            "already-text".to_string(),
        )])),
        ..Default::default()
    };
    let text = decode_secret_to_text(&secret);
    assert!(text.starts_with("Secret: prod/db-creds\n\n"), "got {text:?}");
    assert!(text.contains("password:\n  hunter2"), "got {text:?}");
    assert!(text.contains("plain:\n  already-text"), "got {text:?}");
    assert!(text.ends_with('\n') && !text.ends_with("\n\n"), "one trailing newline: {text:?}");
}

/// An empty Secret says so in the one way that matters: it names the secret
/// and shows no keys, rather than looking like a failed fetch.
#[test]
fn decoding_an_empty_secret_names_it_and_stops() {
    use k8s_openapi::api::core::v1::Secret;
    let secret = Secret {
        metadata: kube::api::ObjectMeta {
            name: Some("empty".to_string()),
            namespace: Some("ns".to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    assert_eq!(decode_secret_to_text(&secret), "Secret: ns/empty\n");
}
