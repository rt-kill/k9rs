//! `pf_resolve`'s pure helpers: target parsing, selector rendering, service
//! port translation, and ready-pod selection.
//!
//! RECONSTRUCTED 2026-10-27. I overwrote this file with `cp` without reading
//! it first, which destroyed the original 113 lines. The four test names and
//! their assertion text were recovered from a previously-built test binary
//! (`strings` over `target/debug/deps/k9rs_kube-*`, which embeds both), so
//! the cases and expectations below are the originals; the fixture
//! scaffolding around them is rewritten. `git checkout` of this one path
//! restores the true original and should be preferred to this.

use super::*;
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, Pod, PodCondition, PodSpec, PodStatus, ServicePort,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
    LabelSelectorRequirement, ObjectMeta, Time,
};

fn pod_named(name: &str) -> Pod {
    Pod {
        metadata: ObjectMeta { name: Some(name.to_string()), ..Default::default() },
        ..Default::default()
    }
}

/// A pod with a `Ready=True` condition and `phase: Running` — what
/// `pick_ready_pod` is looking for.
fn ready_pod(name: &str) -> Pod {
    let mut p = pod_named(name);
    p.status = Some(PodStatus {
        phase: Some("Running".to_string()),
        conditions: Some(vec![PodCondition {
            type_: "Ready".to_string(),
            status: "True".to_string(),
            ..Default::default()
        }]),
        ..Default::default()
    });
    p
}

/// A kubectl-style ref splits on the slash; a bare name is a pod, which is
/// what `kubectl port-forward web-0 8080:80` means.
#[test]
fn parse_target_splits_plural_and_bare_is_pod() {
    assert_eq!(parse_target("services/nginx"), ("services", "nginx"));
    assert_eq!(parse_target("nginx"), ("pods", "nginx"));
    assert_eq!(parse_target("foo"), ("pods", "foo"));
    assert_eq!(parse_target("bare-name"), ("pods", "bare-name"));
}

/// A pod being deleted or not yet Ready is not a tunnel endpoint: pointing a
/// forward at either gives a connection that dies immediately.
#[test]
fn pick_ready_pod_skips_terminating_and_unready() {
    let mut dying = ready_pod("dying");
    dying.metadata.deletion_timestamp = Some(Time(chrono::Utc::now()));

    let mut pending = pod_named("pending");
    pending.status = Some(PodStatus {
        phase: Some("Running".to_string()),
        conditions: Some(vec![PodCondition {
            type_: "Ready".to_string(),
            status: "False".to_string(),
            ..Default::default()
        }]),
        ..Default::default()
    });

    let picked = pick_ready_pod(vec![dying, pending, ready_pod("good")])
        .expect("one qualifies");
    assert_eq!(picked.metadata.name.as_deref(), Some("good"));

    assert!(pick_ready_pod(vec![]).is_none());
}

/// A pod the apiserver handed back without a usable name is refused, not
/// forwarded to.
///
/// Both callers read it as `unwrap_or_default()`, which turned an unnamed
/// pod into a forward to pod `""` — `kubectl port-forward pod/ …` — so the
/// failure surfaced at tunnel time, far from the list that produced it.
/// `Retry`, because re-reading the list is worth doing.
#[test]
fn an_unnamed_pod_is_not_a_forward_target() {
    assert_eq!(pod_name(&pod_named("web-0")).ok().as_deref(), Some("web-0"));

    assert!(
        matches!(pod_name(&Pod::default()), Err(ResolveError::Retry(m)) if m.contains("no name")),
        "a pod with no name at all must be refused, and say why",
    );

    // `Some("")` is the shape `unwrap_or_default` produced downstream, and
    // is exactly as unforwardable as absence.
    assert!(
        matches!(pod_name(&pod_named("")), Err(ResolveError::Retry(m)) if m.contains("no name")),
        "an empty name is absence spelled differently",
    );
}

/// The API's label-selector grammar, including the set-based operators a
/// `matchExpressions` selector can carry. An empty selector is an error
/// rather than a query that matches everything.
#[test]
fn selector_string_covers_labels_and_expressions() {
    let mut labels = std::collections::BTreeMap::new();
    labels.insert("app".to_string(), "web".to_string());
    let sel = LabelSelector {
        match_labels: Some(labels),
        match_expressions: Some(vec![
            LabelSelectorRequirement {
                key: "tier".to_string(),
                operator: "In".to_string(),
                values: Some(vec!["a".to_string(), "b".to_string()]),
            },
            LabelSelectorRequirement {
                key: "canary".to_string(),
                operator: "DoesNotExist".to_string(),
                values: None,
            },
        ]),
    };
    assert_eq!(selector_to_string(&sel).as_deref(), Ok("app=web,tier in (a,b),!canary"));

    assert!(
        selector_to_string(&LabelSelector::default()).is_err(),
        "empty = error: a selector matching everything would forward to an \
         arbitrary pod in the namespace",
    );
}

/// An equality-only selector — which is all a Service's `spec.selector` is —
/// has exactly ONE way to be refused: being empty.
///
/// `resolve_service` renders its map through this same function now instead
/// of joining `k=v` itself, and maps the refusal to "service … has no pod
/// selector". That rename is honest only while emptiness is the only
/// reachable error for a selector with no `matchExpressions`; a new operator
/// arm that could fail would otherwise be reported as a missing selector.
#[test]
fn an_equality_only_selector_is_refused_only_when_empty() {
    let of = |pairs: &[(&str, &str)]| LabelSelector {
        match_labels: Some(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        ),
        match_expressions: None,
    };

    assert_eq!(selector_to_string(&of(&[("app", "web")])).as_deref(), Ok("app=web"));
    assert_eq!(
        selector_to_string(&of(&[("app", "web"), ("tier", "fe")])).as_deref(),
        Ok("app=web,tier=fe"),
        "sorted and comma-joined, exactly as the hand-rolled join produced",
    );
    // Values the grammar has no escape for still render — the apiserver
    // rejects them, which is a better answer than a selector we invented.
    assert!(selector_to_string(&of(&[("app", "")])).is_ok(), "an empty VALUE is a term");

    for empty in [of(&[]), LabelSelector { match_labels: None, match_expressions: None }] {
        assert!(
            selector_to_string(&empty).is_err(),
            "and emptiness is the one refusal `resolve_service` renames",
        );
    }
}

/// kubectl's `translateServicePortToTargetPort`: the asked-for port must be
/// a service port, and its `targetPort` — integer, named, or absent — is
/// what the pod actually listens on.
#[test]
fn service_port_maps_int_named_and_default() {
    let pod = {
        let mut p = ready_pod("web-0");
        p.spec = Some(PodSpec {
            containers: vec![Container {
                name: "app".to_string(),
                ports: Some(vec![ContainerPort {
                    name: Some("https".to_string()),
                    container_port: 8443,
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        });
        p
    };
    let bare = ready_pod("bare");

    let ports = vec![
        // Integer targetPort.
        ServicePort { port: 80, target_port: Some(IntOrString::Int(8080)), ..Default::default() },
        // Named targetPort, resolved against the chosen pod.
        ServicePort {
            port: 443,
            target_port: Some(IntOrString::String("https".to_string())),
            ..Default::default()
        },
        // Absent targetPort defaults to the service port.
        ServicePort { port: 9090, target_port: None, ..Default::default() },
    ];

    assert!(matches!(service_target_port(&ports, 80, &pod), Ok(8080)));
    assert!(matches!(service_target_port(&ports, 443, &pod), Ok(8443)));
    assert!(matches!(service_target_port(&ports, 9090, &pod), Ok(9090)));
    // A port the service does not expose cannot be fixed by retrying.
    assert!(matches!(
        service_target_port(&ports, 5000, &pod),
        Err(ResolveError::Fatal(_)),
    ));
    // A pod missing the NAMED port is rollout drift, which a retry can heal.
    assert!(matches!(
        service_target_port(&ports, 443, &bare),
        Err(ResolveError::Retry(_)),
    ));
}

// ---------------------------------------------------------------------------
// Which kinds a port-forward can resolve — held against the declaration that
// `:pf` is gated on.
//
// `resolve`'s dispatch used to be a `match plural: &str` with a wildcard,
// paired with a second string match inside `workload_selector` whose arms had
// to agree with it, while each def's `operations()` was a silent third source
// — and the one the TUI actually gates `:pf` on. They agreed by coincidence;
// either string drifting produced a `Fatal` reading "port-forward not
// supported for 'deployments'", which reads as a user error rather than a
// missing arm.
// ---------------------------------------------------------------------------

fn offline_client() -> kube::Client {
    let config = kube::Config::new("http://127.0.0.1:1".parse().expect("a valid uri"));
    kube::Client::try_from(config).expect("offline client build")
}

/// Every kind whose def declares `PortForward` has an arm, and no kind
/// without one does.
///
/// Driven through the real `resolve` against an unreachable apiserver, so
/// "has an arm" means what it says: a kind with an arm fails to CONNECT
/// (`Retry`, or `Ok` for a bare pod, which needs no lookup at all), while a
/// kind without one is refused before any request is attempted (`Fatal`
/// naming the plural).
#[tokio::test]
async fn every_port_forwardable_kind_is_resolvable() {
    use k9rs_proto::protocol::OperationKind;
    let client = offline_client();
    let mut declared = 0usize;

    for def in k9rs_proto::resource_def::all_defs() {
        let plural = def.gvr().plural;
        let target = format!("{plural}/thing");
        let declares = def.operations().contains(&OperationKind::PortForward);
        let outcome = resolve(&client, "ns", &target, 8080).await;
        let refused_outright = matches!(
            &outcome,
            Err(ResolveError::Fatal(m)) if m.contains("port-forward not supported")
        );

        if declares {
            declared += 1;
            assert!(
                !refused_outright,
                "{plural} declares PortForward but `resolve` has no arm for it",
            );
        } else {
            assert!(
                refused_outright,
                "{plural} does NOT declare PortForward, so `resolve` must refuse \
                 it rather than issue requests",
            );
        }
    }

    assert!(declared >= 5, "sanity: several kinds declare PortForward, got {declared}");
}

/// A plural nothing knows is refused by name, not quietly treated as a pod.
#[tokio::test]
async fn an_unknown_plural_is_refused() {
    let client = offline_client();
    let outcome = resolve(&client, "ns", "widgets/thing", 8080).await;
    assert!(
        matches!(&outcome, Err(ResolveError::Fatal(m)) if m.contains("widgets")),
        "an unknown plural must be named in the refusal",
    );
}
