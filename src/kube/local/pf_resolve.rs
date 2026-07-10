//! Port-forward target resolution — the part `kubectl port-forward` did
//! for us before the forward went native: turn a `plural/name` target into
//! a concrete `(pod, port)` to tunnel to.
//!
//! - `pods/x` → the pod itself, port used as-is.
//! - `services/x` → the service's selector picks a ready pod, and the
//!   requested port is looked up in the service's ports to find the pod's
//!   `targetPort` (numeric, or named and resolved against the pod's
//!   container ports) — kubectl's `translateServicePortToTargetPort`.
//! - workloads (`deployments/…`, `replicasets/…`, `statefulsets/…`,
//!   `daemonsets/…`, `jobs/…`) → the label selector picks a ready pod,
//!   port used as-is (kubectl only maps ports for services).
//!
//! Resolution runs per supervised attempt, which is exactly what makes the
//! native forward *better* than kubectl's: kubectl pins one pod and dies
//! with it; here a reconnect re-resolves and lands on a healthy replacement.
//!
//! Errors are typed by what the supervisor should do:
//! [`ResolveError::Retry`] for states the cluster can heal (object missing,
//! no ready pods yet), [`ResolveError::Fatal`] for configuration that
//! retrying can't fix (unsupported kind, service has no such port).

use k8s_openapi::api::core::v1::{Pod, Service};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{Api, ListParams};

/// The concrete tunnel endpoint an attempt connects to.
pub struct PodTarget {
    pub pod: String,
    pub port: u16,
}

/// How resolution failed, typed by the supervisor's correct reaction.
pub enum ResolveError {
    /// The cluster may heal this (object not found yet, no ready pods,
    /// transient API error) — back off and re-resolve.
    Retry(String),
    /// Retrying cannot help (unsupported target kind, port not on the
    /// service) — park the forward as Failed.
    Fatal(String),
}

/// Resolve a kubectl-style `plural/name` target to a `(pod, port)`.
pub async fn resolve(
    client: &kube::Client,
    ns: &str,
    target: &str,
    remote_port: u16,
) -> Result<PodTarget, ResolveError> {
    let (plural, name) = parse_target(target);
    match plural {
        // Bare pod: existence and readiness surface at tunnel time (the
        // canary stream fails → retry), so no lookup here.
        "pods" => Ok(PodTarget { pod: name.to_string(), port: remote_port }),
        "services" => resolve_service(client, ns, name, remote_port).await,
        "deployments" | "replicasets" | "statefulsets" | "daemonsets" | "jobs" => {
            let selector = workload_selector(client, ns, plural, name).await?;
            let sel_str = selector_to_string(&selector)
                .map_err(ResolveError::Fatal)?;
            let pod = pick_ready_pod_by_selector(client, ns, &sel_str).await?;
            Ok(PodTarget { pod, port: remote_port })
        }
        other => Err(ResolveError::Fatal(format!(
            "port-forward not supported for '{other}'"
        ))),
    }
}

/// `plural/name`, with a bare name treated as a pod (kubectl-compatible).
fn parse_target(target: &str) -> (&str, &str) {
    match target.split_once('/') {
        Some((plural, name)) => (plural, name),
        None => ("pods", target),
    }
}

async fn resolve_service(
    client: &kube::Client,
    ns: &str,
    name: &str,
    remote_port: u16,
) -> Result<PodTarget, ResolveError> {
    let svc_api: Api<Service> = Api::namespaced(client.clone(), ns);
    let svc = svc_api
        .get(name)
        .await
        .map_err(|e| ResolveError::Retry(format!("service {name}: {e}")))?;
    let spec = svc
        .spec
        .ok_or_else(|| ResolveError::Retry(format!("service {name} has no spec")))?;
    let selector = spec.selector.filter(|s| !s.is_empty()).ok_or_else(|| {
        // Selector-less services (ExternalName, manual Endpoints) have no
        // pods of their own — not something a retry fixes.
        ResolveError::Fatal(format!("service {name} has no pod selector"))
    })?;
    let sel_str = selector
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");
    let pod = fetch_ready_pod(client, ns, &sel_str).await?;

    // kubectl's translateServicePortToTargetPort: the requested port must
    // match a service port; its targetPort (default: the port itself) is
    // what the pod actually listens on. Named targetPorts resolve against
    // the chosen pod's container ports.
    let port = service_target_port(spec.ports.as_deref().unwrap_or(&[]), remote_port, &pod)?;
    let pod_name = pod.metadata.name.unwrap_or_default();
    Ok(PodTarget { pod: pod_name, port })
}

/// Map the requested service port to the pod's target port.
fn service_target_port(
    ports: &[k8s_openapi::api::core::v1::ServicePort],
    remote_port: u16,
    pod: &Pod,
) -> Result<u16, ResolveError> {
    let Some(sp) = ports.iter().find(|p| p.port == remote_port as i32) else {
        return Err(ResolveError::Fatal(format!(
            "service has no port {remote_port}"
        )));
    };
    match &sp.target_port {
        None => Ok(remote_port), // targetPort defaults to port
        Some(IntOrString::Int(i)) => u16::try_from(*i)
            .map_err(|_| ResolveError::Fatal(format!("targetPort {i} out of range"))),
        Some(IntOrString::String(name)) => named_container_port(pod, name).ok_or_else(|| {
            // The selected pod lacking the named port is pod-shape drift a
            // rollout can heal — retry, don't park.
            ResolveError::Retry(format!(
                "pod has no container port named '{name}'"
            ))
        }),
    }
}

/// Look up a named container port on a pod.
fn named_container_port(pod: &Pod, name: &str) -> Option<u16> {
    pod.spec.as_ref()?.containers.iter().find_map(|c| {
        c.ports.as_ref()?.iter().find_map(|p| {
            (p.name.as_deref() == Some(name))
                .then(|| u16::try_from(p.container_port).ok())
                .flatten()
        })
    })
}

/// Fetch the workload's label selector (typed per kind — each is a
/// one-field extraction, and the exhaustive match keeps "which kinds can
/// port-forward" in one visible place).
async fn workload_selector(
    client: &kube::Client,
    ns: &str,
    plural: &str,
    name: &str,
) -> Result<LabelSelector, ResolveError> {
    use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
    use k8s_openapi::api::batch::v1::Job;

    let missing = |e: kube::Error| ResolveError::Retry(format!("{plural}/{name}: {e}"));
    let no_sel = || ResolveError::Retry(format!("{plural}/{name} has no selector"));
    match plural {
        "deployments" => {
            let api: Api<Deployment> = Api::namespaced(client.clone(), ns);
            api.get(name).await.map_err(missing)?.spec.map(|s| s.selector).ok_or_else(no_sel)
        }
        "replicasets" => {
            let api: Api<ReplicaSet> = Api::namespaced(client.clone(), ns);
            api.get(name).await.map_err(missing)?.spec.map(|s| s.selector).ok_or_else(no_sel)
        }
        "statefulsets" => {
            let api: Api<StatefulSet> = Api::namespaced(client.clone(), ns);
            api.get(name).await.map_err(missing)?.spec.map(|s| s.selector).ok_or_else(no_sel)
        }
        "daemonsets" => {
            let api: Api<DaemonSet> = Api::namespaced(client.clone(), ns);
            api.get(name).await.map_err(missing)?.spec.map(|s| s.selector).ok_or_else(no_sel)
        }
        "jobs" => {
            let api: Api<Job> = Api::namespaced(client.clone(), ns);
            api.get(name)
                .await
                .map_err(missing)?
                .spec
                .and_then(|s| s.selector)
                .ok_or_else(no_sel)
        }
        other => Err(ResolveError::Fatal(format!(
            "port-forward not supported for '{other}'"
        ))),
    }
}

/// Render a `LabelSelector` as the API's label-selector query string.
/// Errors (as a message) on an operator we can't express — a config
/// problem, not a transient.
fn selector_to_string(sel: &LabelSelector) -> Result<String, String> {
    let mut parts: Vec<String> = Vec::new();
    if let Some(labels) = &sel.match_labels {
        parts.extend(labels.iter().map(|(k, v)| format!("{k}={v}")));
    }
    for expr in sel.match_expressions.as_deref().unwrap_or(&[]) {
        let key = &expr.key;
        let values = || {
            expr.values
                .as_deref()
                .unwrap_or(&[])
                .join(",")
        };
        match expr.operator.as_str() {
            "In" => parts.push(format!("{key} in ({})", values())),
            "NotIn" => parts.push(format!("{key} notin ({})", values())),
            "Exists" => parts.push(key.clone()),
            "DoesNotExist" => parts.push(format!("!{key}")),
            other => return Err(format!("unsupported selector operator '{other}'")),
        }
    }
    if parts.is_empty() {
        return Err("empty selector".to_string());
    }
    Ok(parts.join(","))
}

async fn pick_ready_pod_by_selector(
    client: &kube::Client,
    ns: &str,
    selector: &str,
) -> Result<String, ResolveError> {
    let pod = fetch_ready_pod(client, ns, selector).await?;
    Ok(pod.metadata.name.unwrap_or_default())
}

async fn fetch_ready_pod(
    client: &kube::Client,
    ns: &str,
    selector: &str,
) -> Result<Pod, ResolveError> {
    let api: Api<Pod> = Api::namespaced(client.clone(), ns);
    let pods = api
        .list(&ListParams::default().labels(selector))
        .await
        .map_err(|e| ResolveError::Retry(format!("listing pods ({selector}): {e}")))?;
    pick_ready_pod(pods.items)
        .ok_or_else(|| ResolveError::Retry(format!("no ready pods match {selector}")))
}

/// First pod that is running, not terminating, and Ready. `None` if no pod
/// qualifies — a retryable state (rollout in progress).
fn pick_ready_pod(pods: Vec<Pod>) -> Option<Pod> {
    pods.into_iter().find(|p| {
        p.metadata.deletion_timestamp.is_none()
            && p.status.as_ref().is_some_and(|s| {
                s.phase.as_deref() == Some("Running")
                    && s.conditions.as_deref().unwrap_or(&[]).iter().any(|c| {
                        c.type_ == "Ready" && c.status == "True"
                    })
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{
        Container, ContainerPort, PodCondition, PodSpec, PodStatus, ServicePort,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;

    fn ready_pod(name: &str) -> Pod {
        Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some("Running".into()),
                conditions: Some(vec![PodCondition {
                    type_: "Ready".into(),
                    status: "True".into(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn parse_target_splits_plural_and_bare_is_pod() {
        assert_eq!(parse_target("services/nginx"), ("services", "nginx"));
        assert_eq!(parse_target("pods/foo"), ("pods", "foo"));
        assert_eq!(parse_target("bare-name"), ("pods", "bare-name"));
    }

    #[test]
    fn selector_string_covers_labels_and_expressions() {
        let sel = LabelSelector {
            match_labels: Some([("app".to_string(), "web".to_string())].into()),
            match_expressions: Some(vec![
                LabelSelectorRequirement {
                    key: "tier".into(),
                    operator: "In".into(),
                    values: Some(vec!["a".into(), "b".into()]),
                },
                LabelSelectorRequirement {
                    key: "canary".into(),
                    operator: "DoesNotExist".into(),
                    values: None,
                },
            ]),
        };
        assert_eq!(
            selector_to_string(&sel).unwrap(),
            "app=web,tier in (a,b),!canary",
        );
        assert!(selector_to_string(&LabelSelector::default()).is_err(), "empty = error");
    }

    #[test]
    fn service_port_maps_int_named_and_default() {
        let mut pod = ready_pod("p");
        pod.spec = Some(PodSpec {
            containers: vec![Container {
                name: "c".into(),
                ports: Some(vec![ContainerPort {
                    name: Some("http".into()),
                    container_port: 8080,
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        });
        let ports = vec![
            ServicePort { port: 80, target_port: Some(IntOrString::String("http".into())), ..Default::default() },
            ServicePort { port: 443, target_port: Some(IntOrString::Int(8443)), ..Default::default() },
            ServicePort { port: 9090, target_port: None, ..Default::default() },
        ];
        // Named targetPort resolves against the pod's container ports.
        assert!(matches!(service_target_port(&ports, 80, &pod), Ok(8080)));
        // Numeric targetPort passes through.
        assert!(matches!(service_target_port(&ports, 443, &pod), Ok(8443)));
        // Absent targetPort defaults to the service port.
        assert!(matches!(service_target_port(&ports, 9090, &pod), Ok(9090)));
        // A port the service doesn't expose is a config error → Fatal.
        assert!(matches!(
            service_target_port(&ports, 5000, &pod),
            Err(ResolveError::Fatal(_)),
        ));
        // Named port missing on the pod is rollout-healable → Retry.
        let bare = ready_pod("q");
        assert!(matches!(
            service_target_port(&ports, 80, &bare),
            Err(ResolveError::Retry(_)),
        ));
    }

    #[test]
    fn pick_ready_pod_skips_terminating_and_unready() {
        let mut terminating = ready_pod("dying");
        terminating.metadata.deletion_timestamp =
            Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
                chrono::Utc::now(),
            ));
        let mut unready = ready_pod("pending");
        unready.status.as_mut().unwrap().conditions = Some(vec![PodCondition {
            type_: "Ready".into(),
            status: "False".into(),
            ..Default::default()
        }]);
        let good = ready_pod("good");
        let picked = pick_ready_pod(vec![terminating, unready, good]).expect("one qualifies");
        assert_eq!(picked.metadata.name.as_deref(), Some("good"));
        assert!(pick_ready_pod(vec![]).is_none());
    }
}
