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
    use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
    use k8s_openapi::api::batch::v1::Job;
    use k9rs_proto::resource_def::BuiltInKind;

    let (plural, name) = parse_target(target);
    let Some(kind) = k9rs_proto::resource_def::def_by_plural(plural).map(|d| d.kind()) else {
        return Err(ResolveError::Fatal(format!(
            "port-forward not supported for '{plural}'"
        )));
    };

    // THE list of kinds a port-forward can resolve, and the only one.
    //
    // It was two lists of plural strings — this dispatch and a
    // `workload_selector` whose arms had to agree with it — plus each def's
    // `operations()` as a silent third, which is what `:pf` is actually
    // gated on. They happened to agree; nothing checked it, and either
    // string drifting yielded a `Fatal` that read like a user error. Keyed
    // on `BuiltInKind` now, held against `operations()` by
    // `every_port_forwardable_kind_is_resolvable`.
    match kind {
        // Bare pod: existence and readiness surface at tunnel time (the
        // canary stream fails → retry), so no lookup here.
        BuiltInKind::Pod => Ok(PodTarget { pod: name.to_string(), port: remote_port }),
        BuiltInKind::Service => resolve_service(client, ns, name, remote_port).await,
        // Each workload names its pods by label selector; the only
        // difference between these arms is which typed `Api` and where the
        // selector sits in that kind's spec (`Job`'s is already optional).
        BuiltInKind::Deployment => {
            let api: Api<Deployment> = Api::namespaced(client.clone(), ns);
            let obj = api.get(name).await.map_err(missing(plural, name))?;
            let sel = obj.spec.map(|s| s.selector).ok_or_else(no_sel(plural, name))?;
            pod_by_selector(client, ns, sel, remote_port).await
        }
        BuiltInKind::ReplicaSet => {
            let api: Api<ReplicaSet> = Api::namespaced(client.clone(), ns);
            let obj = api.get(name).await.map_err(missing(plural, name))?;
            let sel = obj.spec.map(|s| s.selector).ok_or_else(no_sel(plural, name))?;
            pod_by_selector(client, ns, sel, remote_port).await
        }
        BuiltInKind::StatefulSet => {
            let api: Api<StatefulSet> = Api::namespaced(client.clone(), ns);
            let obj = api.get(name).await.map_err(missing(plural, name))?;
            let sel = obj.spec.map(|s| s.selector).ok_or_else(no_sel(plural, name))?;
            pod_by_selector(client, ns, sel, remote_port).await
        }
        BuiltInKind::DaemonSet => {
            let api: Api<DaemonSet> = Api::namespaced(client.clone(), ns);
            let obj = api.get(name).await.map_err(missing(plural, name))?;
            let sel = obj.spec.map(|s| s.selector).ok_or_else(no_sel(plural, name))?;
            pod_by_selector(client, ns, sel, remote_port).await
        }
        BuiltInKind::Job => {
            let api: Api<Job> = Api::namespaced(client.clone(), ns);
            let obj = api.get(name).await.map_err(missing(plural, name))?;
            let sel = obj.spec.and_then(|s| s.selector).ok_or_else(no_sel(plural, name))?;
            pod_by_selector(client, ns, sel, remote_port).await
        }
        _ => Err(ResolveError::Fatal(format!(
            "port-forward not supported for '{plural}'"
        ))),
    }
}

/// Pick a ready pod matching `selector`.
async fn pod_by_selector(
    client: &kube::Client,
    ns: &str,
    selector: LabelSelector,
    remote_port: u16,
) -> Result<PodTarget, ResolveError> {
    let sel_str = selector_to_string(&selector).map_err(ResolveError::Fatal)?;
    let pod = pick_ready_pod_by_selector(client, ns, &sel_str).await?;
    Ok(PodTarget { pod, port: remote_port })
}

/// The two error shapes the workload arms above share: a failed GET is
/// worth retrying (the object may be appearing), a spec with no selector is
/// not a transient.
fn missing<'a>(plural: &'a str, name: &'a str) -> impl FnOnce(kube::Error) -> ResolveError + 'a {
    move |e| ResolveError::Retry(format!("{plural}/{name}: {e}"))
}

fn no_sel<'a>(plural: &'a str, name: &'a str) -> impl FnOnce() -> ResolveError + 'a {
    move || ResolveError::Retry(format!("{plural}/{name} has no selector"))
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
    // A service's `spec.selector` is an equality-only label selector, so it
    // renders through the same grammar as a workload's `LabelSelector`
    // rather than a `k=v` join of its own — which is what it was, 100 lines
    // from `selector_to_string`'s identical `match_labels` arm.
    //
    // With no `match_expressions`, the only way that renderer can refuse is
    // the empty selector, and that is the refusal this maps: selector-less
    // services (ExternalName, manual Endpoints) have no pods of their own,
    // which is not something a retry fixes.
    let sel_str = selector_to_string(&LabelSelector {
        match_labels: spec.selector,
        match_expressions: None,
    })
    .map_err(|_| ResolveError::Fatal(format!("service {name} has no pod selector")))?;
    let pod = fetch_ready_pod(client, ns, &sel_str).await?;

    // kubectl's translateServicePortToTargetPort: the requested port must
    // match a service port; its targetPort (default: the port itself) is
    // what the pod actually listens on. Named targetPorts resolve against
    // the chosen pod's container ports.
    let port = service_target_port(spec.ports.as_deref().unwrap_or(&[]), remote_port, &pod)?;
    Ok(PodTarget { pod: pod_name(&pod)?, port })
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
    pod_name(&pod)
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

/// A chosen pod's name, or a retryable refusal.
///
/// Both call sites were `pod.metadata.name.unwrap_or_default()`, so a pod
/// the apiserver returned without a name produced a port-forward to pod
/// `""` — `kubectl port-forward pod/ …`, which fails later and further away
/// with nothing pointing back here. An unnamed pod is not a pod we can
/// forward to; `Retry` because the list is worth re-reading.
fn pod_name(pod: &Pod) -> Result<String, ResolveError> {
    pod.metadata
        .name
        .clone()
        .filter(|n| !n.is_empty())
        .ok_or_else(|| ResolveError::Retry("the chosen pod has no name".to_string()))
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
#[path = "../tests/local/pf_resolve.rs"]
mod tests;
