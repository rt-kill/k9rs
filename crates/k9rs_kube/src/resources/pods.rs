
use k8s_openapi::api::core::v1::Pod;

use super::ConvertToRow;
use k9rs_proto::resource_def::pods::PodDef;

use crate::resources::k8s_const::*;
use k9rs_proto::row::{CellValue, ContainerInfo, OwnerRefInfo, QuantityUnit, ResourceRow, RowHealth};

// Pod-specific K8s API reason / condition strings used more than once in this
// file but not referenced by other resource converters.
const COND_POD_SCHEDULED: &str = "PodScheduled";
const REASON_SCHEDULING_GATED: &str = "SchedulingGated";
const REASON_NODE_LOST: &str = "NodeLost";

/// Pod status display + health, computed together in one pass.
///
/// Returning both atomically prevents the old bug where `compute_pod_status`
/// emitted a free-form display string and the caller then re-classified
/// it via `match effective_status.as_str()` into `RowHealth`. Any string
/// the builder produced that didn't match the classifier's hardcoded set
/// silently became `RowHealth::Normal`.
pub(crate) struct PodStatus {
    pub display: String,
    pub health: RowHealth,
}

impl PodStatus {
    fn pending(s: impl Into<String>) -> Self {
        Self { display: s.into(), health: RowHealth::Pending }
    }
    fn failed(s: impl Into<String>) -> Self {
        Self { display: s.into(), health: RowHealth::Failed }
    }
}

/// Classify a bare status reason string into a typed health. This is the
/// SOURCE-OF-TRUTH classifier for K8s container/pod state strings, which
/// come in from the API as an open-ended set (custom controllers can add
/// new reasons).
///
/// Its input is always a RAW reason — a string the apiserver wrote — and
/// never one of the display forms this module composes (`Init:…`,
/// `ExitCode:N`, `Signal:N`). All three callers keep that separation on
/// purpose: `compute_pod_status` classifies an init container's `reason`
/// and formats `Init:{reason}` as two steps, settles the `ExitCode:` and
/// `Signal:` forms in their own arms before reaching here, and
/// [`container_health`] hands over a `ContainerStatus` reason untouched. A
/// composed form fed back in would fall to the `_` arm and come out
/// Pending, so `Init:ImagePullBackOff` would lose its red — which is what
/// `a_composed_display_form_never_reaches_the_classifier` holds.
///
/// This said "used only during `compute_pod_status`", which
/// [`container_health`] had contradicted since the day it was written —
/// and its own doc, one screen down, says so.
fn classify_pod_reason(reason: &str) -> RowHealth {
    match reason {
        PHASE_RUNNING | PHASE_SUCCEEDED | PHASE_COMPLETED => RowHealth::Normal,
        PHASE_PENDING | "ContainerCreating" | "PodInitializing" | REASON_TERMINATING
        | REASON_SCHEDULING_GATED | REASON_NOT_READY | PHASE_UNKNOWN => RowHealth::Pending,
        "Failed" | "Error" | "CrashLoopBackOff" | "ImagePullBackOff"
        | "ErrImagePull" | "OOMKilled" | "Evicted" | REASON_NODE_LOST
        | "CreateContainerConfigError" => RowHealth::Failed,
        // Unknown reason string — treat as pending so the row is visible
        // but not flagged as healthy. Previously defaulted to Normal which
        // silently hid misbehaving pods with unusual reasons.
        _ => RowHealth::Pending,
    }
}

/// Computes the effective pod status + health together, matching k9s/kubectl
/// display logic. Handles: NodeLost, SchedulingGated, sidecar init
/// containers, detailed init error reasons (OOMKilled, Signal:N, ExitCode:N),
/// PodInitializing filtering, Completed+Running with PodReady check, and
/// proper deletion timestamp interaction.
pub(crate) fn compute_pod_status(
    phase: &str,
    container_statuses: &[k8s_openapi::api::core::v1::ContainerStatus],
    init_container_statuses: &[k8s_openapi::api::core::v1::ContainerStatus],
    init_containers_spec: &[k8s_openapi::api::core::v1::Container],
    conditions: &[k8s_openapi::api::core::v1::PodCondition],
    reason: &Option<String>,
    has_deletion_timestamp: bool,
) -> PodStatus {
    // Start with the pod-level reason or phase.
    let mut status_str = if let Some(r) = reason {
        if !r.is_empty() { r.clone() } else { phase.to_string() }
    } else {
        phase.to_string()
    };

    // Check for SchedulingGated condition (K8s 1.26+).
    for cond in conditions {
        if cond.type_ == COND_POD_SCHEDULED {
            if let Some(ref r) = cond.reason {
                if r == REASON_SCHEDULING_GATED {
                    status_str = REASON_SCHEDULING_GATED.to_string();
                }
            }
        }
    }

    // Init container phase: check each init container in order.
    // Sidecar init containers (RestartPolicy: Always) that are started+ready
    // are skipped — they're healthy long-running sidecars, not blocking init.
    let total_init = init_container_statuses.len();
    for (i, ics) in init_container_statuses.iter().enumerate() {
        let is_sidecar = init_containers_spec.get(i)
            .and_then(|c| c.restart_policy.as_deref())
            .is_some_and(|p| p == RESTART_ALWAYS);

        if let Some(ref state) = ics.state {
            if let Some(ref terminated) = state.terminated {
                if terminated.exit_code == 0 {
                    continue; // Successfully completed init container.
                }
                // Non-zero exit on an init container is always Failed.
                if let Some(ref r) = terminated.reason {
                    if !r.is_empty() {
                        return PodStatus::failed(format!("Init:{}", r));
                    }
                }
                if let Some(sig) = terminated.signal {
                    if sig != 0 {
                        return PodStatus::failed(format!("Init:Signal:{}", sig));
                    }
                }
                return PodStatus::failed(format!("Init:ExitCode:{}", terminated.exit_code));
            } else if let Some(ref waiting) = state.waiting {
                let reason_str = waiting.reason.as_deref().unwrap_or("");
                // An init container stuck in an error-typed waiting state
                // (ImagePullBackOff, CrashLoopBackOff, ErrImagePull, ...) is
                // Failed — everything else that blocks init progress is
                // Pending.
                if !reason_str.is_empty() && reason_str != "PodInitializing" {
                    let health = classify_pod_reason(reason_str);
                    return PodStatus { display: format!("Init:{}", reason_str), health };
                }
                return PodStatus::pending(format!("Init:{}/{}", i, total_init));
            } else if state.running.is_some() {
                // Sidecar that is started+ready: healthy, skip it.
                if is_sidecar && ics.ready {
                    continue;
                }
                return PodStatus::pending(format!("Init:{}/{}", i, total_init));
            }
        } else {
            // No state at all — container hasn't started.
            return PodStatus::pending(format!("Init:{}/{}", i, total_init));
        }
    }

    // Regular container phase: iterate all containers (assign, don't return early).
    // The last assignment wins, matching k9s's behavior.
    let mut has_running = false;
    for cs in container_statuses.iter().rev() {
        if let Some(ref state) = cs.state {
            if let Some(ref waiting) = state.waiting {
                if let Some(ref r) = waiting.reason {
                    if !r.is_empty() {
                        status_str = r.clone();
                    }
                }
            } else if let Some(ref terminated) = state.terminated {
                if let Some(ref r) = terminated.reason {
                    if !r.is_empty() {
                        status_str = r.clone();
                        continue;
                    }
                }
                if let Some(sig) = terminated.signal {
                    if sig != 0 {
                        status_str = format!("Signal:{}", sig);
                        continue;
                    }
                }
                status_str = format!("ExitCode:{}", terminated.exit_code);
            } else if state.running.is_some() && cs.ready {
                has_running = true;
            }
        }
    }

    // Completed phase with running+ready containers: check PodReady condition.
    if status_str == PHASE_COMPLETED && has_running {
        let pod_ready = conditions.iter().any(|c| c.type_ == COND_READY && c.status == STATUS_TRUE);
        status_str = if pod_ready { PHASE_RUNNING.to_string() } else { REASON_NOT_READY.to_string() };
    }

    // Deletion timestamp handling.
    if has_deletion_timestamp {
        if reason.as_deref() == Some(REASON_NODE_LOST) {
            return PodStatus::pending(PHASE_UNKNOWN);
        }
        return PodStatus::pending(REASON_TERMINATING);
    }

    // Terminated with a signal or non-zero exit code is Failed.
    // ExitCode:0 is a clean exit (e.g. completed Job) — Normal.
    let health = if status_str == "ExitCode:0" {
        RowHealth::Normal
    } else if status_str.starts_with("ExitCode:") || status_str.starts_with("Signal:") {
        RowHealth::Failed
    } else {
        classify_pod_reason(&status_str)
    };
    PodStatus { display: status_str, health }
}

// ---------------------------------------------------------------------------
// PodDef — ResourceDef + ConvertToRow
// ---------------------------------------------------------------------------


impl ConvertToRow<Pod> for PodDef {
    fn convert(pod: Pod) -> ResourceRow {
    // Pods carry two fields no other resource row needs — `owner_references`
    // (for the owner-chain drill down) and `deletion_timestamp` (for the
    // Terminating pseudo-status). Pluck them off the owned metadata before
    // handing the rest to the common extractor, so the pod-specific fields
    // aren't shoehorned into `CommonMeta`.
    let mut metadata = pod.metadata;
    let owner_references: Vec<OwnerRefInfo> = metadata.owner_references.take()
        .unwrap_or_default()
        .into_iter()
        .map(|or| OwnerRefInfo { kind: or.kind, name: or.name, uid: or.uid })
        .collect();
    let deletion_timestamp = metadata.deletion_timestamp.take();
    let meta = crate::resources::CommonMeta::from_k8s(metadata);
    // The LABELS cell is built before the struct is picked apart, so this
    // converter reads the same `labels_cell` as the other eleven.
    let labels = meta.labels_cell();
    let namespace = meta.namespace;
    let name = meta.name;
    let age = meta.age;

    let spec = pod.spec.unwrap_or_default();
    // `spec.node_name` is `None` when the pod isn't yet scheduled. Keep
    // that distinction at the row level so the `ShowNode` action knows the
    // difference between "not a pod" and "pod waiting for a node".
    let node: Option<String> = spec.node_name.clone();
    let node_display: String = node.clone().unwrap_or_default();

    let init_containers_spec = spec.init_containers.as_deref().unwrap_or(&[]);
    let status = pod.status.unwrap_or_default();
    let pod_ip = status.pod_ip.unwrap_or_default();
    let phase = status.phase.clone().unwrap_or_else(|| PHASE_UNKNOWN.to_string());
    let container_statuses = status.container_statuses.unwrap_or_default();
    let init_container_statuses = status.init_container_statuses.unwrap_or_default();
    let conditions = status.conditions.as_deref().unwrap_or(&[]);

    let pod_status = compute_pod_status(
        &phase,
        &container_statuses,
        &init_container_statuses,
        init_containers_spec,
        conditions,
        &status.reason,
        deletion_timestamp.is_some(),
    );
    let effective_status = pod_status.display;
    let health = pod_status.health;

    // Ready count: denominator from spec (not status), includes sidecar init containers.
    // This matches k9s which uses spec.containers.len() + sidecar init count.
    let sidecar_count = init_containers_spec.iter()
        .filter(|c| c.restart_policy.as_deref() == Some(RESTART_ALWAYS))
        .count();
    let sidecar_ready = init_container_statuses.iter()
        .enumerate()
        .filter(|(i, cs)| {
            cs.ready && init_containers_spec.get(*i)
                .and_then(|c| c.restart_policy.as_deref())
                .is_some_and(|p| p == RESTART_ALWAYS)
        })
        .count();
    let total = (spec.containers.len() + sidecar_count) as i32;
    let ready_count = container_statuses.iter().filter(|cs| cs.ready).count() as i32 + sidecar_ready as i32;
    // Restarts: include sidecar init container restarts.
    let sidecar_restarts: i32 = init_container_statuses.iter()
        .enumerate()
        .filter(|(i, _)| init_containers_spec.get(*i)
            .and_then(|c| c.restart_policy.as_deref())
            .is_some_and(|p| p == RESTART_ALWAYS))
        .map(|(_, cs)| cs.restart_count)
        .sum();
    let restarts: i32 = container_statuses.iter().map(|cs| cs.restart_count).sum::<i32>() + sidecar_restarts;

    let init_containers: Vec<ContainerInfo> = init_container_statuses
        .iter()
        .map(|cs| {
            let (state, reason, ready) = container_status_and_ready(cs);
            let health = container_health(ready, &reason);
            ContainerInfo {
                name: cs.name.clone(),
                kind: k9rs_proto::row::ContainerKind::Init,
                image: cs.image.clone(),
                state,
                reason,
                ready,
                restart_count: cs.restart_count,
                health,
            }
        })
        .collect();

    let regular_containers: Vec<ContainerInfo> = container_statuses
        .iter()
        .map(|cs| {
            let (state, reason, ready) = container_status_and_ready(cs);
            let health = container_health(ready, &reason);
            ContainerInfo {
                name: cs.name.clone(),
                kind: k9rs_proto::row::ContainerKind::Regular,
                image: cs.image.clone(),
                state,
                reason,
                ready,
                restart_count: cs.restart_count,
                health,
            }
        })
        .collect();

    let mut containers_vec = init_containers;
    containers_vec.extend(regular_containers);

    // All forwardable container ports — see `resources::forwardable_ports`,
    // which every other PortForward-capable converter reads through
    // `WorkloadContainers`. This built its own: no protocol filter, so a
    // UDP-only port was offered and the forward then looped
    // `Reconnecting`; and keyed off `container_statuses`, so a PENDING pod
    // offered nothing and the form fell back to a free-text 8080 — while
    // the very same pod's Deployment offered the real ports.
    let pf_ports: Vec<u16> = super::forwardable_ports(&spec);

    // QOS class — a closed k8s set. An unexpected value shows "?" rather than
    // silently masquerading as BestEffort (which `_ => "BE"` used to do).
    let qos_short = match status.qos_class.as_deref() {
        Some("Guaranteed") => "GA",
        Some("Burstable") => "BU",
        Some("BestEffort") | None => "BE",
        Some(_) => "?",
    };

    // Outward edges, read BEFORE the spec is picked apart below.
    let references = pod_references(&spec);

    // Service account
    let service_account = spec.service_account_name.unwrap_or_default();

    // Last restart time
    let last_restart = container_statuses.iter()
        .filter_map(|cs| cs.last_state.as_ref()?.terminated.as_ref()?.finished_at.as_ref())
        .map(|t| t.0)
        .max();

    // Readiness gates
    let readiness_gates = {
        let gate_count = spec.readiness_gates.as_ref().map(|g| g.len()).unwrap_or(0);
        if gate_count > 0 {
            let ready_gates = status.conditions.as_ref().map(|conds| {
                conds.iter().filter(|c| {
                    spec.readiness_gates.as_ref().is_some_and(|gates| {
                        gates.iter().any(|g| g.condition_type == c.type_ && c.status == STATUS_TRUE)
                    })
                }).count()
            }).unwrap_or(0);
            format!("{}/{}", ready_gates, gate_count)
        } else {
            String::new()
        }
    };

    // Nominated node (set when pod preempts other pods but can't schedule yet)
    let nominated_node = status.nominated_node_name.unwrap_or_default();

    // --- Resource requests & limits ---
    // Sum across regular containers + sidecar init containers (restartPolicy=Always).
    let mut cpu_req_nano: u64 = 0;
    let mut cpu_lim_nano: u64 = 0;
    let mut mem_req_bytes: u64 = 0;
    let mut mem_lim_bytes: u64 = 0;
    let mut has_cpu_req = false;
    let mut has_cpu_lim = false;
    let mut has_mem_req = false;
    let mut has_mem_lim = false;

    // Helper closure to accumulate resources from a container's ResourceRequirements.
    let mut accum_resources = |res: &Option<k8s_openapi::api::core::v1::ResourceRequirements>| {
        if let Some(ref rr) = res {
            if let Some(ref requests) = rr.requests {
                if let Some(q) = requests.get("cpu") {
                    cpu_req_nano = cpu_req_nano.saturating_add(
                        crate::metrics::parse_cpu_to_nano(&q.0),
                    );
                    has_cpu_req = true;
                }
                if let Some(q) = requests.get("memory") {
                    mem_req_bytes = mem_req_bytes.saturating_add(
                        crate::metrics::parse_mem_to_bytes(&q.0),
                    );
                    has_mem_req = true;
                }
            }
            if let Some(ref limits) = rr.limits {
                if let Some(q) = limits.get("cpu") {
                    cpu_lim_nano = cpu_lim_nano.saturating_add(
                        crate::metrics::parse_cpu_to_nano(&q.0),
                    );
                    has_cpu_lim = true;
                }
                if let Some(q) = limits.get("memory") {
                    mem_lim_bytes = mem_lim_bytes.saturating_add(
                        crate::metrics::parse_mem_to_bytes(&q.0),
                    );
                    has_mem_lim = true;
                }
            }
        }
    };

    // Regular containers
    for c in &spec.containers {
        accum_resources(&c.resources);
    }
    // Sidecar init containers (restartPolicy=Always) contribute to running
    // resource usage just like regular containers.
    for c in init_containers_spec {
        if c.restart_policy.as_deref() == Some(RESTART_ALWAYS) {
            accum_resources(&c.resources);
        }
    }

    let cpu_req_milli = cpu_req_nano / 1_000_000;
    let cpu_lim_milli = cpu_lim_nano / 1_000_000;

    // `health` was computed by `compute_pod_status` above — no string
    // round-trip re-classification.

    // Cell order must match default_headers() and column_defs().
    let cells: Vec<CellValue> = vec![
        CellValue::Text(namespace.clone()),
        CellValue::Text(name.clone()),
        CellValue::Ratio { num: ready_count as u32, denom: total as u32 },
        CellValue::Status { text: effective_status, health },
        CellValue::Count(restarts as i64),
        CellValue::Age(last_restart.map(|t| t.timestamp())),
        CellValue::Placeholder,                                     // CPU (metrics overlay)
        CellValue::Percentage(None),                                // %CPU/R (metrics overlay)
        CellValue::Percentage(None),                                // %CPU/L (metrics overlay)
        CellValue::Placeholder,                                     // MEM (metrics overlay)
        CellValue::Percentage(None),                                // %MEM/R (metrics overlay)
        CellValue::Percentage(None),                                // %MEM/L (metrics overlay)
        // Absent reads and SORTS the same here as in the `CPU`/`MEM` and
        // `%` columns above — see `CellValue::quantity_or_absent`. These
        // four were `Text(String::new())`, which sorts before every real
        // value, so one row answered "no number" three different ways.
        CellValue::quantity_or_absent(has_cpu_req.then_some(cpu_req_milli), QuantityUnit::Millicores),
        CellValue::quantity_or_absent(has_cpu_lim.then_some(cpu_lim_milli), QuantityUnit::Millicores),
        CellValue::quantity_or_absent(has_mem_req.then_some(mem_req_bytes), QuantityUnit::Bytes),
        CellValue::quantity_or_absent(has_mem_lim.then_some(mem_lim_bytes), QuantityUnit::Bytes),
        CellValue::Text(pod_ip),
        CellValue::Text(node_display),
        CellValue::Text(nominated_node),
        CellValue::Text(qos_short.to_string()),
        CellValue::Text(service_account),
        CellValue::Text(readiness_gates),
        labels,
        CellValue::Age(age.map(|t| t.timestamp())),
    ];
    ResourceRow {
        cells,
        name,
        namespace: Some(namespace),
        containers: containers_vec,
        owner_refs: owner_references,
        references,
        pf_ports,
        node,
        health,
        drill_target: Some(k9rs_proto::row::DrillTarget::Derived(
            k9rs_proto::row::DerivedViewKind::Containers,
        )),
        cpu_request: if has_cpu_req { Some(cpu_req_milli) } else { None },
        cpu_limit: if has_cpu_lim { Some(cpu_lim_milli) } else { None },
        mem_request: if has_mem_req { Some(mem_req_bytes) } else { None },
        mem_limit: if has_mem_lim { Some(mem_lim_bytes) } else { None },
        ..Default::default()
    }
    }
}

/// Every object this pod's spec NAMES, deduped.
///
/// The outward edges of a pod, read once at conversion so "what uses this
/// PVC" is a filter over data rather than a grep of pod names. Covers the
/// places a pod can name another object: volumes (including projected
/// sources), env (`envFrom` and `valueFrom`), the service account, and image
/// pull secrets. Anything not on that list is not extractable and therefore
/// not claimable — `U` says what it can see.
pub fn referenceable_kinds() -> &'static [k9rs_proto::resource_def::BuiltInKind] {
    use k9rs_proto::resource_def::BuiltInKind;
    // The closed set `pod_references` can emit. `U` gates on this: without
    // it, `U` on a Deployment, Node, Service or Namespace opened a view
    // titled "Pods that reference deployments/api" containing nothing —
    // reporting a miss as an answer, which is the exact failure the reverse
    // lookup replaced.
    //
    // Kept honest by `every_emittable_reference_kind_is_declared`, which
    // runs a maximal pod spec through `pod_references` and asserts the two
    // sets are equal, both directions. This comment used to say the list was
    // "declared NEXT to it so the two cannot drift" — proximity is not a
    // mechanism, and a fifth `push(…)` below would have compiled, passed
    // every test, and been invisible to `U`.
    &[
        BuiltInKind::PersistentVolumeClaim,
        BuiltInKind::Secret,
        BuiltInKind::ConfigMap,
        BuiltInKind::ServiceAccount,
    ]
}

fn pod_references(
    spec: &k8s_openapi::api::core::v1::PodSpec,
) -> Vec<k9rs_proto::row::ResourceRefInfo> {
    use k9rs_proto::resource_def::BuiltInKind;
    use k9rs_proto::row::ResourceRefInfo;

    let mut out: Vec<ResourceRefInfo> = Vec::new();
    let mut push = |kind: BuiltInKind, name: Option<&String>| {
        let Some(name) = name.filter(|n| !n.is_empty()) else { return };
        let item = ResourceRefInfo { kind, name: name.clone() };
        if !out.contains(&item) {
            out.push(item);
        }
    };

    for v in spec.volumes.iter().flatten() {
        push(
            BuiltInKind::PersistentVolumeClaim,
            v.persistent_volume_claim.as_ref().map(|p| &p.claim_name),
        );
        push(BuiltInKind::Secret, v.secret.as_ref().and_then(|s| s.secret_name.as_ref()));
        push(BuiltInKind::ConfigMap, v.config_map.as_ref().map(|c| &c.name));
        // A projected volume is several sources wearing one mount.
        for src in v.projected.iter().flat_map(|p| p.sources.iter().flatten()) {
            push(BuiltInKind::Secret, src.secret.as_ref().map(|s| &s.name));
            push(BuiltInKind::ConfigMap, src.config_map.as_ref().map(|c| &c.name));
        }
    }

    let all_containers = spec
        .containers
        .iter()
        .chain(spec.init_containers.iter().flatten());
    for c in all_containers {
        for ef in c.env_from.iter().flatten() {
            push(BuiltInKind::Secret, ef.secret_ref.as_ref().map(|s| &s.name));
            push(BuiltInKind::ConfigMap, ef.config_map_ref.as_ref().map(|c| &c.name));
        }
        for e in c.env.iter().flatten() {
            let Some(from) = e.value_from.as_ref() else { continue };
            push(
                BuiltInKind::Secret,
                from.secret_key_ref.as_ref().map(|s| &s.name),
            );
            push(
                BuiltInKind::ConfigMap,
                from.config_map_key_ref.as_ref().map(|c| &c.name),
            );
        }
    }

    push(BuiltInKind::ServiceAccount, spec.service_account_name.as_ref());
    for s in spec.image_pull_secrets.iter().flatten() {
        push(BuiltInKind::Secret, Some(&s.name));
    }
    out
}

/// One container's health, from the SAME classifier that decides its pod's.
///
/// The client used to re-derive this in `app::derived`, keying off
/// `restart_count` and ignoring `reason` — so an `ImagePullBackOff` with zero
/// restarts was Failed on the pod row and Pending on the container row one
/// Enter down, while the cell text said "ImagePullBackOff" in both places.
fn container_health(ready: bool, reason: &Option<String>) -> RowHealth {
    if ready {
        return RowHealth::Normal;
    }
    match reason.as_deref() {
        Some(r) => classify_pod_reason(r),
        // Not ready and the kubelet gave no reason: starting, or terminated
        // cleanly. Pending either way — visible, not flagged.
        None => RowHealth::Pending,
    }
}

/// Extract status string and ready flag from a container status.
fn container_status_and_ready(
    cs: &k8s_openapi::api::core::v1::ContainerStatus,
) -> (k9rs_proto::row::ContainerState, Option<String>, bool) {
    use k9rs_proto::row::ContainerState;
    let ready = cs.ready;
    let (state, reason) = if let Some(ref state) = cs.state {
        if state.running.is_some() {
            (ContainerState::Running, None)
        } else if let Some(ref w) = state.waiting {
            (ContainerState::Waiting, w.reason.clone())
        } else if let Some(ref t) = state.terminated {
            (ContainerState::Terminated, t.reason.clone())
        } else {
            (ContainerState::Unknown, None)
        }
    } else {
        (ContainerState::Unknown, None)
    };
    (state, reason, ready)
}

#[cfg(test)]
#[path = "../tests/resources/pods.rs"]
mod tests;
