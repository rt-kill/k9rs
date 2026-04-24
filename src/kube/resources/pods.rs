
use k8s_openapi::api::core::v1::Pod;

use crate::kube::protocol::{OperationKind, ResourceScope};
use crate::kube::resource_def::*;
use crate::kube::resources::k8s_const::*;
use crate::kube::resources::row::{CellValue, ContainerInfo, OwnerRefInfo, QuantityUnit, ResourceRow, RowHealth};

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
/// new reasons). Used only during `compute_pod_status` — callers never
/// re-classify a display string afterwards.
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

    // Exit code terminated states are always failed — catch them via
    // classify_pod_reason's default (Pending), then override below.
    let health = if status_str.starts_with("ExitCode:") || status_str.starts_with("Signal:") {
        RowHealth::Failed
    } else {
        classify_pod_reason(&status_str)
    };
    PodStatus { display: status_str, health }
}

// ---------------------------------------------------------------------------
// PodDef — ResourceDef + ConvertToRow
// ---------------------------------------------------------------------------

pub struct PodDef;

impl ResourceDef for PodDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Pod }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Pod",
            plural: "pods", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["po", "pod", "pods"] }
    fn short_label(&self) -> &str { "Pods" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "READY", "STATUS", "RESTARTS", "LAST RESTART",
         "CPU", "MEM",
         "CPU/R", "CPU/L", "MEM/R", "MEM/L",
         "%CPU/R", "%CPU/L", "%MEM/R", "%MEM/L",
         "IP", "NODE", "NOMINATED NODE",
         "QOS", "SERVICE-ACCOUNT",
         "READINESS GATES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }
    fn metrics_kind(&self) -> Option<MetricsKind> { Some(MetricsKind::Pod) }

    fn column_defs(&self) -> Vec<ColumnDef> {
        use ColumnDef as C;
        use MetricsColumn::*;
        vec![
            C::new("NAMESPACE"), C::new("NAME"), C::new("READY"), C::new("STATUS"),
            C::new("RESTARTS"), C::age("LAST RESTART"),
            C::new("CPU").with_metrics(Cpu), C::new("MEM").with_metrics(Mem),
            C::extra("CPU/R"), C::extra("CPU/L"), C::extra("MEM/R"), C::extra("MEM/L"),
            C::extra("%CPU/R").with_metrics(CpuPercentRequest),
            C::extra("%CPU/L").with_metrics(CpuPercentLimit),
            C::extra("%MEM/R").with_metrics(MemPercentRequest),
            C::extra("%MEM/L").with_metrics(MemPercentLimit),
            C::new("IP"), C::new("NODE"), C::extra("NOMINATED NODE"),
            C::extra("QOS"),
            C::extra("SERVICE-ACCOUNT"), C::extra("READINESS GATES"),
            C::extra("LABELS"), C::age("AGE"),
        ]
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, Shell, ShowNode, PortForward, ForceKill]
    }
}

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
    let meta = crate::kube::resources::CommonMeta::from_k8s(metadata);
    let namespace = meta.namespace;
    let name = meta.name;
    let labels_str = meta.labels_str;
    let age = meta.age;

    let spec = pod.spec.unwrap_or_default();
    // `spec.node_name` is `None` when the pod isn't yet scheduled. Keep
    // that distinction at the row level so the `ShowNode` action knows the
    // difference between "not a pod" and "pod waiting for a node".
    let node: Option<String> = spec.node_name.clone();
    let node_display: String = node.clone().unwrap_or_default();

    // Build a map of container name -> ports from the spec (for port-forward).
    let spec_ports: std::collections::HashMap<String, Vec<u16>> = spec
        .containers
        .iter()
        .map(|c| {
            let ports = c
                .ports
                .as_ref()
                .map(|ps| ps.iter().map(|p| p.container_port as u16).collect())
                .unwrap_or_default();
            (c.name.clone(), ports)
        })
        .collect();

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
        .map(|cs| ContainerInfo {
            name: cs.name.clone(),
            kind: crate::kube::resources::row::ContainerKind::Init,
        })
        .collect();

    let regular_containers: Vec<ContainerInfo> = container_statuses
        .iter()
        .map(|cs| ContainerInfo {
            name: cs.name.clone(),
            kind: crate::kube::resources::row::ContainerKind::Regular,
        })
        .collect();

    let mut containers_vec = init_containers;
    containers_vec.extend(regular_containers);

    // All container ports across all containers — used by port-forward dialog.
    // Drawn directly from the spec (`spec_ports`) since `ContainerInfo` no
    // longer carries a per-container port list (it was wire-shipped only to
    // be re-flattened here, dead bytes for every other consumer).
    let pf_ports: Vec<u16> = container_statuses
        .iter()
        .flat_map(|cs| spec_ports.get(&cs.name).cloned().unwrap_or_default())
        .collect();

    // QOS class
    let qos = status.qos_class.as_deref().unwrap_or("BestEffort");
    let qos_short = match qos {
        "Guaranteed" => "GA",
        "Burstable" => "BU",
        _ => "BE",
    };

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
                        crate::kube::metrics::parse_cpu_to_nano(&q.0),
                    );
                    has_cpu_req = true;
                }
                if let Some(q) = requests.get("memory") {
                    mem_req_bytes = mem_req_bytes.saturating_add(
                        crate::kube::metrics::parse_mem_to_bytes(&q.0),
                    );
                    has_mem_req = true;
                }
            }
            if let Some(ref limits) = rr.limits {
                if let Some(q) = limits.get("cpu") {
                    cpu_lim_nano = cpu_lim_nano.saturating_add(
                        crate::kube::metrics::parse_cpu_to_nano(&q.0),
                    );
                    has_cpu_lim = true;
                }
                if let Some(q) = limits.get("memory") {
                    mem_lim_bytes = mem_lim_bytes.saturating_add(
                        crate::kube::metrics::parse_mem_to_bytes(&q.0),
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

    let cells: Vec<CellValue> = vec![
        CellValue::Text(namespace.clone()),
        CellValue::Text(name.clone()),
        CellValue::Ratio { num: ready_count as u32, denom: total as u32 },
        CellValue::Status { text: effective_status, health },
        CellValue::Count(restarts as i64),
        CellValue::Age(last_restart.map(|t| t.timestamp())),
        CellValue::Placeholder,                                     // CPU (metrics overlay)
        CellValue::Placeholder,                                     // MEM (metrics overlay)
        if has_cpu_req { CellValue::Quantity { value: cpu_req_milli, unit: QuantityUnit::Millicores } } else { CellValue::Text(String::new()) },
        if has_cpu_lim { CellValue::Quantity { value: cpu_lim_milli, unit: QuantityUnit::Millicores } } else { CellValue::Text(String::new()) },
        if has_mem_req { CellValue::Quantity { value: mem_req_bytes, unit: QuantityUnit::Bytes } } else { CellValue::Text(String::new()) },
        if has_mem_lim { CellValue::Quantity { value: mem_lim_bytes, unit: QuantityUnit::Bytes } } else { CellValue::Text(String::new()) },
        CellValue::Percentage(None),                                // %CPU/R (metrics overlay)
        CellValue::Percentage(None),                                // %CPU/L (metrics overlay)
        CellValue::Percentage(None),                                // %MEM/R (metrics overlay)
        CellValue::Percentage(None),                                // %MEM/L (metrics overlay)
        CellValue::Text(pod_ip),
        CellValue::Text(node_display),
        CellValue::Text(nominated_node),
        CellValue::Text(qos_short.to_string()),
        CellValue::Text(service_account),
        CellValue::Text(readiness_gates),
        CellValue::from_comma_str(&labels_str),
        CellValue::Age(age.map(|t| t.timestamp())),
    ];
    ResourceRow {
        cells,
        name,
        namespace: Some(namespace),
        containers: containers_vec,
        owner_refs: owner_references,
        pf_ports,
        node,
        health,
        cpu_request: if has_cpu_req { Some(cpu_req_milli) } else { None },
        cpu_limit: if has_cpu_lim { Some(cpu_lim_milli) } else { None },
        mem_request: if has_mem_req { Some(mem_req_bytes) } else { None },
        mem_limit: if has_mem_lim { Some(mem_lim_bytes) } else { None },
        ..Default::default()
    }
    }
}
