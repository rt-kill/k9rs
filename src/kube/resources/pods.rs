
use k8s_openapi::api::core::v1::Pod;

use crate::kube::resources::row::{ContainerInfo, OwnerRefInfo, ResourceRow};

/// Computes the effective pod status string, mimicking kubectl's logic.
/// Computes the effective pod status string, matching k9s/kubectl logic.
///
/// Handles: NodeLost, SchedulingGated, sidecar init containers, detailed
/// init error reasons (OOMKilled, Signal:N, ExitCode:N), PodInitializing
/// filtering, Completed+Running with PodReady check, and proper
/// deletion timestamp interaction.
pub(crate) fn compute_pod_status(
    phase: &str,
    container_statuses: &[k8s_openapi::api::core::v1::ContainerStatus],
    init_container_statuses: &[k8s_openapi::api::core::v1::ContainerStatus],
    init_containers_spec: &[k8s_openapi::api::core::v1::Container],
    conditions: &[k8s_openapi::api::core::v1::PodCondition],
    reason: &Option<String>,
    has_deletion_timestamp: bool,
) -> String {
    const NODE_UNREACHABLE: &str = "NodeLost";

    // Start with the pod-level reason or phase.
    let mut status = if let Some(r) = reason {
        if !r.is_empty() { r.clone() } else { phase.to_string() }
    } else {
        phase.to_string()
    };

    // Check for SchedulingGated condition (K8s 1.26+).
    for cond in conditions {
        if cond.type_ == "PodScheduled" {
            if let Some(ref r) = cond.reason {
                if r == "SchedulingGated" {
                    status = "SchedulingGated".to_string();
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
            .map_or(false, |p| p == "Always");

        if let Some(ref state) = ics.state {
            if let Some(ref terminated) = state.terminated {
                if terminated.exit_code == 0 {
                    continue; // Successfully completed init container.
                }
                // Non-zero exit: show detailed reason.
                if let Some(ref r) = terminated.reason {
                    if !r.is_empty() {
                        return format!("Init:{}", r);
                    }
                }
                if let Some(sig) = terminated.signal {
                    if sig != 0 {
                        return format!("Init:Signal:{}", sig);
                    }
                }
                return format!("Init:ExitCode:{}", terminated.exit_code);
            } else if let Some(ref waiting) = state.waiting {
                let reason_str = waiting.reason.as_deref().unwrap_or("");
                // Filter out "PodInitializing" — show Init:i/n instead.
                if !reason_str.is_empty() && reason_str != "PodInitializing" {
                    return format!("Init:{}", reason_str);
                }
                return format!("Init:{}/{}", i, total_init);
            } else if state.running.is_some() {
                // Sidecar that is started+ready: healthy, skip it.
                if is_sidecar && ics.ready {
                    continue;
                }
                return format!("Init:{}/{}", i, total_init);
            }
        } else {
            // No state at all — container hasn't started.
            return format!("Init:{}/{}", i, total_init);
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
                        status = r.clone();
                    }
                }
            } else if let Some(ref terminated) = state.terminated {
                if let Some(ref r) = terminated.reason {
                    if !r.is_empty() {
                        status = r.clone();
                        continue;
                    }
                }
                if let Some(sig) = terminated.signal {
                    if sig != 0 {
                        status = format!("Signal:{}", sig);
                        continue;
                    }
                }
                status = format!("ExitCode:{}", terminated.exit_code);
            } else if state.running.is_some() && cs.ready {
                has_running = true;
            }
        }
    }

    // Completed phase with running+ready containers: check PodReady condition.
    if status == "Completed" && has_running {
        let pod_ready = conditions.iter().any(|c| c.type_ == "Ready" && c.status == "True");
        status = if pod_ready { "Running".to_string() } else { "NotReady".to_string() };
    }

    // Deletion timestamp handling.
    if has_deletion_timestamp {
        if reason.as_deref() == Some(NODE_UNREACHABLE) {
            return "Unknown".to_string();
        }
        return "Terminating".to_string();
    }

    status
}

/// Convert a k8s Pod into a generic ResourceRow with containers, labels, and owner refs in extra.
pub(crate) fn pod_to_row(pod: Pod) -> ResourceRow {
    let metadata = pod.metadata;
    let namespace = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let labels = metadata.labels.unwrap_or_default();
    let owner_references: Vec<OwnerRefInfo> = metadata
        .owner_references
        .unwrap_or_default()
        .into_iter()
        .map(|or| OwnerRefInfo {
            api_version: or.api_version,
            kind: or.kind,
            name: or.name,
            uid: or.uid,
        })
        .collect();
    let deletion_timestamp = metadata.deletion_timestamp;
    let age = metadata.creation_timestamp.map(|t| t.0);

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
    let phase = status.phase.clone().unwrap_or_else(|| "Unknown".to_string());
    let container_statuses = status.container_statuses.unwrap_or_default();
    let init_container_statuses = status.init_container_statuses.unwrap_or_default();
    let conditions = status.conditions.as_deref().unwrap_or(&[]);

    let effective_status = compute_pod_status(
        &phase,
        &container_statuses,
        &init_container_statuses,
        init_containers_spec,
        conditions,
        &status.reason,
        deletion_timestamp.is_some(),
    );

    // Ready count: denominator from spec (not status), includes sidecar init containers.
    // This matches k9s which uses spec.containers.len() + sidecar init count.
    let sidecar_count = init_containers_spec.iter()
        .filter(|c| c.restart_policy.as_deref() == Some("Always"))
        .count();
    let sidecar_ready = init_container_statuses.iter()
        .enumerate()
        .filter(|(i, cs)| {
            cs.ready && init_containers_spec.get(*i)
                .and_then(|c| c.restart_policy.as_deref())
                .map_or(false, |p| p == "Always")
        })
        .count();
    let total = (spec.containers.len() + sidecar_count) as i32;
    let ready_count = container_statuses.iter().filter(|cs| cs.ready).count() as i32 + sidecar_ready as i32;
    let ready = format!("{}/{}", ready_count, total);

    // Restarts: include sidecar init container restarts.
    let sidecar_restarts: i32 = init_container_statuses.iter()
        .enumerate()
        .filter(|(i, _)| init_containers_spec.get(*i)
            .and_then(|c| c.restart_policy.as_deref())
            .map_or(false, |p| p == "Always"))
        .map(|(_, cs)| cs.restart_count)
        .sum();
    let restarts: i32 = container_statuses.iter().map(|cs| cs.restart_count).sum::<i32>() + sidecar_restarts;

    let init_containers: Vec<ContainerInfo> = init_container_statuses
        .iter()
        .map(|cs| {
            let state = if let Some(ref s) = cs.state {
                if s.running.is_some() {
                    "Running".to_string()
                } else if let Some(ref w) = s.waiting {
                    w.reason.clone().unwrap_or_else(|| "Waiting".to_string())
                } else if let Some(ref t) = s.terminated {
                    t.reason.clone().unwrap_or_else(|| "Terminated".to_string())
                } else {
                    "Unknown".to_string()
                }
            } else {
                "Unknown".to_string()
            };
            ContainerInfo {
                name: format!("init:{}", cs.name),
                real_name: cs.name.clone(),
                image: cs.image.clone(),
                ready: cs.ready,
                state,
                restarts: cs.restart_count,
                ports: vec![],
            }
        })
        .collect();

    let regular_containers: Vec<ContainerInfo> = container_statuses
        .iter()
        .map(|cs| {
            let state = if let Some(ref s) = cs.state {
                if s.running.is_some() {
                    "Running".to_string()
                } else if let Some(ref w) = s.waiting {
                    w.reason.clone().unwrap_or_else(|| "Waiting".to_string())
                } else if let Some(ref t) = s.terminated {
                    t.reason.clone().unwrap_or_else(|| "Terminated".to_string())
                } else {
                    "Unknown".to_string()
                }
            } else {
                "Unknown".to_string()
            };
            ContainerInfo {
                ports: spec_ports.get(&cs.name).cloned().unwrap_or_default(),
                name: cs.name.clone(),
                real_name: cs.name.clone(),
                image: cs.image.clone(),
                ready: cs.ready,
                state,
                restarts: cs.restart_count,
            }
        })
        .collect();

    let mut containers_vec = init_containers;
    containers_vec.extend(regular_containers);

    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let _ = labels;

    // All container ports across all containers — used by port-forward dialog.
    let pf_ports: Vec<u16> = containers_vec.iter()
        .flat_map(|c| c.ports.iter().copied())
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
    let last_restart_str = crate::util::format_age(last_restart);

    // Readiness gates
    let readiness_gates = {
        let gate_count = spec.readiness_gates.as_ref().map(|g| g.len()).unwrap_or(0);
        if gate_count > 0 {
            let ready_gates = status.conditions.as_ref().map(|conds| {
                conds.iter().filter(|c| {
                    spec.readiness_gates.as_ref().map_or(false, |gates| {
                        gates.iter().any(|g| g.condition_type == c.type_ && c.status == "True")
                    })
                }).count()
            }).unwrap_or(0);
            format!("{}/{}", ready_gates, gate_count)
        } else {
            String::new()
        }
    };

    ResourceRow {
        cells: vec![
            namespace.clone(),
            name.clone(),
            ready,
            effective_status,
            restarts.to_string(),
            last_restart_str,
            "n/a".to_string(), // CPU (filled by metrics overlay)
            "n/a".to_string(), // MEM (filled by metrics overlay)
            pod_ip,
            node_display,
            qos_short.to_string(),
            service_account,
            readiness_gates,
            labels_str,
            crate::util::format_age(age),
        ],
        name,
        namespace: Some(namespace),
        containers: containers_vec,
        owner_refs: owner_references,
        pf_ports,
        node,
        crd_info: None,
        drill_target: None,
    }
}
