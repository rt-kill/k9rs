use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Pod;

use crate::kube::resources::row::{ContainerInfo, ExtraValue, OwnerRefInfo, ResourceRow};

/// Computes the effective pod status string, mimicking kubectl's logic.
pub(crate) fn compute_pod_status(
    phase: &str,
    container_statuses: &[k8s_openapi::api::core::v1::ContainerStatus],
    init_container_statuses: &[k8s_openapi::api::core::v1::ContainerStatus],
    reason: &Option<String>,
    has_deletion_timestamp: bool,
) -> String {
    if let Some(r) = reason {
        if !r.is_empty() {
            if has_deletion_timestamp {
                return "Terminating".to_string();
            }
            return r.clone();
        }
    }

    for (i, ics) in init_container_statuses.iter().enumerate() {
        if let Some(ref state) = ics.state {
            if let Some(ref terminated) = state.terminated {
                if terminated.exit_code != 0 {
                    return "Init:Error".to_string();
                }
            } else if let Some(ref waiting) = state.waiting {
                if let Some(ref reason) = waiting.reason {
                    if !reason.is_empty() {
                        return format!("Init:{}", reason);
                    }
                }
                return format!("Init:{}/{}", i, init_container_statuses.len());
            } else if state.running.is_some() {
                return format!("Init:{}/{}", i, init_container_statuses.len());
            }
        }
    }

    // Iterate container statuses in reverse order to surface the most relevant status
    let mut status_str = phase.to_string();
    for cs in container_statuses.iter().rev() {
        if let Some(ref state) = cs.state {
            if let Some(ref waiting) = state.waiting {
                if let Some(ref reason) = waiting.reason {
                    if !reason.is_empty() {
                        return if has_deletion_timestamp {
                            "Terminating".to_string()
                        } else {
                            reason.clone()
                        };
                    }
                }
            } else if let Some(ref terminated) = state.terminated {
                if let Some(ref reason) = terminated.reason {
                    if !reason.is_empty() {
                        status_str = reason.clone();
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
                continue;
            }
        }
    }

    // Handle the case where status is "Completed" but containers are running
    if status_str == "Completed" {
        for cs in container_statuses.iter() {
            if let Some(ref state) = cs.state {
                if state.running.is_some() {
                    status_str = "Running".to_string();
                    break;
                }
            }
        }
    }

    if has_deletion_timestamp {
        return "Terminating".to_string();
    }

    status_str
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
    let node = spec.node_name.unwrap_or_default();

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

    let status = pod.status.unwrap_or_default();
    let pod_ip = status.pod_ip.unwrap_or_default();
    let phase = status.phase.clone().unwrap_or_else(|| "Unknown".to_string());
    let container_statuses = status.container_statuses.unwrap_or_default();
    let init_container_statuses = status.init_container_statuses.unwrap_or_default();

    let effective_status = compute_pod_status(
        &phase,
        &container_statuses,
        &init_container_statuses,
        &status.reason,
        deletion_timestamp.is_some(),
    );

    let total = container_statuses.len() as i32;
    let ready_count = container_statuses.iter().filter(|cs| cs.ready).count() as i32;
    let ready = format!("{}/{}", ready_count, total);
    let restarts: i32 = container_statuses.iter().map(|cs| cs.restart_count).sum();

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

    let mut containers = init_containers;
    containers.extend(regular_containers);

    let mut extra = BTreeMap::new();
    extra.insert("containers".into(), ExtraValue::Containers(containers));
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    extra.insert("labels".into(), ExtraValue::Map(labels));
    extra.insert("owner_references".into(), ExtraValue::OwnerRefs(owner_references));
    extra.insert("node".into(), ExtraValue::Str(node.clone()));

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
            node,
            qos_short.to_string(),
            service_account,
            readiness_gates,
            labels_str,
            crate::util::format_age(age),
        ],
        name,
        namespace,
        extra,
    }
}
