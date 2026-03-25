use std::borrow::Cow;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::Pod;

use super::KubeResource;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubeContainer {
    /// Display name (e.g. "init:install-udf" for init containers, "app" for regular)
    pub name: String,
    /// Actual container name for kubectl commands (always without prefix)
    pub real_name: String,
    pub image: String,
    pub ready: bool,
    pub state: String,
    pub restarts: i32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubePod {
    pub namespace: String,
    pub name: String,
    pub ready: String,
    pub status: String,
    pub restarts: i32,
    pub cpu: String,
    pub mem: String,
    pub node: String,
    pub ip: String,
    pub age: Option<DateTime<Utc>>,
    pub containers: Vec<KubeContainer>,
    pub labels: BTreeMap<String, String>,
}

impl KubeResource for KubePod {
    fn headers() -> &'static [&'static str] {
        &[
            "NAMESPACE", "NAME", "READY", "STATUS", "RESTARTS", "CPU", "MEM", "NODE", "IP", "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.ready),
            Cow::Borrowed(&self.status),
            Cow::Owned(self.restarts.to_string()),
            Cow::Borrowed(&self.cpu),
            Cow::Borrowed(&self.mem),
            Cow::Borrowed(&self.node),
            Cow::Borrowed(&self.ip),
            Cow::Owned(crate::util::format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn kind() -> &'static str {
        "pod"
    }
}

impl From<Pod> for KubePod {
    fn from(pod: Pod) -> Self {
        let metadata = pod.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let labels = metadata.labels.unwrap_or_default();
        let deletion_timestamp = metadata.deletion_timestamp;

        let age = metadata.creation_timestamp.map(|t| t.0);

        let spec = pod.spec.unwrap_or_default();
        let node = spec.node_name.unwrap_or_default();

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

        let init_containers: Vec<KubeContainer> = init_container_statuses
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

                KubeContainer {
                    name: format!("init:{}", cs.name),
                    real_name: cs.name.clone(),
                    image: cs.image.clone(),
                    ready: cs.ready,
                    state,
                    restarts: cs.restart_count,
                }
            })
            .collect();

        let regular_containers: Vec<KubeContainer> = container_statuses
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

                KubeContainer {
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

        KubePod {
            namespace,
            name,
            ready,
            status: effective_status,
            restarts,
            cpu: "n/a".to_string(),
            mem: "n/a".to_string(),
            node,
            ip: pod_ip,
            age,
            containers,
            labels,
        }
    }
}

/// Computes the effective pod status string, mimicking kubectl's logic.
fn compute_pod_status(
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

    // Iterate container statuses in reverse order (matching k9s behavior)
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
