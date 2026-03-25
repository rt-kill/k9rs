use std::borrow::Cow;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::Node;

use super::KubeResource;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubeNode {
    pub name: String,
    pub status: String,
    pub roles: String,
    pub version: String,
    pub cpu_usage: String,
    pub mem_usage: String,
    pub cpu_capacity: String,
    pub mem_capacity: String,
    pub pods: String,
    pub age: Option<DateTime<Utc>>,
    pub labels: BTreeMap<String, String>,
}

impl KubeResource for KubeNode {
    fn headers() -> &'static [&'static str] {
        &[
            "NAME", "STATUS", "ROLES", "VERSION", "CPU", "MEMORY", "PODS", "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.status),
            Cow::Borrowed(&self.roles),
            Cow::Borrowed(&self.version),
            Cow::Owned(format!("{}/{}", self.cpu_usage, self.cpu_capacity)),
            Cow::Owned(format!("{}/{}", self.mem_usage, self.mem_capacity)),
            Cow::Borrowed(&self.pods),
            Cow::Owned(crate::util::format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind() -> &'static str {
        "node"
    }
}

impl From<Node> for KubeNode {
    fn from(node: Node) -> Self {
        let metadata = node.metadata;
        let name = metadata.name.unwrap_or_default();
        let labels = metadata.labels.clone().unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        // Determine roles from labels
        let roles = {
            let mut role_list: Vec<String> = Vec::new();
            for (key, value) in &labels {
                if let Some(role) = key.strip_prefix("node-role.kubernetes.io/") {
                    if role.is_empty() {
                        // When the key suffix is empty, extract role info from value;
                        // but when the value is also empty, skip pushing an empty string.
                        if !value.is_empty() {
                            role_list.push(value.clone());
                        }
                    } else {
                        role_list.push(role.to_string());
                    }
                }
            }
            if role_list.is_empty() {
                "<none>".to_string()
            } else {
                role_list.sort();
                role_list.join(",")
            }
        };

        let spec = node.spec;

        let status_val = node.status.unwrap_or_default();
        let status = {
            let conditions = status_val.conditions.unwrap_or_default();
            let mut node_status = "NotReady".to_string();
            for cond in &conditions {
                if cond.type_ == "Ready" && cond.status == "True" {
                    node_status = "Ready".to_string();
                    break;
                }
            }

            if spec.as_ref().and_then(|s| s.unschedulable).unwrap_or(false) {
                node_status = format!("{},SchedulingDisabled", node_status);
            }

            node_status
        };

        let version = status_val
            .node_info
            .as_ref()
            .map(|info| info.kubelet_version.clone())
            .unwrap_or_default();

        // Capacity resources
        let capacity = status_val.capacity.unwrap_or_default();
        let cpu_capacity = capacity
            .get("cpu")
            .map(|q| q.0.clone())
            .unwrap_or_default();
        let mem_capacity = capacity
            .get("memory")
            .map(|q| q.0.clone())
            .unwrap_or_default();
        let pods_capacity = capacity
            .get("pods")
            .map(|q| q.0.clone())
            .unwrap_or_default();

        KubeNode {
            name,
            status,
            roles,
            version,
            cpu_usage: "n/a".to_string(),
            mem_usage: "n/a".to_string(),
            cpu_capacity,
            mem_capacity,
            pods: pods_capacity,
            age,
            labels,
        }
    }
}
