use k8s_openapi::api::core::v1::Node;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s Node into a generic ResourceRow.
/// CPU and MEM cells are initially "n/a" and mutated in-place by `apply_node_metrics`.
pub(crate) fn node_to_row(node: Node) -> ResourceRow {
    let metadata = node.metadata;
    let name = metadata.name.unwrap_or_default();
    let labels = metadata.labels.clone().unwrap_or_default();
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let age = metadata.creation_timestamp.map(|t| t.0);

    // Determine roles from labels
    let roles = {
        let mut role_list: Vec<String> = Vec::new();
        for (key, value) in &labels {
            if let Some(role) = key.strip_prefix("node-role.kubernetes.io/") {
                if role.is_empty() {
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

    // Taint count
    let taints = spec.as_ref()
        .and_then(|s| s.taints.as_ref())
        .map(|t| t.len())
        .unwrap_or(0);

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

    let node_info = status_val.node_info.as_ref();
    let version = node_info
        .map(|info| info.kubelet_version.clone())
        .unwrap_or_default();
    let arch = node_info
        .map(|info| info.architecture.clone())
        .unwrap_or_default();

    // Addresses
    let addresses = status_val.addresses.unwrap_or_default();
    let internal_ip = addresses.iter()
        .find(|a| a.type_ == "InternalIP")
        .map(|a| a.address.clone())
        .unwrap_or_else(|| "<none>".to_string());
    let external_ip = addresses.iter()
        .find(|a| a.type_ == "ExternalIP")
        .map(|a| a.address.clone())
        .unwrap_or_else(|| "<none>".to_string());

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

    // CPU (col 8) and MEMORY (col 9) are initially n/a — mutated by apply_node_metrics.
    ResourceRow {
        cells: vec![
            name.clone(),
            status,
            roles,
            taints.to_string(),
            version,
            internal_ip,
            external_ip,
            pods_capacity,
            format!("n/a/{}", cpu_capacity),
            format!("n/a/{}", mem_capacity),
            arch,
            labels_str,
            crate::util::format_age(age),
        ],
        name,
        namespace: String::new(),
        extra: Default::default(),
    }
}
