use k8s_openapi::api::core::v1::Node;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{DrillTarget, ResourceRow, RowHealth};

/// Convert a k8s Node into a generic ResourceRow.
/// CPU and MEM cells are initially "n/a" and mutated in-place by `apply_node_metrics`.
pub(crate) fn node_to_row(node: Node) -> ResourceRow {
    let meta = CommonMeta::from_k8s(node.metadata);

    // Determine roles from labels. Checks both `node-role.kubernetes.io/<role>`
    // (standard) and `kubernetes.io/role` (legacy) label conventions.
    let roles = {
        let mut role_list: Vec<String> = Vec::new();
        for (key, value) in &meta.labels {
            if let Some(role) = key.strip_prefix("node-role.kubernetes.io/") {
                if role.is_empty() {
                    if !value.is_empty() {
                        role_list.push(value.clone());
                    }
                } else {
                    role_list.push(role.to_string());
                }
            } else if key == "kubernetes.io/role" && !value.is_empty() {
                role_list.push(value.clone());
            }
        }
        role_list.sort();
        role_list.dedup();
        if role_list.is_empty() {
            "<none>".to_string()
        } else {
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
    // Classify node state + schedulability as typed booleans, then derive
    // both the display string AND the RowHealth from them. The older code
    // built `format!("{},SchedulingDisabled", ...)` and then ran
    // `status.contains("NotReady")` / `.contains("SchedulingDisabled")` —
    // synthesizing a string and re-parsing its own output.
    let is_ready = status_val.conditions.as_deref().unwrap_or(&[])
        .iter()
        .any(|cond| cond.type_ == "Ready" && cond.status == "True");
    let is_scheduling_disabled = spec.as_ref()
        .and_then(|s| s.unschedulable)
        .unwrap_or(false);

    let status = match (is_ready, is_scheduling_disabled) {
        (true, true) => "Ready,SchedulingDisabled".to_string(),
        (true, false) => "Ready".to_string(),
        (false, true) => "NotReady,SchedulingDisabled".to_string(),
        (false, false) => "NotReady".to_string(),
    };
    let health = if !is_ready {
        RowHealth::Failed
    } else if is_scheduling_disabled {
        RowHealth::Pending
    } else {
        RowHealth::Normal
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
    let drill_target = Some(DrillTarget::PodsByField(
        crate::app::nav::K8sFieldSelector::SpecNodeName(meta.name.clone()),
    ));

    ResourceRow {
        cells: vec![
            meta.name.clone(),
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
            meta.labels_str,
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: None,
        health,
        drill_target,
        ..Default::default()
    }
}
