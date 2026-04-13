
use k8s_openapi::api::apps::v1::ReplicaSet;

use crate::kube::resources::row::{DrillTarget, ResourceRow};

/// Convert a k8s ReplicaSet into a generic ResourceRow.
pub(crate) fn replicaset_to_row(rs: ReplicaSet) -> ResourceRow {
    let metadata = rs.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let uid = metadata.uid.unwrap_or_default();
    let labels = metadata.labels.unwrap_or_default();
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let age = metadata.creation_timestamp.map(|t| t.0);

    let desired = rs.spec.and_then(|s| s.replicas).unwrap_or(0);
    let status = rs.status.unwrap_or_default();
    let current = status.replicas;
    let ready = status.ready_replicas.unwrap_or(0);

    let drill_target = if !uid.is_empty() {
        Some(DrillTarget::PodsByOwner {
            uid,
            kind: "ReplicaSet".to_string(),
            name: name.clone(),
        })
    } else {
        Some(DrillTarget::PodsByNameGrep(name.clone()))
    };

    ResourceRow {
        cells: vec![
            ns.clone(), name.clone(), desired.to_string(), current.to_string(),
            ready.to_string(), labels_str, crate::util::format_age(age),
        ],
        name,
        namespace: Some(ns),
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        crd_info: None,
        drill_target,
    }
}
