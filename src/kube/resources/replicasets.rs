
use k8s_openapi::api::apps::v1::ReplicaSet;

use crate::kube::resources::{CommonMeta, WorkloadContainers};
use crate::kube::resources::row::{DrillTarget, ResourceRow, RowHealth};

/// Convert a k8s ReplicaSet into a generic ResourceRow.
pub(crate) fn replicaset_to_row(rs: ReplicaSet) -> ResourceRow {
    let meta = CommonMeta::from_k8s(rs.metadata);

    let spec = rs.spec.unwrap_or_default();
    let desired = spec.replicas.unwrap_or(0);
    let containers = WorkloadContainers::from_pod_spec(spec.template.as_ref().and_then(|t| t.spec.as_ref()));

    let status = rs.status.unwrap_or_default();
    let current = status.replicas;
    let ready = status.ready_replicas.unwrap_or(0);

    let drill_target = if !meta.uid.is_empty() {
        Some(DrillTarget::PodsByOwner {
            uid: meta.uid.clone(),
            kind: crate::kube::resource_def::BuiltInKind::ReplicaSet,
            name: meta.name.clone(),
        })
    } else {
        Some(DrillTarget::PodsByNameGrep(meta.name.clone()))
    };

    let health = if desired == 0 { RowHealth::Pending }
        else if ready < desired { RowHealth::Failed }
        else { RowHealth::Normal };

    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            desired.to_string(), current.to_string(), ready.to_string(),
            containers.names, containers.images,
            meta.labels_str, crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        pf_ports: containers.tcp_ports,
        health,
        drill_target,
        ..Default::default()
    }
}
