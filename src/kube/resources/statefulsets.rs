
use k8s_openapi::api::apps::v1::StatefulSet;

use crate::kube::resources::row::{DrillTarget, ResourceRow};

/// Convert a k8s StatefulSet into a generic ResourceRow.
pub(crate) fn statefulset_to_row(sts: StatefulSet) -> ResourceRow {
    let metadata = sts.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let labels = metadata.labels.unwrap_or_default();
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let age = metadata.creation_timestamp.map(|t| t.0);

    let spec = sts.spec.unwrap_or_default();
    let selector_labels = spec.selector.match_labels.clone().unwrap_or_default();
    let container_ports: Vec<u16> = spec.template.spec.as_ref()
        .map(|ps| ps.containers.iter()
            .flat_map(|c| c.ports.as_ref().into_iter().flatten())
            .filter(|p| p.protocol.as_deref() != Some("UDP"))
            .map(|p| p.container_port as u16)
            .collect())
        .unwrap_or_default();
    let service_name = spec.service_name.clone();
    let container_names = spec.template.spec.as_ref()
        .map(|ps| ps.containers.iter().map(|c| c.name.clone()).collect::<Vec<_>>().join(","))
        .unwrap_or_default();
    let images = spec.template.spec.as_ref()
        .map(|ps| ps.containers.iter().map(|c| c.image.clone().unwrap_or_default()).collect::<Vec<_>>().join(","))
        .unwrap_or_default();

    let desired = spec.replicas.unwrap_or(0);
    let ready_replicas = sts.status.and_then(|s| s.ready_replicas).unwrap_or(0);
    let ready = format!("{}/{}", ready_replicas, desired);

    let drill_target = if !selector_labels.is_empty() {
        Some(DrillTarget::PodsByLabels {
            labels: selector_labels,
            breadcrumb: format!("sts/{}", name),
        })
    } else {
        Some(DrillTarget::PodsByNameGrep(name.clone()))
    };

    ResourceRow {
        cells: vec![ns.clone(), name.clone(), ready, service_name, container_names, images, labels_str, crate::util::format_age(age)],
        name,
        namespace: Some(ns),
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: container_ports,
        node: None,
        crd_info: None,
        drill_target,
    }
}
