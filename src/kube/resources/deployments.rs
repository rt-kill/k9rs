
use k8s_openapi::api::apps::v1::Deployment;

use crate::kube::resources::row::{DrillTarget, ResourceRow};

/// Convert a k8s Deployment into a generic ResourceRow.
pub(crate) fn deployment_to_row(dep: Deployment) -> ResourceRow {
    let metadata = dep.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let uid = metadata.uid.unwrap_or_default();
    let labels = metadata.labels.unwrap_or_default();
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let age = metadata.creation_timestamp.map(|t| t.0);

    let spec = dep.spec.unwrap_or_default();
    let selector_labels = spec.selector.match_labels.clone().unwrap_or_default();

    let container_ports: Vec<u16> = spec.template.spec.as_ref()
        .map(|ps| ps.containers.iter()
            .flat_map(|c| c.ports.as_ref().into_iter().flatten())
            .filter(|p| p.protocol.as_deref() != Some("UDP"))
            .map(|p| p.container_port as u16)
            .collect())
        .unwrap_or_default();

    let container_names = spec.template.spec.as_ref()
        .map(|ps| ps.containers.iter().map(|c| c.name.clone()).collect::<Vec<_>>().join(","))
        .unwrap_or_default();
    let images = spec.template.spec.as_ref()
        .map(|ps| ps.containers.iter().map(|c| c.image.clone().unwrap_or_default()).collect::<Vec<_>>().join(","))
        .unwrap_or_default();

    let desired = spec.replicas.unwrap_or(0);

    let status = dep.status.unwrap_or_default();
    let up_to_date = status.updated_replicas.unwrap_or(0);
    let available = status.available_replicas.unwrap_or(0);

    // READY uses available (ready for minReadySeconds), matching kubectl.
    let ready = format!("{}/{}", available, desired);

    let _ = uid; // uid no longer used on the client; server handles owner filtering

    // Drill-down: deployment → pods by selector labels.
    let drill_target = if !selector_labels.is_empty() {
        Some(DrillTarget::PodsByLabels {
            labels: selector_labels,
            breadcrumb: format!("deploy/{}", name),
        })
    } else {
        Some(DrillTarget::PodsByNameGrep(name.clone()))
    };

    ResourceRow {
        cells: vec![
            ns.clone(), name.clone(), ready,
            up_to_date.to_string(), available.to_string(),
            container_names, images,
            labels_str,
            crate::util::format_age(age),
        ],
        name,
        namespace: ns,
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: container_ports,
        node: None,
        crd_info: None,
        drill_target,
    }
}
