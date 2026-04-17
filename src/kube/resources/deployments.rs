
use k8s_openapi::api::apps::v1::Deployment;

use crate::kube::resources::{CommonMeta, WorkloadContainers};
use crate::kube::resources::row::{DrillTarget, ResourceRow, RowHealth};

/// Convert a k8s Deployment into a generic ResourceRow.
pub(crate) fn deployment_to_row(dep: Deployment) -> ResourceRow {
    let meta = CommonMeta::from_k8s(dep.metadata);

    let spec = dep.spec.unwrap_or_default();
    let selector_labels = spec.selector.match_labels.clone().unwrap_or_default();
    let containers = WorkloadContainers::from_pod_spec(spec.template.spec.as_ref());

    let desired = spec.replicas.unwrap_or(0);
    let status = dep.status.unwrap_or_default();
    let up_to_date = status.updated_replicas.unwrap_or(0);
    let available = status.available_replicas.unwrap_or(0);

    // READY uses available (ready for minReadySeconds), matching kubectl.
    let ready = format!("{}/{}", available, desired);

    // Drill-down: deployment → pods by selector labels.
    let drill_target = if !selector_labels.is_empty() {
        Some(DrillTarget::PodsByLabels {
            labels: selector_labels,
            breadcrumb: format!("deploy/{}", meta.name),
        })
    } else {
        Some(DrillTarget::PodsByNameGrep(meta.name.clone()))
    };

    let health = if desired == 0 { RowHealth::Pending }
        else if available < desired { RowHealth::Failed }
        else { RowHealth::Normal };

    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(), ready,
            up_to_date.to_string(), available.to_string(),
            containers.names, containers.images,
            meta.labels_str,
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        pf_ports: containers.tcp_ports,
        health,
        drill_target,
        ..Default::default()
    }
}
