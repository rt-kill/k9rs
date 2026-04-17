
use k8s_openapi::api::apps::v1::StatefulSet;

use crate::kube::resources::{CommonMeta, WorkloadContainers};
use crate::kube::resources::row::{DrillTarget, ResourceRow, RowHealth};

/// Convert a k8s StatefulSet into a generic ResourceRow.
pub(crate) fn statefulset_to_row(sts: StatefulSet) -> ResourceRow {
    let meta = CommonMeta::from_k8s(sts.metadata);

    let spec = sts.spec.unwrap_or_default();
    let selector_labels = spec.selector.match_labels.clone().unwrap_or_default();
    let containers = WorkloadContainers::from_pod_spec(spec.template.spec.as_ref());
    let service_name = spec.service_name.clone();

    let desired = spec.replicas.unwrap_or(0);
    let ready_replicas = sts.status.and_then(|s| s.ready_replicas).unwrap_or(0);
    let ready = format!("{}/{}", ready_replicas, desired);

    let drill_target = if !selector_labels.is_empty() {
        Some(DrillTarget::PodsByLabels {
            labels: selector_labels,
            breadcrumb: format!("sts/{}", meta.name),
        })
    } else {
        Some(DrillTarget::PodsByNameGrep(meta.name.clone()))
    };

    let health = if desired == 0 { RowHealth::Pending }
        else if ready_replicas < desired { RowHealth::Failed }
        else { RowHealth::Normal };

    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(), ready,
            service_name, containers.names, containers.images,
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
