
use k8s_openapi::api::apps::v1::DaemonSet;

use crate::kube::resources::{CommonMeta, WorkloadContainers};
use crate::kube::resources::row::{DrillTarget, ResourceRow, RowHealth};

/// Convert a k8s DaemonSet into a generic ResourceRow.
pub(crate) fn daemonset_to_row(ds: DaemonSet) -> ResourceRow {
    let meta = CommonMeta::from_k8s(ds.metadata);

    let selector_labels = ds.spec.as_ref()
        .and_then(|s| s.selector.match_labels.clone())
        .unwrap_or_default();
    let pod_spec = ds.spec.as_ref().and_then(|s| s.template.spec.as_ref());
    let containers = WorkloadContainers::from_pod_spec(pod_spec);

    let node_selector = pod_spec
        .and_then(|ps| ps.node_selector.as_ref())
        .map(|ns| {
            let pairs: Vec<String> = ns.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
            if pairs.is_empty() { "<none>".to_string() } else { pairs.join(",") }
        })
        .unwrap_or_else(|| "<none>".to_string());

    let status = ds.status.unwrap_or_default();
    let desired = status.desired_number_scheduled;
    let current = status.current_number_scheduled;
    let ready = status.number_ready;
    let up_to_date = status.updated_number_scheduled.unwrap_or(0);
    let available = status.number_available.unwrap_or(0);

    let drill_target = if !selector_labels.is_empty() {
        Some(DrillTarget::PodsByLabels {
            labels: selector_labels,
            breadcrumb: format!("ds/{}", meta.name),
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
            up_to_date.to_string(), available.to_string(),
            node_selector,
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
