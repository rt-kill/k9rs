use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::DaemonSet;

use crate::kube::resources::row::{ExtraValue, ResourceRow};

/// Convert a k8s DaemonSet into a generic ResourceRow.
pub(crate) fn daemonset_to_row(ds: DaemonSet) -> ResourceRow {
    let metadata = ds.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let uid = metadata.uid.unwrap_or_default();
    let labels = metadata.labels.unwrap_or_default();
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let age = metadata.creation_timestamp.map(|t| t.0);

    let selector_labels = ds.spec.as_ref()
        .and_then(|s| s.selector.match_labels.clone())
        .unwrap_or_default();
    let container_ports: Vec<u16> = ds.spec.as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .map(|ps| ps.containers.iter()
            .flat_map(|c| c.ports.as_ref().into_iter().flatten())
            .filter(|p| p.protocol.as_deref() != Some("UDP"))
            .map(|p| p.container_port as u16)
            .collect())
        .unwrap_or_default();

    let node_selector = ds.spec.as_ref()
        .and_then(|s| s.template.spec.as_ref())
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

    let mut extra = BTreeMap::new();
    extra.insert("selector_labels".into(), ExtraValue::Map(selector_labels));
    extra.insert("uid".into(), ExtraValue::Str(uid));
    extra.insert("container_ports".into(), ExtraValue::Ports(container_ports));

    ResourceRow {
        cells: vec![
            ns.clone(), name.clone(), desired.to_string(), current.to_string(),
            ready.to_string(), up_to_date.to_string(), available.to_string(),
            node_selector,
            labels_str,
            crate::util::format_age(age),
        ],
        name,
        namespace: ns,
        extra,
    }
}
