use k8s_openapi::api::core::v1::ConfigMap;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s ConfigMap into a generic ResourceRow.
pub(crate) fn configmap_to_row(cm: ConfigMap) -> ResourceRow {
    let metadata = cm.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let labels = metadata.labels.unwrap_or_default();
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let age = metadata.creation_timestamp.map(|t| t.0);
    let data_count = cm.data.map(|d| d.len()).unwrap_or(0)
        + cm.binary_data.map(|d| d.len()).unwrap_or(0);
    ResourceRow {
        cells: vec![
            ns.clone(),
            name.clone(),
            data_count.to_string(),
            labels_str,
            crate::util::format_age(age),
        ],
        name,
        namespace: ns,
        extra: Default::default(),
    }
}
