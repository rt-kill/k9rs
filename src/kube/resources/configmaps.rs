use k8s_openapi::api::core::v1::ConfigMap;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s ConfigMap into a generic ResourceRow.
pub(crate) fn configmap_to_row(cm: ConfigMap) -> ResourceRow {
    let meta = CommonMeta::from_k8s(cm.metadata);
    let data_count = cm.data.map(|d| d.len()).unwrap_or(0)
        + cm.binary_data.map(|d| d.len()).unwrap_or(0);
    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            data_count.to_string(),
            meta.labels_str, crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
