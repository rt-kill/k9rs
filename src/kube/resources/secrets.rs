use k8s_openapi::api::core::v1::Secret;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s Secret into a generic ResourceRow.
pub(crate) fn secret_to_row(secret: Secret) -> ResourceRow {
    let metadata = secret.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let labels = metadata.labels.unwrap_or_default();
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let age = metadata.creation_timestamp.map(|t| t.0);
    let secret_type = secret.type_.unwrap_or_else(|| "Opaque".to_string());
    let data_count = secret.data.map(|d| d.len()).unwrap_or(0);
    ResourceRow {
        cells: vec![
            ns.clone(),
            name.clone(),
            secret_type,
            data_count.to_string(),
            labels_str,
            crate::util::format_age(age),
        ],
        name,
        namespace: ns,
        extra: Default::default(),
    }
}
