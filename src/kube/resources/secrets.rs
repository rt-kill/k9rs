use k8s_openapi::api::core::v1::Secret;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s Secret into a generic ResourceRow.
pub(crate) fn secret_to_row(secret: Secret) -> ResourceRow {
    let meta = CommonMeta::from_k8s(secret.metadata);
    let secret_type = secret.type_.unwrap_or_else(|| "Opaque".to_string());
    let data_count = secret.data.map(|d| d.len()).unwrap_or(0);
    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            secret_type, data_count.to_string(),
            meta.labels_str, crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
