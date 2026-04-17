use k8s_openapi::api::storage::v1::StorageClass;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s StorageClass into a generic ResourceRow.
pub(crate) fn storage_class_to_row(sc: StorageClass) -> ResourceRow {
    let meta = CommonMeta::from_k8s(sc.metadata);
    let provisioner = sc.provisioner;
    let reclaim_policy = sc.reclaim_policy.unwrap_or_else(|| "Delete".to_string());
    let volume_binding_mode = sc.volume_binding_mode.unwrap_or_else(|| "Immediate".to_string());
    let allow_expansion = if sc.allow_volume_expansion.unwrap_or(false) { "true" } else { "false" };
    ResourceRow {
        cells: vec![
            meta.name.clone(),
            provisioner, reclaim_policy, volume_binding_mode,
            allow_expansion.to_string(),
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: None,
        ..Default::default()
    }
}
