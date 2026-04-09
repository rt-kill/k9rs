use k8s_openapi::api::storage::v1::StorageClass;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s StorageClass into a generic ResourceRow.
pub(crate) fn storage_class_to_row(sc: StorageClass) -> ResourceRow {
    let metadata = sc.metadata;
    let name = metadata.name.unwrap_or_default();
    let age = metadata.creation_timestamp.map(|t| t.0);
    let provisioner = sc.provisioner;
    let reclaim_policy = sc.reclaim_policy.unwrap_or_else(|| "Delete".to_string());
    let volume_binding_mode = sc.volume_binding_mode.unwrap_or_else(|| "Immediate".to_string());
    let allow_expansion = if sc.allow_volume_expansion.unwrap_or(false) { "true" } else { "false" };
    ResourceRow {
        cells: vec![name.clone(), provisioner, reclaim_policy, volume_binding_mode, allow_expansion.to_string(), crate::util::format_age(age)],
        name,
        namespace: String::new(),
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        crd_info: None,
        drill_target: None,
    }
}
