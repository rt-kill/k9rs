use k8s_openapi::api::core::v1::PersistentVolume;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s PersistentVolume into a generic ResourceRow.
pub(crate) fn pv_to_row(pv: PersistentVolume) -> ResourceRow {
    use crate::kube::resources::access_mode_short;
    let metadata = pv.metadata;
    let name = metadata.name.unwrap_or_default();
    let age = metadata.creation_timestamp.map(|t| t.0);
    let spec = pv.spec.unwrap_or_default();
    let capacity = spec.capacity.as_ref()
        .and_then(|c| c.get("storage"))
        .map(|q| q.0.clone())
        .unwrap_or_default();
    let access_modes = spec.access_modes.as_ref()
        .map(|modes| modes.iter().map(|m| access_mode_short(m)).collect::<Vec<_>>().join(","))
        .unwrap_or_default();
    let reclaim_policy = spec.persistent_volume_reclaim_policy.unwrap_or_else(|| "Retain".to_string());
    let status = pv.status.and_then(|s| s.phase).unwrap_or_else(|| "Available".to_string());
    let claim = spec.claim_ref
        .map(|cr| {
            let cns = cr.namespace.unwrap_or_default();
            let cn = cr.name.unwrap_or_default();
            if cns.is_empty() { cn } else { format!("{}/{}", cns, cn) }
        })
        .unwrap_or_default();
    let storage_class = spec.storage_class_name.unwrap_or_default();
    ResourceRow {
        cells: vec![name.clone(), capacity, access_modes, reclaim_policy, status, claim, storage_class, crate::util::format_age(age)],
        name,
        namespace: String::new(),
        extra: Default::default(),
    }
}
