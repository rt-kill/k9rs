use k8s_openapi::api::core::v1::Namespace;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s Namespace into a generic ResourceRow.
pub(crate) fn namespace_to_row(ns_obj: Namespace) -> ResourceRow {
    let metadata = ns_obj.metadata;
    let name = metadata.name.unwrap_or_default();
    let age = metadata.creation_timestamp.map(|t| t.0);
    let status = ns_obj.status.and_then(|s| s.phase).unwrap_or_else(|| "Active".to_string());
    ResourceRow {
        cells: vec![name.clone(), status, crate::util::format_age(age)],
        name,
        namespace: String::new(),
        extra: Default::default(),
    }
}
