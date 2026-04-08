use k8s_openapi::api::core::v1::ResourceQuota;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s ResourceQuota into a generic ResourceRow.
pub(crate) fn resource_quota_to_row(rq: ResourceQuota) -> ResourceRow {
    let metadata = rq.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let age = metadata.creation_timestamp.map(|t| t.0);
    let format_qmap = |map: &Option<std::collections::BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>>| -> String {
        match map {
            Some(m) if !m.is_empty() => m.iter().map(|(k, v)| format!("{}: {}", k, v.0)).collect::<Vec<_>>().join(", "),
            _ => String::new(),
        }
    };
    let hard = rq.spec.as_ref().map(|s| format_qmap(&s.hard)).unwrap_or_default();
    let used = rq.status.as_ref().map(|s| format_qmap(&s.used)).unwrap_or_default();
    ResourceRow {
        cells: vec![ns.clone(), name.clone(), hard, used, crate::util::format_age(age)],
        name,
        namespace: ns,
        extra: Default::default(),
    }
}
