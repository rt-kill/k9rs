use k8s_openapi::api::core::v1::ResourceQuota;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s ResourceQuota into a generic ResourceRow.
pub(crate) fn resource_quota_to_row(rq: ResourceQuota) -> ResourceRow {
    let meta = CommonMeta::from_k8s(rq.metadata);
    let format_qmap = |map: &Option<std::collections::BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>>| -> String {
        match map {
            Some(m) if !m.is_empty() => m.iter().map(|(k, v)| format!("{}: {}", k, v.0)).collect::<Vec<_>>().join(", "),
            _ => String::new(),
        }
    };
    let hard = rq.spec.as_ref().map(|s| format_qmap(&s.hard)).unwrap_or_default();
    let used = rq.status.as_ref().map(|s| format_qmap(&s.used)).unwrap_or_default();
    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            hard, used, crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
