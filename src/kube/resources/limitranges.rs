use k8s_openapi::api::core::v1::LimitRange;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s LimitRange into a generic ResourceRow.
pub(crate) fn limit_range_to_row(lr: LimitRange) -> ResourceRow {
    let meta = CommonMeta::from_k8s(lr.metadata);
    let types = lr.spec
        .and_then(|spec| {
            let items: Vec<String> = spec.limits.iter().map(|item| item.type_.clone()).collect();
            if items.is_empty() { None } else { Some(items.join(", ")) }
        })
        .unwrap_or_default();
    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            types, crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
