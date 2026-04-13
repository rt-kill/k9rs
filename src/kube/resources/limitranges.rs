use k8s_openapi::api::core::v1::LimitRange;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s LimitRange into a generic ResourceRow.
pub(crate) fn limit_range_to_row(lr: LimitRange) -> ResourceRow {
    let metadata = lr.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let age = metadata.creation_timestamp.map(|t| t.0);
    let types = lr.spec
        .and_then(|spec| {
            let items: Vec<String> = spec.limits.iter().map(|item| item.type_.clone()).collect();
            if items.is_empty() { None } else { Some(items.join(", ")) }
        })
        .unwrap_or_default();
    ResourceRow {
        cells: vec![ns.clone(), name.clone(), types, crate::util::format_age(age)],
        name,
        namespace: Some(ns),
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        crd_info: None,
        drill_target: None,
    }
}
