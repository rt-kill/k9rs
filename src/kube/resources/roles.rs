use k8s_openapi::api::rbac::v1::Role;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s Role into a generic ResourceRow.
pub(crate) fn role_to_row(role: Role) -> ResourceRow {
    let meta = CommonMeta::from_k8s(role.metadata);
    let rules_count = role.rules.map(|r| r.len()).unwrap_or(0);
    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            rules_count.to_string(), crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
