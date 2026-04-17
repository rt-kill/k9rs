use k8s_openapi::api::rbac::v1::RoleBinding;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s RoleBinding into a generic ResourceRow.
pub(crate) fn role_binding_to_row(rb: RoleBinding) -> ResourceRow {
    let meta = CommonMeta::from_k8s(rb.metadata);
    let role_ref = format!("{}/{}", rb.role_ref.kind, rb.role_ref.name);
    let subjects = rb.subjects
        .map(|subs| subs.iter().map(|s| format!("{}:{}", s.kind, s.name)).collect::<Vec<_>>().join(","))
        .unwrap_or_default();
    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            role_ref, subjects, crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
