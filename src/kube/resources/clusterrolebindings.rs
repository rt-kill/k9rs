use k8s_openapi::api::rbac::v1::ClusterRoleBinding;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s ClusterRoleBinding into a generic ResourceRow.
pub(crate) fn cluster_role_binding_to_row(crb: ClusterRoleBinding) -> ResourceRow {
    let meta = CommonMeta::from_k8s(crb.metadata);
    let role_ref = format!("{}/{}", crb.role_ref.kind, crb.role_ref.name);
    let subjects = crb.subjects
        .map(|subs| subs.iter().map(|s| format!("{}:{}", s.kind, s.name)).collect::<Vec<_>>().join(","))
        .unwrap_or_default();
    ResourceRow {
        cells: vec![
            meta.name.clone(),
            role_ref, subjects,
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: None,
        ..Default::default()
    }
}
