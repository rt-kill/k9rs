use k8s_openapi::api::rbac::v1::ClusterRoleBinding;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s ClusterRoleBinding into a generic ResourceRow.
pub(crate) fn cluster_role_binding_to_row(crb: ClusterRoleBinding) -> ResourceRow {
    let metadata = crb.metadata;
    let name = metadata.name.unwrap_or_default();
    let age = metadata.creation_timestamp.map(|t| t.0);
    let role_ref = format!("{}/{}", crb.role_ref.kind, crb.role_ref.name);
    let subjects = crb.subjects
        .map(|subs| subs.iter().map(|s| format!("{}:{}", s.kind, s.name)).collect::<Vec<_>>().join(","))
        .unwrap_or_default();
    ResourceRow {
        cells: vec![name.clone(), role_ref, subjects, crate::util::format_age(age)],
        name,
        namespace: None,
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        crd_info: None,
        drill_target: None,
    }
}
