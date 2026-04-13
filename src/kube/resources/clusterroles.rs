use k8s_openapi::api::rbac::v1::ClusterRole;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s ClusterRole into a generic ResourceRow.
pub(crate) fn cluster_role_to_row(cr: ClusterRole) -> ResourceRow {
    let metadata = cr.metadata;
    let name = metadata.name.unwrap_or_default();
    let age = metadata.creation_timestamp.map(|t| t.0);
    let rules_count = cr.rules.map(|r| r.len()).unwrap_or(0);
    ResourceRow {
        cells: vec![name.clone(), rules_count.to_string(), crate::util::format_age(age)],
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
