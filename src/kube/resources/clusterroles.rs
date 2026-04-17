use k8s_openapi::api::rbac::v1::ClusterRole;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s ClusterRole into a generic ResourceRow.
pub(crate) fn cluster_role_to_row(cr: ClusterRole) -> ResourceRow {
    let meta = CommonMeta::from_k8s(cr.metadata);
    let rules_count = cr.rules.map(|r| r.len()).unwrap_or(0);
    ResourceRow {
        cells: vec![
            meta.name.clone(),
            rules_count.to_string(),
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: None,
        ..Default::default()
    }
}
