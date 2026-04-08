use k8s_openapi::api::core::v1::ServiceAccount;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s ServiceAccount into a generic ResourceRow.
pub(crate) fn service_account_to_row(sa: ServiceAccount) -> ResourceRow {
    let metadata = sa.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let age = metadata.creation_timestamp.map(|t| t.0);
    let secrets = sa.secrets.map(|s| s.len()).unwrap_or(0);
    ResourceRow {
        cells: vec![ns.clone(), name.clone(), secrets.to_string(), crate::util::format_age(age)],
        name,
        namespace: ns,
        extra: Default::default(),
    }
}
