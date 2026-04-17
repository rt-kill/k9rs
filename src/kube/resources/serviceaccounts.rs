use k8s_openapi::api::core::v1::ServiceAccount;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s ServiceAccount into a generic ResourceRow.
pub(crate) fn service_account_to_row(sa: ServiceAccount) -> ResourceRow {
    let meta = CommonMeta::from_k8s(sa.metadata);
    let secrets = sa.secrets.map(|s| s.len()).unwrap_or(0);
    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            secrets.to_string(), crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
