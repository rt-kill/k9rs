use k8s_openapi::api::coordination::v1::Lease;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

/// Convert a k8s Lease into a generic ResourceRow.
pub(crate) fn lease_to_row(lease: Lease) -> ResourceRow {
    let meta = CommonMeta::from_k8s(lease.metadata);
    let spec = lease.spec.unwrap_or_default();
    let holder = spec.holder_identity.unwrap_or_default();
    let duration = spec.lease_duration_seconds
        .map(|s| format!("{}s", s))
        .unwrap_or_default();
    ResourceRow {
        cells: vec![
            meta.namespace.clone(), meta.name.clone(),
            holder, duration,
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
