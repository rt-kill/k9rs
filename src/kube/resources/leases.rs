use k8s_openapi::api::coordination::v1::Lease;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

// ---------------------------------------------------------------------------
// LeaseDef
// ---------------------------------------------------------------------------

pub struct LeaseDef;

impl ResourceDef for LeaseDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Lease }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "coordination.k8s.io", version: "v1", kind: "Lease",
            plural: "leases", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["lease", "leases"] }
    fn short_label(&self) -> &str { "LEASE" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "HOLDER", "DURATION", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<Lease> for LeaseDef {
    fn convert(lease: Lease) -> ResourceRow {
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
}
