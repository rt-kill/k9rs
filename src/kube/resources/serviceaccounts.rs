use k8s_openapi::api::core::v1::ServiceAccount;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// ServiceAccountDef
// ---------------------------------------------------------------------------

pub struct ServiceAccountDef;

impl ResourceDef for ServiceAccountDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ServiceAccount }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "ServiceAccount",
            plural: "serviceaccounts", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["sa", "serviceaccount", "serviceaccounts"] }
    fn short_label(&self) -> &str { "SA" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "SECRETS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<ServiceAccount> for ServiceAccountDef {
    fn convert(sa: ServiceAccount) -> ResourceRow {
        let meta = CommonMeta::from_k8s(sa.metadata);
        let secrets = sa.secrets.map(|s| s.len()).unwrap_or(0);
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Count(secrets as i64),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
