use k8s_openapi::api::core::v1::Secret;

use crate::kube::protocol::{OperationKind, ResourceScope};
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// SecretDef
// ---------------------------------------------------------------------------

pub struct SecretDef;

impl ResourceDef for SecretDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Secret }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Secret",
            plural: "secrets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["sec", "secret", "secrets"] }
    fn short_label(&self) -> &str { "Secrets" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "TYPE", "DATA", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, DecodeSecret]
    }
}

impl ConvertToRow<Secret> for SecretDef {
    fn convert(secret: Secret) -> ResourceRow {
        let meta = CommonMeta::from_k8s(secret.metadata);
        let secret_type = secret.type_.unwrap_or_else(|| "Opaque".to_string());
        let data_count = secret.data.map(|d| d.len()).unwrap_or(0);
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(secret_type),
            CellValue::Count(data_count as i64),
            CellValue::Text(meta.labels_str),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
