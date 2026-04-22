use k8s_openapi::api::core::v1::Secret;

use crate::kube::protocol::{OperationKind, ResourceScope};
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

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
        ResourceRow {
            cells: vec![
                meta.namespace.clone(), meta.name.clone(),
                secret_type, data_count.to_string(),
                meta.labels_str, crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: Some(meta.namespace),
            ..Default::default()
        }
    }
}
