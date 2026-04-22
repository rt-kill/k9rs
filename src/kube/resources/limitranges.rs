use k8s_openapi::api::core::v1::LimitRange;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

// ---------------------------------------------------------------------------
// LimitRangeDef
// ---------------------------------------------------------------------------

pub struct LimitRangeDef;

impl ResourceDef for LimitRangeDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::LimitRange }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "LimitRange",
            plural: "limitranges", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["limits", "limitrange", "limitranges"] }
    fn short_label(&self) -> &str { "Limits" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "TYPE", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<LimitRange> for LimitRangeDef {
    fn convert(lr: LimitRange) -> ResourceRow {
        let meta = CommonMeta::from_k8s(lr.metadata);
        let types = lr.spec
            .and_then(|spec| {
                let items: Vec<String> = spec.limits.iter().map(|item| item.type_.clone()).collect();
                if items.is_empty() { None } else { Some(items.join(", ")) }
            })
            .unwrap_or_default();
        ResourceRow {
            cells: vec![
                meta.namespace.clone(), meta.name.clone(),
                types, crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: Some(meta.namespace),
            ..Default::default()
        }
    }
}
