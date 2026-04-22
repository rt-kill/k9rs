use k8s_openapi::api::core::v1::Endpoints;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

// ---------------------------------------------------------------------------
// EndpointsDef
// ---------------------------------------------------------------------------

pub struct EndpointsDef;

impl ResourceDef for EndpointsDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Endpoints }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Endpoints",
            plural: "endpoints", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["ep", "endpoints"] }
    fn short_label(&self) -> &str { "EP" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "ENDPOINTS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<Endpoints> for EndpointsDef {
    fn convert(ep: Endpoints) -> ResourceRow {
        let meta = CommonMeta::from_k8s(ep.metadata);
        let endpoints = ep.subsets.unwrap_or_default().iter()
            .flat_map(|subset| {
                let addresses = subset.addresses.as_deref().unwrap_or_default();
                let ports = subset.ports.as_deref().unwrap_or_default();
                addresses.iter().flat_map(move |addr| {
                    let ip = addr.ip.clone();
                    if ports.is_empty() {
                        vec![ip.clone()]
                    } else {
                        ports.iter().map(|p| format!("{}:{}", ip, p.port)).collect()
                    }
                })
            })
            .collect::<Vec<_>>()
            .join(",");
        let endpoints = if endpoints.is_empty() { "<none>".to_string() } else { endpoints };
        ResourceRow {
            cells: vec![
                meta.namespace.clone(), meta.name.clone(),
                endpoints, crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: Some(meta.namespace),
            ..Default::default()
        }
    }
}
