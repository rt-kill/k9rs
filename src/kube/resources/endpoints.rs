use k8s_openapi::api::core::v1::Endpoints;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

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
        // No addresses means the Service in front of this resolves to
        // nothing — the state you are looking for when a call times out and
        // the pods all look Running. It is the whole content of the view.
        let health = if endpoints.is_empty() { RowHealth::Failed } else { RowHealth::Normal };
        let endpoints = if endpoints.is_empty() { "<none>".to_string() } else { endpoints };
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Status { text: endpoints, health },
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            health,
            ..Default::default()
        }
    }
}
