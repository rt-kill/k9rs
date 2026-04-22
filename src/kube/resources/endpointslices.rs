use k8s_openapi::api::discovery::v1::EndpointSlice;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

// ---------------------------------------------------------------------------
// EndpointSliceDef
// ---------------------------------------------------------------------------

pub struct EndpointSliceDef;

impl ResourceDef for EndpointSliceDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::EndpointSlice }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "discovery.k8s.io", version: "v1", kind: "EndpointSlice",
            plural: "endpointslices", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["eps", "endpointslice", "endpointslices"] }
    fn short_label(&self) -> &str { "EPS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "ADDRESSTYPE", "ENDPOINTS", "PORTS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<EndpointSlice> for EndpointSliceDef {
    fn convert(eps: EndpointSlice) -> ResourceRow {
        let meta = CommonMeta::from_k8s(eps.metadata);
        let address_type = eps.address_type;
        let endpoint_count = eps.endpoints.len();
        let ports = eps.ports.as_ref()
            .map(|ports| ports.iter()
                .map(|p| {
                    let port = p.port.map(|n| n.to_string()).unwrap_or_default();
                    let proto = p.protocol.as_deref().unwrap_or("TCP");
                    format!("{}/{}", port, proto)
                })
                .collect::<Vec<_>>()
                .join(","))
            .unwrap_or_default();
        ResourceRow {
            cells: vec![
                meta.namespace.clone(), meta.name.clone(),
                address_type, endpoint_count.to_string(), ports,
                crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: Some(meta.namespace),
            ..Default::default()
        }
    }
}
