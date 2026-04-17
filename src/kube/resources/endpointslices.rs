use k8s_openapi::api::discovery::v1::EndpointSlice;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

/// Convert a k8s EndpointSlice into a generic ResourceRow.
pub(crate) fn endpoint_slice_to_row(eps: EndpointSlice) -> ResourceRow {
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
