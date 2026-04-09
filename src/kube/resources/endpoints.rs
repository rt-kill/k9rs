use k8s_openapi::api::core::v1::Endpoints;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s Endpoints into a generic ResourceRow.
pub(crate) fn endpoints_to_row(ep: Endpoints) -> ResourceRow {
    let metadata = ep.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let age = metadata.creation_timestamp.map(|t| t.0);
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
        cells: vec![ns.clone(), name.clone(), endpoints, crate::util::format_age(age)],
        name,
        namespace: ns,
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        crd_info: None,
        drill_target: None,
    }
}
