use k8s_openapi::api::core::v1::Endpoints;

use super::ConvertToRow;
use k9rs_proto::resource_def::endpoints::EndpointsDef;

use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, ResourceRow, RowHealth};

// ---------------------------------------------------------------------------
// EndpointsDef
// ---------------------------------------------------------------------------


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
