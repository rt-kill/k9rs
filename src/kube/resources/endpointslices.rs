use k8s_openapi::api::discovery::v1::EndpointSlice;

use super::ConvertToRow;
use crate::kube::resource_def::endpointslices::EndpointSliceDef;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

// ---------------------------------------------------------------------------
// EndpointSliceDef
// ---------------------------------------------------------------------------


impl ConvertToRow<EndpointSlice> for EndpointSliceDef {
    fn convert(eps: EndpointSlice) -> ResourceRow {
        let meta = CommonMeta::from_k8s(eps.metadata);
        let address_type = eps.address_type;
        // READY endpoints, not all of them. `:ep` counts ready addresses, so
        // counting every endpoint here made the two views disagree about the
        // same Service: three CrashLooping pods read `<none>` red under `:ep`
        // and `3` green under `:eps`. A nil condition means ready.
        let endpoint_count = eps
            .endpoints
            .iter()
            .filter(|e| e.conditions.as_ref().and_then(|c| c.ready) != Some(false))
            .count();
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
        // Same reading as `endpoints`, now that both count READY: a slice
        // backing nothing is a Service that resolves to nothing.
        let health = if endpoint_count == 0 { RowHealth::Failed } else { RowHealth::Normal };
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(address_type),
            CellValue::Count(endpoint_count as i64),
            CellValue::Text(ports),
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
