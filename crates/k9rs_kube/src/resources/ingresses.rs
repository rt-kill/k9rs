use k8s_openapi::api::networking::v1::Ingress;

use super::ConvertToRow;
use k9rs_proto::resource_def::ingresses::IngressDef;

use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// IngressDef
// ---------------------------------------------------------------------------


impl ConvertToRow<Ingress> for IngressDef {
    fn convert(ing: Ingress) -> ResourceRow {
        let meta = CommonMeta::from_k8s(ing.metadata);
        let spec = ing.spec.unwrap_or_default();
        let class = spec.ingress_class_name.unwrap_or_else(|| "<none>".to_string());
        let hosts = spec.rules.as_ref()
            .map(|rules| rules.iter().filter_map(|r| r.host.clone()).collect::<Vec<_>>().join(","))
            .unwrap_or_default();
        let hosts = if hosts.is_empty() { "*".to_string() } else { hosts };
        let address = ing.status
            .and_then(|s| s.load_balancer)
            .and_then(|lb| lb.ingress)
            .map(|ingresses| ingresses.iter().filter_map(|i| i.ip.clone().or_else(|| i.hostname.clone())).collect::<Vec<_>>().join(","))
            .unwrap_or_default();
        let has_tls = spec.tls.map(|t| !t.is_empty()).unwrap_or(false);
        let ports = if has_tls { "80, 443".to_string() } else { "80".to_string() };
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(class),
            CellValue::Text(hosts),
            CellValue::Text(address),
            CellValue::Text(ports),
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
