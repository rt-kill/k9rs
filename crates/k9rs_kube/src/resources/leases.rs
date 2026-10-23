use k8s_openapi::api::coordination::v1::Lease;

use super::ConvertToRow;
use k9rs_proto::resource_def::leases::LeaseDef;

use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// LeaseDef
// ---------------------------------------------------------------------------


impl ConvertToRow<Lease> for LeaseDef {
    fn convert(lease: Lease) -> ResourceRow {
        let meta = CommonMeta::from_k8s(lease.metadata);
        let spec = lease.spec.unwrap_or_default();
        let holder = spec.holder_identity.unwrap_or_default();
        let duration = spec.lease_duration_seconds
            .map(|s| format!("{}s", s))
            .unwrap_or_default();
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(holder),
            CellValue::Text(duration),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
