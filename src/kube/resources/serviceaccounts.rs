use k8s_openapi::api::core::v1::ServiceAccount;

use super::ConvertToRow;
use crate::kube::resource_def::serviceaccounts::ServiceAccountDef;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// ServiceAccountDef
// ---------------------------------------------------------------------------


impl ConvertToRow<ServiceAccount> for ServiceAccountDef {
    fn convert(sa: ServiceAccount) -> ResourceRow {
        let meta = CommonMeta::from_k8s(sa.metadata);
        let secrets = sa.secrets.map(|s| s.len()).unwrap_or(0);
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Count(secrets as i64),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
