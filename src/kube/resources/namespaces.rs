use k8s_openapi::api::core::v1::Namespace;

use super::ConvertToRow;
use crate::kube::resource_def::namespaces::NamespaceDef;

use crate::kube::resources::k8s_const::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, DrillTarget, ResourceRow, RowHealth};

// ---------------------------------------------------------------------------
// NamespaceDef
// ---------------------------------------------------------------------------


impl ConvertToRow<Namespace> for NamespaceDef {
    fn convert(obj: Namespace) -> ResourceRow {
        let meta = CommonMeta::from_k8s(obj.metadata);
        let status = obj.status.and_then(|s| s.phase).unwrap_or_else(|| "Active".to_string());
        // The name came from the K8s API's NamespaceList -- it's a real
        // identifier, never the user-typed sentinel "all". Use `Named`
        // directly so a namespace literally named `all` would still drill
        // into itself instead of switching to all-namespaces mode.
        let drill_target = Some(DrillTarget::PodsInNamespace(
            crate::kube::protocol::Namespace::Named(meta.name.clone()),
        ));
        let health = match status.as_str() {
            "Active" => RowHealth::Normal,
            REASON_TERMINATING => RowHealth::Pending,
            _ => RowHealth::Failed,
        };
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.name.clone()),
            CellValue::Status { text: status, health },
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: None,
            drill_target,
            health,
            cells,
            ..Default::default()
        }
    }
}
