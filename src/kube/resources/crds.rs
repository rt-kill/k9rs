use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;

use super::ConvertToRow;
use crate::kube::resource_def::crds::CrdDef;

use crate::kube::protocol::{CrdRef, ResourceScope};
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, DrillTarget, ResourceRow};

// ---------------------------------------------------------------------------
// CrdDef
// ---------------------------------------------------------------------------


impl ConvertToRow<CustomResourceDefinition> for CrdDef {
    fn convert(crd: CustomResourceDefinition) -> ResourceRow {
        let meta = CommonMeta::from_k8s(crd.metadata);
        let spec = crd.spec;
        let version = spec
            .versions
            .first()
            .map(|v| v.name.clone())
            .unwrap_or_default();
        let scope = ResourceScope::from_k8s_spec(&spec.scope);

        // Single typed CrdRef shared between crd_info and the BrowseCrd drill
        // target -- was three field-by-field copies of the same shape before.
        let gvr = CrdRef::new(spec.group, version, spec.names.kind, spec.names.plural, scope);
        let scope_label = scope.k8s_label();

        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.name.clone()),
            CellValue::Text(gvr.group.clone()),
            CellValue::Text(gvr.version.clone()),
            CellValue::Text(gvr.kind.clone()),
            CellValue::Text(scope_label.to_string()),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: None,
            crd_info: Some(gvr.clone()),
            drill_target: Some(DrillTarget::BrowseCrd(gvr)),
            cells,
            ..Default::default()
        }
    }
}
