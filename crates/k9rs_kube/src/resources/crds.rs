use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;

use super::ConvertToRow;
use k9rs_proto::resource_def::crds::CrdDef;

use k9rs_proto::protocol::{CrdRef, ResourceScope};
use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, DrillTarget, ResourceRow, RowHealth};

use crate::cache::CachedCrd;

// ---------------------------------------------------------------------------
// CrdDef
// ---------------------------------------------------------------------------


impl ConvertToRow<CustomResourceDefinition> for CrdDef {
    fn convert(crd: CustomResourceDefinition) -> ResourceRow {
        let meta = CommonMeta::from_k8s(crd.metadata);
        let spec = crd.spec;
        // The SAME rule the cache path uses — see `cache::served_version`.
        // This was a plain `versions.first()`, so a CRD keeping an unserved
        // `v1alpha1` first in the list drilled into a version the apiserver
        // will not serve.
        let version = crate::cache::served_version(&spec)
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

// ---------------------------------------------------------------------------
// Cached-form → table rows. Moved here from `cache.rs` 2026-10-27: the two
// `*_to_rows` functions were the only ones in the crate living outside
// `resources/`, and they shared their file with `DiscoveryCache` while
// referencing it zero times in either direction (measured). This crate's
// organizing principle is that an `impl ConvertToRow` lives beside the k8s
// type it reads; a projection of the CACHED form of the same kind belongs
// beside it.
//
// `cached_crd_from_k8s` stayed in `cache.rs`: it produces a cache type, not
// a row, and its doc's claim to be THE single CRD-metadata parser is about
// the cache's two write paths.
// ---------------------------------------------------------------------------

pub fn cached_crds_to_rows(cached: &[CachedCrd]) -> Vec<ResourceRow> {
    cached
        .iter()
        .map(|c| {
            // crd_info is now a type alias for CrdRef — clone the ref directly.
            let crd_info = Some(c.gvr.clone());
            // Drill target wraps the same CrdRef: pressing Enter on a CRD
            // definition pushes a `ResourceId::Crd(...)` view onto the nav
            // stack — handler does `ResourceId::Crd(crd_ref)` from this.
            let drill_target = Some(DrillTarget::BrowseCrd(c.gvr.clone()));
            let scope_label = c.gvr.scope.k8s_label();
            let cells: Vec<CellValue> = vec![
                CellValue::Text(c.name.clone()),
                CellValue::Text(c.gvr.group.clone()),
                CellValue::Text(c.gvr.version.clone()),
                CellValue::Text(c.gvr.kind.clone()),
                CellValue::Text(scope_label.to_string()),
                CellValue::Age(None), // no age from cache
            ];            ResourceRow {
                name: c.name.clone(),
                namespace: None,
                containers: Vec::new(),
                owner_refs: Vec::new(),
                pf_ports: Vec::new(),
                node: None,
                health: RowHealth::Normal,
                crd_info,
                drill_target,
                cells,
                ..Default::default()
            }
        })
        .collect()
}

#[cfg(test)]
#[path = "../tests/resources/crds.rs"]
mod tests;
