use k8s_openapi::api::core::v1::Namespace;

use super::ConvertToRow;
use k9rs_proto::resource_def::namespaces::NamespaceDef;

use crate::resources::k8s_const::*;
use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, DrillTarget, ResourceRow, RowHealth};

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
            k9rs_proto::protocol::Namespace::Named(meta.name.clone()),
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

/// Convert cached namespace names to unified ResourceRow format.
pub fn cached_namespaces_to_rows(names: &[String]) -> Vec<k9rs_proto::row::ResourceRow> {
    names
        .iter()
        .map(|name| {
            let cells: Vec<CellValue> = vec![
                CellValue::Text(name.clone()),
                CellValue::Status { text: "Active".to_string(), health: RowHealth::Normal },
                CellValue::Age(None), // no age from cache
            ];            k9rs_proto::row::ResourceRow {
                name: name.clone(),
                namespace: None,
                containers: Vec::new(),
                owner_refs: Vec::new(),
                pf_ports: Vec::new(),
                node: None,
                health: RowHealth::Normal,
                crd_info: None,
                // Namespace name comes from the K8s API's NamespaceList — it's
                // a real identifier, never the literal string "all". Use the
                // typed `Named` constructor directly: routing this through
                // `from_user_command` would silently switch into all-namespaces
                // mode if a cluster ever had a namespace literally named `all`.
                drill_target: Some(DrillTarget::PodsInNamespace(
                    k9rs_proto::protocol::Namespace::Named(name.clone()),
                )),
                cells,
                ..Default::default()
            }
        })
        .collect()
}
