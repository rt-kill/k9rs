//! Types for discovery data (CRDs and namespaces).
//!
//! Used by the daemon's in-memory discovery cache (DashMap in SessionSharedState)
//! and the session protocol (SessionEvent::Discovery).
//! No disk persistence — cache is tied to the daemon process lifetime.

use serde::{Deserialize, Serialize};

use crate::kube::resources::row::{DrillTarget, ResourceRow};
use crate::kube::protocol::ResourceScope;

/// A printer column from a CRD's additionalPrinterColumns spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrinterColumn {
    pub name: String,
    /// JSONPath expression into the object (e.g., ".spec.nodeClassRef.name").
    pub json_path: String,
    /// Column type (string, integer, date, etc.)
    pub column_type: String,
}

/// Serializable representation of a CRD for the discovery cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedCrd {
    pub name: String,
    pub kind: String,
    pub plural: String,
    pub group: String,
    pub version: String,
    pub scope: String,
    /// Columns from the CRD's additionalPrinterColumns — defines what to show.
    #[serde(default)]
    pub printer_columns: Vec<PrinterColumn>,
}

/// Convert cached CRDs to unified ResourceRow structs with extra metadata.
pub fn cached_crds_to_rows(cached: &[CachedCrd]) -> Vec<ResourceRow> {
    cached
        .iter()
        .map(|c| {
            let scope = ResourceScope::from_scope_str(&c.scope);
            let crd_info = Some(crate::kube::resources::row::CrdRowInfo {
                group: c.group.clone(),
                version: c.version.clone(),
                kind: c.kind.clone(),
                plural: c.plural.clone(),
                scope,
            });
            // Drill target: pressing Enter on a CRD definition browses its instances.
            let drill_target = Some(DrillTarget::BrowseCrd {
                group: c.group.clone(),
                version: c.version.clone(),
                kind: c.kind.clone(),
                plural: c.plural.clone(),
                scope,
            });
            ResourceRow {
                cells: vec![
                    c.name.clone(), c.group.clone(), c.version.clone(),
                    c.kind.clone(), c.scope.clone(), String::new(), // no age from cache
                ],
                name: c.name.clone(),
                namespace: None,
                containers: Vec::new(),
                owner_refs: Vec::new(),
                pf_ports: Vec::new(),
                node: None,
                crd_info,
                drill_target,
            }
        })
        .collect()
}

/// Convert cached namespace names to unified ResourceRow format.
pub fn cached_namespaces_to_rows(names: &[String]) -> Vec<crate::kube::resources::row::ResourceRow> {
    names
        .iter()
        .map(|name| crate::kube::resources::row::ResourceRow {
            cells: vec![name.clone(), "Active".to_string(), String::new()],
            name: name.clone(),
            namespace: None,
            containers: Vec::new(),
            owner_refs: Vec::new(),
            pf_ports: Vec::new(),
            node: None,
            crd_info: None,
            drill_target: Some(DrillTarget::SwitchNamespace(name.clone())),
        })
        .collect()
}
