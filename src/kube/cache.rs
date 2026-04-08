//! Types for discovery data (CRDs and namespaces).
//!
//! Used by the daemon's in-memory discovery cache (DashMap in SessionSharedState)
//! and the session protocol (SessionEvent::Discovery).
//! No disk persistence — cache is tied to the daemon process lifetime.

use serde::{Deserialize, Serialize};

use crate::kube::resources::row::{ExtraValue, ResourceRow};

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
            let mut extra = std::collections::BTreeMap::new();
            extra.insert("group".into(), ExtraValue::Str(c.group.clone()));
            extra.insert("version".into(), ExtraValue::Str(c.version.clone()));
            extra.insert("kind".into(), ExtraValue::Str(c.kind.clone()));
            extra.insert("plural".into(), ExtraValue::Str(c.plural.clone()));
            extra.insert("scope".into(), ExtraValue::Str(c.scope.clone()));
            ResourceRow {
                cells: vec![
                    c.name.clone(), c.group.clone(), c.version.clone(),
                    c.kind.clone(), c.scope.clone(), String::new(), // no age from cache
                ],
                name: c.name.clone(),
                namespace: String::new(),
                extra,
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
            namespace: String::new(),
            extra: Default::default(),
        })
        .collect()
}
