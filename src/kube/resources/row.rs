use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A single resource row in the unified table model.
/// Replaces all 28 typed Kube* structs with a generic representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRow {
    /// Display columns, matching the header order.
    pub cells: Vec<String>,
    /// Resource name (cached for O(1) access in sorts/filters).
    pub name: String,
    /// Resource namespace (empty string for cluster-scoped resources).
    pub namespace: String,
    /// Opaque metadata for action-specific fields (containers, labels, etc.).
    /// Empty for display-only resources.
    pub extra: BTreeMap<String, ExtraValue>,
}

/// Values that can live in the `extra` metadata bag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExtraValue {
    Str(String),
    List(Vec<String>),
    Map(BTreeMap<String, String>),
    Containers(Vec<ContainerInfo>),
    OwnerRefs(Vec<OwnerRefInfo>),
    Ports(Vec<u16>),
}

/// Container info for pods — used by shell, logs, port-forward.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerInfo {
    pub name: String,
    pub real_name: String,
    pub image: String,
    pub ready: bool,
    pub state: String,
    pub restarts: i32,
    pub ports: Vec<u16>,
}

/// Owner reference info for pods — used by owner chain drill-down.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerRefInfo {
    pub api_version: String,
    pub kind: String,
    pub name: String,
    pub uid: String,
}

impl super::KubeResource for ResourceRow {
    fn headers() -> &'static [&'static str] where Self: Sized {
        // ResourceRow uses runtime headers via TableDescriptor.
        // This static fallback is only called by code that doesn't know the concrete type.
        &["NAME"]
    }

    fn row(&self) -> Vec<std::borrow::Cow<'_, str>> {
        self.cells.iter().map(|c| std::borrow::Cow::Borrowed(c.as_str())).collect()
    }

    fn cells(&self) -> &[String] {
        &self.cells
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn kind() -> &'static str where Self: Sized {
        "resource"
    }
}

impl ResourceRow {
    pub fn extra_str(&self, key: &str) -> Option<&str> {
        match self.extra.get(key)? {
            ExtraValue::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn extra_map(&self, key: &str) -> Option<&BTreeMap<String, String>> {
        match self.extra.get(key)? {
            ExtraValue::Map(m) => Some(m),
            _ => None,
        }
    }

    pub fn extra_containers(&self) -> Option<&[ContainerInfo]> {
        match self.extra.get("containers")? {
            ExtraValue::Containers(c) => Some(c),
            _ => None,
        }
    }

    pub fn extra_owner_refs(&self) -> Option<&[OwnerRefInfo]> {
        match self.extra.get("owner_references")? {
            ExtraValue::OwnerRefs(r) => Some(r),
            _ => None,
        }
    }

    pub fn extra_ports(&self, key: &str) -> Option<&[u16]> {
        match self.extra.get(key)? {
            ExtraValue::Ports(p) => Some(p),
            _ => None,
        }
    }

    /// Mutate a cell in-place (e.g., for metrics overlay).
    pub fn set_cell(&mut self, col: usize, value: String) {
        if col < self.cells.len() {
            self.cells[col] = value;
        }
    }
}
