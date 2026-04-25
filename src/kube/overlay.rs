//! User-defined resource overlays loaded from `~/.config/k9rs/overlays/*.yaml`.
//!
//! Overlays layer customizations on top of the frozen built-in resource
//! definitions: custom key bindings, extra columns (JSONPath), and
//! declarative cell coloring rules. They're loaded at startup by both the
//! daemon (for columns/coloring) and the TUI (for key bindings).

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;
use tracing::warn;

use crate::kube::resources::row::RowHealth;

/// Global overlay store, loaded once at startup. Both the daemon and TUI
/// read from the same `~/.config/k9rs/overlays/` directory.
static OVERLAYS: std::sync::LazyLock<HashMap<String, ResourceOverlay>> =
    std::sync::LazyLock::new(load_overlays);

/// Look up the overlay for a resource by its plural name.
pub fn overlay_for(plural: &str) -> Option<&'static ResourceOverlay> {
    OVERLAYS.get(&plural.to_lowercase())
}

/// Get all loaded overlays (for TUI key binding display, etc.)
pub fn all_overlays() -> &'static HashMap<String, ResourceOverlay> {
    &OVERLAYS
}

// ---------------------------------------------------------------------------
// Config types (deserialized from YAML)
// ---------------------------------------------------------------------------

/// A single overlay file targeting one resource type.
#[derive(Debug, Clone, Deserialize)]
pub struct ResourceOverlay {
    /// Plural name of the resource (e.g. "nodeclaims", "pods").
    pub resource: String,
    /// Optional API group for disambiguation when multiple CRDs share a plural.
    #[serde(default)]
    pub group: Option<String>,
    /// Named capabilities with typed implementations. The key is the
    /// capability name referenced by bindings.
    #[serde(default)]
    pub capabilities: HashMap<String, OverlayCapability>,
    /// Key → capability name. Maps a single character to a named capability.
    #[serde(default)]
    pub bindings: HashMap<char, String>,
    #[serde(default)]
    pub columns: Vec<OverlayColumn>,
    #[serde(default)]
    pub coloring: Vec<ColumnColorRule>,
}

/// A named overlay capability. Tagged union — the `type` field selects
/// the variant, sibling fields provide the implementation details.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OverlayCapability {
    /// Navigate to another resource, filtered by a cell value from the
    /// current row. `column` is the table column header to read from.
    Drill {
        target: String,
        column: String,
    },
}

/// An extra column defined by JSONPath extraction.
#[derive(Debug, Clone, Deserialize)]
pub struct OverlayColumn {
    /// Column header name.
    pub header: String,
    /// JSONPath expression to extract from the raw K8s object.
    pub jsonpath: String,
    /// Visibility level (default or extra).
    #[serde(default = "default_level")]
    pub level: OverlayColumnLevel,
}

/// Column visibility level in overlay config. Mapped to the internal
/// `ColumnLevel` enum after deserialization.
#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum OverlayColumnLevel {
    #[default]
    Default,
    Extra,
}

impl From<OverlayColumnLevel> for crate::kube::resource_def::ColumnLevel {
    fn from(l: OverlayColumnLevel) -> Self {
        match l {
            OverlayColumnLevel::Default => crate::kube::resource_def::ColumnLevel::Default,
            OverlayColumnLevel::Extra => crate::kube::resource_def::ColumnLevel::Extra,
        }
    }
}

fn default_level() -> OverlayColumnLevel {
    OverlayColumnLevel::Default
}

/// Coloring rules for a specific column.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnColorRule {
    /// Column header name to match against.
    pub column: String,
    /// Rules evaluated in order; first match wins per column.
    pub rules: Vec<ColorRule>,
}

/// A single coloring rule: predicate + resulting health level.
#[derive(Debug, Clone, Deserialize)]
pub struct ColorRule {
    /// Exact text match on the cell's display value.
    #[serde(rename = "match")]
    pub match_text: Option<String>,
    /// Numeric comparison (e.g. "> 90", "< 10").
    pub when: Option<String>,
    /// Health level to assign when the predicate matches.
    pub health: OverlayHealth,
}

/// Health level in overlay config. Mapped to `RowHealth` after deser.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OverlayHealth {
    Normal,
    Pending,
    Failed,
}

impl From<OverlayHealth> for RowHealth {
    fn from(h: OverlayHealth) -> Self {
        match h {
            OverlayHealth::Normal => RowHealth::Normal,
            OverlayHealth::Pending => RowHealth::Pending,
            OverlayHealth::Failed => RowHealth::Failed,
        }
    }
}

// ---------------------------------------------------------------------------
// Loader
// ---------------------------------------------------------------------------

/// Load all overlay files from `~/.config/k9rs/overlays/`.
/// Returns a map keyed by resource plural name. Errors in individual
/// files are logged and skipped — one bad overlay doesn't break the rest.
pub fn load_overlays() -> HashMap<String, ResourceOverlay> {
    let mut overlays = HashMap::new();
    let home = match std::env::var("HOME") {
        Ok(h) => h,
        Err(_) => return overlays,
    };
    let dir = Path::new(&home).join(".config/k9rs/overlays");
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(_) => return overlays, // directory doesn't exist — no overlays
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml")
            && path.extension().and_then(|e| e.to_str()) != Some("yml")
        {
            continue;
        }
        match std::fs::read_to_string(&path) {
            Ok(content) => match serde_yaml::from_str::<ResourceOverlay>(&content) {
                Ok(overlay) => {
                    // Validate bindings reference defined capabilities.
                    for (key, cap_name) in &overlay.bindings {
                        if !overlay.capabilities.contains_key(cap_name) {
                            warn!(
                                "overlay: binding key='{}' references unknown capability '{}' in {}",
                                key, cap_name, path.display()
                            );
                        }
                    }
                    // Validate drill capabilities reference columns that exist
                    // in the overlay's column definitions or are common built-in
                    // columns (NAME, NAMESPACE, STATUS, NODE, etc.).
                    // Validate drill capabilities reference known columns.
                    for (cap_name, cap) in &overlay.capabilities {
                        let column = match cap {
                            OverlayCapability::Drill { column, .. } => column,
                        };
                        let has_overlay_col = overlay.columns.iter()
                            .any(|c| c.header.eq_ignore_ascii_case(column));
                        if !has_overlay_col {
                            tracing::debug!(
                                "overlay: capability '{}' references column '{}' not in overlay columns (may be built-in) in {}",
                                cap_name, column, path.display()
                            );
                        }
                    }
                    let key = overlay.resource.to_lowercase();
                    if overlays.contains_key(&key) {
                        warn!("overlay: duplicate definition for '{}', using {}", key, path.display());
                    }
                    overlays.insert(key, overlay);
                }
                Err(e) => {
                    warn!("overlay: failed to parse {}: {}", path.display(), e);
                }
            },
            Err(e) => {
                warn!("overlay: failed to read {}: {}", path.display(), e);
            }
        }
    }
    if !overlays.is_empty() {
        let names: Vec<&str> = overlays.keys().map(|s| s.as_str()).collect();
        tracing::info!("overlays: loaded {} overlay(s): {}", overlays.len(), names.join(", "));
    }
    overlays
}

// ---------------------------------------------------------------------------
// Coloring evaluation
// ---------------------------------------------------------------------------

/// Evaluate coloring rules against a row's cells. If any rule matches,
/// the row's health is upgraded to the rule's health (Failed > Pending > Normal).
/// The converter's original health is the starting point — overlay rules
/// can only make it worse, not better.
pub fn evaluate_coloring(
    row: &mut crate::kube::resources::row::ResourceRow,
    headers: &[String],
    rules: &[ColumnColorRule],
) {
    let mut worst = row.health;
    for rule in rules {
        let col_idx = match headers.iter().position(|h| h.eq_ignore_ascii_case(&rule.column)) {
            Some(i) => i,
            None => continue,
        };
        let cell = match row.cells.get(col_idx) {
            Some(c) => c,
            None => continue,
        };
        let cell_str = cell.to_string();
        for cr in &rule.rules {
            let matched = if let Some(ref text) = cr.match_text {
                cell_str.contains(text.as_str())
            } else if let Some(ref when) = cr.when {
                evaluate_numeric_predicate(&cell_str, when)
            } else {
                false
            };
            if matched {
                let rule_health: RowHealth = cr.health.into();
                if (rule_health as u8) > (worst as u8) {
                    worst = rule_health;
                }
            }
        }
    }
    row.health = worst;
}

/// Parse and evaluate a numeric predicate like "> 90" or "< 10".
fn evaluate_numeric_predicate(cell_str: &str, predicate: &str) -> bool {
    let pred = predicate.trim();
    let (op, threshold_str) = if let Some(rest) = pred.strip_prefix(">=") {
        (">=", rest.trim())
    } else if let Some(rest) = pred.strip_prefix("<=") {
        ("<=", rest.trim())
    } else if let Some(rest) = pred.strip_prefix('>') {
        (">", rest.trim())
    } else if let Some(rest) = pred.strip_prefix('<') {
        ("<", rest.trim())
    } else {
        return false;
    };
    let threshold: f64 = match threshold_str.parse() {
        Ok(v) => v,
        Err(_) => return false,
    };
    // Strip common suffixes (%, m, Mi, etc.) before parsing the cell value.
    let cleaned = cell_str
        .trim()
        .trim_end_matches('%')
        .trim_end_matches('m')
        .trim_end_matches("Mi")
        .trim_end_matches("Ki")
        .trim_end_matches("Gi");
    let value: f64 = match cleaned.parse() {
        Ok(v) => v,
        Err(_) => return false,
    };
    match op {
        ">" => value > threshold,
        ">=" => value >= threshold,
        "<" => value < threshold,
        "<=" => value <= threshold,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_predicate_gt() {
        assert!(evaluate_numeric_predicate("95", "> 90"));
        assert!(!evaluate_numeric_predicate("85", "> 90"));
    }

    #[test]
    fn numeric_predicate_with_percent() {
        assert!(evaluate_numeric_predicate("95%", "> 90"));
        assert!(!evaluate_numeric_predicate("50%", "> 90"));
    }

    #[test]
    fn numeric_predicate_gte() {
        assert!(evaluate_numeric_predicate("90", ">= 90"));
        assert!(!evaluate_numeric_predicate("89", ">= 90"));
    }

    #[test]
    fn numeric_predicate_lt() {
        assert!(evaluate_numeric_predicate("5", "< 10"));
        assert!(!evaluate_numeric_predicate("15", "< 10"));
    }

    #[test]
    fn numeric_predicate_invalid() {
        assert!(!evaluate_numeric_predicate("not-a-number", "> 90"));
        assert!(!evaluate_numeric_predicate("50", "bad predicate"));
    }

    #[test]
    fn evaluate_coloring_exact_match() {
        use crate::kube::resources::row::{CellValue, ResourceRow};
        let headers = vec!["STATUS".to_string()];
        let rules = vec![ColumnColorRule {
            column: "STATUS".into(),
            rules: vec![ColorRule {
                match_text: Some("CrashLoopBackOff".into()),
                when: None,
                health: OverlayHealth::Failed,
            }],
        }];
        let mut row = ResourceRow {
            cells: vec![CellValue::Text("CrashLoopBackOff".into())],
            ..Default::default()
        };
        evaluate_coloring(&mut row, &headers, &rules);
        assert_eq!(row.health, RowHealth::Failed);
    }

    #[test]
    fn evaluate_coloring_no_downgrade() {
        use crate::kube::resources::row::{CellValue, ResourceRow};
        let headers = vec!["STATUS".to_string()];
        let rules = vec![ColumnColorRule {
            column: "STATUS".into(),
            rules: vec![ColorRule {
                match_text: Some("Running".into()),
                when: None,
                health: OverlayHealth::Normal,
            }],
        }];
        let mut row = ResourceRow {
            cells: vec![CellValue::Text("Running".into())],
            health: RowHealth::Failed, // converter says Failed
            ..Default::default()
        };
        evaluate_coloring(&mut row, &headers, &rules);
        // Overlay can't downgrade Failed → Normal
        assert_eq!(row.health, RowHealth::Failed);
    }

    #[test]
    fn evaluate_coloring_numeric() {
        use crate::kube::resources::row::{CellValue, ResourceRow};
        let headers = vec!["CPU%".to_string()];
        let rules = vec![ColumnColorRule {
            column: "CPU%".into(),
            rules: vec![ColorRule {
                match_text: None,
                when: Some("> 90".into()),
                health: OverlayHealth::Failed,
            }],
        }];
        let mut row = ResourceRow {
            cells: vec![CellValue::Percentage(Some(95))],
            ..Default::default()
        };
        evaluate_coloring(&mut row, &headers, &rules);
        assert_eq!(row.health, RowHealth::Failed);
    }

    #[test]
    fn deserialize_overlay_yaml() {
        let yaml = r#"
resource: nodeclaims
capabilities:
  show-node:
    type: drill
    target: nodes
    column: NODE
bindings:
  o: show-node
columns:
  - header: "INSTANCE TYPE"
    jsonpath: ".spec.instanceType"
coloring:
  - column: "STATUS"
    rules:
      - match: "NotReady"
        health: failed
"#;
        let overlay: ResourceOverlay = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(overlay.resource, "nodeclaims");
        assert_eq!(overlay.bindings.get(&'o'), Some(&"show-node".to_string()));
        assert!(overlay.capabilities.contains_key("show-node"));
        if let OverlayCapability::Drill { ref target, ref column } = overlay.capabilities["show-node"] {
            assert_eq!(target, "nodes");
            assert_eq!(column, "NODE");
        } else {
            panic!("expected Drill capability");
        }
        assert_eq!(overlay.columns.len(), 1);
        assert_eq!(overlay.columns[0].header, "INSTANCE TYPE");
        assert_eq!(overlay.coloring.len(), 1);
    }
}
