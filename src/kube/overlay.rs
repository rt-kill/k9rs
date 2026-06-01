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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct OverlayColumn {
    /// Column header name.
    pub header: String,
    /// JSONPath expression to extract from the raw K8s object.
    /// Required for CRDs (no compile-time schema). Omitted for built-in
    /// resources where the column references an existing converter field.
    #[serde(default)]
    pub jsonpath: Option<String>,
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
#[serde(deny_unknown_fields)]
pub struct ColumnColorRule {
    /// Column header name to match against.
    pub column: String,
    /// Rules evaluated in order; first match wins per column.
    pub rules: Vec<ColorRule>,
}

/// A single coloring rule: predicate + resulting health level.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
                    // Check column/jsonpath consistency.
                    let is_builtin = crate::kube::resource_defs::REGISTRY
                        .by_alias(&overlay.resource).is_some();
                    for oc in &overlay.columns {
                        if is_builtin && oc.jsonpath.is_some() {
                            warn!(
                                "overlay: column '{}' has jsonpath on built-in resource '{}' — jsonpath is ignored for built-ins (use level to control visibility) in {}",
                                oc.header, overlay.resource, path.display()
                            );
                        }
                        if !is_builtin && oc.jsonpath.is_none() {
                            warn!(
                                "overlay: column '{}' has no jsonpath on CRD '{}' — column will have no data in {}",
                                oc.header, overlay.resource, path.display()
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
// Pre-resolved column render rules (client-side, not serializable)
// ---------------------------------------------------------------------------

/// A typed rendering predicate, parsed once from overlay YAML at
/// construction time. Analogous to `FormFieldKind` in the form system:
/// declared from shared config, evaluated generically by the client.
#[derive(Debug, Clone)]
pub(crate) enum RenderPredicate {
    /// Substring match against the cell's display text.
    Contains(String),
    /// Numeric comparison against the cell's display value (with unit
    /// suffixes stripped: `%`, `m`, `Mi`, `Ki`, `Gi`).
    Gte(f64),
    Gt(f64),
    Lte(f64),
    Lt(f64),
}

impl RenderPredicate {
    /// Parse an overlay `when` string (e.g., `">= 90"`) into a typed
    /// predicate. Returns `None` if the string is malformed.
    pub(crate) fn parse_when(s: &str) -> Option<Self> {
        let s = s.trim();
        let (variant, threshold_str): (fn(f64) -> Self, &str) = if let Some(rest) = s.strip_prefix(">=") {
            (Self::Gte, rest.trim())
        } else if let Some(rest) = s.strip_prefix("<=") {
            (Self::Lte, rest.trim())
        } else if let Some(rest) = s.strip_prefix('>') {
            (Self::Gt, rest.trim())
        } else if let Some(rest) = s.strip_prefix('<') {
            (Self::Lt, rest.trim())
        } else {
            return None;
        };
        threshold_str.parse::<f64>().ok().map(variant)
    }

    /// Evaluate this predicate against a cell's display string.
    pub(crate) fn matches(&self, cell_str: &str) -> bool {
        match self {
            Self::Contains(text) => cell_str.contains(text.as_str()),
            Self::Gte(t) => parse_numeric_cell(cell_str).is_some_and(|v| v >= *t),
            Self::Gt(t) => parse_numeric_cell(cell_str).is_some_and(|v| v > *t),
            Self::Lte(t) => parse_numeric_cell(cell_str).is_some_and(|v| v <= *t),
            Self::Lt(t) => parse_numeric_cell(cell_str).is_some_and(|v| v < *t),
        }
    }
}

/// Strip common unit suffixes and parse a cell's display string as f64.
/// Handles: `%`, `m` (millicores), binary SI (`Ki`, `Mi`, `Gi`, `Ti`),
/// and decimal SI (`K`/`k`, `M`, `G`, `T`).
fn parse_numeric_cell(cell_str: &str) -> Option<f64> {
    cell_str
        .trim()
        .trim_end_matches('%')
        .trim_end_matches('m')
        // Binary SI (must come before single-char decimal to avoid
        // partial stripping: "Mi" stripped as "i" then "M").
        .trim_end_matches("Ti")
        .trim_end_matches("Gi")
        .trim_end_matches("Mi")
        .trim_end_matches("Ki")
        // Decimal SI
        .trim_end_matches('T')
        .trim_end_matches('G')
        .trim_end_matches('M')
        .trim_end_matches('K')
        .trim_end_matches('k')
        .parse::<f64>()
        .ok()
}

/// A single rendering rule: predicate + style to apply when matched.
#[derive(Debug, Clone)]
pub(crate) struct RenderRule {
    pub(crate) predicate: RenderPredicate,
    pub(crate) style: RowHealth,
}

/// Pre-resolved rendering rules for one column. Built once from overlay
/// config when the table descriptor arrives. Analogous to `FormFieldKind`
/// in the form system: the "schema" half of a cell, set once, evaluated
/// generically at render time against the "value" half (`CellValue`).
#[derive(Debug, Clone, Default)]
pub struct ColumnRenderRules {
    pub(crate) rules: Vec<RenderRule>,
}

impl ColumnRenderRules {
    /// Evaluate this column's rules against a cell's display string.
    /// First match wins. Returns the style to apply, or `None` to inherit
    /// the row's style.
    pub fn evaluate(&self, cell_str: &str) -> Option<RowHealth> {
        for rule in &self.rules {
            if rule.predicate.matches(cell_str) {
                return Some(rule.style);
            }
        }
        None
    }
}

/// Build pre-resolved column render rules for a resource, indexed by
/// data column index (parallel to `cells`). Resolves overlay YAML rules
/// to column positions ONCE at construction time — no string-based header
/// lookups per row per frame.
///
/// Called when a table descriptor arrives or updates. The returned vec
/// is passed to `prepare_view` for per-cell style evaluation.
pub fn build_column_rules(headers: &[String], plural: &str) -> Vec<ColumnRenderRules> {
    let mut result = vec![ColumnRenderRules::default(); headers.len()];
    let Some(overlay) = overlay_for(plural) else {
        return result;
    };
    if overlay.coloring.is_empty() {
        return result;
    }
    for col_rule in &overlay.coloring {
        let col_idx = match headers.iter().position(|h| h.eq_ignore_ascii_case(&col_rule.column)) {
            Some(i) => i,
            None => continue,
        };
        for cr in &col_rule.rules {
            let predicate = if let Some(ref text) = cr.match_text {
                if text.is_empty() {
                    warn!("overlay: empty match text for column '{}' in resource '{}' (would match everything)",
                        col_rule.column, plural);
                    continue;
                }
                RenderPredicate::Contains(text.clone())
            } else if let Some(ref when) = cr.when {
                match RenderPredicate::parse_when(when) {
                    Some(p) => p,
                    None => {
                        warn!("overlay: invalid predicate '{}' for column '{}' in resource '{}'",
                            when, col_rule.column, plural);
                        continue;
                    }
                }
            } else {
                continue;
            };
            let style: RowHealth = cr.health.into();
            result[col_idx].rules.push(RenderRule { predicate, style });
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- RenderPredicate tests --------------------------------------------------

    #[test]
    fn predicate_parse_gte() {
        let p = RenderPredicate::parse_when(">= 90").unwrap();
        assert!(p.matches("95%"));
        assert!(p.matches("90"));
        assert!(!p.matches("89"));
    }

    #[test]
    fn predicate_parse_gt() {
        let p = RenderPredicate::parse_when("> 90").unwrap();
        assert!(p.matches("95"));
        assert!(!p.matches("90"));
        assert!(!p.matches("85"));
    }

    #[test]
    fn predicate_parse_lt() {
        let p = RenderPredicate::parse_when("< 10").unwrap();
        assert!(p.matches("5"));
        assert!(!p.matches("15"));
    }

    #[test]
    fn predicate_parse_invalid() {
        assert!(RenderPredicate::parse_when("bad predicate").is_none());
    }

    #[test]
    fn predicate_contains() {
        let p = RenderPredicate::Contains("CrashLoop".into());
        assert!(p.matches("CrashLoopBackOff"));
        assert!(!p.matches("Running"));
    }

    #[test]
    fn predicate_numeric_strips_units() {
        let p = RenderPredicate::parse_when(">= 70").unwrap();
        assert!(p.matches("72%"));
        assert!(p.matches("800m"));
        assert!(!p.matches("50%"));
    }

    // -- build_column_rules + evaluate tests ----------------------------------

    #[test]
    fn build_and_evaluate_column_rules() {
        let headers = vec!["STATUS".to_string(), "%CPU/R".to_string()];
        let _rules = build_column_rules(&headers, "pods");
        // Without a loaded overlay for "pods" in tests, rules are empty.
        // Directly test ColumnRenderRules evaluation instead.
        let col = ColumnRenderRules {
            rules: vec![
                RenderRule {
                    predicate: RenderPredicate::Contains("CrashLoopBackOff".into()),
                    style: RowHealth::Failed,
                },
            ],
        };
        assert_eq!(col.evaluate("CrashLoopBackOff"), Some(RowHealth::Failed));
        assert_eq!(col.evaluate("Running"), None);
    }

    #[test]
    fn column_rules_numeric_evaluation() {
        let col = ColumnRenderRules {
            rules: vec![
                RenderRule {
                    predicate: RenderPredicate::parse_when(">= 90").unwrap(),
                    style: RowHealth::Failed,
                },
                RenderRule {
                    predicate: RenderPredicate::parse_when(">= 70").unwrap(),
                    style: RowHealth::Pending,
                },
            ],
        };
        assert_eq!(col.evaluate("95%"), Some(RowHealth::Failed));
        assert_eq!(col.evaluate("72%"), Some(RowHealth::Pending));
        assert_eq!(col.evaluate("50%"), None);
    }

    #[test]
    fn column_rules_first_match_wins() {
        let col = ColumnRenderRules {
            rules: vec![
                RenderRule {
                    predicate: RenderPredicate::parse_when(">= 90").unwrap(),
                    style: RowHealth::Failed,
                },
                RenderRule {
                    predicate: RenderPredicate::parse_when(">= 70").unwrap(),
                    style: RowHealth::Pending,
                },
            ],
        };
        // 95% matches both rules, but first match (Failed) wins.
        assert_eq!(col.evaluate("95%"), Some(RowHealth::Failed));
    }

    #[test]
    fn predicate_na_value_does_not_match() {
        // Metrics placeholder "n/a" should never match numeric predicates.
        let p = RenderPredicate::parse_when(">= 70").unwrap();
        assert!(!p.matches("n/a"));
        assert!(!p.matches(""));
    }

    #[test]
    fn build_column_rules_missing_column_is_empty() {
        // Overlay references a column that doesn't exist in headers.
        let headers = vec!["NAME".to_string(), "STATUS".to_string()];
        let rules = build_column_rules(&headers, "nonexistent-resource");
        // No overlay for this resource → all rules empty.
        assert!(rules.iter().all(|r| r.rules.is_empty()));
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
        let OverlayCapability::Drill { ref target, ref column } = overlay.capabilities["show-node"];
        assert_eq!(target, "nodes");
        assert_eq!(column, "NODE");
        assert_eq!(overlay.columns.len(), 1);
        assert_eq!(overlay.columns[0].header, "INSTANCE TYPE");
        assert_eq!(overlay.coloring.len(), 1);
    }
}
