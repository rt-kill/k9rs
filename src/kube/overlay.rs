//! User-defined resource overlays loaded from `~/.config/k9rs/overlays/*.yaml`.
//!
//! Overlays layer customizations on top of the frozen built-in resource
//! definitions: custom key bindings, extra columns (JSONPath), and
//! declarative cell coloring rules. They're loaded at startup by both the
//! daemon (for columns/coloring) and the TUI (for key bindings).

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use crate::kube::resources::row::RowHealth;

/// Global overlay store, loaded once at startup. Both the daemon and TUI
/// read from the same `~/.config/k9rs/overlays/` directory.
static OVERLAYS: std::sync::LazyLock<(HashMap<String, ResourceOverlay>, Vec<String>)> =
    std::sync::LazyLock::new(|| {
        let mut problems = Vec::new();
        let map = load_overlays_reporting(&mut problems);
        (map, problems)
    });

/// Look up the overlay for a resource by its plural name.
pub fn overlay_for(plural: &str) -> Option<&'static ResourceOverlay> {
    OVERLAYS.0.get(&plural.to_lowercase())
}

/// Everything wrong with the user's overlay files, in load order.
///
/// Overlays are the one user-facing extension point, and every check in this
/// module reported through `warn!` — which goes to `io::sink` unless the user
/// happens to pass `--log-file`. So a typo'd column, an unknown capability, a
/// duplicate file and a YAML parse error were all *silently* discarded: the
/// fix for "the node overlay sat dead for months" shipped into /dev/null.
/// The TUI surfaces these at startup instead.
pub fn overlay_diagnostics() -> &'static [String] {
    &OVERLAYS.1
}

/// Coloring rules that name a column the resource does not have.
///
/// Split out so the check is testable without a filesystem. Case-insensitive,
/// matching how `build_column_rules` resolves headers — the point is to catch
/// a genuine typo (`CPU%` for `%CPU`), not a capitalisation difference that
/// would have worked anyway.
fn unknown_coloring_columns<'a>(
    overlay: &'a ResourceOverlay,
    known: &[&str],
) -> Vec<&'a str> {
    overlay
        .coloring
        .iter()
        .map(|r| r.column.as_str())
        .filter(|col| !known.iter().any(|h| h.eq_ignore_ascii_case(col)))
        .collect()
}

/// The key an overlay is stored under: the CANONICAL plural of whatever its
/// `resource:` field names.
///
/// Overlays used to be stored under the raw `resource:` string and looked up
/// by plural, so an overlay written as `resource: po` — which the load-time
/// validation accepts, *because that check itself resolves aliases* — was
/// filed under `po`, never matched `pods`, and reported "loaded 1 overlay(s)"
/// on the way to doing nothing at all. Resolving here means the two halves
/// agree by construction.
///
/// An unrecognised name stays as written: CRDs are not in the registry and
/// are looked up by their plural directly.
fn canonical_key(resource: &str) -> String {
    crate::kube::protocol::ResourceId::from_alias(resource)
        .map(|rid| rid.plural().to_lowercase())
        .unwrap_or_else(|| resource.to_lowercase())
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

/// A single coloring rule: ONE trigger plus the health it assigns.
///
/// The trigger used to be two `Option<String>`s — `match` and `when` — of
/// which exactly one had to be present. That is four invalid states, and each
/// one had a runtime guard: both set (match silently wins), neither set
/// (never fires), an empty `match` (fires on everything), an unparseable
/// `when`. Every guard was warn-and-continue, so a contradictory rule LOADED
/// and then quietly did the wrong thing — and they were written out twice,
/// once at load and again in `build_column_rules`, which also re-parsed
/// `when` on every call.
///
/// Now the trigger is parsed and validated ONCE, at deserialize, into the
/// same [`RenderPredicate`] the renderer evaluates. A rule that cannot say
/// what it means fails its file, reported through `overlay_diagnostics` like
/// any other parse error — which is already what a typo'd key does, because
/// `deny_unknown_fields` has always been on this struct. "Both `match` and
/// `when`" being a mere warning was the inconsistency.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "RawColorRule")]
pub struct ColorRule {
    /// What makes this rule fire. Exactly one, by construction.
    pub(crate) trigger: RenderPredicate,
    /// Health level to assign when it does.
    pub health: OverlayHealth,
}

/// The on-disk shape of a [`ColorRule`], before validation. Exists only as
/// `try_from`'s input: the two-`Option` form is what the user writes, and is
/// not a form the rest of the program should be able to hold.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawColorRule {
    #[serde(rename = "match")]
    match_text: Option<String>,
    when: Option<String>,
    health: OverlayHealth,
}

impl TryFrom<RawColorRule> for ColorRule {
    type Error = String;

    fn try_from(raw: RawColorRule) -> Result<Self, Self::Error> {
        let trigger = match (raw.match_text, raw.when) {
            (Some(_), Some(_)) => {
                return Err("sets both `match` and `when` — a rule fires on one \
                            thing, so give it one"
                    .to_string())
            }
            (None, None) => {
                return Err("has neither `match` nor `when`, so it could never \
                            fire"
                    .to_string())
            }
            (Some(text), None) if text.is_empty() => {
                return Err("has an empty `match`, which would colour every row"
                    .to_string())
            }
            (Some(text), None) => RenderPredicate::Contains(text),
            (None, Some(when)) => RenderPredicate::parse_when(&when).ok_or_else(|| {
                format!("has an invalid `when` predicate `{when}` (expected e.g. `>= 90`)")
            })?,
        };
        Ok(Self { trigger, health: raw.health })
    }
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

/// Load every overlay file from `~/.config/k9rs/overlays/`, collecting each
/// complaint as a user-facing string.
///
/// Returns a map keyed by resource plural. One bad overlay never breaks the
/// rest: its file is reported and skipped. Every `warn!` below also pushes a
/// report, so the two cannot drift — the log line is for `--log-file`, the
/// report is for the person who wrote the overlay and is looking at a TUI.
///
/// (A discarding `load_overlays()` wrapper sat here with no callers. `pub`,
/// so nothing flagged it.)
pub fn load_overlays_reporting(problems: &mut Vec<String>) -> HashMap<String, ResourceOverlay> {
    let Ok(home) = std::env::var("HOME") else { return HashMap::new() };
    load_overlays_from(&Path::new(&home).join(".config/k9rs/overlays"), problems)
}

/// `load_overlays_reporting` over a given directory.
///
/// The directory is a PARAMETER so the diagnostics path is testable without
/// mutating `HOME` — which is process-global, and this suite runs in
/// parallel. The earlier test for a refused coloring rule could only check
/// serde in isolation; what matters to the user is that the refusal reaches
/// the startup flash, and that is this function's output.
pub fn load_overlays_from(
    dir: &Path,
    problems: &mut Vec<String>,
) -> HashMap<String, ResourceOverlay> {
    // One site, two destinations — a `warn!` that the user can also see.
    macro_rules! warn {
        ($($arg:tt)*) => {{
            tracing::warn!($($arg)*);
            problems.push(format!($($arg)*));
        }};
    }
    let mut overlays = HashMap::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return overlays, // directory doesn't exist — no overlays
    };
    // SORTED: `read_dir` yields filesystem order, so with two files claiming
    // the same resource the winner would otherwise differ between machines —
    // and between runs after an unrelated file was touched.
    let mut paths: Vec<std::path::PathBuf> = entries.flatten().map(|e| e.path()).collect();
    paths.sort();
    for path in paths {
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
                    // Does each coloring rule name a column this resource
                    // actually has?
                    //
                    // `build_column_rules` looks columns up by header and
                    // quietly yields empty rules for one that doesn't exist,
                    // so a typo is a silent no-op — and the typo is easy:
                    // node util columns are `%CPU`/`%MEM`, and `CPU%`/`MEM%`
                    // reads just as naturally. The user's node overlay sat
                    // dead for months because of exactly that, while the
                    // STATUS rules in the same file worked, which made it
                    // look like partial support rather than a mistake.
                    //
                    // Only checkable for BUILT-INS: a CRD's columns come from
                    // the cluster's printer columns plus this overlay's own
                    // `columns:`, neither of which is known here.
                    if let Some(def) = crate::kube::resource_defs::REGISTRY
                        .by_alias(&overlay.resource)
                    {
                        // `default_headers()` — the MANDATORY header list,
                        // which all 33 built-ins implement. This read
                        // `column_defs()`, an optional tagging hook only 4 of
                        // them override, so the check false-positived on 30
                        // resources: an overlay colouring `READY` on
                        // deployments was told the column does not exist,
                        // with an empty "Columns: " list, while the rule
                        // worked perfectly at runtime. Its own test passed a
                        // hand-built list and so never touched the registry.
                        let headers = def.default_headers();
                        let known: Vec<&str> = headers
                            .iter()
                            .map(String::as_str)
                            .chain(overlay.columns.iter().map(|c| c.header.as_str()))
                            .collect();
                        for missing in unknown_coloring_columns(&overlay, &known) {
                            warn!(
                                "overlay: coloring targets column '{}', which '{}' does not \
                                 have — the rules will never apply. Columns: {} (in {})",
                                missing,
                                overlay.resource,
                                known.join(", "),
                                path.display(),
                            );
                        }
                    }
                    // Validate coloring rules at load. `build_column_rules`
                    // checks them too, but only lazily when the resource is
                    // first viewed — surfacing a malformed rule here means a
                    // typo'd threshold is reported at startup, not on navigation.
                    // A coloring rule that cannot say what it means no longer
                    // reaches here: `ColorRule`'s `try_from` refuses it at
                    // deserialize, and the file's parse error is reported
                    // above like any other.
                    let key = canonical_key(&overlay.resource);
                    if let Some(prior) = overlays.get(&key) {
                        // Same plural from two files. If their `group`s differ
                        // this is a genuine ambiguity the lookup cannot
                        // resolve — every caller has only a plural — so say
                        // which one is in force rather than letting directory
                        // order decide invisibly.
                        warn!(
                            "overlay: '{}' is defined twice (group {:?} and {:?}); \
                             lookup is by plural only, so {} is IGNORED",
                            key, prior.group, overlay.group, path.display(),
                        );
                        continue;
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
            // No guards and no parsing: the trigger was validated once, at
            // deserialize, into exactly this type. This used to re-parse
            // every `when` on every call and warn-and-skip the bad ones —
            // the same four decisions the loader had already made.
            result[col_idx]
                .rules
                .push(RenderRule { predicate: cr.trigger.clone(), style: cr.health.into() });
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/kube/overlay.rs"]
mod tests;
