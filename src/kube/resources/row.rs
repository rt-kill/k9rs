use std::collections::BTreeMap;
use std::cmp::Ordering;
use std::fmt;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::util::{format_age_secs, format_cpu, format_mem};

/// Row health indicator, computed server-side by each converter.
/// The client reads this for row coloring — no resource-type-specific
/// knowledge needed on the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum RowHealth {
    /// Healthy / running / ready.
    #[default]
    Normal,
    /// In-progress / starting / pending.
    Pending,
    /// Error / degraded / not-ready.
    Failed,
}

// ---------------------------------------------------------------------------
// CellValue — typed cell representation
// ---------------------------------------------------------------------------

/// Unit of a quantity cell, used to select the right formatter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QuantityUnit {
    Millicores,
    Bytes,
}

/// A typed cell value. Variant order is **load-bearing** for bincode
/// (positional encoding) — do NOT reorder or insert between existing
/// variants after Phase 1 is merged.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CellValue {
    Text(String),
    Ratio { num: u32, denom: u32 },
    Quantity { value: u64, unit: QuantityUnit },
    Age(Option<i64>),
    Count(i64),
    Bool(bool),
    List(Vec<String>),
    Status { text: String, health: RowHealth },
    Percentage(Option<u64>),
    Placeholder,
}

impl CellValue {
    /// Build a `List` from a pre-joined comma-separated string.
    /// Returns an empty list if the input is empty.
    pub fn from_comma_str(s: &str) -> Self {
        if s.is_empty() {
            CellValue::List(vec![])
        } else {
            CellValue::List(s.split(',').filter(|p| !p.is_empty()).map(String::from).collect())
        }
    }
}

// ---- Display ----------------------------------------------------------------

impl fmt::Display for CellValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CellValue::Text(s) => f.write_str(s),
            CellValue::Ratio { num, denom } => write!(f, "{}/{}", num, denom),
            CellValue::Quantity { value, unit } => match unit {
                QuantityUnit::Millicores => {
                    let formatted = format_cpu(&format!("{}m", value));
                    f.write_str(&formatted)
                }
                QuantityUnit::Bytes => {
                    let formatted = format_mem(&value.to_string());
                    f.write_str(&formatted)
                }
            },
            CellValue::Age(Some(epoch_secs)) => {
                let elapsed = Utc::now().timestamp() - epoch_secs;
                let formatted = format_age_secs(elapsed);
                f.write_str(&formatted)
            }
            CellValue::Age(None) => f.write_str("<unknown>"),
            CellValue::Count(n) => write!(f, "{}", n),
            CellValue::Bool(b) => f.write_str(if *b { "true" } else { "false" }),
            CellValue::List(items) => f.write_str(&items.join(",")),
            CellValue::Status { text, .. } => f.write_str(text),
            CellValue::Percentage(Some(v)) => write!(f, "{}%", v),
            CellValue::Percentage(None) => f.write_str("n/a"),
            CellValue::Placeholder => f.write_str("n/a"),
        }
    }
}

// ---- Ordering ---------------------------------------------------------------

/// Returns a discriminant index for cross-variant comparison.
/// Mirrors the declaration order in the enum.
fn cell_discriminant(v: &CellValue) -> u8 {
    match v {
        CellValue::Text(_) => 0,
        CellValue::Ratio { .. } => 1,
        CellValue::Quantity { .. } => 2,
        CellValue::Age(_) => 3,
        CellValue::Count(_) => 4,
        CellValue::Bool(_) => 5,
        CellValue::List(_) => 6,
        CellValue::Status { .. } => 7,
        CellValue::Percentage(_) => 8,
        CellValue::Placeholder => 9,
    }
}

impl Ord for CellValue {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (CellValue::Text(a), CellValue::Text(b)) => a.cmp(b),

            (CellValue::Ratio { num: an, denom: ad }, CellValue::Ratio { num: bn, denom: bd }) => {
                let fa = *an as f64 / (*ad).max(1) as f64;
                let fb = *bn as f64 / (*bd).max(1) as f64;
                fa.partial_cmp(&fb).unwrap_or(an.cmp(bn))
            }

            (CellValue::Quantity { value: a, .. }, CellValue::Quantity { value: b, .. }) => {
                a.cmp(b)
            }

            // Age: Some sorts before None. Larger epoch (more recent) sorts later.
            (CellValue::Age(a), CellValue::Age(b)) => match (a, b) {
                (Some(a_val), Some(b_val)) => a_val.cmp(b_val),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },

            (CellValue::Count(a), CellValue::Count(b)) => a.cmp(b),

            (CellValue::Bool(a), CellValue::Bool(b)) => a.cmp(b),

            (CellValue::List(a), CellValue::List(b)) => {
                a.len().cmp(&b.len()).then_with(|| {
                    let ja = a.join(",");
                    let jb = b.join(",");
                    ja.cmp(&jb)
                })
            }

            (CellValue::Status { text: a, .. }, CellValue::Status { text: b, .. }) => a.cmp(b),

            // Percentage: Some sorts before None.
            (CellValue::Percentage(a), CellValue::Percentage(b)) => match (a, b) {
                (Some(a_val), Some(b_val)) => a_val.cmp(b_val),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },

            (CellValue::Placeholder, CellValue::Placeholder) => Ordering::Equal,

            // Cross-variant: compare by discriminant (deterministic but meaningless).
            _ => cell_discriminant(self).cmp(&cell_discriminant(other)),
        }
    }
}

impl PartialOrd for CellValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A single resource row in the unified table model.
/// Replaces all 28 typed Kube* structs with a generic representation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceRow {
    /// Typed cell values, in header order. Sort, filter, and render all
    /// operate on these directly via `CellValue::cmp()` and
    /// `CellValue::to_string()`.
    pub cells: Vec<CellValue>,
    /// Resource name (cached for O(1) access in sorts/filters).
    pub name: String,
    /// Resource namespace. `None` for cluster-scoped resources.
    pub namespace: Option<String>,
    /// What happens when the user presses Enter on this row.
    /// Set by the converter (server-side); the client reads this blindly
    /// to construct the appropriate nav action — no K8s knowledge needed.
    /// `None` means describe-on-Enter.
    pub drill_target: Option<DrillTarget>,
    /// Container metadata (pods only). Used by the client to render container
    /// selectors and by the server for owner-chain port resolution.
    pub containers: Vec<ContainerInfo>,
    /// Owner references (server-side for OwnerUid post-filtering).
    pub owner_refs: Vec<OwnerRefInfo>,
    /// Port-forward metadata: suggested local/remote ports for this resource.
    /// Used by the client to populate the port-forward dialog.
    pub pf_ports: Vec<u16>,
    /// CRD definition metadata (only set on rows in the `crds` table).
    /// Used by the client for command completion and autocomplete.
    pub crd_info: Option<CrdRowInfo>,
    /// Node name. `Some(name)` for pods scheduled to a node, `None` for
    /// every other resource type AND for unscheduled pods. The client uses
    /// this for `ShowNode` navigation; non-pod rows skip the action because
    /// the field is `None` rather than the empty string.
    pub node: Option<String>,
    /// Server-computed health for row coloring. The client reads this
    /// directly instead of parsing cells per resource type.
    #[serde(default)]
    pub health: RowHealth,
    /// Summed CPU request across all containers, in millicores (pods only).
    /// Used by the metrics overlay to compute %CPU/R.
    ///
    /// NOTE: no `skip_serializing_if` — bincode is positional, so skipping a
    /// field shifts every subsequent field and deserialization reads garbage.
    /// The `#[serde(default)]` is harmless (bincode ignores it) but left for
    /// forward-compat with JSON snapshots.
    #[serde(default)]
    pub cpu_request: Option<u64>,
    /// Summed CPU limit across all containers, in millicores (pods only).
    #[serde(default)]
    pub cpu_limit: Option<u64>,
    /// Summed memory request across all containers, in bytes (pods only).
    #[serde(default)]
    pub mem_request: Option<u64>,
    /// Summed memory limit across all containers, in bytes (pods only).
    #[serde(default)]
    pub mem_limit: Option<u64>,
}

/// CRD definition metadata (for rows in the `crds` table). Type alias over
/// [`crate::kube::protocol::CrdRef`] — the wire shape is identical, and
/// using one type means converters/consumers can hand the value straight
/// to anything that takes a `CrdRef` (e.g. `ResourceId::Crd(CrdRef)` for
/// nav drill-downs).
pub type CrdRowInfo = crate::kube::protocol::CrdRef;

/// Container info for pods — used by shell, logs, port-forward.
///
/// Only the fields the client actually reads ride the wire. The server
/// uses additional intermediate values (image, ready, state, restarts,
/// container-level ports) at construction time but distills them into
/// the parent row's cells / `pf_ports` / `health` before serializing.
///
/// `kind` distinguishes init from regular containers as a typed enum;
/// the UI renders the `init:` prefix from the discriminant rather than
/// from a string-prefix encoding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerInfo {
    /// Container name as `kubectl exec/logs` expects it. The UI
    /// renders it with an `init:` prefix when `kind == Init`, and
    /// passes it verbatim into `LogContainer::Named` on shell/log.
    pub name: String,
    /// Init vs regular. Typed so the UI doesn't string-parse `name`.
    #[serde(default)]
    pub kind: ContainerKind,
}

/// Init vs regular container. Defaults to `Regular` for forward compat
/// with snapshots that pre-date this field.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContainerKind {
    #[default]
    Regular,
    Init,
}

/// Owner reference info for pods — used by owner chain drill-down.
/// Only the fields the client reads (kind/name/uid for breadcrumbs and
/// chain matching) ride the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerRefInfo {
    pub kind: String,
    pub name: String,
    pub uid: String,
}

/// What happens when the user presses Enter on a row. Set by the converter
/// (server-side) so the client doesn't need K8s knowledge to drill down.
///
/// The client reads this blindly and constructs the appropriate nav action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DrillTarget {
    /// Switch to a different namespace (used by namespace rows). Typed
    /// as `Namespace` because this is a *selection* (the user picking a
    /// scope), not a location string.
    SwitchNamespace(crate::kube::protocol::Namespace),
    /// Push a CRD-instance view onto the nav stack. Wraps a [`crate::kube::protocol::CrdRef`]
    /// so the drill handler can build a `ResourceId::Crd(...)` directly
    /// without re-marshaling fields.
    BrowseCrd(crate::kube::protocol::CrdRef),
    /// Drill down to pods filtered by label selector (deploy/sts/ds/svc/job).
    PodsByLabels {
        labels: BTreeMap<String, String>,
        /// Display label for the breadcrumb (e.g., "deploy/my-app").
        breadcrumb: String,
    },
    /// Drill down to pods filtered by ownerReference UID (replicaset/job).
    PodsByOwner {
        uid: String,
        /// Parent kind, typed. Producers have a [`BuiltInKind`] in hand
        /// already; stringifying and re-parsing on the client was extra
        /// motion. Breadcrumb display fetches the human string via
        /// [`crate::kube::resource_defs::REGISTRY`].
        kind: crate::kube::resource_def::BuiltInKind,
        name: String,
    },
    /// Drill down to pods filtered by a typed K8s field selector.
    /// Replaces the older `PodsByField { field: String, value: String }`
    /// shape — the typed enum carries the field path so producers can't
    /// fat-finger `"spec.nodeName"`.
    PodsByField(crate::app::nav::K8sFieldSelector),
    /// Drill down to pods by name prefix (fallback when no selector exists).
    PodsByNameGrep(String),
    /// Drill down to jobs owned by a parent resource (via ownerReference
    /// UID). Produced server-side by the cronjobs converter today (so
    /// `kind` is `CronJob`), but typed so it doesn't assume the parent
    /// kind at the client.
    JobsByOwner {
        uid: String,
        kind: crate::kube::resource_def::BuiltInKind,
        name: String,
    },
}

impl super::KubeResource for ResourceRow {
    fn cells(&self) -> &[CellValue] {
        &self.cells
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn namespace(&self) -> &str {
        self.namespace.as_deref().unwrap_or("")
    }
}

impl ResourceRow {
    /// Mutate a cell in-place (e.g., for metrics overlay). No-op if `col`
    /// is out of bounds.
    pub fn set_cell(&mut self, col: usize, value: CellValue) {
        if col < self.cells.len() {
            self.cells[col] = value;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    // ---- Display ------------------------------------------------------------

    #[test]
    fn display_text() {
        assert_eq!(CellValue::Text("hello".into()).to_string(), "hello");
    }

    #[test]
    fn display_ratio() {
        assert_eq!(
            CellValue::Ratio { num: 3, denom: 5 }.to_string(),
            "3/5"
        );
    }

    #[test]
    fn display_quantity_millicores() {
        // 500m stays as "500m"
        assert_eq!(
            CellValue::Quantity { value: 500, unit: QuantityUnit::Millicores }.to_string(),
            "500m"
        );
        // 2000m normalizes to "2" (whole cores)
        assert_eq!(
            CellValue::Quantity { value: 2000, unit: QuantityUnit::Millicores }.to_string(),
            "2"
        );
    }

    #[test]
    fn display_quantity_bytes() {
        // 1 GiB in bytes
        let gib = 1024 * 1024 * 1024;
        assert_eq!(
            CellValue::Quantity { value: gib, unit: QuantityUnit::Bytes }.to_string(),
            "1Gi"
        );
    }

    #[test]
    fn display_age_none() {
        assert_eq!(CellValue::Age(None).to_string(), "<unknown>");
    }

    #[test]
    fn display_count() {
        assert_eq!(CellValue::Count(42).to_string(), "42");
        assert_eq!(CellValue::Count(-1).to_string(), "-1");
    }

    #[test]
    fn display_bool() {
        assert_eq!(CellValue::Bool(true).to_string(), "true");
        assert_eq!(CellValue::Bool(false).to_string(), "false");
    }

    #[test]
    fn display_list() {
        assert_eq!(
            CellValue::List(vec!["a".into(), "b".into(), "c".into()]).to_string(),
            "a,b,c"
        );
        assert_eq!(CellValue::List(vec![]).to_string(), "");
    }

    #[test]
    fn display_status() {
        assert_eq!(
            CellValue::Status { text: "Running".into(), health: RowHealth::Normal }.to_string(),
            "Running"
        );
    }

    #[test]
    fn display_percentage() {
        assert_eq!(CellValue::Percentage(Some(85)).to_string(), "85%");
        assert_eq!(CellValue::Percentage(None).to_string(), "n/a");
    }

    #[test]
    fn display_placeholder() {
        assert_eq!(CellValue::Placeholder.to_string(), "n/a");
    }

    // ---- Ordering -----------------------------------------------------------

    #[test]
    fn ord_text() {
        let a = CellValue::Text("alpha".into());
        let b = CellValue::Text("beta".into());
        assert_eq!(a.cmp(&b), Ordering::Less);
    }

    #[test]
    fn ord_count_numeric() {
        let c2 = CellValue::Count(2);
        let c10 = CellValue::Count(10);
        // Numeric, not lexicographic: 2 < 10
        assert_eq!(c2.cmp(&c10), Ordering::Less);
    }

    #[test]
    fn ord_ratio_by_fraction() {
        // 1/3 < 1/2
        let r1 = CellValue::Ratio { num: 1, denom: 3 };
        let r2 = CellValue::Ratio { num: 1, denom: 2 };
        assert_eq!(r1.cmp(&r2), Ordering::Less);

        // 3/4 > 2/4
        let r3 = CellValue::Ratio { num: 3, denom: 4 };
        let r4 = CellValue::Ratio { num: 2, denom: 4 };
        assert_eq!(r3.cmp(&r4), Ordering::Greater);
    }

    #[test]
    fn ord_age_some_before_none() {
        let some_age = CellValue::Age(Some(1000));
        let no_age = CellValue::Age(None);
        // Some sorts before None
        assert_eq!(some_age.cmp(&no_age), Ordering::Less);
    }

    #[test]
    fn ord_age_larger_epoch_sorts_later() {
        let older = CellValue::Age(Some(1000));
        let newer = CellValue::Age(Some(2000));
        assert_eq!(older.cmp(&newer), Ordering::Less);
    }

    #[test]
    fn ord_bool_false_lt_true() {
        assert_eq!(CellValue::Bool(false).cmp(&CellValue::Bool(true)), Ordering::Less);
    }

    #[test]
    fn ord_percentage_some_before_none() {
        let some_pct = CellValue::Percentage(Some(50));
        let no_pct = CellValue::Percentage(None);
        assert_eq!(some_pct.cmp(&no_pct), Ordering::Less);
    }

    #[test]
    fn ord_percentage_numeric() {
        let low = CellValue::Percentage(Some(10));
        let high = CellValue::Percentage(Some(90));
        assert_eq!(low.cmp(&high), Ordering::Less);
    }

    #[test]
    fn ord_quantity() {
        let small = CellValue::Quantity { value: 100, unit: QuantityUnit::Millicores };
        let big = CellValue::Quantity { value: 500, unit: QuantityUnit::Millicores };
        assert_eq!(small.cmp(&big), Ordering::Less);
    }

    #[test]
    fn ord_list_by_length_then_content() {
        let short = CellValue::List(vec!["a".into()]);
        let long = CellValue::List(vec!["a".into(), "b".into()]);
        assert_eq!(short.cmp(&long), Ordering::Less);

        // Same length: compare by joined content
        let ab = CellValue::List(vec!["a".into(), "b".into()]);
        let ac = CellValue::List(vec!["a".into(), "c".into()]);
        assert_eq!(ab.cmp(&ac), Ordering::Less);
    }

    #[test]
    fn ord_placeholder_equal() {
        assert_eq!(CellValue::Placeholder.cmp(&CellValue::Placeholder), Ordering::Equal);
    }

    #[test]
    fn ord_cross_variant_deterministic() {
        let text = CellValue::Text("z".into());
        let count = CellValue::Count(0);
        // Text (disc 0) < Count (disc 4)
        assert_eq!(text.cmp(&count), Ordering::Less);
    }

    // ---- Bincode roundtrip --------------------------------------------------

    #[test]
    fn bincode_roundtrip() {
        let values = vec![
            CellValue::Text("hello".into()),
            CellValue::Ratio { num: 3, denom: 5 },
            CellValue::Quantity { value: 500, unit: QuantityUnit::Millicores },
            CellValue::Quantity { value: 1024 * 1024, unit: QuantityUnit::Bytes },
            CellValue::Age(Some(1713600000)),
            CellValue::Age(None),
            CellValue::Count(42),
            CellValue::Bool(true),
            CellValue::List(vec!["a".into(), "b".into()]),
            CellValue::Status { text: "Running".into(), health: RowHealth::Normal },
            CellValue::Percentage(Some(85)),
            CellValue::Percentage(None),
            CellValue::Placeholder,
        ];

        for val in &values {
            let encoded = bincode::serialize(val).expect("serialize");
            let decoded: CellValue = bincode::deserialize(&encoded).expect("deserialize");
            assert_eq!(&decoded, val, "roundtrip failed for {:?}", val);
        }
    }

    // ---- set_cell -------------------------------------------------------------

    #[test]
    fn set_cell_in_bounds() {
        let mut row = ResourceRow {
            cells: vec![CellValue::Placeholder, CellValue::Placeholder],
            ..Default::default()
        };
        row.set_cell(1, CellValue::Count(7));
        assert_eq!(row.cells[1], CellValue::Count(7));
    }

    #[test]
    fn set_cell_out_of_bounds_noop() {
        let mut row = ResourceRow {
            cells: vec![CellValue::Placeholder],
            ..Default::default()
        };
        // Should not panic
        row.set_cell(5, CellValue::Count(99));
        assert_eq!(row.cells.len(), 1);
    }

    // ---- Default cells is empty ---------------------------------------

    #[test]
    fn default_cells_empty() {
        let row = ResourceRow::default();
        assert!(row.cells.is_empty());
    }

    // ---- from_comma_str --------------------------------------------------

    #[test]
    fn from_comma_str_empty() {
        assert_eq!(CellValue::from_comma_str(""), CellValue::List(vec![]));
    }

    #[test]
    fn from_comma_str_single() {
        assert_eq!(
            CellValue::from_comma_str("foo"),
            CellValue::List(vec!["foo".into()])
        );
    }

    #[test]
    fn from_comma_str_multiple() {
        assert_eq!(
            CellValue::from_comma_str("a,b,c"),
            CellValue::List(vec!["a".into(), "b".into(), "c".into()])
        );
    }

    #[test]
    fn from_comma_str_trailing_comma() {
        // Trailing comma should not produce an empty entry.
        assert_eq!(
            CellValue::from_comma_str("a,b,"),
            CellValue::List(vec!["a".into(), "b".into()])
        );
    }
}
