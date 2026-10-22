//! Cached CRD presentation metadata — the wire half.
//!
//! `cache.rs` was two files in one: these types ride the wire (the daemon's
//! discovery poll sends them, the TUI's `AppEvent::Discovery` reads them)
//! while the rest of that file is the daemon's own cache — a `CACHE_EPOCH`
//! static, per-context state, and the parser that turns a live
//! `CustomResourceDefinition` into a `CachedCrd`.
//!
//! Only the parser needed k8s-openapi, so only the parser stayed behind, as
//! a free function: an inherent `impl CachedCrd` could not follow the type
//! across a crate line.

use serde::{Deserialize, Serialize};

use crate::protocol::{CrdRef, ResourceScope};

/// A printer column from a CRD's additionalPrinterColumns spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrinterColumn {
    pub name: String,
    /// JSONPath expression into the object (e.g., ".spec.nodeClassRef.name").
    pub json_path: String,
    /// Column type. Typed enum so the dynamic-watcher rendering path
    /// can `match` instead of `if col.column_type == "date"` — the
    /// boundary parse from K8s schema strings happens once at
    /// discovery time.
    pub column_type: PrinterColumnType,
}

/// Closed enum of the K8s `additionalPrinterColumns.type` values we know
/// how to render. K8s defines a small set; unknown types fall through to
/// `Other(String)` so we don't lose information at the wire boundary, but
/// every render branch checks the typed variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrinterColumnType {
    /// `date` — RFC3339 timestamps rendered as age strings.
    Date,
    /// `string` — plain text. The default for K8s printer columns.
    String,
    /// `integer` / `number` — numeric, currently rendered as plain text.
    Integer,
    Number,
    /// `boolean` — true/false, rendered as plain text.
    Boolean,
    /// Anything K8s might add later — preserved verbatim, treated like
    /// String at render time.
    Other(String),
}

impl PrinterColumnType {
    /// Parse the raw K8s schema string into the typed variant.
    pub fn from_k8s(s: &str) -> Self {
        match s {
            "date" => PrinterColumnType::Date,
            "string" => PrinterColumnType::String,
            "integer" => PrinterColumnType::Integer,
            "number" => PrinterColumnType::Number,
            "boolean" => PrinterColumnType::Boolean,
            other => PrinterColumnType::Other(other.to_string()),
        }
    }

    /// True if this column should be formatted as an age string at render
    /// time. The dynamic watcher branches on this instead of comparing
    /// against the literal `"date"`.
    pub fn is_date(&self) -> bool {
        matches!(self, PrinterColumnType::Date)
    }
}

/// Serializable representation of a CRD for the discovery cache. Carries
/// the typed [`CrdRef`] for identity (group/version/kind/plural/scope) and
/// the printer-column list separately. The previous layout duplicated the
/// GVR fields and stored `scope` as a free-text string parsed back via a
/// `from_scope_str(_)` helper — both gone now.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedCrd {
    pub name: String,
    /// Typed GVR + scope. Identity (Hash/Eq) is `(group, version, plural)`.
    pub gvr: CrdRef,
    /// Columns from the CRD's additionalPrinterColumns — defines what to show.
    #[serde(default)]
    pub printer_columns: Vec<PrinterColumn>,
}

impl CachedCrd {
    /// Convenience accessors for callers that just want one field — keeps
    /// the call sites short without exposing `c.gvr.kind` everywhere.
    pub fn group(&self) -> &str { &self.gvr.group }
    pub fn version(&self) -> &str { &self.gvr.version }
    pub fn kind(&self) -> &str { &self.gvr.kind }
    pub fn plural(&self) -> &str { &self.gvr.plural }
    pub fn scope(&self) -> ResourceScope { self.gvr.scope }
}

