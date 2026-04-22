//! Types for discovery data (CRDs and namespaces).
//!
//! Used by the daemon's in-memory discovery cache ([`DiscoveryCache`] in
//! `SessionSharedState`) and the session protocol (SessionEvent::Discovery).
//! No disk persistence — cache is tied to the daemon process lifetime.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::kube::protocol::{ContextId, ContextName, CrdRef, ResourceScope};
use crate::kube::resources::row::{DrillTarget, ResourceRow, RowHealth};

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

/// Convert cached CRDs to unified ResourceRow structs with extra metadata.
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
            ResourceRow {
                cells: vec![
                    c.name.clone(), c.gvr.group.clone(), c.gvr.version.clone(),
                    c.gvr.kind.clone(), scope_label.to_string(), String::new(), // no age from cache
                ],
                name: c.name.clone(),
                namespace: None,
                containers: Vec::new(),
                owner_refs: Vec::new(),
                pf_ports: Vec::new(),
                node: None,
                health: RowHealth::Normal,
                crd_info,
                drill_target,
                ..Default::default()
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// DiscoveryCache — per-context entry, per-field atomic swaps
// ---------------------------------------------------------------------------

use std::sync::Arc;
use crate::util::AtomicOption;

/// All cached discovery data for a single [`ContextId`]. Each field is an
/// independent [`AtomicOption`] so writers can update one without touching
/// the other — partial-success poisoning is structurally impossible, since a
/// failed fetch writes nothing and the successful fetch only swaps its own
/// field.
///
/// New per-context data (server version, capability flags, etc.) slots in as
/// another field here without needing a parallel `DashMap` keyed the same
/// way, and readers that already have a `&PerContext` get constant-time
/// access to everything.
///
/// Values are `Arc<T>` so `load_cloned` — which clones to produce a
/// concurrent-safe peek — is cheap (an Arc refcount bump, not a deep clone
/// of the inner Vec).
#[derive(Default)]
pub struct PerContext {
    pub namespaces: AtomicOption<Arc<Vec<String>>>,
    pub crds: AtomicOption<Arc<Vec<CachedCrd>>>,
}

/// Daemon-wide cache of discovery data keyed by [`ContextId`] (server_url +
/// user, so two contexts aliased to the same cluster share one entry).
///
/// One entry per context, each field swappable independently. See
/// [`PerContext`] for the per-field guarantees.
#[derive(Default)]
pub struct DiscoveryCache {
    entries: DashMap<ContextId, Arc<PerContext>>,
}

impl DiscoveryCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return (or lazily create) the per-context entry. The returned Arc
    /// lets the caller perform multiple field updates without re-taking the
    /// shard lock.
    fn entry_for(&self, ctx: &ContextId) -> Arc<PerContext> {
        if let Some(r) = self.entries.get(ctx) {
            return r.clone();
        }
        self.entries.entry(ctx.clone()).or_default().clone()
    }

    /// Swap in a new namespace list. Only callers with a successful fetch
    /// should invoke this — a failed fetch should leave the prior value
    /// untouched, which it does if you simply don't call this.
    pub fn set_namespaces(&self, ctx: ContextId, namespaces: Vec<String>) {
        self.entry_for(&ctx)
            .namespaces
            .store(Some(Arc::new(namespaces)), std::sync::atomic::Ordering::AcqRel);
    }

    /// Swap in a new CRD list. Same contract as `set_namespaces`.
    pub fn set_crds(&self, ctx: ContextId, crds: Vec<CachedCrd>) {
        self.entry_for(&ctx)
            .crds
            .store(Some(Arc::new(crds)), std::sync::atomic::Ordering::AcqRel);
    }

    /// Read the cached namespace list (cloned), if any.
    pub fn namespaces(&self, ctx: &ContextId) -> Option<Vec<String>> {
        let entry = self.entries.get(ctx)?;
        entry.namespaces.load_cloned().map(|arc| (*arc).clone())
    }

    /// Read the cached CRD list (cloned), if any.
    pub fn crds(&self, ctx: &ContextId) -> Option<Vec<CachedCrd>> {
        let entry = self.entries.get(ctx)?;
        entry.crds.load_cloned().map(|arc| (*arc).clone())
    }

    /// Find the printer-columns list for a specific CRD under a context.
    /// Avoids exposing the cache structure to call sites that only want one
    /// CRD's columns.
    pub fn printer_columns_for(
        &self,
        ctx: &ContextId,
        group: &str,
        plural: &str,
    ) -> Option<Vec<PrinterColumn>> {
        let entry = self.entries.get(ctx)?;
        let crds = entry.crds.load_cloned()?;
        crds.iter()
            .find(|c| c.gvr.group == group && c.gvr.plural == plural)
            .map(|c| c.printer_columns.clone())
    }

    /// Wipe every cached entry. Returns the number of contexts removed for
    /// user-facing feedback.
    pub fn clear_all(&self) -> usize {
        let n = self.entries.len();
        self.entries.clear();
        n
    }

    /// Wipe entries whose [`ContextName`] matches. Returns the number of
    /// contexts removed.
    pub fn clear_context(&self, name: &ContextName) -> usize {
        let removed = self.entries.iter().filter(|e| e.key().name == *name).count();
        self.entries.retain(|cid, _| cid.name != *name);
        removed
    }
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
            health: RowHealth::Normal,
            crd_info: None,
            // Namespace name comes from the K8s API's NamespaceList — it's
            // a real identifier, never the literal string "all". Use the
            // typed `Named` constructor directly: routing this through
            // `from_user_command` would silently switch into all-namespaces
            // mode if a cluster ever had a namespace literally named `all`.
            drill_target: Some(DrillTarget::SwitchNamespace(
                crate::kube::protocol::Namespace::Named(name.clone()),
            )),
            ..Default::default()
        })
        .collect()
}
