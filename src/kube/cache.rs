//! Types for discovery data (CRDs and namespaces).
//!
//! Used by the daemon's in-memory discovery cache ([`DiscoveryCache`] in
//! `SessionSharedState`) and the session protocol (SessionEvent::Discovery).
//! No disk persistence — cache is tied to the daemon process lifetime.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::kube::protocol::{ContextId, ContextName, CrdRef, ResourceScope};
use crate::kube::resources::row::{CellValue, DrillTarget, ResourceRow, RowHealth};

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
    /// Parse a live `CustomResourceDefinition` into cache form. THE single
    /// parser for CRD presentation metadata — the bulk discovery poll and
    /// the on-demand per-watcher resolution both go through here, so the
    /// two paths can never disagree about how a recipe is read.
    pub fn from_k8s(
        crd: &k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
    ) -> Option<Self> {
        let name = crd.metadata.name.clone()?;
        let spec = &crd.spec;
        let served = spec.versions.iter().find(|v| v.served).or(spec.versions.first());
        let version = served.map(|v| v.name.clone()).unwrap_or_default();
        let printer_columns: Vec<PrinterColumn> = served
            .and_then(|v| v.additional_printer_columns.as_ref())
            .map(|cols| {
                cols.iter()
                    .map(|c| PrinterColumn {
                        name: c.name.clone(),
                        json_path: c.json_path.clone(),
                        column_type: PrinterColumnType::from_k8s(&c.type_),
                    })
                    .collect()
            })
            .unwrap_or_default();
        let scope = crate::kube::protocol::ResourceScope::from_k8s_spec(&spec.scope);
        Some(Self {
            name,
            gvr: CrdRef::new(
                spec.group.clone(),
                version,
                spec.names.kind.clone(),
                spec.names.plural.clone(),
                scope,
            ),
            printer_columns,
        })
    }

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
            let cells: Vec<CellValue> = vec![
                CellValue::Text(c.name.clone()),
                CellValue::Text(c.gvr.group.clone()),
                CellValue::Text(c.gvr.version.clone()),
                CellValue::Text(c.gvr.kind.clone()),
                CellValue::Text(scope_label.to_string()),
                CellValue::Age(None), // no age from cache
            ];            ResourceRow {
                name: c.name.clone(),
                namespace: None,
                containers: Vec::new(),
                owner_refs: Vec::new(),
                pf_ports: Vec::new(),
                node: None,
                health: RowHealth::Normal,
                crd_info,
                drill_target,
                cells,
                ..Default::default()
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// DiscoveryCache — per-context entry, per-field atomic swaps
// ---------------------------------------------------------------------------

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use crate::util::AtomicOption;

/// Monotonic epoch for the discovery cache's coarse last-access timestamps,
/// initialized on first use. `now_secs` reads whole seconds elapsed since.
static CACHE_EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Seconds since [`CACHE_EPOCH`]. Coarse (whole seconds) — ample resolution for
/// a TTL measured in minutes, and cheap to stash in an `AtomicU64`.
fn now_secs() -> u64 {
    CACHE_EPOCH.elapsed().as_secs()
}

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
    /// CRD list. A `Mutex` (not the lock-free `AtomicOption`) because
    /// `merge_crd` is a read-modify-write: the lock-free peek can return a
    /// SPURIOUS `None` under reader contention, which the old merge mapped
    /// to `unwrap_or_default()` — replacing the ENTIRE cached list with a
    /// single element. The `Mutex` makes the RMW atomic (no lost updates,
    /// no spurious empty); readers still clone the inner `Arc` out under
    /// the (rarely contended) lock, so a holder keeps its snapshot alive
    /// lock-free after the read.
    pub crds: std::sync::Mutex<Option<Arc<Vec<CachedCrd>>>>,
    /// Coarse last-access time (seconds since [`CACHE_EPOCH`]), bumped on every
    /// read and write. A [`DiscoveryCache::sweep_stale`] pass evicts entries
    /// idle past the TTL, so the map stays bounded by *active* contexts rather
    /// than by every [`ContextId`] ever served.
    last_access: AtomicU64,
}

/// Lock the CRD list, recovering from a poisoned lock (the guarded data is
/// a plain `Option<Arc<Vec<..>>>` — a panic while held leaves it in a valid
/// state, so poisoning carries no torn invariant to honor).
fn lock_crds(
    m: &std::sync::Mutex<Option<Arc<Vec<CachedCrd>>>>,
) -> std::sync::MutexGuard<'_, Option<Arc<Vec<CachedCrd>>>> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl PerContext {
    /// A new entry stamped as accessed *now*, so a sweep that races creation
    /// can't evict it in the window before its first read/write `touch`.
    fn fresh() -> Self {
        Self { last_access: AtomicU64::new(now_secs()), ..Default::default() }
    }

    /// Mark the entry used now — keeps it out of the next TTL sweep.
    fn touch(&self) {
        self.last_access.store(now_secs(), Ordering::Relaxed);
    }
}

/// Daemon-wide cache of discovery data keyed by [`ContextId`] (server_url +
/// credential fingerprint, so two contexts aliased to the same cluster + creds
/// share one entry).
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

    /// Return (or lazily create) the per-context entry, stamping it accessed.
    /// The returned Arc lets the caller perform multiple field updates without
    /// re-taking the shard lock. Use it inline and drop it — don't hold it
    /// across a TTL window: a concurrent [`sweep_stale`] could detach it from
    /// the map (the Arc stays valid, but later writes through it become
    /// invisible to fresh lookups, which re-create the entry).
    fn entry_for(&self, ctx: &ContextId) -> Arc<PerContext> {
        let entry = if let Some(r) = self.entries.get(ctx) {
            r.clone()
        } else {
            self.entries
                .entry(ctx.clone())
                .or_insert_with(|| Arc::new(PerContext::fresh()))
                .clone()
        };
        entry.touch();
        entry
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
        *lock_crds(&self.entry_for(&ctx).crds) = Some(Arc::new(crds));
    }

    /// Merge ONE freshly-resolved CRD into the cached list (replace by
    /// group+plural, else append) — the on-demand resolution path shares
    /// its knowledge so the next subscribe hits warm. Atomic swap like
    /// `set_crds`; a concurrent bulk refresh simply wins (it contains
    /// this CRD anyway).
    pub fn merge_crd(&self, ctx: ContextId, crd: CachedCrd) {
        let entry = self.entry_for(&ctx);
        // Whole read-modify-write under ONE lock: no concurrent merge can
        // interleave (lost update), and the read can't spuriously see an
        // empty list and clobber it.
        let mut guard = lock_crds(&entry.crds);
        let mut crds: Vec<CachedCrd> = guard.as_ref().map(|arc| (**arc).clone()).unwrap_or_default();
        match crds
            .iter_mut()
            .find(|c| c.gvr.group == crd.gvr.group && c.gvr.plural == crd.gvr.plural)
        {
            Some(slot) => *slot = crd,
            None => crds.push(crd),
        }
        *guard = Some(Arc::new(crds));
    }

    /// Read the cached namespace list (cloned), if any.
    pub fn namespaces(&self, ctx: &ContextId) -> Option<Vec<String>> {
        let entry = self.entries.get(ctx)?;
        entry.touch();
        entry.namespaces.load_cloned().map(|arc| (*arc).clone())
    }

    /// Read the cached CRD list (cloned), if any.
    pub fn crds(&self, ctx: &ContextId) -> Option<Vec<CachedCrd>> {
        let entry = self.entries.get(ctx)?;
        entry.touch();
        // Clone the Arc out under the lock, then deep-clone lock-free so the
        // guard (and the DashMap ref) release before the Vec clone.
        let arc = lock_crds(&entry.crds).clone()?;
        Some((*arc).clone())
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
        entry.touch();
        // Clone the Arc out under the lock, then search lock-free.
        let crds = lock_crds(&entry.crds).clone()?;
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

    /// Evict entries idle (no read or write) for at least `ttl`. Returns the
    /// number removed. Active contexts — those a session is still reading or a
    /// poller is refreshing — keep their `last_access` fresh and survive; only
    /// abandoned ones are reclaimed. Eviction is always safe: a later access
    /// simply re-fetches and re-inserts. This is what bounds the otherwise
    /// daemon-lifetime map for a long-lived, many-cluster daemon.
    pub fn sweep_stale(&self, ttl: Duration) -> usize {
        self.sweep_older_than(now_secs().saturating_sub(ttl.as_secs()))
    }

    /// Eviction mechanism for [`sweep_stale`], split from the `now_secs` policy
    /// so it can be exercised with a deterministic cutoff in tests. Removes
    /// every entry whose `last_access` is at or before `cutoff_secs`.
    fn sweep_older_than(&self, cutoff_secs: u64) -> usize {
        let before = self.entries.len();
        self.entries
            .retain(|_, entry| entry.last_access.load(Ordering::Relaxed) > cutoff_secs);
        before - self.entries.len()
    }
}

/// Convert cached namespace names to unified ResourceRow format.
pub fn cached_namespaces_to_rows(names: &[String]) -> Vec<crate::kube::resources::row::ResourceRow> {
    names
        .iter()
        .map(|name| {
            let cells: Vec<CellValue> = vec![
                CellValue::Text(name.clone()),
                CellValue::Status { text: "Active".to_string(), health: RowHealth::Normal },
                CellValue::Age(None), // no age from cache
            ];            crate::kube::resources::row::ResourceRow {
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
                    crate::kube::protocol::Namespace::Named(name.clone()),
                )),
                cells,
                ..Default::default()
            }
        })
        .collect()
}

#[cfg(test)]
#[path = "../tests/kube/cache_discovery_ttl.rs"]
mod discovery_ttl_tests;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/kube/cache.rs"]
mod tests;
