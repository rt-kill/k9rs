//! Shared row data sources — the DATA half of the scope/view split.
//!
//! One [`RowStore`] per live subscription. The store holds protocol truth
//! (rows in **wire order**, headers, the stream-epoch floor) plus
//! data-keyed annotations (marks, change-flash) — and nothing
//! presentational. Everything the user actually sees is derived per frame
//! by [`derive_view`], a pure function over (store contents, metrics, an
//! element's predicate chain + sort + draft). Nav elements hold
//! [`RowSource`] handles — a store `Arc` plus the accumulated client-side
//! predicate chain — handed down at construction. Grep-on-grep is
//! literally [`RowSource::narrowed`]: the child's source is the parent's
//! source plus one predicate.
//!
//! # Ownership / locking
//!
//! The `Mutex` here is aliasing machinery, not synchronization. Every
//! access happens on the session task: the event loop applies stream
//! events (each event CARRIES its destination store `Arc` — bridges never
//! touch a store), key handlers toggle marks, and the renderer reads
//! under [`RowStore::with_read`] on the same task. `try_lock().expect()`
//! encodes the convention: contention is a bug to surface, never a wait.
//!
//! # Metrics
//!
//! Metrics are a derive-time pure overlay: wire rows keep their empty
//! metric cells in the store forever; [`MetricsLens::effective`] resolves
//! the displayed value while materializing. Sorting and grepping therefore
//! see overlaid values, and a delta upsert can never blank CPU/MEM (the
//! old mutate-rows-then-re-overlay dance is gone).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::app::table::TableDataState;
use crate::app::types::DeltaTracker;
use crate::kube::overlay::ColumnRenderRules;
use crate::kube::protocol::{
    MetricsUsage, NodeName, ObjectKey, ResourceId, RowChange, TableBaseline, TableDelta,
};
use crate::kube::resources::row::{CellValue, QuantityUnit, ResourceRow, RowHealth};
use crate::kube::resources::KubeResource;
use crate::util::SearchPattern;

/// Stable identity of a row — the key marks, flash, and removes are
/// addressed by.
pub fn row_key(row: &ResourceRow) -> ObjectKey {
    ObjectKey::new(row.namespace.clone().unwrap_or_default(), row.name.clone())
}

/// `row_key(row) == *key` without the allocation (per-row on presence
/// scans and identity lookups).
pub(crate) fn row_matches_key(row: &ResourceRow, key: &ObjectKey) -> bool {
    row.name == key.name && row.namespace.as_deref().unwrap_or("") == key.namespace
}

/// Whether two rows have the same identity (ns + name), allocation-free.
fn rows_same_identity(a: &ResourceRow, b: &ResourceRow) -> bool {
    a.name == b.name && a.namespace == b.namespace
}

// ---------------------------------------------------------------------------
// RowPredicate — one client-side refinement
// ---------------------------------------------------------------------------

/// A client-side row predicate — the payload of a `RowFilter` element.
/// Server-side filters (labels / field selectors / owner chains) never
/// appear here: they are query-spec data on the subscription-owning
/// element, applied by the daemon.
#[derive(Debug)]
pub enum RowPredicate {
    /// Text grep across all columns (`/`).
    Grep(crate::app::nav::CompiledGrep),
    /// Grep restricted to one DATA column (`~`). `header` is captured at
    /// creation so the crumb reads `~STATUS:x`, not a raw index.
    ColumnGrep {
        pattern: crate::app::nav::CompiledGrep,
        col: usize,
        header: String,
    },
    /// Typed health predicate: keep rows whose `RowHealth != Normal`.
    Fault,
}

impl RowPredicate {
    /// Whether `row` passes. `cells` are the row's *effective* (metrics-
    /// overlaid) display strings, one per data column — greps match what
    /// the user sees.
    pub fn matches(&self, row: &ResourceRow, cells: &[String]) -> bool {
        match self {
            RowPredicate::Grep(g) => cells.iter().any(|c| g.pattern().is_match(c)),
            RowPredicate::ColumnGrep { pattern, col, .. } => {
                cells.get(*col).is_some_and(|c| pattern.pattern().is_match(c))
            }
            RowPredicate::Fault => !matches!(row.health, RowHealth::Normal),
        }
    }
}

// ---------------------------------------------------------------------------
// RowStore — one live data source, shared by Arc
// ---------------------------------------------------------------------------

/// What a stream event asks the store to do. Client-internal; the wire
/// types pass through untouched.
pub enum StorePayload {
    /// Replaces everything (rows + headers). "A stream is Baseline then
    /// Deltas; Baseline may recur and REPLACES."
    Baseline(TableBaseline),
    /// Idempotent incremental change (upsert replaces-or-inserts; remove
    /// of an absent key is a no-op).
    Delta(TableDelta),
    /// The subscription failed; the UI shows the message instead of rows.
    Failed(String),
    /// The daemon's watch stopped feeding this store (cluster-side). Rows
    /// stay resident and remain the last known truth — they just stopped
    /// tracking the cluster, and the view says so instead of implying they
    /// are current.
    Stale(String),
    /// The watch resumed with no row change to report. `Baseline` and
    /// `Delta` clear staleness implicitly (data arriving IS liveness); this
    /// is the quiet recovery, where nothing changed while the watch was
    /// down and nothing else would clear it.
    Live,
}

/// One live data source. Owned strongly by the subscription-owning
/// element; shared (via `Arc`) with every derived element's [`RowSource`]
/// and, transiently, with queued events still carrying it. Rows are pure
/// protocol truth: wire order, never sorted, never overlaid.
#[derive(Debug)]
pub struct RowStore {
    /// Resource plural, pinned at construction — resolves overlay column
    /// rules when headers (re)arrive.
    plural: String,
    /// These rows are seeded in-process rather than delivered by the
    /// daemon. Lives on the STORE, not on the query, so every derived view
    /// (`/` filter, projection) inherits it without threading a flag: what
    /// is client-owned is the row set, and anything reading that row set is
    /// equally independent of the connection.
    client_owned: bool,
    /// Lock-free data version: one `Acquire` load per frame decides
    /// cache-hit vs re-derive. Bumped by every content mutation.
    generation: AtomicU64,
    inner: Mutex<RowStoreInner>,
}

/// The store's contents. Reads happen through [`RowStore::with_read`];
/// all mutation goes through `RowStore` methods so the generation bump
/// can never be bypassed.
#[derive(Debug)]
pub struct RowStoreInner {
    /// Rows in wire order. Ordering is a VIEW concern ([`derive_view`]).
    pub rows: Vec<ResourceRow>,
    /// Column headers from the last Baseline (headers ride only baselines).
    pub headers: Vec<String>,
    /// Pre-resolved overlay coloring rules, parallel to `headers`.
    pub column_rules: Vec<ColumnRenderRules>,
    /// Initializing / Ready / Stale / Failed lifecycle.
    pub state: TableDataState,
    /// Stream-epoch floor: events with `epoch < floor` are stale (from a
    /// superseded stream targeting this same store) and are dropped. An
    /// accepted Baseline sets the floor; [`RowStore::expect_epoch`] raises
    /// it eagerly the moment a successor stream exists.
    epoch_floor: u64,
    /// Marked rows, keyed by identity. DATA, not view state: marks are
    /// pruned atomically with the row mutation that invalidates them, and
    /// batch operations read identities. Shared by every element over
    /// this store (marks survive filter push/pop, as they always have).
    marked: HashSet<ObjectKey>,
    /// Change-flash record (which rows recently changed). DATA: two
    /// elements over one store both deserve honest flashes. Presentation
    /// (the highlight itself) is the view's job.
    pub flash: DeltaTracker,
}

impl RowStore {
    /// A fresh store in `Initializing` state. `plural` pins overlay
    /// column-rule resolution for this resource.
    pub fn new(plural: impl Into<String>) -> Arc<Self> {
        Self::build(plural, false)
    }

    /// A store whose rows the CLIENT seeds (the kubeconfig's contexts).
    /// Marks the row set as connection-independent — see `client_owned`.
    pub fn client(plural: impl Into<String>) -> Arc<Self> {
        Self::build(plural, true)
    }

    /// Whether these rows are seeded in-process. Read by the render layer
    /// through `Element::liveness`: a dead daemon says nothing about rows
    /// that never came from it.
    pub fn is_client_owned(&self) -> bool {
        self.client_owned
    }

    fn build(plural: impl Into<String>, client_owned: bool) -> Arc<Self> {
        Arc::new(Self {
            plural: plural.into(),
            client_owned,
            generation: AtomicU64::new(0),
            inner: Mutex::new(RowStoreInner {
                rows: Vec::new(),
                headers: Vec::new(),
                column_rules: Vec::new(),
                state: TableDataState::Initializing,
                epoch_floor: 0,
                marked: HashSet::new(),
                flash: DeltaTracker::new(),
            }),
        })
    }

    /// Single-writer convention, encoded: the session task is the only
    /// toucher, so the lock is never contended. A wait here would mean a
    /// second writer exists — surface it, don't hide it.
    fn lock(&self) -> MutexGuard<'_, RowStoreInner> {
        self.inner
            .try_lock()
            .expect("RowStore lock contended — single-writer (session task) convention violated")
    }

    /// Current data version. One atomic load; no lock.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn bump(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// Read access for derivation / render / selection. The guard never
    /// escapes; the closure runs on the same task as every writer.
    pub fn with_read<R>(&self, f: impl FnOnce(&RowStoreInner) -> R) -> R {
        f(&self.lock())
    }

    /// Raise the epoch floor. Called by the subscribe path BEFORE the new
    /// stream's handle is returned — from that instant, every event of a
    /// superseded stream is rejected, closing the "stale delta flickers
    /// back after Ctrl-R" window.
    pub fn expect_epoch(&self, epoch: u64) {
        let mut inner = self.lock();
        inner.epoch_floor = inner.epoch_floor.max(epoch);
    }

    /// Flag the store as (re)initializing WITHOUT dropping rows — the
    /// reconnect-revive counterpart to [`RowStore::clear`]. Rows stay
    /// resident so the recovery Baseline can replace them in place (flash /
    /// mark continuity), but `data_state()` now honestly reports
    /// `Initializing`, so the view renders a "Connecting…" screen instead of
    /// the stale, no-longer-live rows. Generation is deliberately NOT bumped:
    /// no row DATA changed, the render gate keys off `state` (not the view),
    /// and the recovery Baseline bumps generation when it lands.
    pub fn mark_reinitializing(&self) {
        self.lock().state = TableDataState::Initializing;
    }

    /// Flag resident rows as no longer fed — the counterpart to
    /// [`RowStore::mark_reinitializing`] for rows brought back from a
    /// previous visit to a context. Deliberately NOT epoch-gated: this is a
    /// LOCAL transition the client is making about its own cache, not
    /// something a stream said, and the store's floor is still whatever its
    /// last baseline set. Only `Ready` goes stale — the same rule
    /// `StorePayload::Stale` follows.
    pub fn mark_stale(&self, reason: impl Into<String>) {
        let mut inner = self.lock();
        if matches!(inner.state, TableDataState::Ready) {
            inner.state = TableDataState::Stale(reason.into());
        }
    }

    /// Sole stream-data write path. Epoch-gated; see [`StorePayload`] for
    /// the per-variant semantics.
    pub fn apply(&self, epoch: u64, payload: StorePayload) {
        let mut inner = self.lock();
        if epoch < inner.epoch_floor {
            return; // stale stream: superseded before this event landed
        }
        match payload {
            StorePayload::Baseline(b) => {
                inner.epoch_floor = epoch;
                if inner.headers != b.headers {
                    inner.column_rules =
                        crate::kube::overlay::build_column_rules(&b.headers, &self.plural);
                    inner.headers = b.headers;
                }
                // Flash BEFORE replacing rows: rebaseline compares against
                // surviving hashes (cross-recovery/refresh continuity).
                inner.flash.rebaseline(&b.rows);
                if !inner.marked.is_empty() {
                    let present: HashSet<ObjectKey> = b.rows.iter().map(row_key).collect();
                    inner.marked.retain(|k| present.contains(k));
                }
                inner.rows = b.rows;
                inner.state = TableDataState::Ready;
            }
            StorePayload::Delta(d) => {
                // Plan-then-apply over a transient index (cannot drift —
                // it lives only inside this call). Batches carry at most
                // one change per key (daemon invariant), so two phases
                // suffice; application is idempotent by construction.
                let inner = &mut *inner;
                // Steady-state batches are tiny (coalesced per key), so a
                // full O(rows) ObjectKey→index HashMap — two String allocs
                // per row — is wasteful for a handful of changes. Build it
                // only for large batches (relist, big churn); otherwise
                // locate each change by allocation-free linear scan.
                const INDEX_THRESHOLD: usize = 16;
                let index: Option<HashMap<ObjectKey, usize>> = if d.changes.len() > INDEX_THRESHOLD {
                    Some(inner.rows.iter().enumerate().map(|(i, r)| (row_key(r), i)).collect())
                } else {
                    None
                };
                let locate_upsert = |rows: &[ResourceRow], row: &ResourceRow| -> Option<usize> {
                    match &index {
                        Some(m) => m.get(&row_key(row)).copied(),
                        None => rows.iter().position(|r| rows_same_identity(r, row)),
                    }
                };
                let locate_key = |rows: &[ResourceRow], key: &ObjectKey| -> Option<usize> {
                    match &index {
                        Some(m) => m.get(key).copied(),
                        None => rows.iter().position(|r| row_matches_key(r, key)),
                    }
                };
                let mut replace: Vec<(usize, ResourceRow)> = Vec::new();
                let mut append: Vec<ResourceRow> = Vec::new();
                let mut dead: Vec<bool> = vec![false; inner.rows.len()];
                for change in &d.changes {
                    match change {
                        RowChange::Upsert(row) => match locate_upsert(&inner.rows, row) {
                            Some(i) => replace.push((i, row.clone())),
                            None => append.push(row.clone()),
                        },
                        RowChange::Remove(key) => {
                            if let Some(i) = locate_key(&inner.rows, key) {
                                dead[i] = true;
                                // Marks die with their row — atomic with
                                // the removal that invalidates them.
                                inner.marked.remove(key);
                            }
                        }
                    }
                }
                for (i, row) in replace {
                    inner.rows[i] = row;
                }
                if dead.iter().any(|&d| d) {
                    let mut keep = dead.iter().map(|&d| !d);
                    inner.rows.retain(|_| keep.next().unwrap_or(true));
                }
                inner.rows.extend(append);
                inner.flash.apply_changes(&d.changes);
                // Data arriving IS liveness: a delta can only come from a
                // watch that is feeding again.
                if matches!(inner.state, TableDataState::Stale(_)) {
                    inner.state = TableDataState::Ready;
                }
            }
            StorePayload::Failed(msg) => {
                inner.state = TableDataState::Failed(msg);
            }
            StorePayload::Stale(reason) => {
                // Only `Ready` can go stale. A store still `Initializing`
                // has no rows to freeze (it is already showing a loading
                // screen), and `Failed` is terminal — downgrading a real
                // error to "stale" would hide it behind a softer message.
                if matches!(inner.state, TableDataState::Ready) {
                    inner.state = TableDataState::Stale(reason);
                }
            }
            StorePayload::Live => {
                if matches!(inner.state, TableDataState::Stale(_)) {
                    inner.state = TableDataState::Ready;
                }
            }
        }
        drop(inner);
        self.bump();
    }

    /// Back to `Initializing` (Ctrl-R refresh, seeding a reused store).
    /// Rows drop; marks and flash hashes both deliberately SURVIVE. Flash
    /// hashes are memory of the past (rows that changed across the refresh
    /// still flash after the recovery baseline); marks are intent for the
    /// future (the recovery baseline re-anchors them, retaining only keys
    /// that return — the same prune as any baseline). While `Initializing`
    /// the rows are UNKNOWN, not absent: anything that OPERATES on marks
    /// must intersect with present rows at use time
    /// (`get_marked_resource_infos` does), so the window is unobservable
    /// by batch dispatch.
    pub fn clear(&self) {
        let mut inner = self.lock();
        inner.rows.clear();
        inner.state = TableDataState::Initializing;
        drop(inner);
        self.bump();
    }

    // --- Marks (data-keyed; ops are element-mediated) ---------------------

    /// Toggle a mark; `Some(now_marked)`, or `None` when the key names no
    /// present row — a keypress through a stale coalesced frame (the row
    /// vanished between paint and key). Refusing the insert here keeps
    /// `marked ⊆ rows` an invariant of the write paths: a ghost mark
    /// could never be pruned by deltas (the daemon won't re-Remove an
    /// absent key) and would wedge select mode until the next baseline.
    /// No generation bump: marks are read fresh each frame, never baked
    /// into the derived view (parity with the fused table).
    pub fn toggle_mark(&self, key: &ObjectKey) -> Option<bool> {
        let mut inner = self.lock();
        if inner.marked.remove(key) {
            Some(false)
        } else if inner.rows.iter().any(|r| row_matches_key(r, key)) {
            inner.marked.insert(key.clone());
            Some(true)
        } else {
            None
        }
    }

    /// Mark every key (span-mark: the element computes the span from its
    /// own derived order and hands us identities). Keys naming no present
    /// row are dropped — same stale-frame guard as [`Self::toggle_mark`].
    pub fn mark_keys(&self, keys: impl IntoIterator<Item = ObjectKey>) {
        let mut inner = self.lock();
        let inner = &mut *inner;
        let present: HashSet<ObjectKey> = inner.rows.iter().map(row_key).collect();
        inner.marked.extend(keys.into_iter().filter(|k| present.contains(k)));
    }

    /// Unmark every key (span-unmark of a contiguous marked block).
    pub fn unmark_keys<'k>(&self, keys: impl IntoIterator<Item = &'k ObjectKey>) {
        let mut inner = self.lock();
        for key in keys {
            inner.marked.remove(key);
        }
    }

    pub fn clear_marks(&self) {
        self.lock().marked.clear();
    }

    pub fn has_marks(&self) -> bool {
        !self.lock().marked.is_empty()
    }

    /// Marked identities, for batch operations.
    pub fn marked_keys(&self) -> Vec<ObjectKey> {
        self.lock().marked.iter().cloned().collect()
    }

    // --- Flash maintenance -------------------------------------------------

    /// Drop flash entries older than `max_age`; returns whether anything
    /// expired (a repaint signal). Called by the tick for the TOP store
    /// only — covered stores expire lazily on their next tick-as-top.
    pub fn expire_flash(&self, max_age: std::time::Duration) -> bool {
        self.lock().flash.expire(max_age)
    }
}

impl RowStoreInner {
    pub fn is_marked(&self, key: &ObjectKey) -> bool {
        self.marked.contains(key)
    }

    pub fn marked_count(&self) -> usize {
        self.marked.len()
    }

    /// The marked set, for render-time lookups (borrow lives inside the
    /// caller's `with_read` closure).
    pub fn marked(&self) -> &HashSet<ObjectKey> {
        &self.marked
    }
}

// ---------------------------------------------------------------------------
// RowSource — the handle handed down at derivation
// ---------------------------------------------------------------------------

/// A composable row source: backing store + the accumulated client-side
/// predicate chain + the metrics binding (if the resource has one). This
/// is the "handed reference" of the nav model — a `RowFilter` element's
/// OUTPUT (what it hands the next child) is its own input narrowed by its
/// predicate. Children hold clones; references only ever point backward,
/// so strong Arcs are leak-free by construction.
#[derive(Debug, Clone)]
pub struct RowSource {
    store: Arc<RowStore>,
    predicates: Vec<Arc<RowPredicate>>,
    metrics: Option<MetricsBinding>,
}

impl RowSource {
    /// An unfiltered source over `store`. `metrics` is resolved once, at
    /// query construction, and inherited by every narrowed child.
    pub fn new(store: Arc<RowStore>, metrics: Option<MetricsBinding>) -> Self {
        Self { store, predicates: Vec::new(), metrics }
    }

    /// The entire derivation handoff: this source plus one predicate.
    pub fn narrowed(&self, predicate: Arc<RowPredicate>) -> Self {
        let mut child = self.clone();
        child.predicates.push(predicate);
        child
    }

    pub fn store(&self) -> &Arc<RowStore> {
        &self.store
    }

    /// Inherited from the store this source (and every narrowing of it)
    /// reads.
    pub fn is_client_owned(&self) -> bool {
        self.store.is_client_owned()
    }

    pub fn predicates(&self) -> &[Arc<RowPredicate>] {
        &self.predicates
    }

    pub fn metrics(&self) -> Option<&MetricsBinding> {
        self.metrics.as_ref()
    }

    /// Data version of the backing store (metrics version is a separate
    /// cache-key component — see the element layer's derive key).
    pub fn generation(&self) -> u64 {
        self.store.generation()
    }
}

// ---------------------------------------------------------------------------
// MetricsHub — app-owned usage data, overlaid at derive time
// ---------------------------------------------------------------------------

/// Latest metrics-server usage, replaced wholesale per poll. `None` maps
/// mean "never polled" — distinct from "polled, currently empty", which
/// blanks absent nodes to n/a (the poller answered; missing means gone).
#[derive(Debug, Default)]
pub struct MetricsHub {
    version: AtomicU64,
    pods: Mutex<Option<HashMap<ObjectKey, MetricsUsage>>>,
    nodes: Mutex<Option<HashMap<NodeName, MetricsUsage>>>,
}

impl MetricsHub {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Version for derive-cache keys: bumped on every data change.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    pub fn set_pods(&self, usage: HashMap<ObjectKey, MetricsUsage>) {
        let mut slot = self.pods.try_lock().expect("MetricsHub: single-writer convention violated");
        // Skip the version bump when the map is unchanged: the bump
        // invalidates every pods-table view memo, forcing a full
        // O(rows×cols) re-derive on the next paint. A 30s metrics poll on
        // an idle cluster (or one with no metrics-server) would otherwise
        // re-derive a 10k-pod table for nothing.
        if slot.as_ref() == Some(&usage) {
            return;
        }
        *slot = Some(usage);
        self.version.fetch_add(1, Ordering::Release);
    }

    pub fn set_nodes(&self, usage: HashMap<NodeName, MetricsUsage>) {
        let mut slot = self.nodes.try_lock().expect("MetricsHub: single-writer convention violated");
        if slot.as_ref() == Some(&usage) {
            return;
        }
        *slot = Some(usage);
        self.version.fetch_add(1, Ordering::Release);
    }

    /// Context switch: all usage is stale, back to never-polled.
    pub fn clear(&self) {
        *self.pods.try_lock().expect("MetricsHub: single-writer convention violated") = None;
        *self.nodes.try_lock().expect("MetricsHub: single-writer convention violated") = None;
        self.version.fetch_add(1, Ordering::Release);
    }
}

/// Which metric columns a resource's rows carry, resolved once from the
/// registry's typed column metadata at binding construction.
#[derive(Debug, Clone, Copy)]
struct PodCols {
    cpu: Option<usize>,
    mem: Option<usize>,
    pct_cpu_r: Option<usize>,
    pct_cpu_l: Option<usize>,
    pct_mem_r: Option<usize>,
    pct_mem_l: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct NodeCols {
    cpu: Option<usize>,
    cpu_alloc: Option<usize>,
    cpu_pct: Option<usize>,
    mem: Option<usize>,
    mem_alloc: Option<usize>,
    mem_pct: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
enum MetricsCols {
    Pod(PodCols),
    Node(NodeCols),
}

/// A resource's connection to the [`MetricsHub`]: which hub map feeds it
/// and which data columns are metric columns. Rides the [`RowSource`],
/// assigned at query construction, inherited by narrowed children.
#[derive(Debug, Clone)]
pub struct MetricsBinding {
    hub: Arc<MetricsHub>,
    cols: MetricsCols,
}

impl MetricsBinding {
    /// Resolve the binding for a resource, if it has metrics columns.
    /// (Only built-ins do; the registry's typed `ColumnDef.metrics` tags
    /// are the single source for column positions.)
    pub fn for_rid(rid: &ResourceId, hub: &Arc<MetricsHub>) -> Option<Self> {
        use crate::kube::resource_def::{MetricsColumn as MC, MetricsKind};
        let def = crate::kube::resource_defs::REGISTRY.by_kind(rid.built_in_kind()?);
        let kind = def.metrics_kind()?;
        let col = |tag: MC| def.column_defs().iter().position(|c| c.metrics == Some(tag));
        let cols = match kind {
            MetricsKind::Pod => MetricsCols::Pod(PodCols {
                cpu: col(MC::Cpu),
                mem: col(MC::Mem),
                pct_cpu_r: col(MC::CpuPercentRequest),
                pct_cpu_l: col(MC::CpuPercentLimit),
                pct_mem_r: col(MC::MemPercentRequest),
                pct_mem_l: col(MC::MemPercentLimit),
            }),
            MetricsKind::Node => MetricsCols::Node(NodeCols {
                cpu: col(MC::Cpu),
                cpu_alloc: col(MC::CpuAlloc),
                cpu_pct: col(MC::CpuPercent),
                mem: col(MC::Mem),
                mem_alloc: col(MC::MemAlloc),
                mem_pct: col(MC::MemPercent),
            }),
        };
        Some(Self { hub: Arc::clone(hub), cols })
    }

    /// Hub data version (derive-cache key component).
    pub fn version(&self) -> u64 {
        self.hub.version()
    }

    /// Lock the relevant hub map for one derive. Same single-task
    /// convention as [`RowStore`]: never contended, never held across
    /// anything that could re-enter.
    pub fn lens(&self) -> MetricsLens<'_> {
        match self.cols {
            MetricsCols::Pod(cols) => MetricsLens(LensInner::Pods {
                map: self.hub.pods.try_lock().expect("MetricsHub: lens while writing"),
                cols,
            }),
            MetricsCols::Node(cols) => MetricsLens(LensInner::Nodes {
                map: self.hub.nodes.try_lock().expect("MetricsHub: lens while writing"),
                cols,
            }),
        }
    }
}

/// A locked, column-resolved view of the hub for one derive pass.
pub struct MetricsLens<'a>(LensInner<'a>);

enum LensInner<'a> {
    Pods {
        map: MutexGuard<'a, Option<HashMap<ObjectKey, MetricsUsage>>>,
        cols: PodCols,
    },
    Nodes {
        map: MutexGuard<'a, Option<HashMap<NodeName, MetricsUsage>>>,
        cols: NodeCols,
    },
}

impl MetricsLens<'_> {
    /// The effective value for `(row, data column)`, or `None` to use the
    /// stored cell. Pure — this is the entire metrics overlay.
    fn effective(&self, row: &ResourceRow, col: usize) -> Option<CellValue> {
        fn pct_of(current: u64, limit: Option<u64>) -> CellValue {
            CellValue::Percentage(
                limit.filter(|&l| l > 0).map(|l| current.saturating_mul(100) / l),
            )
        }
        match &self.0 {
            LensInner::Pods { map, cols } => {
                let usage = map.as_ref()?.get(&row_key(row))?;
                if Some(col) == cols.cpu {
                    return Some(CellValue::Quantity { value: usage.cpu_milli, unit: QuantityUnit::Millicores });
                }
                if Some(col) == cols.mem {
                    return Some(CellValue::Quantity { value: usage.mem_bytes, unit: QuantityUnit::Bytes });
                }
                if Some(col) == cols.pct_cpu_r { return Some(pct_of(usage.cpu_milli, row.cpu_request)); }
                if Some(col) == cols.pct_cpu_l { return Some(pct_of(usage.cpu_milli, row.cpu_limit)); }
                if Some(col) == cols.pct_mem_r { return Some(pct_of(usage.mem_bytes, row.mem_request)); }
                if Some(col) == cols.pct_mem_l { return Some(pct_of(usage.mem_bytes, row.mem_limit)); }
                None
            }
            LensInner::Nodes { map, cols } => {
                let map = map.as_ref()?; // never polled → stored cells
                match map.get(row.name.as_str()) {
                    Some(usage) => {
                        if Some(col) == cols.cpu {
                            return Some(CellValue::Quantity { value: usage.cpu_milli, unit: QuantityUnit::Millicores });
                        }
                        if Some(col) == cols.mem {
                            return Some(CellValue::Quantity { value: usage.mem_bytes, unit: QuantityUnit::Bytes });
                        }
                        let alloc_of = |ac: Option<usize>| {
                            ac.and_then(|c| row.cells.get(c)).and_then(|c| c.quantity_value()).unwrap_or(0)
                        };
                        if Some(col) == cols.cpu_pct {
                            let alloc = alloc_of(cols.cpu_alloc);
                            return Some(CellValue::Percentage(
                                (alloc > 0).then(|| usage.cpu_milli.saturating_mul(100) / alloc),
                            ));
                        }
                        if Some(col) == cols.mem_pct {
                            let alloc = alloc_of(cols.mem_alloc);
                            return Some(CellValue::Percentage(
                                (alloc > 0).then(|| usage.mem_bytes.saturating_mul(100) / alloc),
                            ));
                        }
                        None
                    }
                    None => {
                        // Polled, but this node absent this cycle: show n/a
                        // rather than a frozen stale value.
                        if Some(col) == cols.cpu || Some(col) == cols.mem {
                            return Some(CellValue::Placeholder);
                        }
                        if Some(col) == cols.cpu_pct || Some(col) == cols.mem_pct {
                            return Some(CellValue::Percentage(None));
                        }
                        None
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// derive_view — the materialization (the "view" of the nav model)
// ---------------------------------------------------------------------------

/// Sort order for a derive: DATA column index + direction. Element state
/// (it survives cover/reveal); the derive just applies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortSpec {
    pub col: usize,
    pub ascending: bool,
}

impl Default for SortSpec {
    fn default() -> Self {
        Self { col: 0, ascending: true }
    }
}

/// Everything a derive needs beyond the store contents. All borrowed from
/// the element — the derive is pure over (store, metrics, this).
pub struct DeriveSpec<'a> {
    pub predicates: &'a [Arc<RowPredicate>],
    /// Uncommitted filter-input text, compiled by the caller (changes per
    /// keystroke; caching would churn).
    pub draft: Option<&'a SearchPattern>,
    pub sort: SortSpec,
    /// DATA indices of the visible columns, in display order.
    pub visible_cols: &'a [usize],
    /// Headers for the visible columns (parallel to `visible_cols`) —
    /// seeds column widths.
    pub headers: &'a [&'a str],
    /// Per-column width ceiling, parallel to `headers`. Resolved by
    /// `ColumnPolicy::max_width_for` (the column's own declaration, else the
    /// global `ui.maxColumnWidth`) — a single number for every column of
    /// every resource was never a policy, just a backstop.
    pub max_col_widths: &'a [u16],
}

/// The materialized view: parallel arrays of visible-column display
/// strings, health tags, per-cell styles, and identity keys, in final
/// screen order, plus ready column widths. Ephemeral by design — always
/// fully re-derivable, memoizable as a pure cache, droppable at any time.
#[derive(Debug)]
pub struct PreparedView {
    pub rows: Vec<Vec<String>>,
    pub health: Vec<RowHealth>,
    /// `cell_style[row][col]`: `Some(h)` if that cell has its own coloring
    /// (overlay rules or `CellValue::Status`), `None` inherits row style.
    pub cell_style: Vec<Vec<Option<RowHealth>>>,
    /// Row identities in screen order — the bridge between a screen
    /// position (cursor) and data identity (marks, drills, batch ops).
    pub keys: Vec<ObjectKey>,
    /// Natural per-column widths (header-seeded, cell-expanded, padded,
    /// clamped to `max_col_width`).
    pub col_widths: Vec<u16>,
    /// Total rows in the store (pre-filter), for the `filtered/total`
    /// count display.
    pub total_rows: usize,
    /// DATA indices of the visible columns, in display order — the bridge
    /// between a visible-column cursor and cell data (sort by column,
    /// column-restricted grep).
    pub visible_cols: Vec<usize>,
    /// Headers of the visible columns (parallel to `visible_cols`).
    pub headers: Vec<String>,
}

/// Materialize a view: overlay metrics, filter by the predicate chain +
/// draft, sort by the effective sort-column value, project visible
/// columns, style cells. Pure over its inputs; the element memoizes the
/// result keyed by (store generation, metrics version, sort, draft,
/// column level, width clamp).
///
/// Takes `rows` + `column_rules` rather than a whole [`RowStoreInner`] so
/// live projections (a derived view's projected child rows) share the
/// exact same pipeline as store-backed views.
pub fn derive_view(
    rows_in: &[ResourceRow],
    column_rules: &[ColumnRenderRules],
    metrics: Option<&MetricsLens<'_>>,
    spec: &DeriveSpec<'_>,
) -> PreparedView {
    // Steps 1–3 — the IDENTITY half (shared with `derive_key_order`, the
    // selection fallback): effective strings, filter, sort.
    let (order, mut eff) = filter_and_sort(rows_in, metrics, spec.predicates, spec.draft, spec.sort);
    let has_filter = !spec.predicates.is_empty() || spec.draft.is_some();

    // 4. Materialize the VISIBLE cells in screen order. When `eff` exists
    //    (filtered), MOVE each visible cell out of it (each row index and
    //    each visible column is used exactly once, so `mem::take` is safe
    //    and avoids re-allocating strings we already built). Otherwise
    //    stringify the visible cells directly — hidden columns are never
    //    materialized at all.
    //
    //    These two branches are EQUIVALENT because rows always carry a cell
    //    for every schema column: converters emit metric columns as
    //    `CellValue::Placeholder`, so `cells.len() == headers.len()` and no
    //    visible index exceeds `eff[i]`'s length. If a converter ever emits
    //    a SHORT row, the filtered branch would blank the out-of-range
    //    visible cells (`get_mut` → None → "") while the unfiltered branch
    //    would synthesize them via the metrics lens — so keep rows
    //    full-width (the whole derive already assumes it: sort and
    //    cell_style index `row.cells` directly).
    let rows: Vec<Vec<String>> = order
        .iter()
        .map(|&i| {
            if has_filter {
                let full = &mut eff[i];
                spec.visible_cols
                    .iter()
                    .map(|&ci| full.get_mut(ci).map(std::mem::take).unwrap_or_default())
                    .collect()
            } else {
                spec.visible_cols.iter().map(|&ci| effective_cell(&rows_in[i], ci, metrics)).collect()
            }
        })
        .collect();
    let cell_style: Vec<Vec<Option<RowHealth>>> = order
        .iter()
        .zip(rows.iter())
        .map(|(&i, row_strs)| {
            let row = &rows_in[i];
            spec.visible_cols
                .iter()
                .zip(row_strs.iter())
                .map(|(&ci, cell_str)| {
                    // 1. Overlay column rules — only for cells that exist
                    //    (a missing cell's empty placeholder must not match).
                    if let Some(rules) = column_rules.get(ci) {
                        if row.cells.get(ci).is_some() {
                            if let Some(style) = rules.evaluate(cell_str) {
                                return Some(style);
                            }
                        }
                    }
                    // 2. CellValue::Status { health } from the converter.
                    if let Some(CellValue::Status { health, .. }) = row.cells.get(ci) {
                        if *health != RowHealth::Normal {
                            return Some(*health);
                        }
                    }
                    None
                })
                .collect()
        })
        .collect();
    let health: Vec<RowHealth> = order.iter().map(|&i| rows_in[i].health).collect();
    let keys: Vec<ObjectKey> = order.iter().map(|&i| row_key(&rows_in[i])).collect();
    let col_widths = column_widths(spec.headers, &rows, spec.max_col_widths);

    PreparedView {
        rows,
        health,
        cell_style,
        keys,
        col_widths,
        total_rows: rows_in.len(),
        visible_cols: spec.visible_cols.to_vec(),
        headers: spec.headers.iter().map(|h| (*h).to_string()).collect(),
    }
}

/// The effective display value of one cell (metrics lens overlaid).
fn effective_cell(row: &ResourceRow, ci: usize, metrics: Option<&MetricsLens<'_>>) -> String {
    match metrics.and_then(|m| m.effective(row, ci)) {
        Some(v) => v.to_string(),
        None => row.cells.get(ci).map(|c| c.to_string()).unwrap_or_default(),
    }
}

/// Steps 1–3 of the derive — the IDENTITY half: effective strings (built
/// only when a filter must scan all columns; greps span the full row),
/// predicate+draft filter, sort by the EFFECTIVE value at the sort column
/// (metrics columns sort by live usage), typed comparison via
/// `CellValue::cmp`, stable (namespace, name) tiebreaker. Returns
/// (screen order, effective strings) — the strings ride along so
/// `derive_view` can move materialized cells out instead of
/// re-stringifying them.
///
/// Deliberately free of presentation inputs (visible columns, width
/// clamp, column level): filtering scans ALL columns and the sort column
/// is a DATA index, so the screen ORDER is pure over element-owned state
/// plus store contents. That purity is what lets selection resolve
/// without a painted view.
fn filter_and_sort(
    rows_in: &[ResourceRow],
    metrics: Option<&MetricsLens<'_>>,
    predicates: &[Arc<RowPredicate>],
    draft: Option<&SearchPattern>,
    sort: SortSpec,
) -> (Vec<usize>, Vec<Vec<String>>) {
    // 1. Effective strings for EVERY cell — needed ONLY when a filter must
    //    scan all columns. In the default browse state (no predicate, no
    //    draft) this whole O(rows×cols) pass is skipped.
    let has_filter = !predicates.is_empty() || draft.is_some();
    let eff: Vec<Vec<String>> = if has_filter {
        rows_in
            .iter()
            .map(|row| (0..row.cells.len()).map(|ci| effective_cell(row, ci, metrics)).collect())
            .collect()
    } else {
        Vec::new()
    };

    // 2. Filter: every committed predicate AND the uncommitted draft. With
    //    no filter, every row passes and `eff` is unused.
    let mut order: Vec<usize> = if has_filter {
        (0..rows_in.len())
            .filter(|&i| {
                let row = &rows_in[i];
                let strings = &eff[i];
                predicates.iter().all(|p| p.matches(row, strings))
                    && draft.is_none_or(|d| strings.iter().any(|c| d.is_match(c)))
            })
            .collect()
    } else {
        (0..rows_in.len()).collect()
    };

    // 3. Sort. Screen order = this order.
    {
        let empty = CellValue::Text(String::new());
        let value_at = |i: usize| -> std::borrow::Cow<'_, CellValue> {
            let row = &rows_in[i];
            match metrics.and_then(|m| m.effective(row, sort.col)) {
                Some(v) => std::borrow::Cow::Owned(v),
                None => std::borrow::Cow::Borrowed(row.cells.get(sort.col).unwrap_or(&empty)),
            }
        };
        order.sort_by(|&a, &b| {
            let primary = value_at(a).as_ref().cmp(value_at(b).as_ref());
            let primary = if sort.ascending { primary } else { primary.reverse() };
            primary.then_with(|| {
                let (ra, rb) = (&rows_in[a], &rows_in[b]);
                ra.namespace().cmp(rb.namespace()).then_with(|| ra.name().cmp(rb.name()))
            })
        });
    }
    (order, eff)
}

/// The row-identity order a derive over these inputs would produce —
/// `derive_view`'s `keys`, without materializing any presentation.
/// Selection reads fall back to this when no painted view exists (the
/// memo is dropped on cover and absent on fresh elements): since
/// presentation params never influence WHICH rows show or their order,
/// the fallback resolves exactly what the next paint will show.
pub fn derive_key_order(
    rows_in: &[ResourceRow],
    metrics: Option<&MetricsLens<'_>>,
    predicates: &[Arc<RowPredicate>],
    draft: Option<&SearchPattern>,
    sort: SortSpec,
) -> Vec<ObjectKey> {
    let (order, _) = filter_and_sort(rows_in, metrics, predicates, draft, sort);
    order.iter().map(|&i| row_key(&rows_in[i])).collect()
}

/// Natural per-column display widths: seed from header widths, expand to
/// the widest cell, pad (`+3`: left border + two spaces), clamp.
fn column_widths(headers: &[&str], rows: &[Vec<String>], max_col_widths: &[u16]) -> Vec<u16> {
    use unicode_width::UnicodeWidthStr;
    if headers.is_empty() {
        return Vec::new();
    }
    let mut widths: Vec<u16> = headers.iter().map(|h| h.width() as u16 + 2).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.width() as u16);
            }
        }
    }
    for (i, w) in widths.iter_mut().enumerate() {
        // A header must stay readable even under a tight per-column cap, so
        // the ceiling never cuts below the header itself.
        let header_floor = headers[i].width() as u16 + 3;
        let cap = max_col_widths.get(i).copied().unwrap_or(u16::MAX).max(header_floor);
        *w = (*w + 3).min(cap);
    }
    widths
}


// ---------------------------------------------------------------------------
// LineStore — one live log stream's buffer (the log analogue of RowStore)
// ---------------------------------------------------------------------------

/// One live log stream's line buffer. Owned strongly by the `LogSession`
/// element; shared (via `Arc`) with `LogFilter` children and queued
/// events. Same single-writer/lock discipline as [`RowStore`]; same
/// epoch-floor succession rule (a `since`-range restart replaces the
/// stream but reuses the store — the floor keeps the dead stream's
/// queued lines out).
#[derive(Debug)]
pub struct LineStore {
    generation: AtomicU64,
    inner: Mutex<LineStoreInner>,
}

#[derive(Debug)]
pub struct LineStoreInner {
    /// Ring of typed log lines, bounded by `max_lines`.
    pub lines: std::collections::VecDeque<crate::kube::protocol::LogLine>,
    max_lines: usize,
    /// Total lines ever evicted from the front. Elements holding scroll
    /// positions self-heal at read time by diffing against the count they
    /// last saw — the store never reaches into elements.
    pub evicted: u64,
    /// Whether the stream behind this store is still delivering (false
    /// once the daemon closed the log substream).
    pub live: bool,
    epoch_floor: u64,
}

impl LineStore {
    pub fn new(max_lines: usize) -> Arc<Self> {
        Arc::new(Self {
            generation: AtomicU64::new(0),
            inner: Mutex::new(LineStoreInner {
                lines: std::collections::VecDeque::new(),
                max_lines,
                evicted: 0,
                live: true,
                epoch_floor: 0,
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, LineStoreInner> {
        self.inner
            .try_lock()
            .expect("LineStore lock contended — single-writer (session task) convention violated")
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn bump(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    pub fn with_read<R>(&self, f: impl FnOnce(&LineStoreInner) -> R) -> R {
        f(&self.lock())
    }

    /// Raise the epoch floor — called by the log-subscribe path BEFORE the
    /// new stream's handle is returned (restart succession).
    pub fn expect_epoch(&self, epoch: u64) {
        let mut inner = self.lock();
        inner.epoch_floor = inner.epoch_floor.max(epoch);
    }

    /// Append one line (epoch-gated). Evicts from the front past
    /// `max_lines`, counting evictions for element-side scroll healing.
    pub fn push(&self, epoch: u64, line: crate::kube::protocol::LogLine) {
        let mut inner = self.lock();
        if epoch < inner.epoch_floor {
            return;
        }
        if inner.lines.len() >= inner.max_lines {
            inner.lines.pop_front();
            inner.evicted += 1;
        }
        inner.lines.push_back(line);
        inner.live = true;
        drop(inner);
        self.bump();
    }

    /// The stream behind this store ended (daemon closed the substream).
    pub fn mark_ended(&self, epoch: u64) {
        let mut inner = self.lock();
        if epoch < inner.epoch_floor {
            return;
        }
        inner.live = false;
        drop(inner);
        self.bump();
    }

    /// Clear for a range restart (`since` change / ClearLogs). The ring
    /// empties; the eviction counter keeps counting forward so element
    /// scroll healing stays monotonic.
    pub fn clear(&self) {
        let mut inner = self.lock();
        let dropped = inner.lines.len() as u64;
        inner.lines.clear();
        inner.evicted += dropped;
        drop(inner);
        self.bump();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/app/store.rs"]
mod tests;
