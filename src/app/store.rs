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
//! under [`RowStore::with_read`] on the same task. The convention is
//! enforced by the lock itself — see [`crate::util::one_task`], which also
//! says why one task (and not merely one at a time) is what the epoch floor
//! and the delta tracker assume.
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
use std::sync::{Arc, MutexGuard};

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
    /// Whether `row` passes.
    ///
    /// `cells` are the row's *effective* (metrics-overlaid) strings, one per
    /// DATA column; `visible` indexes the ones currently on screen.
    ///
    /// A bare grep scans only the visible ones, per cell. Two decisions,
    /// both deliberate:
    ///
    /// - **Only what is displayed.** It used to scan every cell the row
    ///   carried, so `/pimp` on a deployment list matched `CONTAINERS`,
    ///   `IMAGES` and `LABELS` — columns hidden at the default level — and
    ///   returned rows with no visible `pimp` anywhere. A match you cannot
    ///   see is one you can neither predict nor check, and the convenience
    ///   was luck: it happened to hit the column you meant. `Ctrl-w` now
    ///   widens what you SEARCH as well as what you see, so the rule is one
    ///   sentence and the evidence is always on screen.
    /// - **Per cell, not per rendered row.** Never the painted line: that
    ///   would let a pattern span the gap between two columns and match
    ///   padding and truncation artefacts. The cell value is also the FULL
    ///   one, not the width-clipped display string — eliding a value is a
    ///   rendering accident, and a column you are looking at should be
    ///   searchable whether or not it fits.
    pub fn matches(&self, row: &ResourceRow, cells: &[String], visible: &[usize]) -> bool {
        match self {
            RowPredicate::Grep(g) => visible
                .iter()
                .filter_map(|&i| cells.get(i))
                .any(|c| g.pattern().is_match(c)),
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
    inner: crate::util::one_task::OneTask<RowStoreInner>,
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
            inner: crate::util::one_task::OneTask::new("RowStore", RowStoreInner {
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

    /// The single-toucher convention, enforced by the lock itself — see
    /// [`crate::util::one_task`]. This used to be the FIFTH copy of the
    /// `try_lock().expect("… convention violated")` line, and it caught only
    /// the half of the rule where two touchers overlap in time.
    fn lock(&self) -> MutexGuard<'_, RowStoreInner> {
        self.inner.lock()
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

    /// Whether an event stamped `epoch` would be accepted rather than
    /// floored out as belonging to a superseded stream. Test-only: the
    /// production path just calls `apply` and lets the floor decide.
    #[cfg(test)]
    pub(crate) fn would_accept_epoch(&self, epoch: u64) -> bool {
        epoch >= self.lock().epoch_floor
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
#[derive(Debug)]
pub struct MetricsHub {
    version: AtomicU64,
    pods: crate::util::one_task::OneTask<Option<HashMap<ObjectKey, MetricsUsage>>>,
    nodes: crate::util::one_task::OneTask<Option<HashMap<NodeName, MetricsUsage>>>,
}

impl Default for MetricsHub {
    fn default() -> Self {
        // Hand-written rather than derived: each slot carries the name that
        // goes in the panic, and a `Default` that named neither would make
        // the two indistinguishable in the one message that matters.
        Self {
            version: AtomicU64::new(0),
            pods: crate::util::one_task::OneTask::new("MetricsHub::pods", None),
            nodes: crate::util::one_task::OneTask::new("MetricsHub::nodes", None),
        }
    }
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
        let mut slot = self.pods.lock();
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
        let mut slot = self.nodes.lock();
        if slot.as_ref() == Some(&usage) {
            return;
        }
        *slot = Some(usage);
        self.version.fetch_add(1, Ordering::Release);
    }

    /// Context switch: all usage is stale, back to never-polled.
    pub fn clear(&self) {
        *self.pods.lock() = None;
        *self.nodes.lock() = None;
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
                map: self.hub.pods.lock(),
                cols,
            }),
            MetricsCols::Node(cols) => MetricsLens(LensInner::Nodes {
                map: self.hub.nodes.lock(),
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
    let (order, mut eff) = filter_and_sort(rows_in, metrics, spec.predicates, spec.draft, spec.sort, spec.visible_cols);
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
                    // 1. TakeoverOp column rules — only for cells that exist
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
                    // 3. A typed percentage colours itself, so a utilization
                    //    column is never born invisible waiting for someone
                    //    to spell its header right in a YAML file. Read the
                    //    EFFECTIVE value: converters emit these as
                    //    `Percentage(None)` placeholders and the metrics lens
                    //    supplies the number (see `pods.rs`), so the stored
                    //    cell is empty for exactly the columns that matter.
                    //    The lens is only consulted for cells already known
                    //    to be percentages.
                    if matches!(row.cells.get(ci), Some(CellValue::Percentage(_))) {
                        return metrics
                            .and_then(|m| m.effective(row, ci))
                            .or_else(|| row.cells.get(ci).cloned())
                            .and_then(|v| v.utilization_health());
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
    // `visible`: the data columns currently on screen — what a bare grep may scan.
    visible: &[usize],
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
                predicates.iter().all(|p| p.matches(row, strings, visible))
                    // The DRAFT scans the same columns as a committed grep.
                    // It used to scan every cell, so a pattern matched while
                    // you typed it and then lost rows the moment you pressed
                    // Enter — the two paths disagreeing about their own rule.
                    && draft.is_none_or(|d| {
                        visible
                            .iter()
                            .filter_map(|&i| strings.get(i))
                            .any(|c| d.is_match(c))
                    })
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
/// Selection reads fall back to this when no painted view exists (the memo
/// is dropped on cover and absent on fresh elements).
///
/// It used to say presentation never influences which rows show. That
/// stopped being true when a bare grep was restricted to the DISPLAYED
/// columns: the visible set is now an input, and the caller passes the level
/// the last derive used (`TableInteraction::derived_level`) so this still
/// resolves exactly what the next paint will.
pub fn derive_key_order(
    rows_in: &[ResourceRow],
    metrics: Option<&MetricsLens<'_>>,
    predicates: &[Arc<RowPredicate>],
    draft: Option<&SearchPattern>,
    sort: SortSpec,
    visible: &[usize],
) -> Vec<ObjectKey> {
    let (order, _) = filter_and_sort(rows_in, metrics, predicates, draft, sort, visible);
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

/// A log line as the CLIENT holds it: the wire line, plus the one form every
/// consumer derives from it.
///
/// [`LogLine`](crate::kube::protocol::LogLine) is a wire message — `content`
/// is exactly the bytes the workload wrote, ANSI and all. Almost nothing on
/// the client wants those bytes: the grep, the highlight offsets, the width
/// measurement and the export all want the VISIBLE text, and each of them
/// used to derive it again at the point of use. That is how `/workers=` came
/// to miss a line reading `workers=0` — one call site out of four was still
/// matching the raw form, and which patterns worked was decided by where a
/// logger happened to put its colour codes.
///
/// So the store's element is the PAIR, built once on arrival:
///
/// - A line cannot be held here without its visible text, so forgetting to
///   derive it is unexpressable rather than merely discouraged.
/// - A stored line never changes, so the derivation has no invalidation rule
///   to get wrong. This is a materialization, not a cache.
/// - It was also several allocations per line per frame for a value that
///   cannot change. Measuring a wrapped 50,000-line buffer cost 49 ms a
///   frame, nearly all of it re-deriving this.
///
/// [`Deref`](std::ops::Deref) to the wire line, because a stored line IS one
/// — `source`, `timestamp` and `content` are read exactly as before. The two
/// projections that need the derived body ([`Self::segments`],
/// [`Self::flat_text`]) live HERE and only here, so there is no bare-
/// `LogLine` spelling of either to reach for by accident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredLine {
    line: crate::kube::protocol::LogLine,
    /// `None` when `line.content` IS its own visible text — the common case
    /// for a logger that does not colour, and no second copy for it.
    visible: Option<String>,
}

impl std::ops::Deref for StoredLine {
    type Target = crate::kube::protocol::LogLine;
    fn deref(&self) -> &Self::Target {
        &self.line
    }
}

impl From<crate::kube::protocol::LogLine> for StoredLine {
    fn from(line: crate::kube::protocol::LogLine) -> Self {
        let visible = match crate::util::visible_text(&line.content) {
            // The raw content already IS its visible text.
            std::borrow::Cow::Borrowed(_) => None,
            std::borrow::Cow::Owned(cleaned) => Some(cleaned),
        };
        Self { line, visible }
    }
}

impl StoredLine {
    /// What the screen shows for the body — and therefore the offset space a
    /// pattern matches in and a highlight indexes. Equals the concatenation
    /// of the renderer's spans by construction; see
    /// [`crate::util::visible_text`].
    pub fn visible(&self) -> &str {
        self.visible.as_deref().unwrap_or(&self.line.content)
    }

    /// This line's parts, in render order, with the separator each carries.
    ///
    /// Borrowed throughout — this is a projection, not a second copy. The
    /// measurer, the renderer and both matchers consume THIS, so none of
    /// them can disagree with another about which parts a line has or what
    /// text they hold.
    pub fn segments(
        &self,
    ) -> impl Iterator<Item = (crate::kube::protocol::Role, &str, &'static str)> {
        use crate::kube::protocol::Role;
        [
            self.line.source.as_deref().map(|t| (Role::Source, t, " ")),
            self.line.timestamp.as_deref().map(|t| (Role::Timestamp, t, " ")),
            // The BODY is the only part carrying the workload's own ANSI,
            // and what it yields is the VISIBLE text. So a grep matches what
            // is on screen, a highlight lands on the glyphs it meant, and an
            // export carries the same characters.
            Some((Role::Body, self.visible(), "")),
        ]
        .into_iter()
        .flatten()
    }

    /// Flat text form for clipboard / file export.
    ///
    /// Export carries the SUBSTANCE of a line, not a transcript of the cells
    /// it was drawn in: both pieces of provenance ride along regardless of
    /// what the display toggles are showing, because `t` governs how much of
    /// a 97-column terminal the chrome may eat — a question that has no
    /// meaning in a file. The same rule that keeps the export from being
    /// truncated to the visible window keeps it from being thinned to the
    /// visible fields.
    ///
    /// The body is the line's VISIBLE text: colour is chrome, and a file is
    /// not a terminal. This used to be the callers' job (all three said
    /// `strip_ansi(&l.flat_text())`, through a tokenizer that was not the
    /// renderer's), which meant the fourth call site would have decided
    /// again.
    ///
    /// It also used to include the source and drop the timestamp, which was
    /// not a decision — it predates `timestamp` being a typed field at all,
    /// so it carried one half of the provenance and silently lost the other.
    pub fn flat_text(&self) -> std::borrow::Cow<'_, str> {
        let body = self.visible();
        match (&self.line.timestamp, &self.line.source) {
            (Some(ts), Some(c)) => std::borrow::Cow::Owned(format!("{} [{}] {}", ts, c, body)),
            (Some(ts), None) => std::borrow::Cow::Owned(format!("{} {}", ts, body)),
            (None, Some(c)) => std::borrow::Cow::Owned(format!("[{}] {}", c, body)),
            (None, None) => std::borrow::Cow::Borrowed(body),
        }
    }
}

/// One live log stream's line buffer. Owned strongly by the `LogSession`
/// element; shared (via `Arc`) with `LogFilter` children and queued
/// events. Same single-writer/lock discipline as [`RowStore`]; same
/// epoch-floor succession rule (a `since`-range restart replaces the
/// stream but reuses the store — the floor keeps the dead stream's
/// queued lines out).
#[derive(Debug)]
pub struct LineStore {
    generation: AtomicU64,
    inner: crate::util::one_task::OneTask<LineStoreInner>,
}

/// Which END of an over-long stream a store keeps.
///
/// The ring is tail-anchored by default, which is right for a follow: the
/// newest lines are the ones you want. It is exactly wrong for a history
/// request. `since=24h` on a busy service streams oldest-first, so the old
/// lines arrive, fill the ring, and are evicted by the newer ones behind
/// them — the client keeps the NEWEST N of a range asked for precisely to
/// see the oldest. Asking for more history made it strictly worse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retain {
    /// Drop from the front; keep the newest. Follows and plain tails.
    Tail,
    /// Drop arrivals once full; keep the oldest. History requests.
    Head,
}

#[derive(Debug)]
pub struct LineStoreInner {
    /// Ring of materialized log lines, bounded by `max_lines`.
    pub lines: std::collections::VecDeque<StoredLine>,
    max_lines: usize,
    retain: Retain,
    /// Arrivals dropped because a `Head` store was already full — reported,
    /// because "you are seeing the first N of more than N" is a different
    /// statement from "this is all there was".
    pub truncated: u64,
    /// Total lines ever evicted from the front. Elements holding scroll
    /// positions self-heal at read time by diffing against the count they
    /// last saw — the store never reaches into elements.
    pub evicted: u64,
    /// Distinct source tags, in FIRST-SEEN order.
    ///
    /// The palette slot a source gets is its index here, not a hash of its
    /// name. Hashing was both weak and wrong-tool: `31 mod 8 == -1` made the
    /// bucket an alternating sum of the bytes, so replica names sharing a
    /// long prefix collapsed onto the few low bits of a short suffix — and
    /// even a perfect hash puts four sources in eight buckets with a 59%
    /// chance of a collision. The set is KNOWN (the daemon plans it, and
    /// every line carries its tag), so assignment gives distinct colours up
    /// to the palette size by construction.
    ///
    /// Bounded by `MAX_LOG_FANOUT`, so the linear scan below is over at most
    /// twenty short strings.
    pub sources: Vec<String>,
    /// Whether the stream behind this store is still delivering (false
    /// once the daemon closed the log substream). Set by every arrival, so
    /// it answers "is data flowing", NOT "did this stream finish".
    pub live: bool,
    /// The stream reached an ORDERLY end — the container exited, or we
    /// stopped it ourselves. Sticky: only [`LineStore::clear`] (a restart)
    /// takes it back down.
    ///
    /// Separate from `live` because `live` cannot answer this. `push` sets
    /// `live = true` on every arrival, and a deliberately-stopped stream
    /// keeps delivering for a moment (`abort` is asynchronous and the event
    /// queue is already full), so the first straggler flipped `live` back
    /// and made a finished log look like a dead one — which
    /// `revive_if_dead` then "recovered" by clearing it.
    ended: bool,
    epoch_floor: u64,
}

impl LineStore {
    pub fn new(max_lines: usize) -> Arc<Self> {
        Arc::new(Self {
            generation: AtomicU64::new(0),
            inner: crate::util::one_task::OneTask::new("LineStore", LineStoreInner {
                lines: std::collections::VecDeque::new(),
                max_lines,
                retain: Retain::Tail,
                truncated: 0,
                evicted: 0,
                sources: Vec::new(),
                live: true,
                ended: false,
                epoch_floor: 0,
            }),
        })
    }

    /// Same convention, same enforcement — see [`crate::util::one_task`].
    fn lock(&self) -> MutexGuard<'_, LineStoreInner> {
        self.inner.lock()
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn bump(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    /// This store's retention cap — a derived store inherits it.
    pub fn max_lines(&self) -> usize {
        self.lock().max_lines
    }

    /// Choose which end to keep. Set by the element when it opens or
    /// restarts a stream, before any line can arrive.
    pub fn set_retain(&self, retain: Retain) {
        self.lock().retain = retain;
    }

    /// Whether this store has stopped accepting lines because it is a full
    /// `Head` store — i.e. it has the oldest N and there is more behind.
    /// Distinct source tags in first-seen order — the palette assignment.
    /// Deliberately NOT reset by `clear`: a range restart re-reads the same
    /// pods, and a pod changing colour because you pressed `5` would be the
    /// hash problem wearing a different hat.
    pub fn sources(&self) -> Vec<String> {
        self.lock().sources.clone()
    }

    /// Whether this stream reached an orderly end — see `ended`.
    pub fn has_ended(&self) -> bool {
        self.lock().ended
    }

    pub fn head_is_full(&self) -> bool {
        let inner = self.lock();
        inner.retain == Retain::Head && inner.lines.len() >= inner.max_lines
    }

    /// Append a batch of already-matched lines (a derived store's drain).
    /// Epoch-free: a derived store has exactly one producer, the element
    /// that owns it, so there is no succession to gate.
    pub fn extend_derived(&self, lines: impl IntoIterator<Item = StoredLine>) {
        let mut inner = self.lock();
        let mut added = false;
        for line in lines {
            if inner.lines.len() >= inner.max_lines {
                inner.lines.pop_front();
                inner.evicted += 1;
            }
            if let Some(src) = &line.source {
                if !inner.sources.iter().any(|s| s == src) {
                    inner.sources.push(src.clone());
                }
            }
            inner.lines.push_back(line);
            added = true;
        }
        drop(inner);
        if added {
            self.bump();
        }
    }

    /// Mirror the source stream's liveness onto a derived store, so the
    /// "Waiting for logs…" / "No logs." branches read the same either way.
    pub fn set_live(&self, live: bool) {
        let mut inner = self.lock();
        if inner.live != live {
            inner.live = live;
            drop(inner);
            self.bump();
        }
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
            match inner.retain {
                Retain::Tail => {
                    inner.lines.pop_front();
                    inner.evicted += 1;
                }
                // Full and keeping the head: the oldest N are what was
                // asked for, so the arrival is what goes.
                Retain::Head => {
                    inner.truncated += 1;
                    // Deliberately does NOT touch `live`: a refused arrival
                    // says nothing about liveness that was not already true,
                    // and asserting it here is what let a straggler
                    // un-finish a stream we had just stopped.
                    drop(inner);
                    self.bump();
                    return;
                }
            }
        }
        if let Some(src) = &line.source {
            if !inner.sources.iter().any(|s| s == src) {
                inner.sources.push(src.clone());
            }
        }
        // ARRIVAL: the one place a wire line becomes a stored one, so the
        // derivation happens exactly once per line in its whole lifetime.
        inner.lines.push_back(StoredLine::from(line));
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
        inner.ended = true;
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
        inner.truncated = 0;
        inner.evicted += dropped;
        // A clear precedes a fresh stream, so the previous stream's ending
        // is no longer this store's ending. Without this, a range restart
        // over a log that had ended left `ended` set forever and
        // `revive_if_dead` became a permanent no-op — the opposite failure,
        // a log that genuinely died and could never be recovered.
        inner.ended = false;
        inner.live = true;
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
