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
    /// Initializing / Ready / Failed lifecycle.
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
        Arc::new(Self {
            plural: plural.into(),
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
                let index: HashMap<ObjectKey, usize> = inner
                    .rows
                    .iter()
                    .enumerate()
                    .map(|(i, r)| (row_key(r), i))
                    .collect();
                let mut replace: Vec<(usize, ResourceRow)> = Vec::new();
                let mut append: Vec<ResourceRow> = Vec::new();
                let mut dead: Vec<bool> = vec![false; inner.rows.len()];
                for change in &d.changes {
                    match change {
                        RowChange::Upsert(row) => match index.get(&row_key(row)) {
                            Some(&i) => replace.push((i, row.clone())),
                            None => append.push(row.clone()),
                        },
                        RowChange::Remove(key) => {
                            if let Some(&i) = index.get(key) {
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
            }
            StorePayload::Failed(msg) => {
                inner.state = TableDataState::Failed(msg);
            }
        }
        drop(inner);
        self.bump();
    }

    /// Back to `Initializing` (Ctrl-R refresh, seeding a reused store).
    /// Marks die with the rows. Flash HASHES survive deliberately: rows
    /// that changed across the refresh still flash after the recovery
    /// baseline (same continuity the global tracker used to provide).
    pub fn clear(&self) {
        let mut inner = self.lock();
        inner.rows.clear();
        inner.marked.clear();
        inner.state = TableDataState::Initializing;
        drop(inner);
        self.bump();
    }

    // --- Marks (data-keyed; ops are element-mediated) ---------------------

    /// Toggle a mark; returns `true` if the key is now marked. No
    /// generation bump: marks are read fresh each frame, never baked into
    /// the derived view (parity with the fused table).
    pub fn toggle_mark(&self, key: &ObjectKey) -> bool {
        let mut inner = self.lock();
        if inner.marked.remove(key) {
            false
        } else {
            inner.marked.insert(key.clone());
            true
        }
    }

    /// Mark every key (span-mark: the element computes the span from its
    /// own derived order and hands us identities).
    pub fn mark_keys(&self, keys: impl IntoIterator<Item = ObjectKey>) {
        let mut inner = self.lock();
        inner.marked.extend(keys);
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
        *self.pods.try_lock().expect("MetricsHub: single-writer convention violated") = Some(usage);
        self.version.fetch_add(1, Ordering::Release);
    }

    pub fn set_nodes(&self, usage: HashMap<NodeName, MetricsUsage>) {
        *self.nodes.try_lock().expect("MetricsHub: single-writer convention violated") = Some(usage);
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
    pub max_col_width: u16,
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
    // 1. Effective display strings for EVERY cell of EVERY row — one pass
    //    feeds filtering (greps span all columns), styling, and display.
    let eff: Vec<Vec<String>> = rows_in
        .iter()
        .map(|row| {
            row.cells
                .iter()
                .enumerate()
                .map(|(ci, cell)| match metrics.and_then(|m| m.effective(row, ci)) {
                    Some(v) => v.to_string(),
                    None => cell.to_string(),
                })
                .collect()
        })
        .collect();

    // 2. Filter: every committed predicate AND the uncommitted draft.
    let mut order: Vec<usize> = (0..rows_in.len())
        .filter(|&i| {
            let row = &rows_in[i];
            let strings = &eff[i];
            spec.predicates.iter().all(|p| p.matches(row, strings))
                && spec
                    .draft
                    .is_none_or(|d| strings.iter().any(|c| d.is_match(c)))
        })
        .collect();

    // 3. Sort by the EFFECTIVE value at the sort column (metrics columns
    //    sort by live usage), typed comparison via CellValue::cmp, stable
    //    (namespace, name) tiebreaker. Screen order = this order.
    {
        let empty = CellValue::Text(String::new());
        let value_at = |i: usize| -> std::borrow::Cow<'_, CellValue> {
            let row = &rows_in[i];
            match metrics.and_then(|m| m.effective(row, spec.sort.col)) {
                Some(v) => std::borrow::Cow::Owned(v),
                None => std::borrow::Cow::Borrowed(row.cells.get(spec.sort.col).unwrap_or(&empty)),
            }
        };
        order.sort_by(|&a, &b| {
            let primary = value_at(a).as_ref().cmp(value_at(b).as_ref());
            let primary = if spec.sort.ascending { primary } else { primary.reverse() };
            primary.then_with(|| {
                let (ra, rb) = (&rows_in[a], &rows_in[b]);
                ra.namespace().cmp(rb.namespace()).then_with(|| ra.name().cmp(rb.name()))
            })
        });
    }

    // 4. Materialize in screen order.
    let rows: Vec<Vec<String>> = order
        .iter()
        .map(|&i| {
            spec.visible_cols
                .iter()
                .map(|&ci| eff[i].get(ci).cloned().unwrap_or_default())
                .collect()
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
    let col_widths = column_widths(spec.headers, &rows, spec.max_col_width);

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

/// Natural per-column display widths: seed from header widths, expand to
/// the widest cell, pad (`+3`: left border + two spaces), clamp.
fn column_widths(headers: &[&str], rows: &[Vec<String>], max_col_width: u16) -> Vec<u16> {
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
    for w in &mut widths {
        *w = (*w + 3).min(max_col_width);
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
mod tests {
    use super::*;
    use crate::app::nav::CompiledGrep;
    use crate::kube::protocol::{TableBaseline, TableDelta};
    use crate::kube::resource_def::BuiltInKind;

    fn rid() -> ResourceId {
        ResourceId::BuiltIn(BuiltInKind::Pod)
    }

    fn row(name: &str, ns: &str, cells: &[&str]) -> ResourceRow {
        ResourceRow {
            name: name.into(),
            namespace: Some(ns.into()),
            cells: cells.iter().map(|c| CellValue::Text((*c).to_string())).collect(),
            ..Default::default()
        }
    }

    fn baseline(rows: Vec<ResourceRow>) -> StorePayload {
        StorePayload::Baseline(TableBaseline {
            resource: rid(),
            headers: vec!["NAME".into(), "STATUS".into()],
            rows,
        })
    }

    fn delta(changes: Vec<RowChange>) -> StorePayload {
        StorePayload::Delta(TableDelta { changes })
    }

    fn key(ns: &str, name: &str) -> ObjectKey {
        ObjectKey::new(ns.to_string(), name.to_string())
    }

    fn spec<'a>(
        predicates: &'a [Arc<RowPredicate>],
        visible: &'a [usize],
        headers: &'a [&'a str],
    ) -> DeriveSpec<'a> {
        DeriveSpec {
            predicates,
            draft: None,
            sort: SortSpec::default(),
            visible_cols: visible,
            headers,
            max_col_width: 40,
        }
    }

    #[test]
    fn baseline_replaces_and_delta_edits_in_wire_order() {
        let store = RowStore::new("pods");
        store.apply(1, baseline(vec![row("b", "ns", &["b", "ok"]), row("a", "ns", &["a", "ok"])]));
        // Wire order preserved — the store never sorts.
        store.with_read(|i| {
            assert_eq!(i.rows[0].name, "b");
            assert_eq!(i.state, TableDataState::Ready);
        });
        let g1 = store.generation();
        store.apply(
            1,
            delta(vec![
                RowChange::Upsert(row("a", "ns", &["a", "changed"])),
                RowChange::Upsert(row("c", "ns", &["c", "new"])),
                RowChange::Remove(key("ns", "b")),
                RowChange::Remove(key("ns", "ghost")), // absent: no-op
            ]),
        );
        store.with_read(|i| {
            let names: Vec<&str> = i.rows.iter().map(|r| r.name.as_str()).collect();
            assert_eq!(names, ["a", "c"]); // replaced in place, compacted, appended
            assert_eq!(i.rows[0].cells[1].to_string(), "changed");
        });
        assert!(store.generation() > g1);
    }

    #[test]
    fn epoch_floor_gates_stale_streams() {
        let store = RowStore::new("pods");
        store.apply(5, baseline(vec![row("live", "ns", &["live", "ok"])]));
        // A successor stream exists the moment expect_epoch runs.
        store.expect_epoch(9);
        // Predecessor events (epoch < 9) are rejected — baseline AND delta.
        store.apply(5, baseline(vec![row("stale", "ns", &["stale", "old"])]));
        store.apply(8, delta(vec![RowChange::Remove(key("ns", "live"))]));
        store.with_read(|i| assert_eq!(i.rows[0].name, "live"));
        // The successor's baseline lands.
        store.apply(9, baseline(vec![row("fresh", "ns", &["fresh", "ok"])]));
        store.with_read(|i| assert_eq!(i.rows[0].name, "fresh"));
    }

    #[test]
    fn marks_prune_on_baseline_and_remove() {
        let store = RowStore::new("pods");
        store.apply(1, baseline(vec![row("a", "ns", &["a", "ok"]), row("b", "ns", &["b", "ok"])]));
        assert!(store.toggle_mark(&key("ns", "a")));
        assert!(store.toggle_mark(&key("ns", "b")));
        store.apply(1, delta(vec![RowChange::Remove(key("ns", "a"))]));
        assert_eq!(store.marked_keys(), vec![key("ns", "b")]);
        // Baseline without b prunes it too.
        store.apply(2, baseline(vec![row("c", "ns", &["c", "ok"])]));
        assert!(store.marked_keys().is_empty());
    }

    #[test]
    fn clear_keeps_flash_continuity_across_refresh() {
        let store = RowStore::new("pods");
        store.apply(1, baseline(vec![row("a", "ns", &["a", "Running"])]));
        store.clear(); // Ctrl-R
        store.with_read(|i| {
            assert!(i.rows.is_empty());
            assert_eq!(i.state, TableDataState::Initializing);
        });
        // Recovery baseline: the row changed while we weren't looking — it
        // must flash (hash continuity survived the clear).
        store.apply(2, baseline(vec![row("a", "ns", &["a", "CrashLoopBackOff"])]));
        store.with_read(|i| assert!(i.flash.changed_rows().contains_key(&key("ns", "a"))));
    }

    #[test]
    fn derive_filters_sorts_and_projects() {
        let store = RowStore::new("pods");
        store.apply(
            1,
            baseline(vec![
                row("web-2", "ns", &["web-2", "Running"]),
                row("web-1", "ns", &["web-1", "Failed"]),
                row("db-1", "ns", &["db-1", "Running"]),
            ]),
        );
        let preds = [Arc::new(RowPredicate::Grep(CompiledGrep::new("web")))];
        let visible = [0usize, 1usize];
        let headers = ["NAME", "STATUS"];
        let mut sp = spec(&preds, &visible, &headers);
        sp.sort = SortSpec { col: 0, ascending: false };
        let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,None, &sp));
        // db-1 filtered out; descending by NAME.
        assert_eq!(view.rows.iter().map(|r| r[0].as_str()).collect::<Vec<_>>(), ["web-2", "web-1"]);
        assert_eq!(view.keys[0], key("ns", "web-2"));
        assert_eq!(view.total_rows, 3);
        assert_eq!(view.col_widths.len(), 2);
    }

    #[test]
    fn narrowed_composes_grep_on_grep() {
        let store = RowStore::new("pods");
        store.apply(
            1,
            baseline(vec![
                row("web-api", "ns", &["web-api", "ok"]),
                row("web-cache", "ns", &["web-cache", "ok"]),
                row("db-api", "ns", &["db-api", "ok"]),
            ]),
        );
        let base = RowSource::new(Arc::clone(&store), None);
        let first = base.narrowed(Arc::new(RowPredicate::Grep(CompiledGrep::new("web"))));
        let second = first.narrowed(Arc::new(RowPredicate::Grep(CompiledGrep::new("api"))));
        assert_eq!(second.predicates().len(), 2);
        let visible = [0usize];
        let headers = ["NAME"];
        let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,None, &spec(second.predicates(), &visible, &headers)));
        assert_eq!(view.rows.len(), 1);
        assert_eq!(view.rows[0][0], "web-api");
        // The parent chain is untouched — sources are values.
        assert_eq!(first.predicates().len(), 1);
    }

    #[test]
    fn fault_and_draft_predicates() {
        let store = RowStore::new("pods");
        let mut bad = row("bad", "ns", &["bad", "CrashLoop"]);
        bad.health = RowHealth::Failed;
        store.apply(1, baseline(vec![row("good", "ns", &["good", "Running"]), bad]));
        let preds = [Arc::new(RowPredicate::Fault)];
        let visible = [0usize];
        let headers = ["NAME"];
        let mut sp = spec(&preds, &visible, &headers);
        let draft = SearchPattern::new("ba");
        sp.draft = Some(&draft);
        let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,None, &sp));
        assert_eq!(view.rows.iter().map(|r| r[0].as_str()).collect::<Vec<_>>(), ["bad"]);
    }

    #[test]
    fn pod_metrics_overlay_display_and_sort() {
        let hub = MetricsHub::new();
        let binding = MetricsBinding::for_rid(&rid(), &hub).expect("pods have metrics columns");
        // Resolve the real CPU column index from the registry so the test
        // rows can size their cells accordingly.
        let MetricsCols::Pod(cols) = binding.cols else { panic!("pod binding") };
        let cpu_col = cols.cpu.expect("pod def has a CPU column");

        let mk_row = |name: &str| {
            let mut r = ResourceRow {
                name: name.into(),
                namespace: Some("ns".into()),
                ..Default::default()
            };
            r.cells = (0..=cpu_col).map(|_| CellValue::Text(String::new())).collect();
            r.cells[0] = CellValue::Text(name.into());
            r
        };
        let store = RowStore::new("pods");
        store.apply(
            1,
            StorePayload::Baseline(TableBaseline {
                resource: rid(),
                headers: (0..=cpu_col).map(|i| format!("H{i}")).collect(),
                rows: vec![mk_row("low"), mk_row("high")],
            }),
        );

        // Before any poll: stored (empty) cells win.
        let visible = [cpu_col];
        let headers = ["CPU"];
        let preds: [Arc<RowPredicate>; 0] = [];
        let sp_plain = spec(&preds, &visible, &headers);
        let lens = binding.lens();
        let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,Some(&lens), &sp_plain));
        assert_eq!(view.rows[0][0], "");
        drop(lens);

        let mut usage = HashMap::new();
        usage.insert(key("ns", "high"), MetricsUsage { cpu_milli: 900, mem_bytes: 0, ..Default::default() });
        usage.insert(key("ns", "low"), MetricsUsage { cpu_milli: 100, mem_bytes: 0, ..Default::default() });
        hub.set_pods(usage);

        // Overlaid values display AND drive the sort (descending by CPU).
        let mut sp = spec(&preds, &visible, &headers);
        sp.sort = SortSpec { col: cpu_col, ascending: false };
        let lens = binding.lens();
        let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,Some(&lens), &sp));
        assert_eq!(view.rows[0][0], "900m");
        assert_eq!(view.keys[0], key("ns", "high"));
        assert_eq!(view.rows[1][0], "100m");
    }

    #[test]
    fn node_metrics_absent_vs_never_polled() {
        use crate::kube::protocol::NodeName;
        let hub = MetricsHub::new();
        let node_rid = ResourceId::BuiltIn(BuiltInKind::Node);
        let binding = MetricsBinding::for_rid(&node_rid, &hub).expect("nodes have metrics columns");
        let MetricsCols::Node(cols) = binding.cols else { panic!("node binding") };
        let cpu_col = cols.cpu.expect("node def has a CPU column");

        let mut r = ResourceRow { name: "worker-1".into(), namespace: None, ..Default::default() };
        r.cells = (0..=cpu_col).map(|_| CellValue::Text("stored".into())).collect();
        let store = RowStore::new("nodes");
        store.apply(
            1,
            StorePayload::Baseline(TableBaseline {
                resource: node_rid.clone(),
                headers: (0..=cpu_col).map(|i| format!("H{i}")).collect(),
                rows: vec![r],
            }),
        );
        let visible = [cpu_col];
        let headers = ["CPU"];
        let preds: [Arc<RowPredicate>; 0] = [];
        let sp = spec(&preds, &visible, &headers);

        // Never polled → stored cell shows.
        let lens = binding.lens();
        let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,Some(&lens), &sp));
        assert_eq!(view.rows[0][0], "stored");
        drop(lens);

        // Polled, node absent → n/a placeholder, not a frozen stale value.
        hub.set_nodes(HashMap::<NodeName, MetricsUsage>::new());
        let lens = binding.lens();
        let view = store.with_read(|i| derive_view(&i.rows, &i.column_rules,Some(&lens), &sp));
        assert_eq!(view.rows[0][0], CellValue::Placeholder.to_string());
    }

    #[test]
    fn line_store_pushes_evicts_and_counts() {
        use crate::kube::protocol::LogLine;
        let store = LineStore::new(3);
        for i in 0..5 {
            store.push(1, LogLine { content: format!("l{i}"), container: None });
        }
        store.with_read(|inner| {
            assert_eq!(inner.lines.len(), 3);
            assert_eq!(inner.evicted, 2, "front evictions are counted for scroll healing");
            assert_eq!(inner.lines.front().unwrap().content, "l2");
        });
        // clear keeps the counter monotonic.
        store.clear();
        store.with_read(|inner| {
            assert!(inner.lines.is_empty());
            assert_eq!(inner.evicted, 5);
        });
    }

    #[test]
    fn line_store_epoch_floor_gates_stale_streams() {
        use crate::kube::protocol::LogLine;
        let store = LineStore::new(10);
        store.push(1, LogLine { content: "old".into(), container: None });
        // A range restart raised the floor before the old stream's queued
        // lines drained.
        store.expect_epoch(5);
        store.push(1, LogLine { content: "stale".into(), container: None });
        store.mark_ended(1); // the old stream's EOF must not mark the new one dead
        store.with_read(|inner| {
            assert_eq!(inner.lines.len(), 1);
            assert!(inner.live);
        });
        store.push(5, LogLine { content: "fresh".into(), container: None });
        store.mark_ended(5);
        store.with_read(|inner| {
            assert_eq!(inner.lines.back().unwrap().content, "fresh");
            assert!(!inner.live);
        });
    }

    #[test]
    fn failed_is_epoch_gated_and_sets_state() {
        let store = RowStore::new("pods");
        store.apply(3, baseline(vec![row("a", "ns", &["a", "ok"])]));
        store.expect_epoch(7);
        // A superseded stream's death must not mark the successor failed.
        store.apply(3, StorePayload::Failed("old stream died".into()));
        store.with_read(|i| assert_eq!(i.state, TableDataState::Ready));
        store.apply(7, StorePayload::Failed("real failure".into()));
        store.with_read(|i| {
            assert_eq!(i.state, TableDataState::Failed("real failure".into()));
            // Rows are retained — the UI decides what to show for Failed.
            assert_eq!(i.rows.len(), 1);
        });
    }
}
