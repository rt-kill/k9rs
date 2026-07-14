//! Live query system for watching Kubernetes resources.
//!
//! A `LiveQuery` is a running watcher for one resource type. The k8s watch
//! stream already IS a delta stream — the watcher task keeps a store of
//! converted [`ResourceRow`]s and, every flush tick, broadcasts only the
//! rows that actually changed ([`WatcherMsg::Delta`]). Full state
//! ([`TableBaseline`]) is built on demand via an actor-ask, only when a
//! subscriber joins, lags, or a bridge resubscribes. Nothing diffs: the
//! only set arithmetic anywhere is the relist tombstone (`store − seen`).
//!
//! `WatcherCache` manages a per-process cache of live queries using `Weak`
//! references so watchers die naturally when no `Subscription` holds them.
//!
//! Watcher dispatch uses the trait-based registry: each built-in resource
//! has a type-erased `WatcherSpawner` closure captured at startup. CRDs
//! go through `subscribe_dynamic` and `run_dynamic_live_watcher`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};

use dashmap::DashMap;

use futures::{StreamExt, TryStreamExt};
use kube::api::GroupVersionKind;
use kube::runtime::watcher::{self, Event as WatcherEvent};
use kube::{Api, Client, Resource};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::kube::cache::PrinterColumn;
use crate::kube::protocol::{
    ObjectKey, ResourceScope, RowChange, TableBaseline, TableDelta,
};
use crate::kube::resources::row::ResourceRow;

// ---------------------------------------------------------------------------
// Fanout types — lossless-with-signal deltas + on-demand baselines
// ---------------------------------------------------------------------------

/// What the watcher task broadcasts. Deltas ride a `broadcast` ring: a
/// subscriber that keeps up sees every batch; one that lags gets
/// `RecvError::Lagged` and recovers by asking for a fresh baseline —
/// overlap between that baseline and already-buffered deltas is harmless
/// because application is idempotent (full-row upserts; remove-of-absent
/// is a no-op). `Dead` is sent exactly once before the task exits, so data
/// and terminal error ride the same ownership path.
#[derive(Debug)]
pub(crate) enum WatcherMsg {
    Delta(TableDelta),
    Dead(String),
}

/// Baseline request, answered by the watcher task from its store. The
/// asker must hold its broadcast receiver BEFORE sending the ask (cursor
/// pinned first): the reply is built after the cursor position, so
/// baseline and buffered deltas can only overlap, never gap.
pub(crate) struct BaselineAsk {
    /// `true` (bridge resubscribe behind a client with stale data): defer
    /// the reply until the in-progress LIST completes, so the client swaps
    /// once, atomically. `false` (fresh join): reply as soon as there is
    /// anything real to show — deferred only while the FIRST list is still
    /// empty, so a joining client never sees a false "No resources found".
    pub after_initial_list: bool,
    pub reply: oneshot::Sender<TableBaseline>,
}

/// Broadcast ring capacity, in delta batches. At one batch per 200ms flush
/// tick this is ~13s of sustained-churn tolerance for a stalled consumer
/// before it must re-baseline — bounded memory per subscriber, and one
/// slow subscriber only ever costs itself a resync.
const FANOUT_RING: usize = 64;

// ---------------------------------------------------------------------------
// Watcher-internal state — ListPhase + PendingChanges
// ---------------------------------------------------------------------------

/// Which listing phase the watcher is in. Replaces the old smeared
/// `had_success` + `init_dirty` + `steady_dirty` booleans.
///
/// The store is NOT cleared on relist: `InitApply` upserts only rows that
/// actually differ from the retained entry (an O(1) per-event equality,
/// possible because row converters are pure functions of the object —
/// `CellValue::Age` carries the timestamp, rendering is client-side), and
/// `InitDone` tombstones `store − seen`. Relist wire cost is therefore
/// O(actual changes), and a client never sees a blank table.
enum ListPhase {
    /// A LIST is in progress. `seen` accumulates enumerated keys for the
    /// InitDone tombstone sweep (`store − seen` covers deletions no matter
    /// how many aborted attempts preceded — the store is the full current
    /// belief); it resets on a repeated `Init` while the store keeps
    /// serving reads. `first` is true until the first LIST ever completes
    /// — it gates join-ask deferral and the fail-fast error path.
    Listing { seen: HashSet<ObjectKey>, first: bool },
    Steady,
}

enum ChangeKind {
    Upsert,
    Remove,
}

/// Changes accumulated since the last flush. Key + kind only — upserted
/// rows are cloned from the store ONCE at flush, so a batch is always
/// consistent with the store and carries at most one change per key
/// (last-writer-wins), making client application order-independent.
#[derive(Default)]
struct PendingChanges(HashMap<ObjectKey, ChangeKind>);

impl PendingChanges {
    fn upsert(&mut self, key: ObjectKey) {
        self.0.insert(key, ChangeKind::Upsert);
    }
    fn remove(&mut self, key: ObjectKey) {
        self.0.insert(key, ChangeKind::Remove);
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    /// Drain into a wire batch, cloning upserted rows from the store.
    fn drain(&mut self, store: &HashMap<ObjectKey, ResourceRow>) -> TableDelta {
        let changes = self
            .0
            .drain()
            .filter_map(|(key, kind)| match kind {
                ChangeKind::Upsert => store.get(&key).cloned().map(RowChange::Upsert),
                ChangeKind::Remove => Some(RowChange::Remove(key)),
            })
            .collect();
        TableDelta { changes }
    }
}

// ---------------------------------------------------------------------------
// QueryKey
// ---------------------------------------------------------------------------

/// Key for looking up a live query in the cache.
/// Uses `ContextId` so watchers are shared by actual cluster endpoint,
/// not by context name (which can collide across kubeconfig files).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct QueryKey {
    pub context: crate::kube::protocol::ContextId,
    pub namespace: crate::kube::protocol::Namespace,
    pub resource: crate::kube::protocol::ResourceId,
    /// Server-side filter. A filtered subscription creates a separate watcher
    /// from an unfiltered one (different API queries to K8s).
    pub filter: Option<crate::kube::protocol::SubscriptionFilter>,
}

// ---------------------------------------------------------------------------
// LiveQuery
// ---------------------------------------------------------------------------

/// A running watcher that broadcasts delta batches and answers baseline
/// asks. When all `Subscription` handles are dropped and the `Arc` strong
/// count reaches zero, the watcher task is aborted via the `Drop` impl.
pub struct LiveQuery {
    /// Delta fanout — see [`WatcherMsg`]. Subscribing (`.subscribe()`)
    /// pins a cursor; dropping the receiver is deregistration.
    delta_tx: broadcast::Sender<Arc<WatcherMsg>>,
    /// Baseline actor-ask into the watcher task (the store's sole owner).
    ask_tx: mpsc::Sender<BaselineAsk>,
    /// The watcher task handle — aborted on drop.
    task: JoinHandle<()>,
    /// What this query watches.
    pub key: QueryKey,
    /// Coalesces grace tasks: at most ONE grace-period task is ever in
    /// flight per `LiveQuery`. The first `Subscription::drop` that sees
    /// `grace_in_flight` false flips it to true and spawns the task;
    /// subsequent drops see `true` and don't spawn duplicates. The task
    /// resets the flag right before it drops the Arc, so a new
    /// subscribe/drop cycle later can spawn a fresh grace.
    grace_in_flight: std::sync::atomic::AtomicBool,
}

impl Drop for LiveQuery {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

/// What callers hold. The watcher stays alive as long as any Subscription
/// exists (Arc refcount > 0). The broadcast cursor is pinned at
/// construction — BEFORE any baseline can be asked — which is what makes
/// baseline-vs-buffered-deltas overlap-only (never gap).
pub struct Subscription {
    /// The query key identifying which resource stream this subscription is for.
    pub key: QueryKey,
    /// Delta stream cursor (see [`WatcherMsg`] for lag semantics).
    rx: broadcast::Receiver<Arc<WatcherMsg>>,
    /// Baseline ask channel into the watcher task.
    ask: mpsc::Sender<BaselineAsk>,
    /// Prevents the LiveQuery from being dropped.
    _keepalive: Arc<LiveQuery>,
}

impl Subscription {
    /// Build a subscription from a live watcher handle. Cursor pinned here.
    fn attach(key: QueryKey, lq: Arc<LiveQuery>) -> Self {
        Self {
            key,
            rx: lq.delta_tx.subscribe(),
            ask: lq.ask_tx.clone(),
            _keepalive: lq,
        }
    }
}

/// Page size for the initial LIST request. Each page is a single HTTP response;
/// too large and the connection drops mid-transfer, too small and continue tokens
/// expire before all pages are fetched. 1000 items ≈ 3MB per page.
/// Watcher page size — loaded from daemon config at runtime.
pub(crate) fn watcher_page_size() -> u32 {
    crate::kube::daemon_config::daemon_config().watcher_page_size
}


/// Delta flush interval — one broadcast batch per tick, at most. Also the
/// progressive-paint cadence during an initial LIST (no size gate: each
/// row is sent exactly once, so total init bytes ≈ one full list at any
/// cluster size — the old 2000-row cap is gone, dissolved not raised).
pub(crate) const INIT_FLUSH_INTERVAL_MS: u64 = 200;
/// If no kube-rs watch events arrive within this window, the watcher
/// self-terminates with Dead. The daemon bridge's retry loop creates a
/// fresh watcher with a new initial list.
pub(crate) const STALE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
/// Backoff config — loaded from daemon config at runtime.
pub(crate) fn initial_backoff_ms() -> u64 {
    crate::kube::daemon_config::daemon_config().backoff.initial_ms
}
pub(crate) fn max_backoff_ms() -> u64 {
    crate::kube::daemon_config::daemon_config().backoff.max_ms
}
pub(crate) fn max_elapsed_ms() -> u64 {
    crate::kube::daemon_config::daemon_config().backoff.max_elapsed_ms
}

impl Drop for Subscription {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        // Shared grace dance (see `crate::kube::spawn_grace`). A *dead*
        // watcher (self-terminated task) skips grace so its cache entry is
        // reclaimed and the next subscribe builds a fresh watcher; a live one
        // is kept warm for the window. Claim/reset ride the concrete
        // `grace_in_flight` flag, coalescing to at most one grace task.
        crate::kube::spawn_grace(
            self._keepalive.clone(),
            |lq| lq.task.is_finished(),
            |lq| {
                lq.grace_in_flight
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            },
            |lq| lq.grace_in_flight.store(false, Ordering::Release),
        );
    }
}

impl Subscription {
    /// Ask the watcher for a full baseline; the returned oneshot resolves
    /// when the (possibly deferred) reply arrives, or errors if the watcher
    /// task is gone — callers treat that as watcher death. Synchronous send
    /// so the caller can `select!` over the reply and `recv()` without a
    /// borrow conflict.
    pub fn request_baseline(&self, after_initial_list: bool) -> oneshot::Receiver<TableBaseline> {
        let (reply, rx) = oneshot::channel();
        let _ = self.ask.try_send(BaselineAsk { after_initial_list, reply });
        rx
    }

    /// Receive the next delta batch (or terminal `Dead`). `Err(Lagged)` =
    /// this subscriber fell behind the ring: recover by re-asking for a
    /// baseline and continuing — overlap with buffered batches is
    /// idempotent. `Err(Closed)` = the watcher task exited.
    pub(crate) async fn recv(&mut self) -> Result<Arc<WatcherMsg>, broadcast::error::RecvError> {
        self.rx.recv().await
    }
}

// ---------------------------------------------------------------------------
// WatcherCache
// ---------------------------------------------------------------------------

/// Per-process cache of live queries. Stores `Weak` references so watchers
/// die naturally when no one holds a `Subscription`.
pub struct WatcherCache {
    entries: DashMap<QueryKey, Weak<LiveQuery>>,
}

impl Default for WatcherCache {
    fn default() -> Self {
        Self::new()
    }
}

impl WatcherCache {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Subscribe to a built-in resource type. Reuses an existing watcher if
    /// the Weak upgrades, otherwise creates a new one via the typed registry
    /// path. `kind` is taken explicitly so the caller (who's already
    /// pattern-matched on `ResourceId::BuiltIn(kind)`) hands the discriminant
    /// in — no runtime `.expect()` required downstream.
    pub fn subscribe(
        &self,
        key: QueryKey,
        kind: crate::kube::resource_def::BuiltInKind,
        make_client: impl FnOnce() -> Client,
        streaming_lists: bool,
    ) -> Subscription {
        self.subscribe_with(key, move |k| Self::create_watcher(k, kind, &make_client(), streaming_lists))
    }

    /// Shared cache lookup + insert scaffolding for both `subscribe` and
    /// `subscribe_dynamic`. The `create` closure runs only when no live
    /// watcher exists for `key`.
    ///
    /// Sequence:
    ///   1. Reap dead `Weak` entries (cheap O(n)).
    ///   2. Fast path: opportunistic `get()` — if a live Arc upgrades, return
    ///      a fresh subscription handle backed by it.
    ///   3. Slow path: take the `entry()` lock for atomic check-and-insert
    ///      (a concurrent caller may have raced past our fast-path read), and
    ///      either reuse the race winner's watcher or call `create` to spawn
    ///      a new one and store its `Weak` in the map.
    ///
    /// The trace label ("typed" vs "dynamic") is derived from the key's
    /// `ResourceId` discriminant so callers don't need to pass a magic
    /// string in.
    fn subscribe_with<F>(&self, key: QueryKey, create: F) -> Subscription
    where
        F: FnOnce(&QueryKey) -> Arc<LiveQuery>,
    {
        use dashmap::mapref::entry::Entry;

        let label = if matches!(&key.resource, crate::kube::protocol::ResourceId::Crd(_) | crate::kube::protocol::ResourceId::CrdUnresolved(_)) {
            "dynamic"
        } else {
            "typed"
        };

        self.reap_dead();

        if let Some(weak) = self.entries.get(&key) {
            if let Some(arc) = weak.upgrade() {
                if !arc.task.is_finished() {
                    tracing::debug!("WatcherCache: reusing existing {} watcher for {:?}", label, key);
                    return Subscription::attach(key, arc);
                }
                tracing::debug!("WatcherCache: existing {} watcher is dead, replacing for {:?}", label, key);
            }
        }

        match self.entries.entry(key.clone()) {
            Entry::Occupied(mut e) => {
                if let Some(arc) = e.get().upgrade() {
                    if !arc.task.is_finished() {
                        tracing::debug!("WatcherCache: reusing {} watcher (race winner) for {:?}", label, key);
                        return Subscription::attach(key, arc);
                    }
                }
                tracing::debug!("WatcherCache: creating new {} watcher for {:?}", label, key);
                let live_query = create(&key);
                e.insert(Arc::downgrade(&live_query));
                Subscription::attach(key, live_query)
            }
            Entry::Vacant(e) => {
                tracing::debug!("WatcherCache: creating new {} watcher for {:?}", label, key);
                let live_query = create(&key);
                e.insert(Arc::downgrade(&live_query));
                Subscription::attach(key, live_query)
            }
        }
    }

    /// Internal: spawn a watcher task and return the LiveQuery + initial receiver.
    ///
    /// Only built-in resources reach this path — CRDs go through
    /// `create_dynamic_watcher`, and locals never touch `WatcherCache` at
    /// all. The typed `kind: BuiltInKind` is passed in explicitly by the
    /// caller (who has already destructured `ResourceId::BuiltIn(kind)`),
    /// so the dispatch is compile-time-checked and there's nothing to
    /// `.expect()`.
    fn create_watcher(
        key: &QueryKey,
        kind: crate::kube::resource_def::BuiltInKind,
        client: &Client,
        streaming_lists: bool,
    ) -> Arc<LiveQuery> {
        let (delta_tx, _) = broadcast::channel(FANOUT_RING);
        let (ask_tx, ask_rx) = mpsc::channel(16);

        let args = crate::kube::resource_defs::registry::WatcherArgs {
            client: client.clone(),
            namespace: key.namespace.clone(),
            delta_tx: delta_tx.clone(),
            ask_rx,
            filter: key.filter.clone(),
            streaming_lists,
        };

        let task = crate::kube::resource_defs::REGISTRY.spawn_watcher_for_kind(kind, args);

        Arc::new(LiveQuery {
            delta_tx,
            ask_tx,
            task,
            key: key.clone(),
            grace_in_flight: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Remove the cache entry for a key. Used by `handle_refresh` for dynamic
    /// resources so the next `subscribe_dynamic` creates a fresh watcher.
    pub fn remove(&self, key: &QueryKey) {
        self.entries.remove(key);
    }

    /// Reap dead `Weak` entries from the cache. Called opportunistically
    /// from `subscribe`/`subscribe_force` so a long-running daemon that
    /// churns through many distinct subscriptions doesn't accumulate
    /// dead-weak slots forever (each slot is a `QueryKey` clone +
    /// pointer + counter, ~100 B; not catastrophic but unbounded).
    fn reap_dead(&self) {
        self.entries.retain(|_, weak| weak.strong_count() > 0);
    }

    /// Force-subscribe: removes any existing Weak entry for the key and creates
    /// a new watcher unconditionally. The old watcher's grace task still holds
    /// its Arc, so it will live out its grace period — that's fine. The cache
    /// now points to the new watcher.
    pub fn subscribe_force(
        &self,
        key: QueryKey,
        kind: crate::kube::resource_def::BuiltInKind,
        client: &Client,
        streaming_lists: bool,
    ) -> Subscription {
        self.reap_dead();

        let live_query = Self::create_watcher(&key, kind, client, streaming_lists);
        self.entries.insert(key.clone(), Arc::downgrade(&live_query));

        Subscription::attach(key, live_query)
    }
}

// ---------------------------------------------------------------------------
// Dynamic CRD instance watcher
// ---------------------------------------------------------------------------

impl WatcherCache {
    /// Subscribe to a dynamic CRD resource type. Reuses an existing watcher if
    /// the Weak upgrades, otherwise creates a new one via the dynamic-object
    /// watcher path.
    // The args are the dynamic-resource descriptor (gvk/plural/scope/printer
    // columns) plus the client factory and streaming flag — all intrinsic to
    // opening a watcher. Kept flat rather than threaded through a struct in
    // this hot path (matches the `#[allow]` on the table renderer).
    #[allow(clippy::too_many_arguments)]
    pub fn subscribe_dynamic(
        &self,
        key: QueryKey,
        make_client: impl FnOnce() -> Client,
        gvk: GroupVersionKind,
        plural: String,
        scope: ResourceScope,
        printer_columns: Vec<PrinterColumn>,
        streaming_lists: bool,
    ) -> Subscription {
        self.subscribe_with(key, move |k| {
            Self::create_dynamic_watcher(k, &make_client(), gvk, plural, scope, printer_columns, streaming_lists)
        })
    }

    /// Internal: spawn a dynamic watcher task and return the LiveQuery + initial receiver.
    fn create_dynamic_watcher(
        key: &QueryKey,
        client: &Client,
        gvk: GroupVersionKind,
        plural: String,
        scope: ResourceScope,
        printer_columns: Vec<PrinterColumn>,
        streaming_lists: bool,
    ) -> Arc<LiveQuery> {
        let (delta_tx, _) = broadcast::channel(FANOUT_RING);
        let (ask_tx, ask_rx) = mpsc::channel(16);
        let task_client = client.clone();
        let task_ns = key.namespace.clone();
        let task_tx = delta_tx.clone();
        let task = tokio::spawn(async move {
            crate::kube::live_query_dynamic::run_dynamic_live_watcher(task_client, task_ns, task_tx, ask_rx, gvk, plural, scope, printer_columns, streaming_lists).await;
        });
        Arc::new(LiveQuery {
            delta_tx,
            ask_tx,
            task,
            key: key.clone(),
            grace_in_flight: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

// ---------------------------------------------------------------------------
// Generic typed watcher loop with debounce
// ---------------------------------------------------------------------------

/// Extracts the `ObjectKey` from a Kubernetes resource.
fn obj_key<K: Resource<DynamicType = D>, D>(obj: &K) -> ObjectKey {
    let meta = obj.meta();
    ObjectKey::new(
        meta.namespace.clone().unwrap_or_default(),
        meta.name.clone().unwrap_or_default(),
    )
}

/// Runs a typed `kube::runtime::watcher` stream, maintaining a local store
/// of converted rows and broadcasting per-flush delta batches. Baselines are
/// answered on demand from the store (see [`BaselineAsk`] for deferral
/// semantics). Called by the type-erased `WatcherSpawner` closures.
/// Build the kube watch stream for a watcher task: page size, semantics,
/// and server-side filters (Labels/Field push down to the API; OwnerUid is
/// post-filtered per-change in the bridge because the K8s API can't filter
/// by ownerReference). Split from the loop so tests can inject a stream.
pub(crate) fn watch_stream<K, D>(
    api: Api<K>,
    filter: &Option<crate::kube::protocol::SubscriptionFilter>,
    streaming_lists: bool,
) -> futures::stream::BoxStream<'static, Result<WatcherEvent<K>, watcher::Error>>
where
    K: Resource<DynamicType = D>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + serde::de::DeserializeOwned
        + 'static,
    D: Send + Sync + 'static,
{
    let mut watcher_config = watcher::Config::default()
        .page_size(watcher_page_size())
        .any_semantic();
    if streaming_lists {
        watcher_config = watcher_config.streaming_lists();
    }
    match filter {
        Some(crate::kube::protocol::SubscriptionFilter::Labels(map)) => {
            let sel = crate::kube::protocol::SubscriptionFilter::labels_to_selector(map);
            watcher_config = watcher_config.labels(&sel);
        }
        Some(crate::kube::protocol::SubscriptionFilter::Field(f)) => {
            watcher_config = watcher_config.fields(f);
        }
        Some(crate::kube::protocol::SubscriptionFilter::OwnerUid(_)) | None => {}
    }
    watcher::watcher(api, watcher_config).boxed()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_typed_watcher<K, C, D>(
    mut stream: futures::stream::BoxStream<'static, Result<WatcherEvent<K>, watcher::Error>>,
    delta_tx: broadcast::Sender<Arc<WatcherMsg>>,
    mut ask_rx: mpsc::Receiver<BaselineAsk>,
    convert: C,
    resource_id: crate::kube::protocol::ResourceId,
    headers: Vec<String>,
    namespace: &crate::kube::protocol::Namespace,
) where
    K: Resource<DynamicType = D>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + serde::de::DeserializeOwned
        + 'static,
    C: Fn(K) -> ResourceRow + Send + 'static,
    D: Send + Sync + 'static,
{

    let mut store: HashMap<ObjectKey, ResourceRow> = HashMap::new();
    let mut pending = PendingChanges::default();
    let mut phase = ListPhase::Listing { seen: HashSet::new(), first: true };
    // Asks parked until their deferral condition clears (see BaselineAsk).
    // Dropped-on-exit oneshots surface as Err at the bridge = watcher death.
    let mut deferred_joins: Vec<oneshot::Sender<TableBaseline>> = Vec::new();
    let mut deferred_relist: Vec<oneshot::Sender<TableBaseline>> = Vec::new();

    let mut backoff_ms: u64 = initial_backoff_ms();
    let mut backoff_start = std::time::Instant::now();

    let mut flush_timer = tokio::time::interval(std::time::Duration::from_millis(INIT_FLUSH_INTERVAL_MS));
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let rt = resource_id.plural().to_string();
    let ns_label: &str = match namespace {
        crate::kube::protocol::Namespace::All => "all",
        crate::kube::protocol::Namespace::Named(n) => n.as_str(),
    };

    let make_baseline = |store: &HashMap<ObjectKey, ResourceRow>| TableBaseline {
        resource: resource_id.clone(),
        headers: headers.clone(),
        rows: store.values().cloned().collect(),
    };

    let mut exit_reason: Option<String> = None;

    loop {
        tokio::select! {
            timeout_result = tokio::time::timeout(STALE_TIMEOUT, stream.try_next()) => {
                let event_result = match timeout_result {
                    Ok(r) => r,
                    Err(_) => {
                        warn!("live_query: watcher stale for {}({}), no events in {:?}", rt, ns_label, STALE_TIMEOUT);
                        exit_reason = Some(format!("watch stale: no events in {:?}", STALE_TIMEOUT));
                        break;
                    }
                };
                match event_result {
                    Ok(Some(event)) => {
                        match event {
                            WatcherEvent::Init => {
                                // Relist begins. The store is NOT cleared —
                                // it keeps serving baseline asks, and
                                // InitDone tombstones `store − seen`.
                                match &mut phase {
                                    ListPhase::Steady => {
                                        phase = ListPhase::Listing { seen: HashSet::new(), first: false };
                                    }
                                    ListPhase::Listing { seen, .. } => seen.clear(),
                                }
                                backoff_start = std::time::Instant::now();
                                info!("live_query: starting list for {}({})", rt, ns_label);
                            }
                            WatcherEvent::InitApply(obj) => {
                                let key = obj_key(&obj);
                                if let ListPhase::Listing { seen, .. } = &mut phase {
                                    seen.insert(key.clone());
                                }
                                let row = convert(obj);
                                // Upsert-if-changed: an unchanged row costs
                                // nothing on the wire (relist = O(actual
                                // changes); converters are pure, see ListPhase).
                                if store.get(&key) != Some(&row) {
                                    store.insert(key.clone(), row);
                                    pending.upsert(key);
                                }
                            }
                            WatcherEvent::InitDone => {
                                backoff_ms = initial_backoff_ms();
                                backoff_start = std::time::Instant::now();
                                if let ListPhase::Listing { seen, .. } = &phase {
                                    // Tombstones: everything the relist did
                                    // not enumerate is gone.
                                    let dead: Vec<ObjectKey> = store.keys()
                                        .filter(|k| !seen.contains(*k))
                                        .cloned()
                                        .collect();
                                    for key in dead {
                                        store.remove(&key);
                                        pending.remove(key);
                                    }
                                }
                                phase = ListPhase::Steady;
                                info!("live_query: list complete for {}({}), {} items", rt, ns_label, store.len());
                                // Flush accumulated changes immediately —
                                // the user is waiting.
                                if !pending.is_empty() {
                                    let _ = delta_tx.send(Arc::new(WatcherMsg::Delta(pending.drain(&store))));
                                }
                                // Answer everyone parked on "after the list".
                                for reply in deferred_relist.drain(..).chain(deferred_joins.drain(..)) {
                                    let _ = reply.send(make_baseline(&store));
                                }
                            }
                            WatcherEvent::Apply(obj) => {
                                backoff_ms = initial_backoff_ms();
                                backoff_start = std::time::Instant::now();
                                let key = obj_key(&obj);
                                let row = convert(obj);
                                // Same upsert-if-changed rule: heartbeat /
                                // metadata-only churn never reaches the wire.
                                if store.get(&key) != Some(&row) {
                                    store.insert(key.clone(), row);
                                    pending.upsert(key);
                                }
                            }
                            WatcherEvent::Delete(obj) => {
                                backoff_ms = initial_backoff_ms();
                                backoff_start = std::time::Instant::now();
                                let key = obj_key(&obj);
                                if store.remove(&key).is_some() {
                                    pending.remove(key);
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        debug!("live_query: stream ended for {}", rt);
                        if !pending.is_empty() {
                            let _ = delta_tx.send(Arc::new(WatcherMsg::Delta(pending.drain(&store))));
                        }
                        break;
                    }
                    Err(e) => {
                        // Fail-fast iff the FIRST list never completed:
                        // almost certainly permanent (RBAC, unknown type).
                        if matches!(phase, ListPhase::Listing { first: true, .. }) {
                            warn!("live_query: initial load failed for {}: {}", rt, e);
                            exit_reason = Some(format!("{}", e));
                            break;
                        }
                        if backoff_start.elapsed().as_millis() as u64 > max_elapsed_ms() {
                            warn!("live_query: watcher for {} failed for over 2 minutes, giving up: {}", rt, e);
                            exit_reason = Some(format!("{}", e));
                            break;
                        }
                        warn!("live_query: watcher error for {}: {}, retrying in {}ms", rt, e, backoff_ms);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(max_backoff_ms());
                    }
                }
            }
            _ = flush_timer.tick() => {
                if !pending.is_empty() {
                    let _ = delta_tx.send(Arc::new(WatcherMsg::Delta(pending.drain(&store))));
                }
                // A join parked on "first list still empty" unparks at the
                // first flush with real rows: its baseline is the first
                // page, and the progressive deltas continue from there.
                if !deferred_joins.is_empty() && !store.is_empty() {
                    for reply in deferred_joins.drain(..) {
                        let _ = reply.send(make_baseline(&store));
                    }
                }
            }
            Some(ask) = ask_rx.recv() => {
                let defer_relist = ask.after_initial_list
                    && matches!(phase, ListPhase::Listing { .. });
                let defer_join = !ask.after_initial_list
                    && matches!(phase, ListPhase::Listing { first: true, .. })
                    && store.is_empty();
                if defer_relist {
                    deferred_relist.push(ask.reply);
                } else if defer_join {
                    deferred_joins.push(ask.reply);
                } else {
                    let _ = ask.reply.send(make_baseline(&store));
                }
            }
        }
    }

    // Terminal: data and death ride the same channel, in order. Deferred
    // asks drop here → their oneshots error at the bridge = watcher death.
    let reason = exit_reason.unwrap_or_else(|| format!("watcher stream ended for {}", rt));
    let _ = delta_tx.send(Arc::new(WatcherMsg::Dead(reason)));
}

// ---------------------------------------------------------------------------
// Watcher-loop tests — the injected stream lets these drive raw
// WatcherEvents and assert the delta/baseline contract without a cluster.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod watcher_tests {
    use super::*;
    use futures::StreamExt;
    use k8s_openapi::api::core::v1::ConfigMap;
    use std::time::Duration;
    use tokio::sync::broadcast::error::RecvError;

    fn cm(ns: &str, name: &str, val: &str) -> ConfigMap {
        let mut c = ConfigMap {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(ns.to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        c.data = Some([("v".to_string(), val.to_string())].into());
        c
    }

    /// Pure converter mirroring the registry pattern: identity + one value
    /// cell. Deterministic (the relist-equality invariant).
    fn convert(c: ConfigMap) -> ResourceRow {
        let val = c
            .data
            .as_ref()
            .and_then(|d| d.get("v"))
            .cloned()
            .unwrap_or_default();
        ResourceRow {
            name: c.metadata.name.unwrap_or_default(),
            namespace: Some(c.metadata.namespace.unwrap_or_default()),
            cells: vec![crate::kube::resources::row::CellValue::Text(val)],
            ..Default::default()
        }
    }

    struct Harness {
        events: mpsc::UnboundedSender<Result<WatcherEvent<ConfigMap>, watcher::Error>>,
        delta_rx: broadcast::Receiver<Arc<WatcherMsg>>,
        ask_tx: mpsc::Sender<BaselineAsk>,
        task: JoinHandle<()>,
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn spawn_harness() -> Harness {
        let (events, erx) = mpsc::unbounded_channel();
        let stream = tokio_stream::wrappers::UnboundedReceiverStream::new(erx).boxed();
        let (delta_tx, delta_rx) = broadcast::channel(FANOUT_RING);
        let (ask_tx, ask_rx) = mpsc::channel(16);
        let rid = crate::kube::protocol::ResourceId::BuiltIn(
            crate::kube::resource_def::BuiltInKind::ConfigMap,
        );
        let task = tokio::spawn(async move {
            run_typed_watcher(
                stream,
                delta_tx,
                ask_rx,
                convert,
                rid,
                vec!["NAME".into(), "V".into()],
                &crate::kube::protocol::Namespace::All,
            )
            .await;
        });
        Harness { events, delta_rx, ask_tx, task }
    }

    impl Harness {
        fn send(&self, ev: WatcherEvent<ConfigMap>) {
            self.events.send(Ok(ev)).expect("watcher task alive");
        }

        async fn next_delta(&mut self) -> TableDelta {
            let msg = tokio::time::timeout(Duration::from_millis(800), self.delta_rx.recv())
                .await
                .expect("expected a delta within the flush window")
                .expect("channel open");
            match &*msg {
                WatcherMsg::Delta(d) => d.clone(),
                WatcherMsg::Dead(r) => panic!("unexpected Dead({r})"),
            }
        }

        async fn expect_quiet(&mut self) {
            let res =
                tokio::time::timeout(Duration::from_millis(500), self.delta_rx.recv()).await;
            assert!(res.is_err(), "expected NO delta, got {:?}", res.map(|m| m.map(|a| format!("{:?}", a))));
        }

        fn ask(&self, after_initial_list: bool) -> oneshot::Receiver<TableBaseline> {
            let (reply, rx) = oneshot::channel();
            self.ask_tx
                .try_send(BaselineAsk { after_initial_list, reply })
                .expect("ask channel open");
            rx
        }

        /// Drive a full first list to Steady with the given objects.
        async fn init_steady(&mut self, objs: Vec<ConfigMap>) {
            self.send(WatcherEvent::Init);
            for o in objs {
                self.send(WatcherEvent::InitApply(o));
            }
            self.send(WatcherEvent::InitDone);
            // InitDone flushes immediately.
            let _ = self.next_delta().await;
        }
    }

    fn keys_of(delta: &TableDelta) -> (Vec<String>, Vec<String>) {
        let mut ups = Vec::new();
        let mut rms = Vec::new();
        for c in &delta.changes {
            match c {
                RowChange::Upsert(r) => ups.push(r.name.clone()),
                RowChange::Remove(k) => rms.push(k.name.clone()),
            }
        }
        ups.sort();
        rms.sort();
        (ups, rms)
    }

    /// A1/B4/E4: a join during an EMPTY first list is deferred (no false
    /// "No resources found"), unparks at the first real page, and the
    /// progressive deltas keep flowing after it.
    #[tokio::test]
    async fn join_defers_until_first_real_page() {
        let mut h = spawn_harness();
        let bl = h.ask(false);
        h.send(WatcherEvent::Init);
        // Still empty: the ask must be parked, not answered with [].
        tokio::time::sleep(Duration::from_millis(300)).await;
        h.send(WatcherEvent::InitApply(cm("ns", "a", "1")));
        // First flush with data: delta broadcast + deferred join answered.
        let d = h.next_delta().await;
        assert_eq!(keys_of(&d).0, vec!["a"]);
        let baseline = tokio::time::timeout(Duration::from_millis(800), bl)
            .await
            .expect("join baseline unparked at first page")
            .expect("watcher alive");
        assert_eq!(baseline.rows.len(), 1);
        assert_eq!(baseline.headers, vec!["NAME", "V"]);
        // Progressive init continues as deltas.
        h.send(WatcherEvent::InitApply(cm("ns", "b", "1")));
        h.send(WatcherEvent::InitDone);
        let d = h.next_delta().await;
        assert_eq!(keys_of(&d).0, vec!["b"]);
        // Steady ask answers immediately with the full store.
        let baseline = h.ask(false).await.expect("steady baseline");
        assert_eq!(baseline.rows.len(), 2);
    }

    /// C1/C2/H1: relist does NOT clear the store (asks answered mid-relist
    /// from retained data), re-sends only rows that actually changed, and
    /// tombstones exactly the not-re-enumerated keys at InitDone.
    #[tokio::test]
    async fn relist_is_o_of_actual_changes_with_tombstones() {
        let mut h = spawn_harness();
        h.init_steady(vec![cm("ns", "a", "1"), cm("ns", "b", "1")]).await;

        h.send(WatcherEvent::Init); // relist begins
        h.send(WatcherEvent::InitApply(cm("ns", "a", "1"))); // unchanged
        // Mid-relist join: store retained → immediate, full answer.
        let baseline = h.ask(false).await.expect("mid-relist baseline");
        assert_eq!(baseline.rows.len(), 2, "store not cleared during relist");
        h.send(WatcherEvent::InitApply(cm("ns", "c", "1"))); // new
        h.send(WatcherEvent::InitDone); // b was not re-enumerated
        let d = h.next_delta().await;
        let (ups, rms) = keys_of(&d);
        assert_eq!(ups, vec!["c"], "unchanged rows must not re-send");
        assert_eq!(rms, vec!["b"], "tombstone = store − seen");
    }

    /// The resubscribe deferral: `after_initial_list` asks park through the
    /// whole relist and resolve with the post-InitDone store.
    #[tokio::test]
    async fn after_initial_list_ask_defers_to_initdone() {
        let mut h = spawn_harness();
        h.init_steady(vec![cm("ns", "a", "1")]).await;
        h.send(WatcherEvent::Init);
        // Two independent channels (events vs asks): give the loop a beat
        // to consume Init so the ask observes the relist in progress. (An
        // ask racing ahead of Init is semantically a valid pre-relist
        // answer — this test pins the mid-relist deferral specifically.)
        tokio::time::sleep(Duration::from_millis(50)).await;
        let bl = h.ask(true);
        h.send(WatcherEvent::InitApply(cm("ns", "c", "1")));
        tokio::time::sleep(Duration::from_millis(300)).await; // flush passes; still parked
        h.send(WatcherEvent::InitDone);
        let baseline = tokio::time::timeout(Duration::from_millis(800), bl)
            .await
            .expect("resolved at InitDone")
            .expect("watcher alive");
        let mut names: Vec<_> = baseline.rows.iter().map(|r| r.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["c"], "a tombstoned, c present");
    }

    /// H2/E4: metadata-only churn (identical converted row) never reaches
    /// the wire; a real change does.
    #[tokio::test]
    async fn upsert_if_changed_suppresses_identical_rows() {
        let mut h = spawn_harness();
        h.init_steady(vec![cm("ns", "a", "1")]).await;
        h.send(WatcherEvent::Apply(cm("ns", "a", "1"))); // heartbeat
        h.expect_quiet().await;
        h.send(WatcherEvent::Apply(cm("ns", "a", "2"))); // real change
        let d = h.next_delta().await;
        assert_eq!(keys_of(&d).0, vec!["a"]);
    }

    /// A2 (daemon half): deleting an absent key emits nothing; deleting a
    /// present key emits exactly one Remove.
    #[tokio::test]
    async fn delete_absent_is_silent() {
        let mut h = spawn_harness();
        h.init_steady(vec![cm("ns", "a", "1")]).await;
        h.send(WatcherEvent::Delete(cm("ns", "ghost", "1")));
        h.expect_quiet().await;
        h.send(WatcherEvent::Delete(cm("ns", "a", "1")));
        let d = h.next_delta().await;
        let (ups, rms) = keys_of(&d);
        assert!(ups.is_empty());
        assert_eq!(rms, vec!["a"]);
    }

    /// The at-most-one-change-per-key batch invariant: within one flush
    /// window, last-writer-wins and the row is cloned from the store at
    /// flush (apply→apply→delete collapses to one Remove; delete→apply
    /// collapses to one Upsert carrying the final value).
    #[tokio::test]
    async fn pending_changes_coalesce_per_key() {
        let mut h = spawn_harness();
        h.init_steady(vec![cm("ns", "a", "1"), cm("ns", "b", "1")]).await;
        // a: modify, modify, delete → one Remove.
        h.send(WatcherEvent::Apply(cm("ns", "a", "2")));
        h.send(WatcherEvent::Apply(cm("ns", "a", "3")));
        h.send(WatcherEvent::Delete(cm("ns", "a", "3")));
        // b: delete, recreate → one Upsert with the final value.
        h.send(WatcherEvent::Delete(cm("ns", "b", "1")));
        h.send(WatcherEvent::Apply(cm("ns", "b", "9")));
        let d = h.next_delta().await;
        let (ups, rms) = keys_of(&d);
        assert_eq!(ups, vec!["b"]);
        assert_eq!(rms, vec!["a"]);
        let keys: std::collections::HashSet<_> =
            d.changes.iter().map(|c| c.key()).collect();
        assert_eq!(keys.len(), d.changes.len(), "at most one change per key");
        let RowChange::Upsert(row) = d
            .changes
            .iter()
            .find(|c| matches!(c, RowChange::Upsert(_)))
            .unwrap()
        else {
            unreachable!()
        };
        assert_eq!(
            row.cells[0],
            crate::kube::resources::row::CellValue::Text("9".into()),
            "upsert carries the store's final value",
        );
    }

    /// C5: stream end → pending flushed, then Dead as the final message.
    #[tokio::test]
    async fn dead_is_terminal_and_ordered() {
        let mut h = spawn_harness();
        h.init_steady(vec![cm("ns", "a", "1")]).await;
        h.send(WatcherEvent::Apply(cm("ns", "a", "2")));
        // Close the stream before the flush tick: the loop must flush the
        // pending change, then send Dead.
        let (dead_events, _keep) = mpsc::unbounded_channel();
        let old = std::mem::replace(&mut h.events, dead_events);
        drop(old);
        let d = h.next_delta().await;
        assert_eq!(keys_of(&d).0, vec!["a"]);
        let last = tokio::time::timeout(Duration::from_millis(800), h.delta_rx.recv())
            .await
            .expect("Dead arrives")
            .expect("channel open");
        assert!(matches!(&*last, WatcherMsg::Dead(_)));
        match h.delta_rx.recv().await {
            Err(RecvError::Closed) => {}
            other => panic!("nothing may follow Dead, got {other:?}"),
        }
    }
}
