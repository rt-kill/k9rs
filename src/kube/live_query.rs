//! Live query system for watching Kubernetes resources.
//!
//! A `LiveQuery` represents a running watcher for a specific resource type.
//! It wraps the kube watch stream and produces typed `ResourceUpdate` values
//! via a `tokio::sync::watch` channel.
//!
//! `WatcherCache` manages a per-process cache of live queries using `Weak`
//! references so watchers die naturally when no `Subscription` holds them.
//!
//! Watcher dispatch uses the trait-based registry: each built-in resource
//! has a type-erased `WatcherSpawner` closure captured at startup. CRDs
//! go through `subscribe_dynamic` and `run_dynamic_live_watcher`.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::kube::GRACE_PERIOD_SECS;

use dashmap::DashMap;

use futures::{StreamExt, TryStreamExt};
use kube::api::GroupVersionKind;
use kube::runtime::watcher::{self, Event as WatcherEvent};
use kube::{Api, Client, Resource};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::event::ResourceUpdate;
use crate::kube::cache::PrinterColumn;
use crate::kube::protocol::{ObjectKey, ResourceScope};
use crate::kube::resources::KubeResource;

// ---------------------------------------------------------------------------
// WatcherSnapshot — single ownership path for data + error state
// ---------------------------------------------------------------------------

/// State machine carried over the `watch` channel that feeds a `LiveQuery`.
/// The watcher task publishes `Live(update)` per snapshot and transitions to
/// `Dead(reason)` exactly once before it exits — so both data and terminal
/// error ride the same ownership path. The watch channel's own synchronization
/// is all the coordination the design needs; there is no side-channel mutex
/// for error state.
#[derive(Debug, Clone)]
pub(crate) enum WatcherSnapshot {
    /// Initial state — watcher has not published a snapshot yet.
    Pending,
    /// A snapshot is available.
    Live(ResourceUpdate),
    /// Terminal state — the watcher task is exiting with this reason.
    Dead(String),
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

/// A running watcher that produces typed `ResourceUpdate` snapshots.
/// When all `Subscription` handles are dropped and the `Arc` strong count
/// reaches zero, the watcher task is aborted via the `Drop` impl.
pub struct LiveQuery {
    /// Latest typed snapshot or terminal error state.
    snapshot_tx: watch::Sender<WatcherSnapshot>,
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

/// What callers hold. Clone-able. Provides access to the latest snapshot.
/// The watcher stays alive as long as any Subscription exists (Arc refcount > 0).
#[derive(Clone)]
pub struct Subscription {
    /// The query key identifying which resource stream this subscription is for.
    pub key: QueryKey,
    /// Receive snapshot/terminal-state updates. `pub(crate)` because its item
    /// type (`WatcherSnapshot`) is a private implementation detail — external
    /// users go through the `current()` / `last_error()` / `changed()` methods.
    pub(crate) snapshot_rx: watch::Receiver<WatcherSnapshot>,
    /// Prevents the LiveQuery from being dropped.
    _keepalive: Arc<LiveQuery>,
}

/// Page size for the initial LIST request. Each page is a single HTTP response;
/// too large and the connection drops mid-transfer, too small and continue tokens
/// expire before all pages are fetched. 1000 items ≈ 3MB per page.
pub(crate) const WATCHER_PAGE_SIZE: u32 = 1000;
/// Interval for flushing intermediate snapshots during the initial list.
pub(crate) const INIT_FLUSH_INTERVAL_MS: u64 = 200;
/// Maximum store size for which intermediate flushes fire during the
/// initial list. Above this threshold we skip intermediate snapshots and
/// wait for `InitDone` — cloning + sorting + serializing thousands of rows
/// every 200ms produces multi-MB payloads that choke the yamux pipe with
/// diminishing UX return (the table just shows a blur of growing data).
/// The full snapshot after `InitDone` still delivers all the data.
pub(crate) const INIT_FLUSH_ROW_LIMIT: usize = 2_000;
/// Initial backoff duration for watcher retries (milliseconds).
pub(crate) const INITIAL_BACKOFF_MS: u64 = 300;
/// Maximum single-sleep backoff cap (milliseconds).
pub(crate) const MAX_BACKOFF_MS: u64 = 30_000;
/// Maximum total elapsed time before giving up on retries (milliseconds).
pub(crate) const MAX_ELAPSED_MS: u64 = 120_000;

impl Drop for Subscription {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;
        // Only keep the watcher alive via grace period if it's still running.
        // Dead watchers should be dropped immediately so the cache entry is
        // cleaned up and a fresh watcher can be created on next subscribe.
        if self._keepalive.task.is_finished() {
            return;
        }
        // Coalesce: at most one grace task per LiveQuery. CAS-set the
        // `grace_in_flight` flag; if we lose, another drop already spawned
        // the task and we just drop our own Arc here. If we win, spawn.
        if self._keepalive
            .grace_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        // Use `Handle::try_current` so dropping a Subscription outside a
        // tokio runtime (e.g. during process shutdown after the runtime
        // has been torn down) doesn't panic. If there's no runtime, reset
        // the flag and let the Arc drop here.
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self._keepalive.grace_in_flight.store(false, Ordering::Release);
            return;
        };
        let arc = self._keepalive.clone();
        handle.spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(GRACE_PERIOD_SECS)).await;
            // Reset the flag BEFORE dropping the Arc so that a future
            // subscribe-then-drop cycle (with the same LiveQuery, if it
            // somehow survives this drop) can spawn a fresh grace task.
            arc.grace_in_flight.store(false, Ordering::Release);
            drop(arc);
        });
    }
}

impl Subscription {
    /// Get the current snapshot (None if the watcher hasn't published yet,
    /// or if the watcher died — in which case `last_error()` returns Some).
    pub fn current(&mut self) -> Option<ResourceUpdate> {
        match &*self.snapshot_rx.borrow_and_update() {
            WatcherSnapshot::Live(update) => Some(update.clone()),
            WatcherSnapshot::Pending | WatcherSnapshot::Dead(_) => None,
        }
    }

    /// Wait for the next snapshot update.
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.snapshot_rx.changed().await
    }

    /// The last error message from the watcher task (if it died with an error).
    pub fn last_error(&self) -> Option<String> {
        match &*self.snapshot_rx.borrow() {
            WatcherSnapshot::Dead(msg) => Some(msg.clone()),
            WatcherSnapshot::Pending | WatcherSnapshot::Live(_) => None,
        }
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
        client: &Client,
    ) -> Subscription {
        let client = client.clone();
        self.subscribe_with(key, move |k| Self::create_watcher(k, kind, &client))
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
        F: FnOnce(&QueryKey) -> (Arc<LiveQuery>, watch::Receiver<WatcherSnapshot>),
    {
        use dashmap::mapref::entry::Entry;

        let label = if matches!(&key.resource, crate::kube::protocol::ResourceId::Crd(_)) {
            "dynamic"
        } else {
            "typed"
        };

        self.reap_dead();

        if let Some(weak) = self.entries.get(&key) {
            if let Some(arc) = weak.upgrade() {
                if !arc.task.is_finished() {
                    tracing::info!("WatcherCache: reusing existing {} watcher for {:?}", label, key);
                    return Subscription {
                        key,
                        snapshot_rx: arc.snapshot_tx.subscribe(),
                        _keepalive: arc,
                    };
                }
                tracing::info!("WatcherCache: existing {} watcher is dead, replacing for {:?}", label, key);
            }
        }

        match self.entries.entry(key.clone()) {
            Entry::Occupied(mut e) => {
                if let Some(arc) = e.get().upgrade() {
                    if !arc.task.is_finished() {
                        tracing::info!("WatcherCache: reusing {} watcher (race winner) for {:?}", label, key);
                        return Subscription {
                            key,
                            snapshot_rx: arc.snapshot_tx.subscribe(),
                            _keepalive: arc,
                        };
                    }
                }
                tracing::info!("WatcherCache: creating new {} watcher for {:?}", label, key);
                let (live_query, snapshot_rx) = create(&key);
                e.insert(Arc::downgrade(&live_query));
                Subscription { key, snapshot_rx, _keepalive: live_query }
            }
            Entry::Vacant(e) => {
                tracing::info!("WatcherCache: creating new {} watcher for {:?}", label, key);
                let (live_query, snapshot_rx) = create(&key);
                e.insert(Arc::downgrade(&live_query));
                Subscription { key, snapshot_rx, _keepalive: live_query }
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
    ) -> (Arc<LiveQuery>, watch::Receiver<WatcherSnapshot>) {
        let (snapshot_tx, snapshot_rx) = watch::channel(WatcherSnapshot::Pending);

        let args = crate::kube::resource_defs::registry::WatcherArgs {
            client: client.clone(),
            namespace: key.namespace.clone(),
            snapshot_tx: snapshot_tx.clone(),
            filter: key.filter.clone(),
        };

        let task = crate::kube::resource_defs::REGISTRY.spawn_watcher_for_kind(kind, args);

        let live_query = Arc::new(LiveQuery {
            snapshot_tx,
            task,
            key: key.clone(),
            grace_in_flight: std::sync::atomic::AtomicBool::new(false),
        });
        (live_query, snapshot_rx)
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
    ) -> Subscription {
        self.entries.remove(&key);
        self.reap_dead();

        let (live_query, snapshot_rx) = Self::create_watcher(&key, kind, client);
        self.entries.insert(key.clone(), Arc::downgrade(&live_query));

        Subscription {
            key,
            snapshot_rx,
            _keepalive: live_query,
        }
    }
}

// ---------------------------------------------------------------------------
// Dynamic CRD instance watcher
// ---------------------------------------------------------------------------

impl WatcherCache {
    /// Subscribe to a dynamic CRD resource type. Reuses an existing watcher if
    /// the Weak upgrades, otherwise creates a new one via the dynamic-object
    /// watcher path.
    pub fn subscribe_dynamic(
        &self,
        key: QueryKey,
        client: &Client,
        gvk: GroupVersionKind,
        plural: String,
        scope: ResourceScope,
        printer_columns: Vec<PrinterColumn>,
    ) -> Subscription {
        let client = client.clone();
        self.subscribe_with(key, move |k| {
            Self::create_dynamic_watcher(k, &client, gvk, plural, scope, printer_columns)
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
    ) -> (Arc<LiveQuery>, watch::Receiver<WatcherSnapshot>) {
        let (snapshot_tx, snapshot_rx) = watch::channel(WatcherSnapshot::Pending);
        let task_client = client.clone();
        let task_ns = key.namespace.clone();
        let task_tx = snapshot_tx.clone();
        let task = tokio::spawn(async move {
            crate::kube::live_query_dynamic::run_dynamic_live_watcher(task_client, task_ns, task_tx, gvk, plural, scope, printer_columns).await;
        });
        let live_query = Arc::new(LiveQuery {
            snapshot_tx,
            task,
            key: key.clone(),
            grace_in_flight: std::sync::atomic::AtomicBool::new(false),
        });
        (live_query, snapshot_rx)
    }
}

// ---------------------------------------------------------------------------
// Generic typed watcher loop with debounce
// ---------------------------------------------------------------------------

/// Extracts the `ObjectKey` from a Kubernetes resource.
fn obj_key<K: Resource<DynamicType = ()>>(obj: &K) -> ObjectKey {
    let meta = obj.meta();
    ObjectKey::new(
        meta.namespace.clone().unwrap_or_default(),
        meta.name.clone().unwrap_or_default(),
    )
}

/// Clone all items from the store, sort by (namespace, name), and return as
/// a `Vec`. Used by the InitDone immediate publish, the init flush timer,
/// and the steady-state debounce timer.
fn sorted_snapshot<T>(store: &HashMap<ObjectKey, T>) -> Vec<T>
where
    T: Clone + crate::kube::resources::KubeResource,
{
    let mut items: Vec<T> = store.values().cloned().collect();
    items.sort_by(|a, b| {
        let key_a = (a.namespace(), a.name());
        let key_b = (b.namespace(), b.name());
        key_a.cmp(&key_b)
    });
    items
}

/// Runs a typed `kube::runtime::watcher` stream, maintaining a local cache.
///
/// Events are debounced during `InitApply` bursts (every 100ms). `Apply`,
/// `Delete`, and `InitDone` flush immediately. Snapshots are wrapped into a
/// `ResourceUpdate` variant via `wrap` and sent through the `watch::Sender`.
///
/// Called by the type-erased `WatcherSpawner` closures in the registry.
pub(crate) async fn run_typed_watcher<K, T, C, W>(
    api: Api<K>,
    snapshot_tx: watch::Sender<WatcherSnapshot>,
    convert: C,
    wrap: W,
    namespace: &crate::kube::protocol::Namespace,
    filter: Option<crate::kube::protocol::SubscriptionFilter>,
) where
    K: Resource<DynamicType = ()>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + serde::de::DeserializeOwned
        + 'static,
    T: Clone + Send + 'static + KubeResource,
    C: Fn(K) -> T + Send + 'static,
    W: Fn(Vec<T>) -> ResourceUpdate + Send + 'static,
{
    let mut watcher_config = watcher::Config::default()
        .page_size(WATCHER_PAGE_SIZE)
        .any_semantic();      // serve from API server cache (resourceVersion=0), much faster
    // Apply server-side filters to the watcher (pushed to K8s API).
    // OwnerUid is NOT applied here — it's post-filtered after snapshot creation
    // because the K8s API doesn't support filtering by ownerReference.
    match &filter {
        Some(crate::kube::protocol::SubscriptionFilter::Labels(map)) => {
            let sel = crate::kube::protocol::SubscriptionFilter::labels_to_selector(map);
            watcher_config = watcher_config.labels(&sel);
        }
        Some(crate::kube::protocol::SubscriptionFilter::Field(f)) => {
            watcher_config = watcher_config.fields(f);
        }
        Some(crate::kube::protocol::SubscriptionFilter::OwnerUid(_)) | None => {}
    }
    let mut stream = watcher::watcher(api, watcher_config).boxed();

    let mut store: HashMap<ObjectKey, T> = HashMap::new();
    let mut backoff_ms: u64 = INITIAL_BACKOFF_MS;
    let mut backoff_start = std::time::Instant::now();
    // Track whether we've ever received data successfully. If the initial
    // LIST fails (RBAC, wrong resource, etc.), fail immediately instead of
    // retrying for 2 minutes — it's almost certainly a permanent error.
    let mut had_success = false;
    let mut init_dirty = false; // tracks whether we have unsent InitApply items
    let mut steady_dirty = false; // tracks whether Apply/Delete changed the store

    // Timer for flushing snapshots. During the initial list this fires
    // every 200ms for progressive display (if the store is small enough).
    // In steady state it fires every 200ms but only rebuilds when
    // `steady_dirty` is set — debouncing rapid Apply/Delete events
    // (e.g., 2000 node heartbeats/sec) into one snapshot rebuild per tick.
    let mut flush_timer = tokio::time::interval(std::time::Duration::from_millis(INIT_FLUSH_INTERVAL_MS));
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Trace labels: `rt` from the K8s type metadata (e.g. "pods"), `ns_label`
    // from the typed namespace. Both are derived — no string round-trip.
    let rt = K::plural(&());
    let ns_label: &str = match namespace {
        crate::kube::protocol::Namespace::All => "all",
        crate::kube::protocol::Namespace::Named(n) => n.as_str(),
    };

    // Reason we exit the loop. `None` means natural stream end; `Some` means
    // an error caused the break. Written at each break site; read once after
    // the loop to compose the terminal `WatcherSnapshot::Dead(...)`.
    let mut exit_reason: Option<String> = None;

    loop {
        tokio::select! {
            event_result = stream.try_next() => {
                match event_result {
                    Ok(Some(event)) => {
                        match event {
                            WatcherEvent::Init => {
                                store.clear();
                                backoff_start = std::time::Instant::now();
                                info!("live_query: starting initial list for {}({})", rt, ns_label);
                            }
                            WatcherEvent::InitApply(obj) => {
                                let key = obj_key(&obj);
                                store.insert(key, convert(obj));
                                init_dirty = true;
                            }
                            WatcherEvent::InitDone => {
                                had_success = true;
                                backoff_ms = INITIAL_BACKOFF_MS;
                                backoff_start = std::time::Instant::now();
                                info!("live_query: initial list complete for {}({}), {} items", rt, ns_label, store.len());
                                // Publish immediately — the user is waiting
                                // for the initial list, no reason to debounce.
                                let items = sorted_snapshot(&store);
                                let _ = snapshot_tx.send(WatcherSnapshot::Live(wrap(items)));
                            }
                            WatcherEvent::Apply(obj) => {
                                had_success = true;
                                backoff_ms = INITIAL_BACKOFF_MS;
                                backoff_start = std::time::Instant::now();
                                let key = obj_key(&obj);
                                store.insert(key, convert(obj));
                                steady_dirty = true;
                            }
                            WatcherEvent::Delete(obj) => {
                                had_success = true;
                                backoff_ms = INITIAL_BACKOFF_MS;
                                backoff_start = std::time::Instant::now();
                                let key = obj_key(&obj);
                                store.remove(&key);
                                steady_dirty = true;
                            }
                        }
                    }
                    Ok(None) => {
                        debug!("live_query: stream ended for {}", rt);
                        break;
                    }
                    Err(e) => {
                        if !had_success {
                            // Initial LIST failed — almost certainly permanent
                            // (RBAC, unknown resource, etc.). Fail immediately.
                            warn!("live_query: initial load failed for {}: {}", rt, e);
                            exit_reason = Some(format!("{}", e));
                            break;
                        }
                        if backoff_start.elapsed().as_millis() as u64 > MAX_ELAPSED_MS {
                            warn!("live_query: watcher for {} failed for over 2 minutes, giving up: {}", rt, e);
                            exit_reason = Some(format!("{}", e));
                            break;
                        }
                        warn!("live_query: watcher error for {}: {}, retrying in {}ms", rt, e, backoff_ms);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                    }
                }
            }
            _ = flush_timer.tick() => {
                // Intermediate init flush: progressive display for small stores.
                if init_dirty && !store.is_empty() && store.len() <= INIT_FLUSH_ROW_LIMIT {
                    info!("live_query: flushing intermediate snapshot for {}({}) ({} items)", rt, ns_label, store.len());
                    init_dirty = false;
                    let items = sorted_snapshot(&store);
                    let _ = snapshot_tx.send(WatcherSnapshot::Live(wrap(items)));
                }
                // Steady-state debounce: coalesce rapid Apply/Delete events
                // (e.g., node heartbeats) into one snapshot rebuild per tick
                // instead of one per event. With 2000 nodes, this reduces
                // bridge traffic from ~50 snapshots/sec to ~5.
                if steady_dirty {
                    steady_dirty = false;
                    let items = sorted_snapshot(&store);
                    let _ = snapshot_tx.send(WatcherSnapshot::Live(wrap(items)));
                }
            }
        }
    }

    // Transition to the terminal `Dead` state. Consumers see this via
    // `changed()` + `last_error()`; the subsequent `snapshot_tx` drop (when
    // the task returns) closes the channel so `changed()` returns `Err` on
    // the iteration after.
    let reason = exit_reason.unwrap_or_else(|| format!("watcher stream ended for {}", rt));
    let _ = snapshot_tx.send(WatcherSnapshot::Dead(reason));
}
