//! Who is watching what: the per-process registry and the handles that keep
//! a watcher alive.
//!
//! Split out of the 1088-line `live_query` module. This half BUILDS watchers
//! and holds the channels they write to, which is the one-way edge the split
//! rests on — see the module doc.
//!
//! Lifetime is ownership here, not policy: a `Subscription` is a strong hold,
//! its `Drop` opens the grace window, and the `Weak` in the registry is what
//! lets the last holder's departure actually end the watcher.

use std::sync::Arc;


use kube::api::GroupVersionKind;
use kube::Client;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;

use k9rs_proto::protocol::ResourceScope;

use super::watcher::{fanout_ring, BaselineAsk, BaselineReply, WatcherMsg, ASK_QUEUE};
// `DrivenWatcher` is gated, so the IMPORT has to be too — otherwise a plain
// `cargo build` cannot resolve it while `cargo test` can, which is the same
// trap in mirror image as deleting an import only the test path used.
#[cfg(any(test, feature = "testing"))]
use super::watcher::DrivenWatcher;
// ---------------------------------------------------------------------------
// QueryKey
// ---------------------------------------------------------------------------

/// Key for looking up a live query in the cache.
/// Uses `ContextId` so watchers are shared by actual cluster endpoint,
/// not by context name (which can collide across kubeconfig files).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct QueryKey {
    pub context: k9rs_proto::protocol::ContextId,
    pub namespace: k9rs_proto::protocol::Namespace,
    pub resource: k9rs_proto::protocol::ResourceId,
    /// Server-side filter. A filtered subscription creates a separate watcher
    /// from an unfiltered one (different API queries to K8s).
    pub filter: Option<k9rs_proto::protocol::SubscriptionFilter>,
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
    /// Keep-warm window after the last `Subscription` drops. See
    /// [`crate::grace::GraceGate`] — it coalesces to at most one grace task
    /// AND measures the window from the last dropper, so a second session
    /// leaving doesn't inherit the first one's nearly-expired timer.
    grace: crate::grace::GraceGate,
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
impl Drop for Subscription {
    fn drop(&mut self) {
        // Shared grace dance (see `crate::grace::spawn_grace`). A *dead*
        // watcher (self-terminated task) skips grace so its cache entry is
        // reclaimed and the next subscribe builds a fresh watcher; a live one
        // is kept warm for the window.
        crate::grace::spawn_grace(
            self._keepalive.clone(),
            |lq| lq.task.is_finished(),
            |lq| Some(&lq.grace),
        );
    }
}

impl Subscription {
    /// Ask the watcher for a full baseline. `Some(rx)` = the ask is
    /// enqueued; the oneshot resolves when the (possibly deferred) reply
    /// arrives, or errors if the watcher dies after taking it. `None` =
    /// the ask channel is CLOSED, i.e. the watcher task is truly gone.
    ///
    /// Backpressure (`send().await`) rather than `try_send`: if the ask
    /// channel is momentarily full — the watcher is sleeping in its
    /// inline retry backoff — we WAIT for it to drain instead of dropping
    /// the ask and mis-reading that drop as watcher death (which used to
    /// send a live watcher's bridge into a permanent wedge). The send is
    /// awaited to completion before the caller `select!`s on the reply,
    /// so there is no borrow conflict.
    pub async fn request_baseline(
        &self,
        after_initial_list: bool,
    ) -> Option<oneshot::Receiver<BaselineReply>> {
        let (reply, rx) = oneshot::channel();
        match self.ask.send(BaselineAsk { after_initial_list, reply }).await {
            Ok(()) => Some(rx),
            Err(_) => None,
        }
    }

    /// Whether the watcher task behind this subscription has exited. Used
    /// to bound death-reason draining — the `Subscription` keeps the
    /// broadcast `Sender` alive, so the channel's own `Closed` signal can
    /// never fire.
    pub fn watcher_finished(&self) -> bool {
        self._keepalive.task.is_finished()
    }

    /// Receive the next delta batch (or terminal `Dead`). `Err(Lagged)` =
    /// this subscriber fell behind the ring: recover by re-asking for a
    /// baseline and continuing — overlap with buffered batches is
    /// idempotent. `Err(Closed)` = the watcher task exited.
    pub async fn recv(&mut self) -> Result<Arc<WatcherMsg>, broadcast::error::RecvError> {
        self.rx.recv().await
    }
}

// ---------------------------------------------------------------------------
// WatcherCache
// ---------------------------------------------------------------------------

/// Per-process cache of live queries. Stores `Weak` references so watchers
/// die naturally when no one holds a `Subscription`.
pub struct WatcherCache {
    entries: crate::weak_registry::WeakRegistry<QueryKey, LiveQuery>,
}

impl Default for WatcherCache {
    fn default() -> Self {
        Self::new()
    }
}

impl WatcherCache {
    pub fn new() -> Self {
        Self {
            entries: crate::weak_registry::WeakRegistry::new(),
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
        kind: k9rs_proto::resource_def::BuiltInKind,
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
        let label = if matches!(&key.resource, k9rs_proto::protocol::ResourceId::Crd(_) | k9rs_proto::protocol::ResourceId::CrdUnresolved(_)) {
            "dynamic"
        } else {
            "typed"
        };

        // The liveness predicate is the whole of what makes this registry
        // different from the three others: a watcher's task can self-finish
        // while its `Arc` is still held, and handing that out gives the
        // caller a corpse. The other registries pass `|_| true`.
        let (live_query, outcome) = self.entries.get_or_create(
            key.clone(),
            |arc| !arc.task.is_finished(),
            create,
        );
        match outcome {
            crate::weak_registry::Outcome::Reused => {
                tracing::debug!("WatcherCache: reusing existing {} watcher for {:?}", label, key);
            }
            crate::weak_registry::Outcome::ReplacedDead => {
                tracing::debug!("WatcherCache: existing {} watcher was dead, replaced for {:?}", label, key);
            }
            crate::weak_registry::Outcome::Created => {
                tracing::debug!("WatcherCache: creating new {} watcher for {:?}", label, key);
            }
        }
        Subscription::attach(key, live_query)
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
        kind: k9rs_proto::resource_def::BuiltInKind,
        client: &Client,
        streaming_lists: bool,
    ) -> Arc<LiveQuery> {
        let (delta_tx, _) = broadcast::channel(fanout_ring());
        let (ask_tx, ask_rx) = mpsc::channel(ASK_QUEUE);

        let args = crate::resource_defs::registry::WatcherArgs {
            client: client.clone(),
            namespace: key.namespace.clone(),
            delta_tx: delta_tx.clone(),
            ask_rx,
            filter: key.filter.clone(),
            streaming_lists,
        };

        let task = crate::resource_defs::REGISTRY.spawn_watcher_for_kind(kind, args);

        Arc::new(LiveQuery {
            delta_tx,
            ask_tx,
            task,
            key: key.clone(),
            grace: crate::grace::GraceGate::new(crate::grace::watcher_grace()),
        })
    }

    /// Cache slots, live or not. Test-only: the difference between this and
    /// `stats().len()` is exactly the dead-slot backlog, which is what pins
    /// that reporting doesn't reap.
    #[cfg(test)]
    pub(crate) fn entry_count(&self) -> usize {
        self.entries.slot_count()
    }

    /// Every LIVE watcher, for `ctl status`. Dead `Weak`s are skipped rather
    /// than reaped — reporting must not mutate what it reports on.
    pub fn stats(&self) -> Vec<k9rs_proto::protocol::WatcherStat> {
        self.entries
            .live()
            .into_iter()
            .map(|(key, _lq, holders)| k9rs_proto::protocol::WatcherStat {
                context: key.context.name.clone(),
                namespace: key.namespace.display().to_string(),
                resource: key.resource.plural().to_string(),
                filter: key.filter.as_ref().map(|f| format!("{f:?}")),
                holders,
            })
            .collect()
    }

    /// TEST SEAM: seed the cache with a watcher the TEST drives.
    ///
    /// The subscription bridge obtains its `Subscription` from this cache, so
    /// this is the one place a test can stand in for a cluster without
    /// touching the bridge's own code: the next `subscribe` for `key` takes
    /// the ordinary reuse path, finds this entry live, and attaches. The
    /// client factory is never called, no watcher task is spawned, and every
    /// line of the bridge under test is the production one.
    ///
    /// Returns the `Arc` keepalive alongside the driver — the cache holds only
    /// a `Weak`, so dropping the `Arc` is how a test makes the watcher vanish.
    #[cfg(any(test, feature = "testing"))]
    pub fn seed_driven(&self, key: QueryKey) -> (Arc<LiveQuery>, DrivenWatcher) {
        let (delta_tx, _initial_rx) = broadcast::channel(fanout_ring());
        let (ask_tx, ask_rx) = mpsc::channel(ASK_QUEUE);
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        // A task that parks until the test ends it. `LiveQuery` needs one
        // because `task.is_finished()` is how the rest of the system asks
        // "is this watcher dead?" — bounding the bridge's death drain and
        // deciding whether a dropped Subscription gets a grace window.
        let task = tokio::spawn(async move {
            let _ = stop_rx.await;
        });
        let lq = Arc::new(LiveQuery {
            delta_tx: delta_tx.clone(),
            ask_tx,
            task,
            key: key.clone(),
            grace: crate::grace::GraceGate::new(crate::grace::watcher_grace()),
        });
        self.entries.insert(key, &lq);
        (lq, DrivenWatcher { delta_tx, ask_rx, stop: Some(stop_tx) })
    }

    /// Force-subscribe: build a new watcher and put it in the slot
    /// unconditionally, in one locked step (`subscribe_with_force`).
    ///
    /// Deliberately NOT behind the read-only gate, and the gate is not where
    /// to put it: a forced relist changes nothing in the cluster, and
    /// `--readonly` exists to refuse mutations, not to refuse reading again.
    /// Ctrl-R has to work in a read-only session.
    ///
    /// The displaced watcher lives as long as any OTHER session's
    /// `Subscription` holds it — grace only starts once the last one drops —
    /// so on a shared daemon one client's Ctrl-R can leave two watchers (two
    /// apiserver watches, two row stores) for one key until those
    /// subscribers go away. Bounded and self-resolving, but not the 60s the
    /// word "grace" suggests. The cache points at the new watcher from the
    /// moment this returns, so anyone subscribing after it gets that one.
    pub fn subscribe_force(
        &self,
        key: QueryKey,
        kind: k9rs_proto::resource_def::BuiltInKind,
        client: &Client,
        streaming_lists: bool,
    ) -> Subscription {
        self.subscribe_with_force(key, |k| {
            Self::create_watcher(k, kind, client, streaming_lists)
        })
    }

    /// `subscribe_with`'s forcing twin: build a new watcher and put it in the
    /// slot, whatever was there.
    ///
    /// `replace` does both halves UNDER the entry lock, so a concurrent
    /// normal subscribe's check-and-insert is serialized with this rather
    /// than the two interleaving and stranding a just-created watcher
    /// unreachable in the map.
    ///
    /// Shared with the dynamic path, which used to do its own forcing as
    /// `remove(key)` followed by an ordinary get-or-create. Those are two
    /// operations with a gap between them: a sibling session subscribing in
    /// the gap found an EMPTY slot, created its own watcher and filled it,
    /// and the forcing session's get-or-create then happily REUSED that
    /// watcher — so Ctrl-R on a CRD could leave you attached to the very
    /// watcher you asked to discard. Forcing is one decision; it gets one
    /// implementation.
    fn subscribe_with_force<F>(&self, key: QueryKey, create: F) -> Subscription
    where
        F: FnOnce(&QueryKey) -> Arc<LiveQuery>,
    {
        self.entries.reap();
        let live_query = self.entries.replace(key.clone(), create);
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
    // The args are the dynamic-resource descriptor (gvk/plural/scope) plus
    // the discovery handle (the watcher resolves its own printer columns
    // at birth), the client factory, and the streaming flag — all
    // intrinsic to opening a watcher. Kept flat rather than threaded
    // through a struct in this hot path.
    #[allow(clippy::too_many_arguments)]
    pub fn subscribe_dynamic(
        &self,
        key: QueryKey,
        make_client: impl FnOnce() -> Client,
        gvk: GroupVersionKind,
        plural: String,
        scope: ResourceScope,
        discovery: Arc<crate::cache::DiscoveryCache>,
        streaming_lists: bool,
    ) -> Subscription {
        self.subscribe_with(key, move |k| {
            Self::create_dynamic_watcher(k, &make_client(), gvk, plural, scope, discovery, streaming_lists)
        })
    }

    /// [`Self::subscribe_dynamic`] forcing a fresh watcher — the CRD twin of
    /// [`Self::subscribe_force`], through the same one-locked-step
    /// `subscribe_with_force`. See that function for what
    /// `remove`-then-subscribe did instead.
    #[allow(clippy::too_many_arguments)]
    pub fn subscribe_dynamic_force(
        &self,
        key: QueryKey,
        make_client: impl FnOnce() -> Client,
        gvk: GroupVersionKind,
        plural: String,
        scope: ResourceScope,
        discovery: Arc<crate::cache::DiscoveryCache>,
        streaming_lists: bool,
    ) -> Subscription {
        self.subscribe_with_force(key, move |k| {
            Self::create_dynamic_watcher(k, &make_client(), gvk, plural, scope, discovery, streaming_lists)
        })
    }

    /// Internal: spawn a dynamic watcher task and return the LiveQuery + initial receiver.
    fn create_dynamic_watcher(
        key: &QueryKey,
        client: &Client,
        gvk: GroupVersionKind,
        plural: String,
        scope: ResourceScope,
        discovery: Arc<crate::cache::DiscoveryCache>,
        streaming_lists: bool,
    ) -> Arc<LiveQuery> {
        let (delta_tx, _) = broadcast::channel(fanout_ring());
        let (ask_tx, ask_rx) = mpsc::channel(ASK_QUEUE);
        let task_client = client.clone();
        let task_ns = key.namespace.clone();
        let task_tx = delta_tx.clone();
        let task_context = key.context.clone();
        let task = tokio::spawn(async move {
            crate::live_query_dynamic::run_dynamic_live_watcher(task_client, task_ns, task_tx, ask_rx, gvk, plural, scope, discovery, task_context, streaming_lists).await;
        });
        Arc::new(LiveQuery {
            delta_tx,
            ask_tx,
            task,
            key: key.clone(),
            grace: crate::grace::GraceGate::new(crate::grace::watcher_grace()),
        })
    }
}
