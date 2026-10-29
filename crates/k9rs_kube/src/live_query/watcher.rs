//! The watcher task: what it publishes, and the loop that produces it.
//!
//! Split out of the 1088-line `live_query` module on a measured seam — this
//! half names the cache half ZERO times in code. See the module doc for the
//! direction of the edge.
//!
//! The loop is where liveness is decided: `first_failure` is the clock both
//! the "rows are frozen" and the "give up" rules read, and the comment on it
//! is the record of what happens when an event that is not progress resets
//! it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;


use futures::{StreamExt, TryStreamExt};
use kube::runtime::watcher::{self, Event as WatcherEvent};
use kube::{Api, Resource};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, info, warn};

use k9rs_proto::protocol::{
    ObjectKey, RowChange, TableBaseline, TableDelta,
};
use k9rs_proto::row::ResourceRow;
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
pub enum WatcherMsg {
    Delta(TableDelta),
    /// A health TRANSITION, broadcast the moment it happens and never
    /// repeated. A subscriber that attaches DURING a degraded stretch
    /// therefore can't learn from the ring — its cursor is pinned after the
    /// transition went out — so the current value also rides every
    /// [`BaselineReply`].
    Health(WatchHealth),
    Dead(String),
}

/// Whether the watch behind this store is still feeding it — the
/// cluster-side half of liveness (the client↔daemon half is the TUI's
/// `Connection`). Rows are only as live as the watch that produces them,
/// and a broken watch is patient: it relists with backoff for two minutes
/// before it gives up and reports [`WatcherMsg::Dead`], during which every
/// row on screen is frozen. Without a health signal that whole window is
/// indistinguishable from a quiet cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchHealth {
    /// The watch is live: the store tracks the cluster.
    Feeding,
    /// The watch stream is failing and relisting behind a backoff. The
    /// store still holds its last known rows; they just stopped moving.
    Degraded(String),
}

impl WatchHealth {
    /// The transition to broadcast after a watch error, or `None` to stay
    /// put. A pure rule so the two things that make it subtle are testable
    /// without a cluster: one error is NOT staleness (kube-rs surfaces
    /// routine watch restarts as errors and recovers inside a backoff step,
    /// and flagging those would strobe a banner across every open table), and
    /// a degradation is announced ONCE, not re-broadcast per retry.
    ///
    /// `stuck_for` is time since the watch last made progress — not time
    /// since the error — because that is what actually decides whether the
    /// rows on screen have gone cold.
    /// `pub(crate)` since the split: the test mirror now hangs off
    /// `live_query/mod.rs` rather than off this file, so a private method
    /// is no longer reachable from it. The decision it makes is the one
    /// `a_relist_that_keeps_failing_is_reported_stale` is about.
    pub(crate) fn after_error(&self, stuck_for: std::time::Duration, err: &str) -> Option<WatchHealth> {
        match self {
            WatchHealth::Degraded(_) => None,
            WatchHealth::Feeding if stuck_for > DEGRADE_AFTER => {
                Some(WatchHealth::Degraded(err.to_string()))
            }
            WatchHealth::Feeding => None,
        }
    }
}

/// A baseline plus the health of the watch that produced it. They travel
/// together because a joining subscriber pins its broadcast cursor BEFORE
/// asking, so it can never see the [`WatcherMsg::Health`] transition that
/// preceded its own attach — and would otherwise treat an already-degraded
/// watcher's rows as live until the next transition.
pub struct BaselineReply {
    pub baseline: TableBaseline,
    pub health: WatchHealth,
}

/// Baseline request, answered by the watcher task from its store. The
/// asker must hold its broadcast receiver BEFORE sending the ask (cursor
/// pinned first): the reply is built after the cursor position, so
/// baseline and buffered deltas can only overlap, never gap.
pub struct BaselineAsk {
    /// `true` (bridge resubscribe behind a client with stale data): defer
    /// the reply until the in-progress LIST completes, so the client swaps
    /// once, atomically. `false` (fresh join): reply as soon as there is
    /// anything real to show — deferred only while the FIRST list is still
    /// empty, so a joining client never sees a false "No resources found".
    pub after_initial_list: bool,
    pub reply: oneshot::Sender<BaselineReply>,
}

/// Broadcast ring capacity, in delta batches, from config (default 64).
/// At one batch per 200ms flush tick, 64 is ~13s of sustained-churn
/// tolerance for a stalled consumer before it must re-baseline. See
/// [`crate::daemon_config::DaemonConfig::fanout_ring`] for the
/// memory-vs-lag-tolerance tradeoff.
pub(super) fn fanout_ring() -> usize {
    // No `.max(1)`: the field is `NonZeroUsize`, so a `0` is refused where
    // the config file is read rather than repaired here. This was the one
    // of six knobs with a reader-side clamp — because this one PANICS.
    crate::daemon_config::daemon_config().fanout_ring.get()
}

/// How many un-answered [`BaselineAsk`]s the watcher's inbox holds.
///
/// One ask per joining or resubscribing bridge, answered from the watcher's
/// own store, so the queue only grows while the watcher is busy — mid-LIST, or
/// asleep in its retry backoff. 16 is "more sessions than will ever pile onto
/// one watcher in that window"; past it, `ask()` applies backpressure and
/// waits, which is deliberate (see `Subscription::request_baseline` in the
/// `cache` half, on why
/// dropping an ask would be mis-read as watcher death).
///
/// Named because the number was written three times — at both real
/// constructors and at the test seeder — and three spellings of one capacity
/// is three places to disagree about what it means.
pub(super) const ASK_QUEUE: usize = 16;

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

fn watcher_page_size() -> u32 {
    crate::daemon_config::daemon_config().watcher_page_size.get()
}


/// Delta flush interval — one broadcast batch per tick, at most. Also the
/// progressive-paint cadence during an initial LIST (no size gate: each
/// row is sent exactly once, so total init bytes ≈ one full list at any
/// cluster size — the old 2000-row cap is gone, dissolved not raised).
const INIT_FLUSH_INTERVAL_MS: u64 = 200;
// Staleness (a half-open TCP connection after suspend/network loss) is
// detected by the TRANSPORT, not by this module: `client_build.rs` sets
// `read_timeout: 300s` on the kube Config, and the watch request's server
// side recycles every ~290s (`timeoutSeconds` default) with bookmarks
// keeping bytes flowing in between — so a live-but-quiet watch always
// delivers bytes inside the read window, and a dead one errors the stream
// within ~5 minutes, landing in the normal error/backoff/relist path
// below. An event-level timer here CANNOT do this job: "no yielded events
// in N seconds" is the steady state of any quiet resource (bookmarks and
// watch recycling never surface as yielded events), so such a timer either
// never fires (the bug this comment replaced: a `timeout()` rebuilt every
// select iteration, reset by the 200ms flush tick) or kills healthy
// watchers.
/// How long a watch must stay stuck before its rows are declared frozen.
/// Below this, an error is just a watch restart — kube-rs surfaces those
/// and recovers inside a backoff step, and reporting each one would flicker
/// a staleness banner across every open table on routine churn. Above it,
/// nothing has moved for long enough that "live" is a lie.
pub(super) const DEGRADE_AFTER: std::time::Duration = std::time::Duration::from_secs(10);

/// Backoff config — loaded from daemon config at runtime.
fn initial_backoff_ms() -> u64 {
    crate::daemon_config::daemon_config().backoff.initial_ms.get()
}
fn max_backoff_ms() -> u64 {
    crate::daemon_config::daemon_config().backoff.max_ms.get()
}
fn max_elapsed_ms() -> u64 {
    crate::daemon_config::daemon_config().backoff.max_elapsed_ms
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
/// and server-side filters (Labels/Field push down to the API; OwnerUid and
/// References are post-filtered per-change in the bridge because the K8s API
/// can filter on neither). Split from the loop so tests can inject a stream.
pub(crate) fn watch_stream<K, D>(
    api: Api<K>,
    filter: &Option<k9rs_proto::protocol::SubscriptionFilter>,
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
        Some(k9rs_proto::protocol::SubscriptionFilter::Labels(map)) => {
            let sel = k9rs_proto::protocol::SubscriptionFilter::labels_to_selector(map);
            watcher_config = watcher_config.labels(&sel);
        }
        Some(k9rs_proto::protocol::SubscriptionFilter::Field(f)) => {
            watcher_config = watcher_config.fields(f);
        }
        // Post-filtered in the bridge (`row_passes`): neither an
        // ownerReference nor a spec's references can be selected on.
        Some(
            k9rs_proto::protocol::SubscriptionFilter::OwnerUid(_)
            | k9rs_proto::protocol::SubscriptionFilter::References { .. },
        )
        | None => {}
    }
    watcher::watcher(api, watcher_config).boxed()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_typed_watcher<K, C, D>(
    mut stream: futures::stream::BoxStream<'static, Result<WatcherEvent<K>, watcher::Error>>,
    delta_tx: broadcast::Sender<Arc<WatcherMsg>>,
    mut ask_rx: mpsc::Receiver<BaselineAsk>,
    convert: C,
    resource_id: k9rs_proto::protocol::ResourceId,
    headers: Vec<String>,
    namespace: &k9rs_proto::protocol::Namespace,
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
    let mut deferred_joins: Vec<oneshot::Sender<BaselineReply>> = Vec::new();
    let mut deferred_relist: Vec<oneshot::Sender<BaselineReply>> = Vec::new();

    let mut backoff_ms: u64 = initial_backoff_ms();
    // When this watch started failing CONTINUOUSLY, or `None` while it is
    // healthy. Both liveness decisions below read it: `DEGRADE_AFTER` to
    // call the rows frozen, `max_elapsed_ms` to give up and report `Dead`.
    //
    // This was an `Instant` reset by `Init` as well as by real progress,
    // and `Init` is not progress: kube-runtime yields `Ok(Event::Init)`
    // BEFORE it issues the list (`watcher.rs` `State::Empty`) and returns
    // to `State::Empty` when the list fails, so **every retry re-emitted
    // it**. Two opposite failures came out of that one line:
    //
    //   - a permanently failing relist (RBAC revoked, CRD deleted, API
    //     group gone) cycled `Init` → list fails → `Err`, so the clock only
    //     ever measured one list request. It never reached `DEGRADE_AFTER`
    //     and never reached `max_elapsed_ms`: the watcher span at maximum
    //     backoff forever, never `Degraded`, never `Dead`, rows frozen on
    //     screen with no banner — the liveness-gate class this file's
    //     health signal exists for.
    //   - and a resource quiet for over two minutes — configmaps, secrets,
    //     nodes, roles, storageclasses, most of the list — had last reset
    //     the clock at its `InitDone`, so its FIRST transient error was
    //     already "two minutes of failures" and killed the watcher. The
    //     bridge then resubscribed with a full re-LIST and a full baseline
    //     resend, which is the cost the delta machinery exists to avoid,
    //     on an ordinary watch blip.
    //
    // An `Option` rather than an always-set `Instant` because "not failing"
    // is a different state from "failing since now", and the old type could
    // not tell them apart. Cleared by `InitDone` (the relist completed, the
    // store is authoritative again) and by `Apply`/`Delete` (steady-state
    // progress) — not by `InitApply`, since a relist that always dies on
    // page two would otherwise reset the clock on page one of every cycle
    // and resurrect the first failure above.
    // The RUNTIME clock, like `retry_at` below and for the same reason: the
    // two are read together (a retry deadline and how long retries have been
    // failing), and a decision driven by a clock no test can advance is a
    // decision no test can reach. `Instant::elapsed` yields a
    // `std::time::Duration` either way, so `after_error` is unchanged.
    let mut first_failure: Option<tokio::time::Instant> = None;
    // When the watch may be polled again after an error. A DEADLINE rather
    // than an inline sleep: `select!` only cancels branch *futures*, so a
    // `sleep().await` inside a branch HANDLER suspends the whole loop —
    // which meant a backed-off watcher (up to 30s) stopped answering
    // baseline asks and stopped flushing. A client attaching in that window
    // got a spinner over perfectly good rows, with no diagnostic, until the
    // sleep ended. Gating only the stream branch keeps `ask_rx` and the
    // flush timer live throughout.
    let mut retry_at: Option<tokio::time::Instant> = None;

    let mut flush_timer = tokio::time::interval(std::time::Duration::from_millis(INIT_FLUSH_INTERVAL_MS));
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let rt = resource_id.plural().to_string();
    let ns_label: &str = match namespace {
        k9rs_proto::protocol::Namespace::All => "all",
        k9rs_proto::protocol::Namespace::Named(n) => n.as_str(),
    };

    // `health` is a parameter rather than a capture so the closure stays
    // usable while the loop below mutates it.
    let make_baseline = |store: &HashMap<ObjectKey, ResourceRow>, health: &WatchHealth| BaselineReply {
        baseline: TableBaseline {
            headers: headers.clone(),
            rows: store.values().cloned().collect(),
        },
        health: health.clone(),
    };

    let mut exit_reason: Option<String> = None;
    let mut health = WatchHealth::Feeding;

    loop {
        tokio::select! {
            // Not polled while backing off (see `retry_at`).
            event_result = stream.try_next(), if retry_at.is_none() => {
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
                                debug!(resource = %rt, namespace = %ns_label, "listing");
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
                                first_failure = None;
                                // The relist finished: the store is
                                // authoritative again. Announce recovery
                                // BEFORE the flush and the deferred replies
                                // below, so both carry the fresh truth.
                                if health != WatchHealth::Feeding {
                                    health = WatchHealth::Feeding;
                                    let _ = delta_tx.send(Arc::new(WatcherMsg::Health(health.clone())));
                                }
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
                                info!(resource = %rt, namespace = %ns_label, items = store.len(), "watch established");
                                // Flush accumulated changes immediately —
                                // the user is waiting.
                                if !pending.is_empty() {
                                    let _ = delta_tx.send(Arc::new(WatcherMsg::Delta(pending.drain(&store))));
                                }
                                // Answer everyone parked on "after the list".
                                for reply in deferred_relist.drain(..).chain(deferred_joins.drain(..)) {
                                    let _ = reply.send(make_baseline(&store, &health));
                                }
                            }
                            WatcherEvent::Apply(obj) => {
                                backoff_ms = initial_backoff_ms();
                                first_failure = None;
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
                                first_failure = None;
                                let key = obj_key(&obj);
                                if store.remove(&key).is_some() {
                                    pending.remove(key);
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        debug!(resource = %rt, "watch stream ended");
                        if !pending.is_empty() {
                            let _ = delta_tx.send(Arc::new(WatcherMsg::Delta(pending.drain(&store))));
                        }
                        break;
                    }
                    Err(e) => {
                        // Fail-fast iff the FIRST list never completed:
                        // almost certainly permanent (RBAC, unknown type).
                        if matches!(phase, ListPhase::Listing { first: true, .. }) {
                            warn!(resource = %rt, "initial load failed: {e}");
                            exit_reason = Some(format!("{}", e));
                            break;
                        }
                        // One clock, read at two thresholds. Started HERE,
                        // on the first failure of a run, so it measures how
                        // long this watch has been failing continuously —
                        // which is what both rules want and what `Init`
                        // resetting it made impossible. See `first_failure`.
                        let failing_for =
                            first_failure.get_or_insert_with(tokio::time::Instant::now).elapsed();
                        if failing_for.as_millis() as u64 > max_elapsed_ms() {
                            warn!(resource = %rt, "giving up after 2 minutes of failures: {e}");
                            exit_reason = Some(format!("{}", e));
                            break;
                        }
                        // The log level follows the SAME threshold as the
                        // staleness decision, for the same reason: kube-rs
                        // surfaces routine watch restarts as errors, so a
                        // warn per retry is noise on a healthy cluster. Warn
                        // ONCE, when the failure has outlived a restart and
                        // we start telling clients their rows are frozen;
                        // every retry before and after that stays at debug.
                        match health.after_error(failing_for, &format!("{}", e)) {
                            Some(next) => {
                                warn!(resource = %rt, "watch stalled, rows are frozen: {e}");
                                health = next;
                                let _ = delta_tx.send(Arc::new(WatcherMsg::Health(health.clone())));
                            }
                            None => debug!(resource = %rt, "watch retry in {backoff_ms}ms: {e}"),
                        }
                        retry_at = Some(
                            tokio::time::Instant::now()
                                + std::time::Duration::from_millis(backoff_ms),
                        );
                        backoff_ms = (backoff_ms * 2).min(max_backoff_ms());
                    }
                }
            }
            // Backoff expiry — re-enables the stream branch above.
            _ = async { if let Some(at) = retry_at { tokio::time::sleep_until(at).await } },
                if retry_at.is_some() =>
            {
                retry_at = None;
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
                        let _ = reply.send(make_baseline(&store, &health));
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
                    let _ = ask.reply.send(make_baseline(&store, &health));
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
// Test seam: BE the watcher
// ---------------------------------------------------------------------------

/// The write ends of a [`LiveQuery`]'s two channels, so a test can play the
/// watcher: push [`WatcherMsg`]s onto the fanout and answer baseline asks by
/// hand. Everything downstream of it — the `Subscription`, the substream
/// bridge, the bincode frames, the muxer — is the real thing.
///
/// Paired with [`WatcherCache::seed_driven`].
#[cfg(any(test, feature = "testing"))]
pub struct DrivenWatcher {
    /// Fanout ring. `send` errors while nobody is attached, so a test should
    /// wait for the bridge's first baseline ask before pushing.
    pub delta_tx: broadcast::Sender<Arc<WatcherMsg>>,
    /// Baseline asks arriving from the bridge, to answer (or deliberately
    /// leave hanging, or drop to simulate a watcher dying mid-ask).
    pub ask_rx: mpsc::Receiver<BaselineAsk>,
    /// Ends the stand-in watcher task, making `watcher_finished()` true.
    pub stop: Option<oneshot::Sender<()>>,
}

#[cfg(any(test, feature = "testing"))]
impl DrivenWatcher {
    /// Take the next baseline ask, failing the test if none arrives. Receiving
    /// one is also the proof that the bridge has attached and pinned its
    /// broadcast cursor — i.e. that `delta_tx.send` will now reach it.
    /// Test-only: the harness drives a watcher by answering its asks.
    #[cfg(any(test, feature = "testing"))]
    pub async fn next_ask(&mut self) -> BaselineAsk {
        tokio::time::timeout(std::time::Duration::from_secs(2), self.ask_rx.recv())
            .await
            .expect("the bridge should ask for a baseline")
            .expect("ask channel open")
    }

    /// Push a message onto the fanout, failing if nobody is listening — a
    /// silent `Err` here would make every downstream assertion vacuous.
    pub fn push(&self, msg: WatcherMsg) {
        self.delta_tx.send(Arc::new(msg)).expect("a bridge should be attached");
    }
}
