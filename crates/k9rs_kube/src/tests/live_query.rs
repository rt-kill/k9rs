//! `WatcherCache` as `ctl status` sees it.

use super::*;

// Named explicitly rather than inherited through `super::*`. These used to
// arrive for free from the one 1088-line `live_query.rs`'s own `use` lines;
// splitting it into `watcher` + `cache` left `mod.rs` holding only the doc
// and the re-exports, which is the right shape and makes this file say what
// it actually depends on.
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use kube::runtime::watcher::{self, Event as WatcherEvent};
use k9rs_proto::protocol::{
    ContextId, ContextName, Namespace, ResourceId, RowChange, TableDelta,
};
use k9rs_proto::row::ResourceRow;
use k9rs_proto::resource_def::BuiltInKind;

fn key(ctx: &str, resource: BuiltInKind) -> QueryKey {
    QueryKey {
        context: ContextId::new(
            ContextName::new(ctx).expect("non-empty"),
            format!("https://{ctx}"),
            1,
        ),
        namespace: Namespace::All,
        resource: ResourceId::BuiltIn(resource),
        filter: None,
    }
}

#[tokio::test]
async fn status_reports_live_watchers_and_forgets_dead_ones() {
    // `ctl status` used to answer only "running, since when", which says
    // nothing about the one thing that actually goes wrong in a long-lived
    // daemon: a watcher that should have been reclaimed and wasn't. The
    // report has to distinguish a live watcher from a cache slot whose `Weak`
    // no longer upgrades, or the number it prints is the number of slots ever
    // used rather than the number of watches currently open.
    let cache = WatcherCache::new();
    let (pods, _pods_driver) = cache.seed_driven(key("prod", BuiltInKind::Pod));
    let (nodes, _nodes_driver) = cache.seed_driven(key("prod", BuiltInKind::Node));

    let stats = cache.stats();
    assert_eq!(stats.len(), 2, "two live watchers");
    let mut resources: Vec<&str> = stats.iter().map(|s| s.resource.as_str()).collect();
    resources.sort_unstable();
    assert_eq!(resources, vec!["nodes", "pods"]);
    assert!(
        stats.iter().all(|s| s.context.as_str() == "prod"),
        "each row names the context that keyed it",
    );

    drop(pods);
    let stats = cache.stats();
    assert_eq!(stats.len(), 1, "a slot whose Weak is dead is not a watcher");
    assert_eq!(stats[0].resource, "nodes");

    drop(nodes);
    assert!(cache.stats().is_empty());
}

#[tokio::test]
async fn asking_what_the_daemon_holds_does_not_change_it() {
    // Reporting must not reap. The obvious implementation of `stats` reuses
    // `reap_dead`, which would make `ctl status` a mutation of the very thing
    // it claims to observe — and would quietly repair the leak it exists to
    // reveal, so the second run of `ctl status` always looks healthy.
    let cache = WatcherCache::new();
    let (pods, _driver) = cache.seed_driven(key("prod", BuiltInKind::Pod));
    drop(pods);

    assert!(cache.stats().is_empty(), "not reported");
    assert_eq!(cache.entry_count(), 1, "but not swept away either");
}

// ---------------------------------------------------------------------------
// watcher_tests (mirrored out of live_query.rs)
// ---------------------------------------------------------------------------

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
            cells: vec![k9rs_proto::row::CellValue::Text(val)],
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
        let (delta_tx, delta_rx) = broadcast::channel(fanout_ring());
        let (ask_tx, ask_rx) = mpsc::channel(16);
        let rid = k9rs_proto::protocol::ResourceId::BuiltIn(
            k9rs_proto::resource_def::BuiltInKind::ConfigMap,
        );
        let task = tokio::spawn(async move {
            run_typed_watcher(
                stream,
                delta_tx,
                ask_rx,
                convert,
                rid,
                vec!["NAME".into(), "V".into()],
                &k9rs_proto::protocol::Namespace::All,
            )
            .await;
        });
        Harness { events, delta_rx, ask_tx, task }
    }

    impl Harness {
        fn send(&self, ev: WatcherEvent<ConfigMap>) {
            self.events.send(Ok(ev)).expect("watcher task alive");
        }

        /// Push a watch error, driving the loop into its retry backoff.
        fn fail_stream(&self) {
            self.events
                .send(Err(watcher::Error::NoResourceVersion))
                .expect("watcher task alive");
        }

        async fn next_delta(&mut self) -> TableDelta {
            let msg = tokio::time::timeout(Duration::from_millis(800), self.delta_rx.recv())
                .await
                .expect("expected a delta within the flush window")
                .expect("channel open");
            match &*msg {
                WatcherMsg::Delta(d) => d.clone(),
                WatcherMsg::Health(h) => panic!("unexpected Health({h:?})"),
                WatcherMsg::Dead(r) => panic!("unexpected Dead({r})"),
            }
        }

        async fn expect_quiet(&mut self) {
            let res =
                tokio::time::timeout(Duration::from_millis(500), self.delta_rx.recv()).await;
            assert!(res.is_err(), "expected NO delta, got {:?}", res.map(|m| m.map(|a| format!("{:?}", a))));
        }

        fn ask(&self, after_initial_list: bool) -> oneshot::Receiver<BaselineReply> {
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

    impl Harness {
        /// The next `WatcherMsg::Health`, skipping deltas, or `None` if the
        /// window closes without one. Deltas are not the question here, and
        /// a failing relist can still flush pending changes.
        async fn next_health(&mut self, window: Duration) -> Option<WatchHealth> {
            let deadline = tokio::time::Instant::now() + window;
            loop {
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                if left.is_zero() {
                    return None;
                }
                match tokio::time::timeout(left, self.delta_rx.recv()).await {
                    Ok(Ok(msg)) => match &*msg {
                        WatcherMsg::Health(h) => return Some(h.clone()),
                        WatcherMsg::Delta(_) => continue,
                        WatcherMsg::Dead(r) => panic!("unexpected Dead({r})"),
                    },
                    Ok(Err(_)) => return None,
                    Err(_) => return None,
                }
            }
        }
    }

    /// A relist that keeps failing is eventually reported STALE.
    ///
    /// kube-runtime yields `Ok(Event::Init)` BEFORE it issues the list
    /// (`watcher.rs`, `State::Empty`) and returns to `State::Empty` when the
    /// list fails, so a permanently failing relist — RBAC revoked, CRD
    /// deleted, API group gone — emits `Err`, `Init`, `Err`, `Init`, … for
    /// as long as it lasts. The staleness clock was reset by `Init` as well
    /// as by real progress, so it only ever measured ONE list request: it
    /// never reached `DEGRADE_AFTER` and never reached `max_elapsed_ms`, and
    /// the watcher span at maximum backoff forever with frozen rows and no
    /// banner. That is the liveness-gate class the health signal exists for.
    ///
    /// Driven on a paused clock, which is why `first_failure` rides the
    /// runtime clock: the decision is a ten-second one and a test that
    /// actually waited would be ten seconds of nothing.
    #[tokio::test(start_paused = true)]
    async fn a_relist_that_keeps_failing_is_reported_stale() {
        let mut h = spawn_harness();
        h.init_steady(vec![cm("ns", "a", "1")]).await;

        // Failure one. The clock starts here.
        h.fail_stream();
        tokio::time::advance(Duration::from_millis(200)).await;
        // The retry's `Init` — the event that used to restart the clock.
        h.send(WatcherEvent::Init);
        tokio::time::advance(Duration::from_millis(200)).await;

        // Past DEGRADE_AFTER (10s), still well inside max_elapsed (120s).
        tokio::time::advance(Duration::from_secs(11)).await;
        h.fail_stream();

        let health = h.next_health(Duration::from_secs(2)).await;
        assert!(
            matches!(health, Some(WatchHealth::Degraded(_))),
            "eleven seconds of continuous failure must report frozen rows, got \
             {health:?} — an `Init` between two errors is not progress",
        );
    }

    /// A resource quiet for longer than the give-up window survives its
    /// first blip.
    ///
    /// The same clock was reset only by events, so a resource with no
    /// traffic — configmaps, secrets, nodes, roles, storageclasses, most of
    /// the list — had last touched it at `InitDone`. Its FIRST transient
    /// error was therefore already "two minutes of failures", and the
    /// watcher died: the bridge resubscribed with a full re-LIST and a full
    /// baseline resend, which is exactly the cost the delta machinery exists
    /// to avoid, on an ordinary watch restart.
    ///
    /// This one could NOT have been written against the old implementation,
    /// and that is worth knowing: its clock was a `std::time::Instant`,
    /// which `tokio::time::advance` cannot move, so the five minutes below
    /// were zero microseconds and the regime was unreachable from a test at
    /// all. Moving the clock onto the runtime is what makes the decision
    /// observable; this pins it from here on rather than reproducing a
    /// failure.
    #[tokio::test(start_paused = true)]
    async fn a_quiet_resource_survives_its_first_watch_error() {
        let mut h = spawn_harness();
        h.init_steady(vec![cm("ns", "a", "1")]).await;

        // Five minutes of nothing at all — the steady state of a quiet
        // resource, which yields no events (bookmarks and watch recycling
        // never surface as yielded events).
        tokio::time::advance(Duration::from_secs(300)).await;

        h.fail_stream();
        tokio::time::advance(Duration::from_millis(200)).await;

        // Alive: no Dead, and not even Degraded off one error.
        let health = h.next_health(Duration::from_millis(500)).await;
        assert!(
            health.is_none(),
            "one error after a quiet spell is a watch restart, not death: got {health:?}",
        );
        // And it still serves — the store was never torn down.
        h.send(WatcherEvent::Init);
        h.send(WatcherEvent::InitApply(cm("ns", "a", "2")));
        h.send(WatcherEvent::InitDone);
        let baseline = h.ask(false).await.expect("the watcher is still alive");
        assert_eq!(baseline.baseline.rows.len(), 1);
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
        assert_eq!(baseline.baseline.rows.len(), 1);
        assert_eq!(baseline.baseline.headers, vec!["NAME", "V"]);
        // Progressive init continues as deltas.
        h.send(WatcherEvent::InitApply(cm("ns", "b", "1")));
        h.send(WatcherEvent::InitDone);
        let d = h.next_delta().await;
        assert_eq!(keys_of(&d).0, vec!["b"]);
        // Steady ask answers immediately with the full store.
        let baseline = h.ask(false).await.expect("steady baseline");
        assert_eq!(baseline.baseline.rows.len(), 2);
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
        assert_eq!(baseline.baseline.rows.len(), 2, "store not cleared during relist");
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
        let mut names: Vec<_> = baseline.baseline.rows.iter().map(|r| r.name.clone()).collect();
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

    /// The at-most-one-change-per-key batch invariant: within ANY flush
    /// window, last-writer-wins and the row is cloned from the store at
    /// flush (apply→apply→delete collapses to one Remove; delete→apply
    /// collapses to one Upsert carrying the final value).
    ///
    /// Asserted over EVERY delta and their net effect, not over one delta.
    /// It used to send five events and read a single batch, which assumed
    /// all five landed in one window — and `tokio::select!` picks at RANDOM
    /// among ready branches, so the event stream and the flush timer are a
    /// coin flip every iteration. The batch boundary could fall mid-sequence
    /// and the first delta carried `Upsert(a)`: a real flake, about one run
    /// in forty, and `start_paused` does NOT fix it because the randomness
    /// is in the select, not the clock.
    ///
    /// The per-batch invariant is still checked exactly — on each batch —
    /// and the end state is what the caller actually depends on, so this is
    /// stronger than the version that happened to see one window.
    #[tokio::test]
    async fn pending_changes_coalesce_per_key() {
        let mut h = spawn_harness();
        h.init_steady(vec![cm("ns", "a", "1"), cm("ns", "b", "1")]).await;
        // a: modify, modify, delete → nets to a Remove.
        h.send(WatcherEvent::Apply(cm("ns", "a", "2")));
        h.send(WatcherEvent::Apply(cm("ns", "a", "3")));
        h.send(WatcherEvent::Delete(cm("ns", "a", "3")));
        // b: delete, recreate → nets to an Upsert with the final value.
        h.send(WatcherEvent::Delete(cm("ns", "b", "1")));
        h.send(WatcherEvent::Apply(cm("ns", "b", "9")));

        // Drain until quiet, folding each batch into the net effect.
        let mut net: std::collections::HashMap<String, Option<k9rs_proto::row::ResourceRow>> =
            std::collections::HashMap::new();
        let mut batches = 0usize;
        loop {
            let got = tokio::time::timeout(Duration::from_millis(800), h.delta_rx.recv()).await;
            let Ok(Ok(msg)) = got else { break };
            let WatcherMsg::Delta(d) = &*msg else { panic!("unexpected {msg:?}") };
            batches += 1;

            // THE invariant, on every batch rather than on a lucky one.
            let keys: std::collections::HashSet<_> = d.changes.iter().map(|c| c.key()).collect();
            assert_eq!(
                keys.len(),
                d.changes.len(),
                "at most one change per key, batch {batches}: {:?}",
                d.changes,
            );

            for c in &d.changes {
                // Keyed by NAME, as `keys_of` does — the fixture is one
                // namespace, so the name is the identity here.
                match c {
                    RowChange::Upsert(row) => {
                        net.insert(row.name.clone(), Some(row.clone()));
                    }
                    RowChange::Remove(k) => {
                        net.insert(k.name.clone(), None);
                    }
                }
            }
        }
        assert!(batches >= 1, "the sends produced at least one batch");

        // `a` is gone; `b` is present carrying the value it ended on.
        assert_eq!(net.get("a"), Some(&None), "a nets to a removal: {net:?}");
        let b = net
            .get("b")
            .expect("b was changed")
            .as_ref()
            .expect("b nets to an upsert, not a removal");
        assert_eq!(
            b.cells[0],
            k9rs_proto::row::CellValue::Text("9".into()),
            "the upsert carries the store's FINAL value, cloned at flush",
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

    /// A healthy watcher vouches for its own baseline. This is the channel
    /// through which a bridge attaching to an ALREADY-degraded shared
    /// watcher learns it missed the transition — the broadcast can't tell
    /// it, since its cursor is pinned after the transition went out.
    #[tokio::test]
    async fn a_baseline_carries_the_health_of_the_watch_that_built_it() {
        let mut h = spawn_harness();
        h.init_steady(vec![cm("ns", "a", "1")]).await;
        let reply = h.ask(false).await.expect("baseline");
        assert_eq!(reply.health, WatchHealth::Feeding);
        assert_eq!(reply.baseline.rows.len(), 1);
    }

    /// A backed-off watcher must still answer baseline asks.
    ///
    /// From the 2026-09-09 audit: the retry `sleep` ran inside a `select!`
    /// branch HANDLER, which suspends the whole loop — so for up to 30s the
    /// watcher stopped answering `ask_rx` and stopped flushing. A client
    /// attaching in that window sat on a spinner over rows the watcher
    /// already had.
    #[tokio::test]
    async fn a_backed_off_watcher_still_answers_baseline_asks() {
        let mut h = spawn_harness();
        h.init_steady(vec![cm("ns", "a", "1")]).await;
        // Drive the watch into its error/backoff path.
        h.fail_stream();
        // Let the loop actually CONSUME the error and enter its backoff —
        // otherwise the ask and the error race in the `select!` and the ask
        // can simply win, which would make this test prove nothing.
        tokio::time::sleep(Duration::from_millis(60)).await;
        // The ask must be answered from the store, not queued behind the
        // backoff. The window is deliberately SHORTER than `initial_ms`
        // (300ms) — with the sleep inside the branch handler the loop cannot
        // reach `ask_rx` until it expires, so this is what distinguishes the
        // two implementations.
        let reply = tokio::time::timeout(Duration::from_millis(120), h.ask(false))
            .await
            .expect("ask answered while backing off")
            .expect("watcher alive");
        assert_eq!(reply.baseline.rows.len(), 1, "served from the resident store");
    }

    /// The two subtleties of the degrade rule, without a cluster or a clock:
    /// a single error is not staleness, and a degradation is announced once.
    #[test]
    fn one_watch_error_is_not_staleness_and_degradation_is_announced_once() {
        let brief = DEGRADE_AFTER / 2;
        let long = DEGRADE_AFTER * 2;

        // Routine restart: kube-rs surfaces it as an error and recovers
        // inside a backoff step. Flagging it would strobe every open table.
        assert_eq!(WatchHealth::Feeding.after_error(brief, "hiccup"), None);

        // Nothing has moved for long enough that "live" is a lie.
        assert_eq!(
            WatchHealth::Feeding.after_error(long, "connection reset"),
            Some(WatchHealth::Degraded("connection reset".into())),
        );

        // Already said so — retries must not re-broadcast it (the ring is
        // small, and a repeat would lag real subscribers into a re-baseline).
        let degraded = WatchHealth::Degraded("connection reset".into());
        assert_eq!(degraded.after_error(long, "connection reset again"), None);
    }
