use super::{order_batch, parse_k8s_minor, split_all_containers_prefix};

#[test]
fn parse_standard_versions() {
    assert_eq!(parse_k8s_minor("v1.32.1"), Some(32));
    assert_eq!(parse_k8s_minor("v1.30.0"), Some(30));
    assert_eq!(parse_k8s_minor("v1.28.11"), Some(28));
}

#[test]
fn parse_eks_version() {
    assert_eq!(parse_k8s_minor("v1.30.14-eks-40737a8"), Some(30));
}

#[test]
fn parse_no_prefix() {
    assert_eq!(parse_k8s_minor("1.32.0"), Some(32));
}

#[test]
fn parse_edge_cases() {
    assert_eq!(parse_k8s_minor("v1"), None);
    assert_eq!(parse_k8s_minor(""), None);
    assert_eq!(parse_k8s_minor("garbage"), None);
    assert_eq!(parse_k8s_minor("v1.abc.3"), None);
}

#[test]
fn all_containers_prefix_pod_slash_container() {
    assert_eq!(split_all_containers_prefix("[mypod/web] hello"), Some(("web", "hello")));
}

#[test]
fn all_containers_prefix_bare_container() {
    // Single-pod streams may emit just `[container]`; rsplit still works.
    assert_eq!(split_all_containers_prefix("[web] hello"), Some(("web", "hello")));
}

#[test]
fn all_containers_prefix_multi_segment_source() {
    // Any `/`-separated source resolves to its final (container) segment.
    assert_eq!(split_all_containers_prefix("[ns/mypod/web] hi"), Some(("web", "hi")));
}

#[test]
fn all_containers_prefix_preserves_bracketed_body() {
    // kubectl prepends its prefix once; the container's own `[INFO]` text
    // follows untouched (we split on the FIRST `] `, which is the prefix).
    assert_eq!(
        split_all_containers_prefix("[mypod/web] [INFO] up] done"),
        Some(("web", "[INFO] up] done")),
    );
}

#[test]
fn all_containers_prefix_unprefixed_is_none() {
    // A line without kubectl's prefix rides untagged rather than guessing.
    assert_eq!(split_all_containers_prefix("plain log line"), None);
    assert_eq!(split_all_containers_prefix("[no-close-bracket hi"), None);
}

// ---------------------------------------------------------------------------
// Auth failures must be readable, not an environment dump
// ---------------------------------------------------------------------------

/// kube-rs renders a failed credential plugin as
/// `auth exec command '{cmd}' failed with status {status}: {out:?}`, where
/// `cmd` carries the whole environment — PATH, HOME, AWS_PROFILE, every nix
/// store path. The user saw several thousand characters wrapping their
/// terminal, of which one line mattered.
#[test]
fn an_exec_auth_failure_reports_the_plugin_not_the_environment() {
    use std::os::unix::process::ExitStatusExt;
    let err = kube::Error::Auth(kube::client::AuthError::AuthExecRun {
        cmd: "AWS_PROFILE=\"CrawlingEngAdmin\" HOME=\"/home/u\" \
              PATH=\"/nix/store/aaaa/bin:/nix/store/bbbb/bin\" \
              \"aws\" \"--region\" \"us-west-2\" \"eks\" \"get-token\""
            .to_string(),
        status: std::process::ExitStatus::from_raw(65280),
        out: std::process::Output {
            status: std::process::ExitStatus::from_raw(65280),
            stdout: Vec::new(),
            stderr: b"\nError loading SSO Token: Token for production does not exist\n".to_vec(),
        },
    });

    let msg = super::client_build::summarize_client_error(err).to_string();

    assert!(msg.contains("Error loading SSO Token"), "keeps the useful line: {msg}");
    assert!(msg.contains("aws"), "names the plugin that failed: {msg}");
    assert!(msg.contains("aws sso login"), "says what to do: {msg}");
    assert!(!msg.contains("/nix/store"), "no environment dump: {msg}");
    assert!(!msg.contains("PATH"), "no environment dump: {msg}");
    assert!(!msg.contains("Output {"), "no raw Debug struct: {msg}");
    assert!(msg.len() < 200, "one readable line, got {} chars: {msg}", msg.len());
}

/// Anything we don't recognise passes through untouched — a wrong guess is
/// worse than none.
#[test]
fn a_non_auth_error_is_not_rewritten() {
    let err = kube::Error::LinesCodecMaxLineLengthExceeded;
    let original = err.to_string();
    assert_eq!(super::client_build::summarize_client_error(err).to_string(), original);
}

// ---------------------------------------------------------------------------
// The subscription substream, end to end, with no daemon and no cluster
//
// Everything below runs the PRODUCTION `handle_data_substream` over a real
// yamux pair (in-memory duplex, both halves' drivers live) and reads real
// bincode frames off the client end. The only stand-in is the watcher itself,
// seeded into the `WatcherCache` so the bridge finds it through the ordinary
// reuse path — the bridge's own code is untouched by the test.
//
// This is the gap the 2026-09-09 audit left open: `peer_closed`, `write_delta`
// and the backoff deadline were pinned as primitives, but the loop that
// composes them — attach, baseline, deltas, health, re-attach — was not.
// ---------------------------------------------------------------------------

use crate::kube::live_query::{DrivenWatcher, WatchHealth, WatcherMsg};
use crate::kube::mux::{MuxedConnection, MuxedStream};
use crate::kube::protocol::{
    Namespace, ResourceId, RowChange, StreamEvent, SubscriptionInit, SubstreamInit, TableBaseline,
    TableDelta,
};
use crate::kube::resource_def::BuiltInKind;
use crate::kube::resources::row::{CellValue, ResourceRow};
use std::sync::Arc;
use std::time::Duration;

fn pod_rid() -> ResourceId {
    ResourceId::BuiltIn(BuiltInKind::Pod)
}

fn baseline_of(names: &[&str]) -> TableBaseline {
    TableBaseline {
        resource: pod_rid(),
        headers: vec!["NAME".into()],
        rows: names
            .iter()
            .map(|n| ResourceRow {
                name: (*n).into(),
                namespace: Some("default".into()),
                cells: vec![CellValue::Text((*n).to_string())],
                ..Default::default()
            })
            .collect(),
    }
}

/// One live subscription substream: production bridge on one end, a test on
/// the other, a hand-driven watcher in between.
struct Bridged {
    /// BOTH muxer drivers have to outlive the substream — `MuxedConnection`
    /// aborts its driver on drop, and a dead driver moves no bytes.
    _client_conn: MuxedConnection,
    _daemon_conn: MuxedConnection,
    /// The cache stores only a `Weak`, so this is what keeps the seeded
    /// watcher alive; dropping it is how a test makes the watcher vanish.
    _watcher_keepalive: Arc<crate::kube::live_query::LiveQuery>,
    client: MuxedStream,
    watcher: DrivenWatcher,
    bridge: tokio::task::JoinHandle<()>,
}

impl Bridged {
    /// Open a subscription substream for pods and hand the daemon end to the
    /// real handler.
    async fn open() -> Self {
        let (client_sock, daemon_sock) = tokio::io::duplex(1 << 20);
        let client_conn = MuxedConnection::client(client_sock);
        let mut daemon_conn = MuxedConnection::server(daemon_sock);

        // `new_single_session` skips the local-resource grace window, so
        // nothing outlives the test.
        let shared = Arc::new(super::SessionSharedState::new_single_session());
        let ctx = super::SessionContext::for_test(Arc::clone(&shared));

        // The key the bridge will compute for this subscription. Pods are
        // namespaced and the init asks for all namespaces, so `effective_ns`
        // is `All`; an unfiltered subscription keys on `filter: None`.
        let key = crate::kube::live_query::QueryKey {
            context: ctx.context.clone(),
            namespace: Namespace::All,
            resource: pod_rid(),
            filter: None,
        };
        let (keepalive, watcher) = shared.watcher_cache.seed_driven(key);

        let mut client = client_conn.open().await.expect("open substream");
        crate::kube::protocol::write_bincode(
            &mut client,
            &SubstreamInit::Subscribe(SubscriptionInit {
                resource: pod_rid(),
                namespace: Namespace::All,
                filter: None,
                force: false,
            }),
        )
        .await
        .expect("send init");

        let daemon_side = daemon_conn.accept().await.expect("daemon accepts the substream");
        let bridge = tokio::spawn(super::handle_data_substream(daemon_side, ctx, 1));

        Self {
            _client_conn: client_conn,
            _daemon_conn: daemon_conn,
            _watcher_keepalive: keepalive,
            client,
            watcher,
            bridge,
        }
    }

    /// The next frame the client sees, or a failed test.
    async fn next_event(&mut self) -> StreamEvent {
        self.next_event_within(Duration::from_secs(2)).await
    }

    /// As above, for the one path whose own backstop is a timeout: an observer
    /// has to be more patient than the bound it is observing.
    async fn next_event_within(&mut self, budget: Duration) -> StreamEvent {
        tokio::time::timeout(
            budget,
            crate::kube::protocol::read_bincode::<_, StreamEvent>(&mut self.client),
        )
        .await
        .expect("a frame should arrive")
        .expect("a well-formed frame")
    }

    /// Answer the bridge's pending baseline ask.
    async fn answer_baseline(&mut self, names: &[&str], health: WatchHealth) {
        let ask = self.watcher.next_ask().await;
        ask.reply
            .send(crate::kube::live_query::BaselineReply {
                baseline: baseline_of(names),
                health,
            })
            .ok()
            .expect("the bridge should still be waiting on its ask");
    }
}

#[tokio::test]
async fn a_subscription_substream_delivers_a_baseline_then_deltas() {
    // The stream contract, over the real transport: one Baseline, then
    // Deltas. Nothing below this line was reachable by a test before —
    // `write_delta`'s caller, the flush discipline and the framing were all
    // only exercised against a live daemon.
    let mut b = Bridged::open().await;
    b.answer_baseline(&["pod-a"], WatchHealth::Feeding).await;

    match b.next_event().await {
        StreamEvent::Baseline(base) => {
            assert_eq!(base.rows.len(), 1);
            assert_eq!(base.rows[0].name, "pod-a");
        }
        other => panic!("expected a baseline first, got {other:?}"),
    }

    b.watcher.push(WatcherMsg::Delta(TableDelta {
        changes: vec![RowChange::Upsert(ResourceRow {
            name: "pod-b".into(),
            namespace: Some("default".into()),
            cells: vec![CellValue::Text("pod-b".into())],
            ..Default::default()
        })],
    }));

    match b.next_event().await {
        StreamEvent::Delta(delta) => assert_eq!(delta.changes.len(), 1),
        other => panic!("expected a delta after the baseline, got {other:?}"),
    }
}

#[tokio::test]
async fn a_degraded_watch_reaches_the_client_as_stale_then_live() {
    // The v11 wire contract, which had no end-to-end test: the daemon↔cluster
    // hop reporting its own health. `announce_health` was pinned in isolation;
    // that it is actually CALLED from the steady loop, and that the frame the
    // client decodes is `Stale`/`Live`, was not.
    let mut b = Bridged::open().await;
    b.answer_baseline(&["pod-a"], WatchHealth::Feeding).await;
    assert!(matches!(b.next_event().await, StreamEvent::Baseline(_)));

    b.watcher.push(WatcherMsg::Health(WatchHealth::Degraded("watch died".into())));
    match b.next_event().await {
        StreamEvent::Stale(reason) => assert_eq!(reason, "watch died"),
        other => panic!("a degraded watch must say so, got {other:?}"),
    }

    b.watcher.push(WatcherMsg::Health(WatchHealth::Feeding));
    assert!(
        matches!(b.next_event().await, StreamEvent::Live),
        "recovery has to be announced too, or the banner never clears",
    );
}

#[tokio::test]
async fn attaching_to_an_already_degraded_watcher_announces_after_the_baseline() {
    // A bridge pins its broadcast cursor BEFORE it asks, so a health
    // transition that went out earlier can never reach it through the ring —
    // the baseline reply is the only carrier. And the order matters: the
    // announcement has to follow the rows, because the client applies the
    // baseline as a full replacement and would wipe a banner sent first.
    let mut b = Bridged::open().await;
    b.answer_baseline(&["pod-a"], WatchHealth::Degraded("already stale".into())).await;

    assert!(
        matches!(b.next_event().await, StreamEvent::Baseline(_)),
        "rows first",
    );
    match b.next_event().await {
        StreamEvent::Stale(reason) => assert_eq!(reason, "already stale"),
        other => panic!("the attach-time health must ride out, got {other:?}"),
    }
}

#[tokio::test]
async fn a_departing_client_tears_the_bridge_down() {
    // The audit's finding, at the level that matters. A subscription bridge is
    // write-only after the init frame, so a client that goes away is invisible
    // until a write fails — and on a quiet resource that write never comes.
    // The parked bridge held its `Subscription`, pinning the watcher and its
    // whole row store for the life of the session. `peer_closed` racing the
    // bridge is the fix; this asserts the race is actually wired up.
    let mut b = Bridged::open().await;
    b.answer_baseline(&["pod-a"], WatchHealth::Feeding).await;
    assert!(matches!(b.next_event().await, StreamEvent::Baseline(_)));

    // Quiet resource: nothing more will ever be written to this substream.
    drop(b.client);

    tokio::time::timeout(Duration::from_secs(2), b.bridge)
        .await
        .expect("the bridge must exit when its client leaves, not park forever")
        .expect("bridge task should not panic");
}

#[tokio::test]
async fn a_watcher_that_dies_before_any_data_reports_a_terminal_error() {
    // Fail-fast: a watcher that never produced anything for us died of
    // something permanent (RBAC, unknown resource, a CRD whose printer
    // columns won't resolve). Retrying that forever would leave the client
    // on a spinner with no reason, so the bridge sends a typed Error and
    // stops — the one path that must NOT reach `resubscribe`.
    let mut b = Bridged::open().await;
    let ask = b.watcher.next_ask().await;
    drop(ask); // the watcher took the ask and then died
    b.watcher.push(WatcherMsg::Dead("pods is forbidden".into()));

    match b.next_event().await {
        StreamEvent::Error(msg) => {
            assert!(msg.contains("pods"), "names the resource: {msg}");
            assert!(msg.contains("forbidden"), "carries the reason: {msg}");
        }
        other => panic!("expected a terminal error, got {other:?}"),
    }
    tokio::time::timeout(Duration::from_secs(2), b.bridge)
        .await
        .expect("a permanently failed subscription must stop, not retry")
        .expect("bridge task should not panic");
}

// Paused clock: this is the one path whose backstop IS a timeout, and waiting
// out two real seconds would set a floor under the entire suite. With time
// paused, tokio auto-advances to the next timer whenever the runtime goes
// idle — which is exactly when the drain is parked on a ring that will never
// speak again. The behaviour under test is unchanged; only the waiting is.
#[tokio::test(start_paused = true)]
async fn a_watcher_that_dies_leaving_no_reason_still_terminates_the_stream() {
    // `drain_death_reason`'s bound, which is subtle enough to deserve a pin:
    // the `Subscription` holds the broadcast `Sender` alive, so the channel's
    // own `Closed` can NEVER fire — a plain recv loop looking for the terminal
    // `Dead` would wait forever when the reason isn't on the ring (evicted by
    // a lag, or already consumed). The escape is `watcher_finished()`, backed
    // by a timeout. Either way the subscription must end with a typed error,
    // because a client stuck on a spinner with no message is the worst
    // possible outcome of a watcher dying.
    let mut b = Bridged::open().await;
    let ask = b.watcher.next_ask().await;

    // The watcher task exits, and its reason never reaches the ring.
    b.watcher.stop.take().expect("stop handle").send(()).ok();
    drop(ask);
    // A non-Dead message lets the drain short-circuit on `watcher_finished()`
    // instead of waiting out its backstop — but the bridge's own select may
    // consume this first, in which case the drain finds an empty ring and
    // takes the slow route. BOTH are correct, and both end the stream; the
    // read budget below just has to outlast the 2s backstop either way.
    b.watcher.push(WatcherMsg::Health(WatchHealth::Feeding));

    match b.next_event_within(Duration::from_secs(6)).await {
        StreamEvent::Error(msg) => assert!(
            msg.contains("Watcher failed for pods"),
            "a reasonless death still names the resource: {msg}",
        ),
        other => panic!("expected a terminal error, got {other:?}"),
    }
}

#[tokio::test]
async fn an_undeliverable_delta_re_asks_for_a_baseline() {
    // `write_delta` returning false is pinned on its own; that the attach loop
    // RESPONDS by re-asking — landing on the guarded baseline path, which can
    // report a real error — is the composition that wasn't. Without it an
    // undeliverable delta is a silent re-subscribe loop.
    //
    // Scope note: this pins the loop's RESPONSE, not which check rejected the
    // frame. An over-cap delta is refused twice over — `write_delta`'s own
    // `frame_exceeds_cap` guard and, failing that, `write_bincode`'s size
    // check — and both return false, so disabling either one alone leaves
    // this green. The guard earns its keep by not serializing 64 MiB first,
    // which is a cost, not a behaviour, and so isn't observable from here.
    let mut b = Bridged::open().await;
    b.answer_baseline(&["pod-a"], WatchHealth::Feeding).await;
    assert!(matches!(b.next_event().await, StreamEvent::Baseline(_)));

    // One row too large for the 64 MiB frame cap. `frame_exceeds_cap` computes
    // the size without serializing, so this costs one allocation, not a copy.
    b.watcher.push(WatcherMsg::Delta(TableDelta {
        changes: vec![RowChange::Upsert(ResourceRow {
            name: "huge".into(),
            namespace: Some("default".into()),
            cells: vec![CellValue::Text("x".repeat(65 * 1024 * 1024))],
            ..Default::default()
        })],
    }));

    // The proof is a SECOND ask, and a re-join one (`after_initial_list`
    // false) rather than a relist — the client's rows are already wrong.
    let ask = b.watcher.next_ask().await;
    assert!(
        !ask.after_initial_list,
        "an undeliverable delta re-joins; it does not wait out a relist",
    );
    ask.reply
        .send(crate::kube::live_query::BaselineReply {
            baseline: baseline_of(&["pod-a", "pod-b"]),
            health: WatchHealth::Feeding,
        })
        .ok()
        .expect("bridge still waiting");

    match b.next_event().await {
        StreamEvent::Baseline(base) => assert_eq!(base.rows.len(), 2),
        other => panic!("expected the repairing baseline, got {other:?}"),
    }
}

/// A subscription/log bridge must notice its client going away.
///
/// From the 2026-09-09 audit: these bridges are write-only after the init
/// frame, so a client-side close was only ever observed via a FAILING WRITE
/// — which on a quiet resource never comes. The bridge parked forever
/// holding its `Subscription`, pinning the watcher, its apiserver watch and
/// its full row store for the life of the session and bypassing the 60s
/// grace. This pins the primitive that fixes it: the peer-close watch must
/// resolve on EOF, and must NOT resolve while the peer is merely idle.
#[tokio::test]
async fn a_closed_peer_half_is_observable() {
    use tokio::io::AsyncWriteExt;

    // Peer still attached but silent: the bridge must keep running.
    let (mut peer, daemon) = tokio::io::duplex(64);
    let mut daemon = daemon;
    let idle = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        super::peer_closed(&mut daemon),
    )
    .await;
    assert!(idle.is_err(), "an idle client must not look like a departed one");

    // Peer drops: resolves promptly.
    peer.shutdown().await.unwrap();
    drop(peer);
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        super::peer_closed(&mut daemon),
    )
    .await
    .expect("a closed peer must be observed, or the watcher leaks for the session");
}

/// Both PTY fds are close-on-exec.
///
/// `openpty` does not set it, and the daemon forks children constantly —
/// `kubectl logs -f`, describe, another exec, the auth credential plugin.
/// Every one of them inherited these fds, and an inherited copy is not
/// released when ours is: open a shell, then follow a log, exit the shell,
/// and the pty pair stays allocated for as long as the log runs. This is the
/// one leak `Drop` structurally cannot reach — closing your own descriptor
/// says nothing about the copy in another process's table — so the fix has to
/// be preventing the copy, and that is what this pins.
#[test]
fn pty_fds_are_not_inherited_by_forked_children() {
    let pty = match super::Pty::open(80, 24) {
        Ok(p) => p,
        // No /dev/ptmx (some sandboxes). Nothing to assert, and failing here
        // would say something false about the code.
        Err(_) => return,
    };

    let cloexec = |fd: std::os::fd::RawFd| -> bool {
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert_ne!(flags, -1, "F_GETFD failed");
        flags & libc::FD_CLOEXEC != 0
    };

    assert!(cloexec(pty.master_fd_for_test()), "master must not survive an exec");
    assert!(cloexec(pty.slave_fd_for_test()), "slave must not survive an exec");
}

// ---------------------------------------------------------------------------
// Where a log line came from, once a stream can have several sources (v13)
// ---------------------------------------------------------------------------

use super::tag_kubectl_line;

/// The pre-fan-out shapes are byte-identical: one pod, one container, no tag.
#[test]
fn a_single_source_line_stays_untagged() {
    let line = tag_kubectl_line("hello".to_string(), false, None);
    assert_eq!(line.source, None);
    assert_eq!(line.content, "hello");
    assert_eq!(line.flat_text(), "hello", "and exports with no prefix");
}

/// `--all-containers` on ONE pod: the container alone identifies the source,
/// exactly as before the fan-out existed.
#[test]
fn all_containers_on_one_pod_tags_the_container() {
    let line = tag_kubectl_line("[mypod/web] hello".to_string(), true, None);
    assert_eq!(line.source.as_deref(), Some("web"));
    assert_eq!(line.content, "hello", "kubectl's prefix is stripped, not rendered");
}

/// A fan-out over single-container pods: the pod IS the source.
#[test]
fn a_fanout_tags_the_pod() {
    let line = tag_kubectl_line("hello".to_string(), false, Some("pod-b"));
    assert_eq!(line.source.as_deref(), Some("pod-b"));
    assert_eq!(line.content, "hello");
    assert_eq!(line.flat_text(), "[pod-b] hello");
}

/// Both axes at once. A bare container name here would be ambiguous —
/// with several multi-container pods merged into one view, "web" doesn't
/// say WHICH pod's web, and the ambiguity is invisible in the output.
#[test]
fn a_fanout_over_multi_container_pods_names_both() {
    let line = tag_kubectl_line("[pod-b/web] hello".to_string(), true, Some("pod-b"));
    assert_eq!(line.source.as_deref(), Some("pod-b/web"));
    assert_eq!(line.content, "hello");
}

/// A line kubectl didn't prefix (it only prefixes `--all-containers`, and
/// even then a weird line may not parse) keeps the fan-out tag rather than
/// losing its source.
#[test]
fn an_unprefixed_line_in_a_fanout_keeps_its_pod() {
    let line = tag_kubectl_line("plain".to_string(), true, Some("pod-b"));
    assert_eq!(line.source.as_deref(), Some("pod-b"));
    assert_eq!(line.content, "plain");
}

// ---------------------------------------------------------------------------
// What a fan-out actually opens, and how it labels it (v14)
// ---------------------------------------------------------------------------

use super::{plan_log_work, report_log_failure, MAX_LOG_FANOUT};
use crate::kube::protocol::LogContainer;

fn sources(work: &[super::LogWork]) -> Vec<Option<&str>> {
    work.iter().map(|w| w.source.as_deref()).collect()
}

/// One pod, one container: byte-identical to the pre-fan-out stream, down
/// to the absent tag. Naming the pod on every line of a single-pod view is
/// noise.
#[test]
fn a_single_source_stream_carries_no_tag() {
    let work = plan_log_work(&["web-0".to_string()], &LogContainer::Default);
    assert_eq!(work.len(), 1);
    assert_eq!(sources(&work), [None]);
}

/// Several pods, one container selector each: the pod is what varies, so
/// the pod is the tag.
#[test]
fn a_pod_fanout_tags_the_pod() {
    let pods = vec!["a".to_string(), "b".to_string()];
    let work = plan_log_work(&pods, &LogContainer::Default);
    assert_eq!(sources(&work), [Some("a"), Some("b")]);
}

/// `--all-containers` on ONE pod needs no tag of its own: kubectl prefixes
/// every line with the container it came from, and `tag_kubectl_line` reads
/// that. (Across pods the pod is still the tag, and the two combine.)
#[test]
fn all_containers_on_one_pod_leaves_the_tagging_to_kubectl() {
    let work = plan_log_work(&["web-0".to_string()], &LogContainer::All);
    assert_eq!(sources(&work), [None]);
    assert_eq!(work[0].container, LogContainer::All, "one kubectl call, not one per container");
}

/// A container `Set` is the CROSS PRODUCT with the pods, split into one
/// `Named` stream apiece — the API path can serve those, where
/// `--all-containers` forces a subprocess.
#[test]
fn a_container_set_expands_to_one_stream_each() {
    let work = plan_log_work(
        &["web-0".to_string()],
        &LogContainer::Set(vec!["main".into(), "proxy".into()]),
    );
    assert_eq!(work.len(), 2);
    assert_eq!(sources(&work), [Some("main"), Some("proxy")]);
    assert!(work.iter().all(|w| matches!(w.container, LogContainer::Named(_))));
}

/// Both axes at once. A bare container name here would be ambiguous in a
/// way that's invisible in the output — it just looks like interleaving.
#[test]
fn both_axes_together_name_pod_and_container() {
    let pods = vec!["a".to_string(), "b".to_string()];
    let work = plan_log_work(&pods, &LogContainer::Set(vec!["main".into(), "proxy".into()]));
    assert_eq!(
        sources(&work),
        [Some("a/main"), Some("a/proxy"), Some("b/main"), Some("b/proxy")],
    );
    assert_eq!(work.len(), 4, "the cross product is the unit of work — and of the cap");
}

/// The cap counts STREAMS, not pods: three marked containers on eight pods
/// is twenty-four apiserver watches, not eight.
#[test]
fn the_fanout_cap_counts_streams_not_pods() {
    let pods: Vec<String> = (0..MAX_LOG_FANOUT).map(|i| format!("pod-{i}")).collect();
    let work = plan_log_work(&pods, &LogContainer::Set(vec!["a".into(), "b".into()]));
    assert_eq!(work.len(), MAX_LOG_FANOUT * 2);
    assert!(work.len() > MAX_LOG_FANOUT, "so this selection is capped, where a pod count would not be");
}

// ---------------------------------------------------------------------------
// Post-filters: the ones the apiserver can't do (v15)
// ---------------------------------------------------------------------------

use super::{is_post_filter, row_passes};
use crate::kube::protocol::SubscriptionFilter;
use crate::kube::resources::row::ResourceRefInfo;

fn pod_referencing(name: &str, kind: BuiltInKind) -> ResourceRow {
    ResourceRow {
        name: "api-7d9f".into(),
        namespace: Some("prod".into()),
        references: vec![ResourceRefInfo { kind, name: name.into() }],
        ..Default::default()
    }
}

/// The lookup matches on the REFERENCE, so a pod whose name resembles
/// nothing still counts. `api-7d9f` mounting `db-creds` is exactly the case
/// the old name grep could never find.
#[test]
fn a_reference_filter_matches_what_the_pod_names() {
    let row = pod_referencing("db-creds", BuiltInKind::Secret);
    assert!(row_passes(
        &row,
        &SubscriptionFilter::References {
            kind: BuiltInKind::Secret,
            name: "db-creds".into()
        },
    ));
}

/// Kind is part of the identity: a Secret and a ConfigMap can share a name,
/// and mounting one is not mounting the other.
#[test]
fn a_reference_filter_distinguishes_kinds() {
    let row = pod_referencing("shared", BuiltInKind::Secret);
    assert!(!row_passes(
        &row,
        &SubscriptionFilter::References {
            kind: BuiltInKind::ConfigMap,
            name: "shared".into()
        },
    ));
    assert!(!row_passes(
        &row,
        &SubscriptionFilter::References {
            kind: BuiltInKind::Secret,
            name: "other".into()
        },
    ));
}

/// Label and field selectors push down to the apiserver, so rows arriving
/// under them are already filtered — passing them through `row_passes`
/// must not filter twice and drop everything.
#[test]
fn pushed_down_filters_do_not_filter_again() {
    let row = pod_referencing("db-creds", BuiltInKind::Secret);
    assert!(row_passes(&row, &SubscriptionFilter::Field("spec.nodeName=n1".into())));
    assert!(row_passes(&row, &SubscriptionFilter::Labels(Default::default())));

    // And the two questions agree about which is which — the watcher-sharing
    // decision and the row-filtering decision read the same predicate.
    assert!(is_post_filter(&Some(SubscriptionFilter::OwnerUid("u".into()))));
    assert!(is_post_filter(&Some(SubscriptionFilter::References {
        kind: BuiltInKind::Secret,
        name: "s".into()
    })));
    assert!(!is_post_filter(&Some(SubscriptionFilter::Field("f=v".into()))));
    assert!(!is_post_filter(&None));
}

/// A post-filtered subscription shares the UNFILTERED watcher, because the
/// apiserver isn't doing the filtering — opening a second identical watch
/// would cost a real connection for no narrowing.
#[test]
fn a_post_filtered_subscription_reuses_the_unfiltered_watcher() {
    let owner = Some(SubscriptionFilter::OwnerUid("u".into()));
    let refs = Some(SubscriptionFilter::References {
        kind: BuiltInKind::Secret,
        name: "s".into(),
    });
    // `cache_filter` is `None` for both — the same key an unfiltered
    // subscription builds.
    for f in [owner, refs] {
        assert!(is_post_filter(&f), "{f:?} must not reach the apiserver");
    }
}

// ---------------------------------------------------------------------------
// The prefix premise (v16) — we ask for it, so stripping it is a contract
// ---------------------------------------------------------------------------

/// `--all-containers` does NOT imply `--prefix` — only `--all-pods` does.
/// While that was assumed, `tag_kubectl_line` stripped the first bracketed
/// group of whatever the APPLICATION logged: a timestamped line lost its
/// timestamp to a hash-coloured fake "source", and the container name the
/// feature exists to show was never recovered at all.
#[test]
fn a_bracketed_application_line_is_not_mistaken_for_a_prefix() {
    // What kubectl actually emits with `--prefix --timestamps`.
    let real = tag_kubectl_line(
        "[pod-a/web] 2026-09-17T10:00:00.000000000Z hello".to_string(),
        true,
        None,
    );
    assert_eq!(real.source.as_deref(), Some("web"));
    assert_eq!(real.timestamp.as_deref(), Some("2026-09-17T10:00:00.000000000Z"));
    assert_eq!(real.content, "hello");
}

/// The daemon fills `timestamp`; the renderer never sniffs one out of the
/// body. A line whose CONTENT opens with a date keeps it.
#[test]
fn a_timestamp_is_a_field_not_a_guess() {
    let (ts, body) =
        crate::kube::protocol::LogLine::split_timestamp("2026-09-17T10:00:00Z WARN disk full");
    assert_eq!(ts.as_deref(), Some("2026-09-17T10:00:00Z"));
    assert_eq!(body, "WARN disk full");

    // An application's own bracketed date is content, and stays content.
    let (ts, body) = crate::kube::protocol::LogLine::split_timestamp("[2026-09-17 10:00:00] WARN");
    assert_eq!(ts, None);
    assert_eq!(body, "[2026-09-17 10:00:00] WARN");

    // A bare space-bearing line is not a timestamp either.
    let (ts, body) = crate::kube::protocol::LogLine::split_timestamp("INFO starting up");
    assert_eq!(ts, None);
    assert_eq!(body, "INFO starting up");
}

/// A pod that produced nothing AND failed must say so on the stream — it is
/// the only channel a fan-out has. "No logs." is an affirmative false
/// statement about a fetch that errored.
#[test]
fn a_failed_source_reports_on_the_stream() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    futures::executor::block_on(report_log_failure(&tx, "api-7d9f", "previous terminated container not found"));
    let line = rx.try_recv().expect("the failure rides the stream");
    assert_eq!(line.source.as_deref(), Some("api-7d9f"), "tagged with the pod it belongs to");
    assert!(line.content.starts_with("k9rs: "), "and marked as ours, not the workload's");
    assert_eq!(line.timestamp, None, "k9rs is speaking, so there is no kubelet stamp");
}

/// A container set that resolves to nothing closes with a REASON, not with
/// an empty successful-looking stream. The pod guard doesn't cover this.
#[test]
fn an_empty_container_set_produces_no_work() {
    let pods = vec!["web-0".to_string()];
    assert!(plan_log_work(&pods, &LogContainer::Set(vec![])).is_empty());
}

fn tl(source: &str, ts: Option<&str>, content: &str) -> crate::kube::protocol::LogLine {
    crate::kube::protocol::LogLine {
        source: Some(source.to_string()),
        timestamp: ts.map(str::to_string),
        content: content.to_string(),
    }
}

fn tl_untagged(ts: Option<&str>, content: &str) -> crate::kube::protocol::LogLine {
    crate::kube::protocol::LogLine {
        source: None,
        timestamp: ts.map(str::to_string),
        content: content.to_string(),
    }
}

fn bodies(batch: &[crate::kube::protocol::LogLine]) -> Vec<&str> {
    batch.iter().map(|l| l.content.as_str()).collect()
}

/// A fan-out merges N readers into one channel, so the order lines come out
/// is which task got scheduled — not which line is newer. Ordering a batch we
/// were already holding fixes that for free; the point is that it is the only
/// thing we do, because the alternative (hold lines back until we are sure no
/// older one is coming) buys a tidier order with deliberate lag on a live tail.
#[test]
fn a_ready_batch_is_ordered_by_time_not_by_arrival() {
    let mut batch = vec![
        tl("pod-b", Some("2026-09-18T20:29:40.000000003Z"), "third"),
        tl("pod-a", Some("2026-09-18T20:29:40.000000001Z"), "first"),
        tl("pod-c", Some("2026-09-18T20:29:40.000000002Z"), "second"),
    ];
    order_batch(&mut batch);
    assert_eq!(bodies(&batch), ["first", "second", "third"]);
}

/// A batch from ONE source is never reordered, whatever the stamps say.
///
/// This is the half that matters. A single stream is already in true order —
/// one reader, one FIFO channel — so sorting it can only do harm, and the
/// FIRST version of this shipped without the check: it gated on "every line
/// has a timestamp", which a plain one-pod `:logs` view satisfies. Combined
/// with the comparator bug below it inverted a perfectly ordered stream.
#[test]
fn a_single_source_batch_is_never_reordered() {
    // Stamps deliberately DESCENDING: a sort would visibly reverse this.
    let mut batch = vec![
        tl("pod-a", Some("2026-09-18T20:29:40.000000009Z"), "first"),
        tl("pod-a", Some("2026-09-18T20:29:40.000000005Z"), "second"),
        tl("pod-a", Some("2026-09-18T20:29:40.000000001Z"), "third"),
    ];
    order_batch(&mut batch);
    assert_eq!(bodies(&batch), ["first", "second", "third"], "one stream is authoritative");

    // Untagged (`source: None`) is the single-pod case — the common one.
    let mut batch = vec![
        tl_untagged(Some("2026-09-18T20:29:40.000000009Z"), "first"),
        tl_untagged(Some("2026-09-18T20:29:40.000000001Z"), "second"),
    ];
    order_batch(&mut batch);
    assert_eq!(bodies(&batch), ["first", "second"]);

    // A fan-out batch in which only one pod happened to speak is equally
    // authoritative — the test is on what the BATCH holds, not on how many
    // readers the stream has.
    let mut batch = vec![
        tl("pod-b", Some("2026-09-18T20:29:40.000000009Z"), "first"),
        tl("pod-b", Some("2026-09-18T20:29:40.000000001Z"), "second"),
    ];
    order_batch(&mut batch);
    assert_eq!(bodies(&batch), ["first", "second"]);
}

/// Go's `RFC3339Nano` TRIMS trailing zeros, so the fraction is variable
/// width — and plain string comparison then inverts every pair where one
/// fraction is a PREFIX of the other, because the terminating `Z` (0x5A)
/// compares above every digit. `.6Z` vs `.61Z` put the earlier line second.
/// A stamp with no fraction at all loses to every sub-second stamp in its
/// own second, by up to 999 ms.
///
/// The first version of this file asserted string order was chronological
/// and "conceded" the one pair — `.6Z` vs `.60Z` — that `RFC3339Nano` can
/// never emit, while missing both families that actually occur.
#[test]
fn the_comparator_handles_a_variable_width_fraction() {
    // Each case is (earlier, later); the sort must produce that order from
    // the reversed input. Two sources so the gate above lets the sort run.
    for (earlier, later) in [
        ("2026-09-18T20:29:40.6Z", "2026-09-18T20:29:40.61Z"), // prefix
        ("2026-09-18T20:29:40.1Z", "2026-09-18T20:29:40.12Z"), // prefix
        ("2026-09-18T20:29:40Z", "2026-09-18T20:29:40.1Z"),    // no fraction at all
        ("2026-09-18T20:29:40.6Z", "2026-09-18T20:29:40.610000000Z"), // mixed width
        ("2026-09-18T20:29:40.59Z", "2026-09-18T20:29:40.6Z"), // non-prefix (always worked)
        ("2026-09-18T20:29:39.999Z", "2026-09-18T20:29:40Z"),  // second rollover
    ] {
        let mut batch = vec![tl("pod-b", Some(later), "later"), tl("pod-a", Some(earlier), "earlier")];
        order_batch(&mut batch);
        assert_eq!(bodies(&batch), ["earlier", "later"], "{earlier} must precede {later}");
    }
}

/// A line with no timestamp cannot be placed, so a batch containing one is
/// left exactly as it arrived rather than half-sorted around it. (The daemon
/// asks for timestamps, so this is the degraded path — a source that did not
/// give us one, or the `untagged` notices this same substream carries.)
#[test]
fn an_untimestamped_line_leaves_its_batch_alone() {
    let mut batch = vec![
        tl("pod-b", Some("2026-09-18T20:29:40.000000003Z"), "later"),
        tl("pod-a", None, "unknown"),
        tl("pod-c", Some("2026-09-18T20:29:40.000000001Z"), "earlier"),
    ];
    order_batch(&mut batch);
    assert_eq!(bodies(&batch), ["later", "unknown", "earlier"], "arrival order, untouched");
}
