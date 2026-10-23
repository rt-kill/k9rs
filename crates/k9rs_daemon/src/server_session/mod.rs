//! Daemon-side session: one per TUI client connection.
//!
//! A `ServerSession` is a tokio task spawned by the daemon for each persistent
//! TUI connection. It reads `SessionCommand`s from the TUI, manages
//! subscriptions via the shared `WatcherCache`, and pushes `SessionEvent`s
//! back over the socket.
//!
//! Wire format: length-prefixed bincode (see protocol.rs).

mod client_build;
mod ops;
mod streaming;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::{mpsc, watch, Semaphore};
use tracing::{debug, info, warn};

use k9rs_kube::live_query::WatcherCache;
use k9rs_proto::protocol::{self, SessionCommand, SessionEvent};
use k9rs_kube::session_env::SessionEnv;


// ---------------------------------------------------------------------------
// InitParams — extracted from the Init command for ergonomic access
// ---------------------------------------------------------------------------

/// How long a failed session stays up waiting for the client to read its
/// error. Only bounds a peer that reads the error and then sits there — the
/// normal path is milliseconds (the client reports the error and hangs up,
/// which ends the wait immediately).
const LINGER_AFTER_FATAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Send a terminal [`SessionEvent::SessionError`] and stay alive until the
/// client has actually read it.
///
/// The lingering is the entire point. A yamux `StreamHandle`'s `poll_flush`
/// is a NO-OP, so `write_bincode` returning `Ok` means the frame reached the
/// muxer's channel — NOT the socket. Only the connection's driver task ever
/// writes it out, and that task is killed (`AbortOnDrop` on
/// `MuxedConnection`) the instant `init_and_run_muxed` returns, which is the
/// instant this session task returns. So every early-return diagnostic used
/// to die in the queue and reach the user as `Failed to read Ready: early
/// eof` — silencing precisely the errors most likely to happen at startup
/// (unknown context, expired credentials, unreachable cluster).
///
/// Reading to EOF is what keeps the driver alive long enough, and the peer's
/// hangup is the acknowledgment: it read the error, reported it, and dropped
/// the connection.
async fn fail_session(
    reader: &mut BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    writer: &mut BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
    message: String,
) {
    use tokio::io::AsyncReadExt;
    if protocol::write_bincode(writer, &SessionEvent::SessionError(message)).await.is_err() {
        return;
    }
    let mut sink = [0u8; 64];
    let _ = tokio::time::timeout(LINGER_AFTER_FATAL, async {
        while reader.read(&mut sink).await.unwrap_or(0) > 0 {}
    })
    .await;
}

/// Holds the fields from `SessionCommand::Init` in a flat struct so
/// `init_and_run` doesn't have to juggle a dozen local variables.
struct InitParams {
    context: Option<protocol::ContextName>,
    namespace: protocol::Namespace,
    readonly: bool,
    kubeconfig_yaml: String,
    env_vars: HashMap<String, String>,
    identity: protocol::ClusterIdentity,
}

// ---------------------------------------------------------------------------
// SessionSharedState — opaque to the daemon, holds kube-aware shared state
// ---------------------------------------------------------------------------

/// Shared state for all `ServerSession`s, created once at daemon startup.
pub struct SessionSharedState {
    pub(super) watcher_cache: WatcherCache,
    /// Daemon-owned local resource sources (port-forwards, etc.).
    /// Shared across every session on this daemon.
    pub local_registry: Arc<k9rs_kube::local::LocalRegistry>,
    /// In-memory discovery cache. Per-resource-type storage means a failed
    /// namespace fetch can't erase a prior cached CRD list and vice-versa —
    /// the cache can't be partially poisoned. `Arc` so dynamic watchers can
    /// hold it: they resolve their own printer columns at birth.
    pub discovery_cache: Arc<k9rs_kube::cache::DiscoveryCache>,
    /// Shared per-`ContextId` metrics + discovery pollers, so N sessions on one
    /// cluster poll it once instead of N times — the same sharing `watcher_cache`
    /// gives resource watches. See [`k9rs_kube::shared_poller`].
    metrics_pollers: k9rs_kube::shared_poller::PollerCache<k9rs_kube::metrics::MetricsSnapshot>,
    discovery_pollers: k9rs_kube::shared_poller::PollerCache<streaming::DiscoverySnapshot>,
    /// Bounds concurrent `kube::Client` builds. Each build can run a blocking
    /// exec credential plugin (`aws eks get-token`); without a cap a reconnect
    /// storm would fork that many auth subprocesses and occupy that many
    /// blocking-pool threads at once. (The old single-owner builder task
    /// effectively capped this at one in-flight build.)
    client_build_semaphore: Semaphore,
    /// Monotonic session ID counter for structured logging.
    next_session_id: std::sync::atomic::AtomicU64,
}

impl Default for SessionSharedState {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionSharedState {
    /// A snapshot of every `Weak`-keyed cache, for `ctl status`. Read-only:
    /// nothing here reaps, so asking what the daemon holds cannot change it.
    pub fn cache_stats(&self) -> protocol::CacheStats {
        protocol::CacheStats {
            watchers: self.watcher_cache.stats(),
            contexts: self.local_registry.stats(),
            discovery_entries: self.discovery_cache.len(),
            metrics_pollers: self.metrics_pollers.live_count(),
            discovery_pollers: self.discovery_pollers.live_count(),
        }
    }

    pub fn new() -> Self {
        Self::with_local_grace(Some(k9rs_kube::grace::locals_grace()))
    }

    /// For `--no-daemon`: this state lives inside a single connection, so a
    /// post-session grace window for local resources could never be
    /// recovered — skip it and tear them down with the connection.
    pub fn new_single_session() -> Self {
        Self::with_local_grace(None)
    }

    fn with_local_grace(local_grace: Option<std::time::Duration>) -> Self {
        Self {
            watcher_cache: WatcherCache::new(),
            local_registry: Arc::new(k9rs_kube::local::LocalRegistry::new(
                k9rs_kube::daemon_config::daemon_config().exec_resources.clone(),
                local_grace,
            )),
            discovery_cache: Arc::new(k9rs_kube::cache::DiscoveryCache::new()),
            metrics_pollers: k9rs_kube::shared_poller::PollerCache::new(),
            discovery_pollers: k9rs_kube::shared_poller::PollerCache::new(),
            client_build_semaphore: Semaphore::new(MAX_CONCURRENT_CLIENT_BUILDS),
            next_session_id: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const EVENT_CHANNEL_CAPACITY: usize = 512;

/// Max concurrent `kube::Client` builds (see [`SessionSharedState`]). Generous
/// enough that opening several clusters at once stays parallel, bounded enough
/// that a connection storm can't fork unbounded auth subprocesses.
const MAX_CONCURRENT_CLIENT_BUILDS: usize = 8;

// ---------------------------------------------------------------------------
// SessionContext — shared between the control loop and substream tasks
// ---------------------------------------------------------------------------

/// Per-session state shared (read-only) between the control loop and each
/// subscription substream task. Wrapped in `Arc` so substream tasks can
/// hold a cheap reference without borrowing `ServerSession`.
///
/// Namespace is NOT stored here — each subscription declares its own
/// namespace in `SubscriptionInit`, so namespace switches don't require
/// mutating this immutable `Arc`.
pub(crate) struct SessionContext {
    pub shared: Arc<SessionSharedState>,
    pub client: kube::Client,
    /// The session's process env, applied to every `kubectl` subprocess the
    /// substream tasks spawn (logs fallback, exec PTY) so they authenticate as
    /// this session rather than the daemon's startup env.
    pub session_env: SessionEnv,
    pub context: protocol::ContextId,
    /// Session ID for structured logging in substream tasks.
    pub session_id: u64,
    pub streaming_lists: bool,
    /// Read-only stance for this session. The control-substream command
    /// loop gates mutations via `SessionCommand::is_mutating`, but exec
    /// substreams ride a DATA substream that skips that gate — so the
    /// exec handler must consult this directly, or a read-only session
    /// could `kubectl exec`/`debug` (a mutation) freely.
    pub readonly: bool,
    /// This session's strong hold on the context's local resources
    /// (port-forwards, exec sources). Held here AND on `ServerSession` so
    /// every path that can serve a local op keeps the slice out of grace;
    /// when the session's last holder drops, the context's grace window
    /// begins.
    pub locals: k9rs_kube::local::ContextKeepalive,
}

#[cfg(test)]
impl SessionContext {
    /// TEST SEAM: a session context with no cluster behind it.
    ///
    /// The `kube::Client` is real but points at a port nothing listens on. It
    /// is deliberately never exercised: the paths under test get their
    /// `Subscription` from a watcher seeded via
    /// [`k9rs_kube::live_query::WatcherCache::seed_driven`], which takes the
    /// cache's reuse branch and so never invokes the client factory. A test
    /// that DOES reach the client will fail by connection-refused rather than
    /// by silently talking to whatever cluster the developer happens to have
    /// configured — the property that matters is that tests never touch a real
    /// daemon or cluster.
    pub(crate) fn for_test(shared: Arc<SessionSharedState>) -> Arc<Self> {
        let context = protocol::ContextId {
            name: protocol::ContextName::new("test-ctx").expect("non-empty"),
            server_url: "http://127.0.0.1:1".to_string(),
            cred_fingerprint: 0,
        };
        let config = kube::Config::new(
            "http://127.0.0.1:1".parse().expect("a valid uri"),
        );
        let locals = shared.local_registry.attach(&context);
        Arc::new(Self {
            client: kube::Client::try_from(config).expect("offline client build"),
            session_env: k9rs_kube::session_env::SessionEnv::new(HashMap::new()),
            context,
            session_id: 1,
            streaming_lists: false,
            readonly: false,
            locals,
            shared,
        })
    }
}

// ---------------------------------------------------------------------------
// ServerSession
// ---------------------------------------------------------------------------

pub struct ServerSession {
    writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
    shared: Arc<SessionSharedState>,
    client: kube::Client,
    /// The session's process env, applied to every `kubectl` subprocess the
    /// handlers spawn (describe/yaml/delete fallbacks, port-forward).
    session_env: SessionEnv,
    context: protocol::ContextId,
    // `namespace` field removed — session doesn't need session-level
    // namespace state. Each subscription carries its own in `SubscriptionInit`.
    readonly: bool,
    /// Strong hold on this context's local resources — the command loop
    /// serves describe/yaml/delete/pf-create for them, so it must keep the
    /// slice out of grace for the session's whole life (see
    /// `SessionContext::locals` for the substream half).
    locals: k9rs_kube::local::ContextKeepalive,
    /// Guard, not a bare handle: `cleanup()` is reachable only on the
    /// normal exit path (the run-loop select can be cancelled by its sibling
    /// arm), so these forwarders are abort-on-drop — dropping the session
    /// reaps them structurally, the same protection the reader task gets
    /// from its inline guard.
    metrics_task: Option<k9rs_util::AbortOnDrop>,
    /// Background loop that periodically re-runs discovery (namespaces +
    /// CRDs), so new namespaces / new CRDs land in the cache and reach the
    /// client without the user having to reconnect. Same guard rationale as
    /// `metrics_task`.
    discovery_refresher_task: Option<k9rs_util::AbortOnDrop>,
    /// Tracks every `handle_*_async` background task (describe, yaml,
    /// delete, apply, scale, restart, decode, cron-trigger,
    /// cron-toggle-suspend, force-kill, save, etc). On `cleanup()` we
    /// `abort_all` so a session that exits mid-operation doesn't leak
    /// detached tasks holding kube clients and channel senders.
    pending_tasks: tokio::task::JoinSet<()>,
    event_tx: mpsc::Sender<SessionEvent>,
}

impl ServerSession {
    pub fn new(
        writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
        shared: Arc<SessionSharedState>,
        client: kube::Client,
        context: protocol::ContextId,
        readonly: bool,
        session_env: SessionEnv,
        locals: k9rs_kube::local::ContextKeepalive,
    ) -> (Self, mpsc::Receiver<SessionEvent>) {
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let session = Self {
            writer,
            shared,
            client,
            session_env,
            context,
            readonly,
            locals,
            metrics_task: None,
            discovery_refresher_task: None,
            pending_tasks: tokio::task::JoinSet::new(),
            event_tx,
        };
        // The receiver goes to the CALLER, not into the struct. It belongs to
        // the run loop, which is the only thing that ever drains it — parking
        // it in an `Option` and `take().expect()`-ing it in `run` encoded
        // "already started" in a field that nothing else reads, and turned a
        // state the type system already prevents (`run` consumes `self`) into
        // a runtime panic.
        (session, event_rx)
    }

    // -----------------------------------------------------------------------
    // Init + Run (entry point called by daemon)
    // -----------------------------------------------------------------------

    /// Create and run a session from a yamux-multiplexed connection.
    ///
    /// Accepts the first inbound substream as the **control channel** and
    /// runs the Init/Ready handshake + command loop on it (unchanged from
    /// the pre-yamux era — same `SessionCommand`/`SessionEvent` bincode
    /// framing). Any subsequent inbound substreams are **subscription
    /// streams**: the first message on each is a `SubscriptionInit`, and
    /// the daemon spawns a bridge that writes `StreamEvent`s back.
    ///
    /// The control loop and the subscription-acceptor loop run concurrently
    /// via `tokio::select!`. When either exits the session is done.
    pub async fn init_and_run_muxed(
        mut mux: k9rs_mux::MuxedConnection,
        shared: Arc<SessionSharedState>,
    ) {
        let session_id = shared.next_session_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        info!(session = session_id, "new session connection");

        // First inbound substream → control channel.
        let Some(control_stream) = mux.accept().await else {
            warn!(session = session_id, "no control substream received");
            return;
        };
        let (ctrl_read, ctrl_write) = tokio::io::split(control_stream);
        let reader: BufReader<Box<dyn AsyncRead + Unpin + Send>> = BufReader::with_capacity(
            protocol::IO_BUFFER_SIZE,
            Box::new(ctrl_read),
        );
        let writer: Box<dyn AsyncWrite + Unpin + Send> = Box::new(ctrl_write);

        // Run the control loop + accept subscription substreams in parallel.
        // init_and_run handles the control stream; we accept subscriptions
        // alongside it. When the control loop exits (TUI disconnected),
        // `mux` drops → driver dies → subscription substream reads return
        // EOF → bridge tasks exit → watchers enter grace period.
        //
        // The session context (kube client, context id, namespace) is built
        // by init_and_run and stashed in `session_ctx_tx` so the subscription
        // acceptor loop can pass it to each substream handler. The watch
        // channel works because init_and_run publishes the context AFTER
        // the handshake (by which time the kube client exists) and before
        // the command loop runs.
        let (session_ctx_tx, session_ctx_rx) = watch::channel::<Option<Arc<SessionContext>>>(None);
        // Track substream tasks in a JoinSet so we can abort them when the
        // session ends. Without this, when the control loop exits the
        // substream tasks keep running until their dead yamux stream
        // eventually errors — they hold `Arc<SessionContext>` and any
        // `Subscription` keepalives during that window.
        let mut substream_tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        let sub_acceptor = async {
            // Wait for the session context to become available, yielding it
            // directly out of the loop so the invariant "context is Some" is
            // discharged by the `if let`, not an `.unwrap()` afterwards.
            let mut rx = session_ctx_rx;
            let ctx: Arc<SessionContext> = loop {
                if let Some(v) = rx.borrow_and_update().clone() { break v; }
                if rx.changed().await.is_err() { return; }
            };
            let mut substream_counter: u64 = 0;
            loop {
                let Some(sub_stream) = mux.accept().await else { break };
                substream_counter += 1;
                let ctx = ctx.clone();
                let sub_id = substream_counter;
                substream_tasks.spawn(async move {
                    handle_data_substream(sub_stream, ctx, sub_id).await;
                });
                // Reap finished substreams opportunistically so the JoinSet
                // doesn't grow with every accepted substream.
                while substream_tasks.try_join_next().is_some() {}
            }
        };

        tokio::select! {
            _ = Self::init_and_run_with_ctx(reader, writer, shared, session_ctx_tx, session_id) => {}
            _ = sub_acceptor => {}
        }

        // Session ended — abort any in-flight substream tasks. Each task
        // holds an Arc<SessionContext> and possibly a Subscription
        // keepalive; aborting drops them, which lets the watcher cache
        // enter its grace period normally.
        substream_tasks.abort_all();
        while substream_tasks.join_next().await.is_some() {}
    }



    /// Variant used by `init_and_run_muxed` — passes the watch sender so we
    /// can publish the `SessionContext` once the kube client is ready.
    async fn init_and_run_with_ctx(
        mut reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
        writer: Box<dyn AsyncWrite + Unpin + Send>,
        shared: Arc<SessionSharedState>,
        session_ctx_tx: watch::Sender<Option<Arc<SessionContext>>>,
        session_id: u64,
    ) {
        let mut buf_writer = BufWriter::with_capacity(protocol::IO_BUFFER_SIZE, writer);
        // Version handshake BEFORE any framed message — a mismatched peer
        // can never mis-parse an enum (see protocol::PROTOCOL_MAGIC).
        // WRITE first so a mismatched client can report the actionable
        // mismatch itself instead of a bare EOF.
        if protocol::write_handshake(&mut buf_writer).await.is_err() {
            return;
        }
        if let Err(e) = protocol::read_handshake(&mut reader).await {
            tracing::warn!("session handshake rejected: {e}");
            return;
        }
        let init = match protocol::read_bincode::<_, SessionCommand>(&mut reader).await {
            Ok(SessionCommand::Init {
                context, namespace, readonly,
                kubeconfig_yaml, env_vars, identity,
            }) => {
                InitParams { context, namespace, readonly, kubeconfig_yaml, env_vars, identity }
            }
            Ok(_) => {
                fail_session(&mut reader, &mut buf_writer,
                    "Expected Init command as first message".to_string()).await;
                return;
            }
            Err(e) => {
                fail_session(&mut reader, &mut buf_writer,
                    format!("Failed to read Init command: {}", e)).await;
                return;
            }
        };
        Self::run_session_inner(init, reader, buf_writer, shared, Some(session_ctx_tx), session_id).await;
    }


    /// Shared implementation.
    async fn run_session_inner(
        init: InitParams,
        mut reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
        mut buf_writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
        shared: Arc<SessionSharedState>,
        session_ctx_tx: Option<watch::Sender<Option<Arc<SessionContext>>>>,
        session_id: u64,
    ) {
        debug!(
            "session init: context={:?}, namespace={:?}, readonly={}",
            init.context, init.namespace, init.readonly
        );

        // 2. Resolve context name.
        let context_name = match init.context.clone() {
            Some(c) => c,
            None => {
                fail_session(&mut reader, &mut buf_writer,
                    "Init command missing context name".to_string()).await;
                return;
            }
        };

        // The session's env, owned and applied to every subprocess spawned on
        // its behalf (the in-process client's exec plugin + every `kubectl`).
        // Never written to the daemon's process env.
        let session_env = SessionEnv::new(init.env_vars.clone());

        // 3. Create kube::Client from kubeconfig + env vars.
        let (client, client_config) = match Self::create_client_from_init(&init, &session_env, &shared).await {
            Ok(c) => {
                debug!("created kube::Client for context '{}'", context_name);
                c
            }
            Err(e) => {
                warn!(context = %context_name, "cannot authenticate: {e}");
                fail_session(&mut reader, &mut buf_writer,
                    format!("Can't connect to '{context_name}' — {e}")).await;
                return;
            }
        };

        // Build ContextId from context name + server URL + a fingerprint of the
        // *resolved credentials* (NOT the user name) — two same-named users with
        // different creds on one cluster must never share a watcher. See ContextId.
        let context_id = protocol::ContextId::new(
            context_name.clone(),
            client_config.cluster_url.to_string(),
            client_build::fingerprint_auth(&client_config.auth_info),
        );

        // 4. Fetch K8s server version (best-effort, non-blocking for Ready).
        let mut identity = init.identity.clone();
        let streaming_lists = if let Ok(info) = client.apiserver_version().await {
            // The API server's version string is attacker-influenceable
            // (compromised/hostile apiserver) and renders in the header
            // through the Paragraph path — sanitize control chars.
            identity.set_k8s_version(k9rs_util::sanitize_terminal(&info.git_version));
            parse_k8s_minor(&info.git_version).is_some_and(|minor| minor >= 32)
        } else {
            false
        };

        let ready = SessionEvent::Ready {
            context: context_name.clone(),
            identity,
            namespaces: vec![],
        };
        if protocol::write_bincode(&mut buf_writer, &ready).await.is_err() {
            return;
        }

        // Attach to the context's local-resource slice (port-forwards, exec
        // sources). One attach per session, cloned to every holder that can
        // serve a local op — the command loop (ServerSession) and the
        // substream tasks (SessionContext). The slice stays alive while any
        // holder lives; the session's end drops them all, starting the
        // context's grace window.
        let locals = shared.local_registry.attach(&context_id);

        // 5. Build ServerSession and enter command loop.
        let (mut session, event_rx) = ServerSession::new(
            buf_writer,
            shared.clone(),
            client.clone(),
            context_id.clone(),
            init.readonly,
            session_env.clone(),
            locals.clone(),
        );

        // Publish the SessionContext so muxed subscription substream tasks
        // can access the kube client + context without needing a reference
        // to the ServerSession itself. Namespace is per-subscription (in
        // SubscriptionInit), not per-session.
        if let Some(tx) = session_ctx_tx {
            let ctx = Arc::new(SessionContext {
                shared,
                client,
                session_env,
                context: context_id,
                session_id,
                streaming_lists,
                readonly: init.readonly,
                locals,
            });
            let _ = tx.send(Some(ctx));
        }

        // Core resources (namespaces, nodes) are now subscribed by the TUI
        // via per-subscription yamux substreams — no server-side auto-subscribe.

        // Eagerly fetch discovery.
        session.handle_get_discovery_async();

        // Keep discovery fresh while the session is alive — new namespaces
        // and new CRDs land in the cache without the user having to
        // disconnect/reconnect.
        session.spawn_discovery_refresher();

        // Spawn metrics polling.
        session.spawn_metrics_poller();

        info!("session ready, entering command loop (context={})", session.context.name);
        if let Err(e) = session.run(reader, event_rx).await {
            info!("session ended: {}", e);
        } else {
            debug!("session ended cleanly");
        }
    }

    /// Build this session's `kube::Client` from the kubeconfig + env the TUI
    /// sent, bounded by the daemon-wide build semaphore. The credentials are
    /// pinned into the config it builds from (see
    /// [`client_build::build_session_client`]) — the daemon never mutates
    /// process-global env, so concurrent tenants don't cross-contaminate.
    async fn create_client_from_init(
        init: &InitParams,
        session_env: &SessionEnv,
        shared: &SessionSharedState,
    ) -> anyhow::Result<(kube::Client, kube::Config)> {
        // Bound concurrent (and individually blocking) client builds so a
        // connection storm can't fork unbounded auth subprocesses or stall
        // every async worker at once. The permit is held across the build.
        let _permit = shared.client_build_semaphore.acquire().await
            .map_err(|_| anyhow::anyhow!("client-build semaphore closed"))?;
        client_build::build_session_client(
            &init.kubeconfig_yaml,
            init.context.as_ref(),
            session_env,
        ).await
    }

    // -----------------------------------------------------------------------
    // Main loop
    // -----------------------------------------------------------------------

    /// Run the session until the TUI disconnects or an error occurs.
    /// Uses a spawned reader task to avoid partial-read corruption from
    /// select! cancellation.
    pub async fn run(
        mut self,
        reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
        mut event_rx: mpsc::Receiver<SessionEvent>,
    ) -> anyhow::Result<()> {
        debug!("ServerSession started: context={}", self.context.name);

        // Spawn a reader task that reads binary commands and sends them
        // through an mpsc channel. This avoids partial-read issues with
        // `select!`. Wrap the abort handle in an RAII guard so the reader
        // is reaped even if the loop body unwinds via panic — without the
        // guard, a panic between spawn and the explicit `abort()` below
        // would orphan the task.
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<SessionCommand>(32);
        let reader_handle = tokio::spawn(async move {
            binary_reader_loop(reader, cmd_tx).await;
        });
        let _reader_guard = k9rs_util::AbortOnDrop::new(reader_handle.abort_handle());

        loop {
            tokio::select! {
                // Branch 1: command from TUI (via reader task)
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            if let Err(e) = self.handle_command(cmd).await {
                                warn!("ServerSession: command error: {}", e);
                                if self
                                    .send_event(&SessionEvent::SessionError(e.to_string()))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                        None => {
                            debug!("ServerSession: TUI disconnected");
                            break;
                        }
                    }
                }

                // Branch 2: event from bridge or log tasks
                Some(event) = event_rx.recv() => {
                    if self.send_event_no_flush(&event).await.is_err() {
                        break;
                    }
                    // Drain pending events and batch-write
                    let mut write_err = false;
                    while let Ok(event) = event_rx.try_recv() {
                        if self.send_event_no_flush(&event).await.is_err() {
                            write_err = true;
                            break;
                        }
                    }
                    if write_err || self.writer.flush().await.is_err() {
                        break;
                    }
                }

                // CRD discovery for substreams is handled in
                // handle_data_substream directly — no
                // sub_ready channel needed.
            }
        }

        // `_reader_guard` aborts the reader on drop, including the unwind
        // path. Explicit cleanup of session state (subscriptions, bridges,
        // metrics poller) still runs here on the normal path.
        drop(_reader_guard);
        self.cleanup().await;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Command dispatch
    // -----------------------------------------------------------------------

    async fn handle_command(&mut self, cmd: SessionCommand) -> anyhow::Result<()> {
        match &cmd {
            SessionCommand::Init { .. } => debug!("session cmd: Init"),
            // Apply carries a full object body which, for a Secret, is the
            // decoded secret YAML — never log it verbatim (the log file may
            // be shared / world-readable via a lax umask; even at 0600 it
            // is durable). Log only the target.
            SessionCommand::Apply { target, .. } => {
                debug!("session cmd: Apply {{ target: {} }}", target.kubectl_target());
            }
            other => debug!("session cmd: {:?}", other),
        }

        // Single readonly gate — classified by the typed `is_mutating`
        // method on `SessionCommand` itself. Exhaustive match on the
        // classifier means adding a new mutating command variant without
        // declaring its classification is a compile error. No per-arm
        // `reject_if_readonly` calls; forgetting to call one becomes
        // structurally impossible.
        if cmd.is_mutating() && self.readonly {
            // Command-level (target-less) rejection: this gate fires on the
            // whole command before any handler runs. The client blocks
            // read-only mutations before the wire too, so no batch tracker
            // can be left waiting on a per-target result here.
            self.reject_command_async("Session is read-only".to_string());
            return Ok(());
        }

        match cmd {
            SessionCommand::Init { .. } => {
                debug!("ServerSession: ignoring duplicate Init");
            }

            // CRUD ops: route by resource backend (K8s vs local). The
            // dispatchers live as private methods so `handle_command`
            // reads as a flat per-variant match.
            SessionCommand::Describe(ref obj) => self.dispatch_describe(obj).await?,
            SessionCommand::Yaml(ref obj) => self.dispatch_yaml(obj).await?,
            SessionCommand::Delete(ref obj) => self.dispatch_delete(obj),
            SessionCommand::Apply { ref target, ref yaml } => self.dispatch_apply(target, yaml.clone()),

            // K8s-only mutating ops. Locals are rejected inside the
            // handlers via `require_capability` / typed kind checks.
            SessionCommand::ForceKill(ref obj) => self.handle_force_kill_async(obj),
            SessionCommand::Scale { ref target, replicas } => self.handle_scale_async(target, replicas),
            SessionCommand::Restart(ref obj) => self.handle_restart_async(obj),
            SessionCommand::DecodeSecret(ref target) => self.handle_decode_secret_async(target),
            SessionCommand::TriggerCronJob(ref target) => self.handle_trigger_cronjob_async(target),
            SessionCommand::ToggleSuspendCronJob(ref target) => self.handle_toggle_suspend_cronjob_async(target),
            SessionCommand::PortForward { ref target, local_port, container_port } => {
                self.handle_port_forward(target, local_port, container_port);
            }

            SessionCommand::GetDiscovery => {
                self.handle_get_discovery_async();
            }

            // Management commands should not arrive on a session connection.
            SessionCommand::Ping | SessionCommand::Status
            | SessionCommand::Shutdown | SessionCommand::Clear { .. } => {
                debug!("ServerSession: ignoring management command on session connection");
            }
        }

        Ok(())
    }

    /// Describe dispatcher: sends the response synchronously for local
    /// resources (the lookup is cheap), spawns the async handler for K8s.
    async fn dispatch_describe(&mut self, obj: &protocol::ObjectRef) -> anyhow::Result<()> {
        if obj.resource.is_local() {
            let content = self.describe_local(&obj.resource, &obj.name);
            self.send_event(&SessionEvent::DescribeResult {
                target: obj.clone(),
                lines: k9rs_kube::describe::describe_lines_from_text(&content),
            }).await?;
        } else {
            self.handle_describe_async(obj);
        }
        Ok(())
    }

    /// Yaml dispatcher: mirrors `dispatch_describe`.
    async fn dispatch_yaml(&mut self, obj: &protocol::ObjectRef) -> anyhow::Result<()> {
        if obj.resource.is_local() {
            let content = self.yaml_local(&obj.resource, &obj.name);
            self.send_event(&SessionEvent::YamlResult {
                target: obj.clone(),
                content,
            }).await?;
        } else {
            self.handle_yaml_async(obj);
        }
        Ok(())
    }

    /// Delete dispatcher: routes by backend. Port-forward "delete" means
    /// "stop the forward" — a daemon-side mutation which the readonly
    /// gate already refused before we got here.
    fn dispatch_delete(&mut self, obj: &protocol::ObjectRef) {
        if obj.resource.is_local() {
            self.handle_delete_local(obj);
        } else {
            self.handle_delete_async(obj);
        }
    }

    /// Apply dispatcher: routes by backend. Same-wire `Apply { target, yaml }`
    /// works for both K8s (server-side apply via kube-rs) and locals (parse
    /// the edit view and reconcile).
    fn dispatch_apply(&mut self, target: &protocol::ObjectRef, yaml: String) {
        if target.resource.is_local() {
            self.handle_apply_local(target, &yaml);
        } else {
            self.handle_apply_async(target, yaml);
        }
    }

    // -----------------------------------------------------------------------
    // -----------------------------------------------------------------------
    // Socket I/O (binary)
    // -----------------------------------------------------------------------

    async fn send_event(&mut self, event: &SessionEvent) -> anyhow::Result<()> {
        protocol::write_bincode(&mut self.writer, event).await
    }

    async fn send_event_no_flush(&mut self, event: &SessionEvent) -> anyhow::Result<()> {
        protocol::write_bincode_no_flush(&mut self.writer, event).await
    }

    /// Render a describe view for a local-resource row by dispatching to the
    /// owning per-context `LocalResourceSource`.
    fn describe_local(&self, rid: &protocol::ResourceId, name: &str) -> String {
        self.local_view(rid, name, "describe", |s, n| s.describe(n))
    }

    /// Render a YAML view for a local-resource row by dispatching to the
    /// owning per-context `LocalResourceSource`.
    fn yaml_local(&self, rid: &protocol::ResourceId, name: &str) -> String {
        self.local_view(rid, name, "yaml", |s, n| s.yaml(n))
    }

    /// Shared helper for describe/yaml on local resources: looks up the
    /// source, calls the accessor, and formats the error/unsupported
    /// fallback. The `view_name` is only used in the "not supported"
    /// message.
    fn local_view(
        &self,
        rid: &protocol::ResourceId,
        name: &str,
        view_name: &str,
        accessor: fn(&dyn k9rs_kube::local::LocalResourceSource, &str) -> Option<Result<String, String>>,
    ) -> String {
        let Some(source) = self.locals.get(rid) else {
            return format!("Unknown local resource type: {}", rid.plural());
        };
        match accessor(&*source, name) {
            Some(Ok(content)) => content,
            Some(Err(e)) => format!("Error: {}", e),
            None => format!("{} is not supported on {}", view_name, rid.plural()),
        }
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    async fn cleanup(&mut self) {
        // Subscription bridges are gone — subscriptions live on per-yamux
        // substreams now. When the mux connection drops (session exits),
        // all substream reads return EOF and the bridge tasks exit naturally.
        // Guards abort on drop; taking them here keeps teardown explicit on
        // the normal path (and drop covers the cancelled-select path).
        drop(self.metrics_task.take());
        drop(self.discovery_refresher_task.take());
        // Abort every pending mutating handler (scale/restart/apply/delete
        // /etc) and drain. Without this, a session that exits while a
        // handler is mid-`api.patch().await` would leave the task running
        // detached against the kube client.
        self.pending_tasks.abort_all();
        while self.pending_tasks.join_next().await.is_some() {}
        debug!("ServerSession: cleaned up");
    }

    /// Spawn a background task and track it on the session's `JoinSet`.
    /// Every `handle_*_async` helper goes through this instead of bare
    /// `tokio::spawn` so that session shutdown can abort all in-flight
    /// work in one place (`cleanup`). Reaps finished tasks opportunistically
    /// so the JoinSet doesn't grow without bound.
    pub(super) fn track_task<F>(&mut self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        // Drain any already-finished tasks before adding a new one.
        while self.pending_tasks.try_join_next().is_some() {}
        self.pending_tasks.spawn(fut);
    }
}

// ---------------------------------------------------------------------------
// Binary reader loop (spawned as a task)
// ---------------------------------------------------------------------------

/// Reads binary SessionCommands from the reader and sends them through the
/// channel. Runs in a separate task so select! cancellation can't corrupt
/// partially-read frames.
async fn binary_reader_loop(
    mut reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    tx: mpsc::Sender<SessionCommand>,
) {
    loop {
        match protocol::read_bincode::<_, SessionCommand>(&mut reader).await {
            Ok(cmd) => {
                if tx.send(cmd).await.is_err() { break; }
            }
            Err(e) => {
                debug!("ServerSession binary reader: {}", e);
                break;
            }
        }
    }
}

// Old bridge functions (`bridge_subscription_to_events`,
// `bridge_local_subscription_to_events`) were deleted — subscriptions now
// flow on per-subscription yamux substreams via
// `handle_data_substream` below.

// ---------------------------------------------------------------------------
// Per-subscription substream handler
// ---------------------------------------------------------------------------

/// Handle a single subscription substream accepted from the yamux
/// connection. Reads the `SubscriptionInit` handshake, sets up the kube
/// watcher via the existing `WatcherCache`, and writes `StreamEvent`s
/// back to the substream until it closes.
///
/// This function is the per-substream counterpart of the old
/// `handle_subscribe_resource` + `bridge_subscription_to_events` path.
/// The key difference: each subscription owns its own substream, so
/// there's no shared `subscriptions: HashMap<ResourceId, ...>` map, no
/// early-return guard, and no rid-keyed collision. The bridge holds a
/// `Subscription` (which keeps the underlying watcher alive via Arc);
/// when the substream closes (TUI dropped it), the Subscription drops
/// and the watcher enters its grace period via the existing
/// `live_query::Subscription::Drop` machinery.
/// Handle a single subscription substream from the yamux connection.
/// Reads `SubscriptionInit`, sets up the watcher, and streams
/// `StreamEvent`s back to the TUI.
/// Dispatch a data substream based on its first message (SubstreamInit).
async fn handle_data_substream(
    stream: k9rs_mux::MuxedStream,
    ctx: Arc<SessionContext>,
    sub_id: u64,
) {
    let (read_half, write_half) = tokio::io::split(stream);
    let mut reader = tokio::io::BufReader::with_capacity(protocol::IO_BUFFER_SIZE, read_half);
    let writer = tokio::io::BufWriter::with_capacity(protocol::IO_BUFFER_SIZE, write_half);

    let init: protocol::SubstreamInit = match protocol::read_bincode(&mut reader).await {
        Ok(i) => i,
        Err(e) => {
            tracing::warn!(session = ctx.session_id, sub = sub_id, "substream: failed to read init: {}", e);
            return;
        }
    };

    let sid = ctx.session_id;
    match init {
        // Subscribe and Log are WRITE-ONLY from here on, so the client
        // dropping its end is invisible to them — the only liveness signal a
        // bridge has is a failing write, and on a quiet resource that write
        // may never come. A parked bridge holds its `Subscription`, which
        // keeps the watcher (and its apiserver watch, and its full row store)
        // alive for the whole session, bypassing the 60s grace entirely; a
        // `follow` log likewise leaves its `kubectl logs -f` running.
        //
        // So race the bridge against the read half we otherwise ignore.
        // Losing the race drops the bridge future, which drops the
        // `Subscription` / kills the child — the teardown that was supposed
        // to happen on close.
        protocol::SubstreamInit::Subscribe(sub_init) => {
            let bridge = handle_subscription_substream_inner(sub_init, writer, ctx, sid, sub_id);
            tokio::pin!(bridge);
            tokio::select! {
                _ = &mut bridge => {}
                _ = peer_closed(&mut reader) => {
                    tracing::debug!(session = sid, sub = sub_id, "subscription substream closed by client");
                }
            }
        }
        protocol::SubstreamInit::Log(log_init) => {
            tracing::debug!(session = sid, sub = sub_id, "log: target={} container={:?}", log_init.target.display(), log_init.container);
            let bridge = handle_log_substream(log_init, writer, ctx);
            tokio::pin!(bridge);
            tokio::select! {
                _ = &mut bridge => {}
                _ = peer_closed(&mut reader) => {
                    tracing::debug!(session = sid, sub = sub_id, "log substream closed by client");
                }
            }
        }
        protocol::SubstreamInit::Exec(exec_init) => {
            tracing::debug!(session = sid, sub = sub_id, "exec: {:?}", exec_init.kubectl_args);
            handle_exec_substream(exec_init, reader, writer, ctx).await;
        }
    }
}

/// Resolves when the peer closes its end of a data substream.
///
/// A client only ever WRITES its `SubstreamInit` and then reads, so anything
/// observable on this half afterwards is EOF or a reset — i.e. the client is
/// gone. Cancel-safe (`read` is), so it is sound as a `select!` branch.
pub(super) async fn peer_closed<R: AsyncRead + Unpin>(reader: &mut R) {
    use tokio::io::AsyncReadExt;
    let mut sink = [0u8; 64];
    while reader.read(&mut sink).await.unwrap_or(0) > 0 {}
}

/// Write a delta, degrading an over-cap one into a fresh baseline request
/// rather than a silent stream death.
///
/// The BASELINE path has guarded this since v8 — an over-cap frame can never
/// be delivered, and retrying it churns (re-attach, re-LIST, re-serialize)
/// forever. Deltas had no such guard: `write_bincode` failed the size check,
/// the bridge returned, the client saw EOF and re-subscribed, and the whole
/// thing repeated with no typed error anywhere. Reachable when one flush
/// window coalesces a very large relist.
///
/// Returning `false` asks the caller to re-attach, which lands on the guarded
/// baseline path — so an oversized delta degrades into either a fresh
/// baseline or a real error message, never a silent loop.
async fn write_delta(
    writer: &mut tokio::io::BufWriter<tokio::io::WriteHalf<k9rs_mux::MuxedStream>>,
    delta: &protocol::TableDelta,
    rid: &protocol::ResourceId,
) -> bool {
    let frame = protocol::StreamEventRef::Delta(delta);
    if protocol::frame_exceeds_cap(&frame) {
        tracing::warn!(
            resource = %rid.plural(),
            changes = delta.changes.len(),
            "delta exceeds the frame cap — re-baselining instead",
        );
        return false;
    }
    protocol::write_bincode(writer, &frame).await.is_ok() && writer.flush().await.is_ok()
}

async fn handle_subscription_substream_inner(
    init: protocol::SubscriptionInit,
    mut writer: tokio::io::BufWriter<tokio::io::WriteHalf<k9rs_mux::MuxedStream>>,
    ctx: Arc<SessionContext>,
    session_id: u64,
    sub_id: u64,
) {
    use k9rs_kube::live_query::QueryKey;
    use tokio::io::AsyncWriteExt;

    let rid = init.resource.clone();
    let filter = init.filter.clone();
    let sub_start = std::time::Instant::now();
    tracing::debug!(session = session_id, sub = sub_id, "subscribe: {}({}) filter={:?}",
        rid.plural(), init.namespace.display(), filter);

    // Local resources branch out into the LocalResourceSource pipeline.
    if rid.is_local() {
        let Some(source) = ctx.locals.get(&rid) else {
            let _ = protocol::write_bincode(&mut writer, &protocol::StreamEvent::Error(
                format!("Unknown local resource: {}", rid.plural())
            )).await;
            return;
        };
        // Capabilities are computed client-side from the typed rid via
        // `ResourceId::capabilities()`; no wire send needed.
        let mut sub = k9rs_kube::local::LocalSubscription::new(
            rid.clone(),
            source.subscribe(),
            source,
        );
        // Local sources are Baseline-only streams by contract (their watch
        // channel is a latest-full-value source; a local Delta is
        // unrepresentable). The client applies them through the same
        // Baseline path as everything else.
        let _ = protocol::write_bincode(&mut writer,
            &protocol::StreamEvent::Baseline(sub.current())).await;
        let _ = writer.flush().await;
        loop {
            if sub.changed().await.is_err() { break; }
            if protocol::write_bincode(&mut writer,
                &protocol::StreamEvent::Baseline(sub.current())).await.is_err() { break; }
            if writer.flush().await.is_err() { break; }
        }
        return;
    }

    // K8s resource: build QueryKey, subscribe via WatcherCache.
    // Namespace comes from the client's SubscriptionInit — each subscription
    // declares its own namespace so the server doesn't need mutable session state.
    let effective_ns = if rid.is_cluster_scoped() {
        protocol::Namespace::All
    } else {
        init.namespace.clone()
    };
    // A POST-filter is applied in the bridge (`row_passes`), not by the
    // apiserver. Exclude it from the QueryKey so those subscriptions reuse
    // the same watcher as unfiltered ones instead of opening a duplicate
    // watch over identical data. Asked through `is_post_filter` rather than
    // matched here, so the two places that decide "does the apiserver do
    // this one?" cannot answer differently.
    let cache_filter = if is_post_filter(&filter) { None } else { filter.clone() };
    let key = QueryKey {
        context: ctx.context.clone(),
        namespace: effective_ns,
        resource: rid.clone(),
        filter: cache_filter,
    };

    // Share the session's already-authenticated client across all of its
    // watchers. Building a *fresh* client per watcher would re-run the exec
    // credential plugin (~800ms, a synchronous subprocess) on an async worker
    // thread — and under the `WatcherCache` shard lock, at that (the SEV-4
    // finding). The clone is just an `Arc` bump; a session's watches multiplex
    // over its one client (HTTP/2), which is the right default.
    let make_client = || ctx.client.clone();

    // CRD arms (resolved and unresolved) produce a (gvk, plural, scope)
    // tuple for the shared subscribe_dynamic path. Built-ins subscribe
    // directly via the typed registry.
    enum CrdOrSub {
        BuiltIn(k9rs_kube::live_query::Subscription),
        Crd(kube::api::GroupVersionKind, String, protocol::ResourceScope),
    }

    let crd_or_sub = match &rid {
        protocol::ResourceId::BuiltIn(kind) => {
            // Built-ins go through the typed registry path — the watcher
            // factory is dispatched on the typed kind, no string match.
            // Hand the destructured `kind` in so the cache doesn't have to
            // re-extract it at runtime.
            CrdOrSub::BuiltIn(if init.force {
                ctx.shared.watcher_cache.subscribe_force(key.clone(), *kind, &make_client(), ctx.streaming_lists)
            } else {
                ctx.shared.watcher_cache.subscribe(key.clone(), *kind, make_client, ctx.streaming_lists)
            })
        }
        protocol::ResourceId::Crd(crd_ref) => {
            // Resolved CRD: GVR is fully populated, use it directly.
            let gvk = kube::api::GroupVersionKind::gvk(&crd_ref.group, &crd_ref.version, &crd_ref.kind);
            CrdOrSub::Crd(gvk, crd_ref.plural.clone(), crd_ref.scope)
        }
        protocol::ResourceId::CrdUnresolved(plural_name) => {
            // Unresolved CRD: the client only knows the plural name.
            // Resolve via API discovery and notify the TUI of the
            // resolved identity.
            match k9rs_kube::describe::api_resource_for(&make_client(), &rid).await {
                Ok((ar, resolved_scope)) => {
                    let gvk = kube::api::GroupVersionKind::gvk(&ar.group, &ar.version, &ar.kind);
                    let resolved_rid = protocol::ResourceId::crd(
                        ar.group.clone(), ar.version.clone(),
                        ar.kind.clone(), ar.plural.clone(), resolved_scope,
                    );
                    let _ = protocol::write_bincode(&mut writer, &protocol::StreamEvent::Resolved {
                        original: rid.clone(),
                        resolved: resolved_rid,
                    }).await;
                    CrdOrSub::Crd(gvk, ar.plural, resolved_scope)
                }
                Err(e) => {
                    tracing::warn!("Failed to resolve resource '{}': {}", plural_name, e);
                    let _ = protocol::write_bincode(&mut writer, &protocol::StreamEvent::Error(
                        format!("Unknown resource: {}", plural_name)
                    )).await;
                    return;
                }
            }
        }
        protocol::ResourceId::Local(_) => {
            // Unreachable — we early-returned above for locals.
            unreachable!("local rids are handled by the early-return branch");
        }
    };

    // (ResubInfo hoisted to module scope for the bridge helpers.)


    let (mut sub, resub) = match crd_or_sub {
        CrdOrSub::BuiltIn(sub) => {
            let kind = match &rid { protocol::ResourceId::BuiltIn(k) => *k, _ => unreachable!() };
            (sub, ResubInfo::BuiltIn { key, kind })
        }
        CrdOrSub::Crd(gvk, plural, scope) => {
            let resolved_key = QueryKey {
                context: key.context,
                namespace: key.namespace,
                resource: protocol::ResourceId::crd(
                    gvk.group.clone(), gvk.version.clone(),
                    gvk.kind.clone(), plural.clone(), scope,
                ),
                filter: key.filter,
            };
            if init.force {
                ctx.shared.watcher_cache.remove(&resolved_key);
            }
            // The watcher resolves its own printer columns at birth (see
            // resolve_printer_columns) — nothing here can hand it a plan
            // built from a cold cache.
            let sub = ctx.shared.watcher_cache.subscribe_dynamic(
                resolved_key.clone(), make_client, gvk.clone(),
                plural.clone(), scope, Arc::clone(&ctx.shared.discovery_cache),
                ctx.streaming_lists,
            );
            (sub, ResubInfo::Dynamic { key: resolved_key, gvk, plural, scope })
        }
    };

    // Bridge: watcher → StreamEvent frames on the substream.
    //
    // Stream contract: Baseline first, then Deltas; a fresh Baseline (same
    // stream) repairs fanout lag; watcher death → resubscribe with backoff
    // while the TUI keeps its last data — the new attach forwards deltas
    // immediately (refreshing the stale table in place) and sends the
    // authoritative post-list Baseline at InitDone, which clears any rows
    // that died during the outage.
    //
    // The bridge is the serialization point: baselines (oneshot ask) and
    // deltas (broadcast ring) are two daemon-internal channels, but this
    // task imposes baseline-before-deltas on the single ordered substream,
    // and idempotent application makes baseline/delta overlap harmless.
    let mut retry_backoff = std::time::Duration::from_secs(5);
    // Join semantics on first attach (baseline as soon as there's data);
    // relist semantics on every re-attach (defer to InitDone).
    let mut after_initial_list = false;
    let mut sent_any_data = false;

    'attach: loop {
        // Cursor is already pinned (sub holds its broadcast receiver), so
        // the baseline can only overlap buffered deltas, never gap.
        // `None` from request_baseline = the ask channel is closed, i.e.
        // the watcher is already gone.
        let step: BaselineStep = match sub.request_baseline(after_initial_list).await {
            None => BaselineStep::Died(None),
            Some(mut baseline_rx) => loop {
                tokio::select! {
                    b = &mut baseline_rx => break match b {
                        Ok(b) => BaselineStep::Got(b),
                        // Reply sender dropped: the watcher took our ask and
                        // then died. Its terminal Dead is on the ring (sent
                        // before the task returns) — captured below.
                        Err(_) => BaselineStep::Died(None),
                    },
                    msg = sub.recv() => match msg {
                        Ok(msg) => match &*msg {
                            // Forward deltas while the baseline is deferred —
                            // keeps the client fresh during a long re-LIST and
                            // keeps the ring drained.
                            k9rs_kube::live_query::WatcherMsg::Delta(delta) => {
                                if sent_any_data
                                    && !write_delta(&mut writer, filter_delta(delta, &filter).as_ref(), &rid).await
                                {
                                    // Either the peer is gone or the frame is
                                    // too big; the deferred baseline that
                                    // follows repairs the latter.
                                    break BaselineStep::Retry;
                                }
                            }
                            // Health transitions ride through even while the
                            // baseline is deferred: a re-LIST that takes a
                            // minute is exactly when the client most needs to
                            // know its rows stopped moving.
                            k9rs_kube::live_query::WatcherMsg::Health(h) => {
                                if sent_any_data && !announce_health(&mut writer, h).await { return; }
                            }
                            // Carry the reason OUT — do NOT fall through to a
                            // re-drain (the terminal Dead is now consumed, and
                            // re-draining a keepalive-held-open channel hangs).
                            k9rs_kube::live_query::WatcherMsg::Dead(reason) => {
                                break BaselineStep::Died(Some(reason.clone()));
                            }
                        },
                        // Lagged BEFORE the baseline landed: the fresh
                        // baseline we re-ask for repairs it — the watcher is
                        // alive, so this is a retry, never a death.
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            break BaselineStep::Retry;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            break BaselineStep::Died(None);
                        }
                    },
                }
            },
        };

        let (baseline, attach_health) = match step {
            BaselineStep::Got(reply) => (reply.baseline, reply.health),
            BaselineStep::Retry => {
                after_initial_list = false;
                continue 'attach;
            }
            BaselineStep::Died(reason) => {
                // Use the captured reason if we have it; otherwise pull the
                // terminal Dead off the ring — BOUNDED (the Subscription
                // keeps the broadcast Sender alive, so a plain recv loop
                // could never see Closed and would hang).
                let detail = match reason {
                    Some(r) => r,
                    None => drain_death_reason(&mut sub).await.unwrap_or_default(),
                };
                // Permanent iff the watcher never produced data for us
                // (first LIST fail-fast: RBAC, unknown resource, CRD
                // printer-column resolve error); transient otherwise.
                if !sent_any_data {
                    let msg = if detail.is_empty() {
                        format!("Watcher failed for {}", rid.plural())
                    } else {
                        format!("Watcher failed for {}: {}", rid.plural(), detail)
                    };
                    let _ = protocol::write_bincode(&mut writer, &protocol::StreamEvent::Error(msg)).await;
                    let _ = writer.flush().await;
                    return;
                }
                // Drop the DEAD subscription before `resubscribe` sleeps
                // out the backoff (up to 60s). `Subscription::drop` is built
                // so a self-terminated watcher skips grace and reclaims at
                // once; holding `sub` across the sleep defeated that, pinning
                // the dead watcher's 64-batch fanout ring — a full row set on
                // a quiet resource — while its successor built a second one.
                // A control-plane upgrade that kills every watch at once made
                // that 2x row memory daemon-wide, in the process hosting
                // every session.
                drop(sub);
                let Some(next) = resubscribe(
                    &ctx, &resub, make_client, &mut retry_backoff, &detail, session_id,
                    &mut writer,
                ).await else { return };
                sub = next;
                after_initial_list = true;
                continue 'attach;
            }
        };

        let baseline = filter_baseline(baseline, &filter);
        // A baseline that exceeds the frame cap can never be delivered;
        // sending it would fail on the write cap AND — after a prior
        // success — churn (re-attach, re-LIST, re-serialize) forever.
        // Surface it as a TERMINAL typed error instead (the Error frame
        // itself is tiny), so the client shows a real message and stops
        // retrying this subscription.
        let baseline = protocol::StreamEvent::Baseline(baseline);
        if protocol::frame_exceeds_cap(&baseline) {
            let msg = format!(
                "{} is too large to stream ({}): try a namespace or label filter",
                rid.plural(), init.namespace.display(),
            );
            let _ = protocol::write_bincode(&mut writer, &protocol::StreamEvent::Error(msg)).await;
            let _ = writer.flush().await;
            return;
        }
        if protocol::write_bincode(&mut writer, &baseline).await.is_err() { return; }
        if writer.flush().await.is_err() { return; }
        if !sent_any_data {
            tracing::info!(session = session_id, sub = sub_id,
                "subscribe: {}({}) baseline ready in {:?}",
                rid.plural(), init.namespace.display(), sub_start.elapsed());
        }
        sent_any_data = true;
        retry_backoff = std::time::Duration::from_secs(5);
        // Attaching to a SHARED watcher that is already degraded: the
        // transition went out before this bridge pinned its cursor, so the
        // ring can never tell us. The baseline reply does. (Announced after
        // the baseline so the client applies rows first, then learns they
        // are frozen — the other order would be cleared by the baseline.)
        if attach_health != k9rs_kube::live_query::WatchHealth::Feeding
            && !announce_health(&mut writer, &attach_health).await
        {
            return;
        }

        loop {
            match sub.recv().await {
                Ok(msg) => match &*msg {
                    k9rs_kube::live_query::WatcherMsg::Delta(delta) => {
                        if !write_delta(&mut writer, filter_delta(delta, &filter).as_ref(), &rid).await {
                            // Re-attach: the baseline path is capped and will
                            // report a real error if the state is undeliverable.
                            after_initial_list = false;
                            continue 'attach;
                        }
                    }
                    k9rs_kube::live_query::WatcherMsg::Health(h) => {
                        if !announce_health(&mut writer, h).await { return; }
                    }
                    k9rs_kube::live_query::WatcherMsg::Dead(reason) => {
                        let detail = reason.clone();
                        drop(msg); // ends the borrow of the frame we just read
                        drop(sub); // see the note at the first `resubscribe`
                        let Some(next) = resubscribe(
                            &ctx, &resub, make_client, &mut retry_backoff, &detail, session_id,
                            &mut writer,
                        ).await else { return };
                        sub = next;
                        after_initial_list = true;
                        continue 'attach;
                    }
                },
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    // Fell behind the ring: repair with a fresh in-band
                    // Baseline (replaces everything client-side), then
                    // continue from the advanced cursor — overlap is
                    // idempotent.
                    tracing::debug!(session = session_id, sub = sub_id,
                        "subscription lagged {} batches for {}, re-baselining", n, rid.plural());
                    after_initial_list = false;
                    continue 'attach;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    let detail = String::new();
                    drop(sub); // see the note at the first `resubscribe`
                    let Some(next) = resubscribe(
                        &ctx, &resub, make_client, &mut retry_backoff, &detail, session_id,
                        &mut writer,
                    ).await else { return };
                    sub = next;
                    after_initial_list = true;
                    continue 'attach;
                }
            }
        }
    }
}

/// Outcome of one baseline-acquisition attempt in the attach loop.
enum BaselineStep {
    /// The baseline arrived — proceed to the steady delta loop. It carries
    /// the watcher's health, which is the ONLY way a bridge attaching to an
    /// already-degraded shared watcher can learn it missed the transition.
    Got(k9rs_kube::live_query::BaselineReply),
    /// The subscriber lagged before the baseline landed; re-ask (the
    /// watcher is alive).
    Retry,
    /// The watcher died. `Some(reason)` when we captured the terminal
    /// Dead directly; `None` when only the ask/channel signalled death
    /// and the reason must be drained off the ring.
    Died(Option<String>),
}

// Capture re-subscription recipe so the bridge can retry after watcher
// death. Built-ins need (key, kind); CRDs need the resolved identity.
enum ResubInfo {
    BuiltIn {
        key: k9rs_kube::live_query::QueryKey,
        kind: k9rs_proto::resource_def::BuiltInKind,
    },
    Dynamic {
        key: k9rs_kube::live_query::QueryKey,
        gvk: kube::api::GroupVersionKind,
        plural: String,
        scope: protocol::ResourceScope,
        // No printer_columns snapshot: the respawned watcher re-resolves
        // its own — a stale or cold plan can't be replayed.
    },
}

/// After a watcher death, pull the terminal `Dead(reason)` off the ring if
/// it's still there (it is sent as the task's final act, BEFORE the task
/// returns — so once we observe the ask/channel signalling death, the Dead
/// is already buffered and this returns in microseconds).
///
/// BOUNDED on two axes: the `Subscription` keeps the broadcast `Sender`
/// alive (via `_keepalive`), so a plain recv loop could never see `Closed`
/// and would hang if the Dead had already been consumed elsewhere. So we
/// short-circuit the instant the watcher task is observably finished-with-
/// no-Dead-pending, and cap the whole thing with a timeout as a backstop.
async fn drain_death_reason(sub: &mut k9rs_kube::live_query::Subscription) -> Option<String> {
    let drained = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match sub.recv().await {
                Ok(msg) => {
                    if let k9rs_kube::live_query::WatcherMsg::Dead(reason) = &*msg {
                        return Some(reason.clone());
                    }
                    // A non-Dead message AND the task already finished means
                    // the terminal Dead was evicted from the ring (a Lagged
                    // overrun) — nothing more is coming. Give up now rather
                    // than waiting out the 2s timeout (the Sender is kept
                    // alive by the Subscription's keepalive, so Closed never
                    // fires to end the loop otherwise).
                    if sub.watcher_finished() {
                        return None;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Skipped past the Dead while lagged: if the task is
                    // gone there is nothing left to find.
                    if sub.watcher_finished() {
                        return None;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    })
    .await;
    drained.unwrap_or(None)
}

/// Tell the client whether the CLUSTER hop is feeding this subscription.
/// One function so every path that changes the answer — watcher transition,
/// bridge backoff, attach-time catch-up — speaks with the same voice.
/// `false` = the substream is broken and the bridge should return.
async fn announce_health(
    writer: &mut tokio::io::BufWriter<tokio::io::WriteHalf<k9rs_mux::MuxedStream>>,
    health: &k9rs_kube::live_query::WatchHealth,
) -> bool {
    use k9rs_kube::live_query::WatchHealth;
    let event = match health {
        WatchHealth::Feeding => protocol::StreamEventRef::Live,
        WatchHealth::Degraded(reason) => protocol::StreamEventRef::Stale(reason),
    };
    protocol::write_bincode(writer, &event).await.is_ok() && writer.flush().await.is_ok()
}

/// Backoff, then re-subscribe (the WatcherCache sees the dead watcher and
/// spawns a fresh one). ±25% jitter prevents thundering herd when many
/// watchers die simultaneously (e.g., after laptop suspend/resume).
///
/// Owns the staleness announcement rather than leaving it to the three call
/// sites: this function IS the outage — it is the only place that knows the
/// client is about to wait, and for how long — so a resubscribe path can't
/// be added that forgets to say so. `None` = the substream broke while
/// announcing; the bridge returns.
///
/// PRECONDITION: the client already has rows. Every caller is downstream of
/// the `!sent_any_data` fail-fast (a watcher that never produced data is a
/// terminal `StreamEvent::Error`, not a retry), so there is always something
/// on screen to declare frozen.
#[allow(clippy::too_many_arguments)]
async fn resubscribe(
    ctx: &Arc<SessionContext>,
    resub: &ResubInfo,
    make_client: impl Fn() -> kube::Client + Clone,
    retry_backoff: &mut std::time::Duration,
    detail: &str,
    session_id: u64,
    writer: &mut tokio::io::BufWriter<tokio::io::WriteHalf<k9rs_mux::MuxedStream>>,
) -> Option<k9rs_kube::live_query::Subscription> {
    let rid_label = match resub {
        ResubInfo::BuiltIn { key, .. } | ResubInfo::Dynamic { key, .. } => key.resource.plural(),
    };
    let jitter_factor = k9rs_util::retry_jitter(rid_label.as_bytes(), retry_backoff.as_millis() as u64);
    let jittered = retry_backoff.mul_f64(jitter_factor);
    tracing::info!(session = session_id,
        "watcher died for {}, retrying in {:?}: {}", rid_label, jittered, detail);
    // Say so BEFORE sleeping — the whole point is that the client is not
    // left painting frozen rows for the length of the backoff (up to a
    // minute).
    let reason = if detail.is_empty() {
        format!("watch for {} stopped — retrying in {:?}", rid_label, jittered)
    } else {
        format!("watch for {} failed ({}) — retrying in {:?}", rid_label, detail, jittered)
    };
    if !announce_health(writer, &k9rs_kube::live_query::WatchHealth::Degraded(reason)).await {
        return None;
    }
    tokio::time::sleep(jittered).await;
    *retry_backoff = (*retry_backoff * 2).min(std::time::Duration::from_secs(60));

    Some(match resub {
        ResubInfo::BuiltIn { key, kind } => {
            ctx.shared.watcher_cache.subscribe(key.clone(), *kind, make_client, ctx.streaming_lists)
        }
        ResubInfo::Dynamic { key, gvk, plural, scope } => {
            ctx.shared.watcher_cache.subscribe_dynamic(
                key.clone(), make_client, gvk.clone(),
                plural.clone(), *scope, Arc::clone(&ctx.shared.discovery_cache),
                ctx.streaming_lists,
            )
        }
    })
}

/// Does this row survive a post-filter?
///
/// The EXHAUSTIVE match is the point: `Labels`/`Field` push down to the API
/// and never reach here, and the two that can't push down must each say so
/// by name. The callers below used to ask `let Some(OwnerUid(uid)) = filter
/// else { return unchanged }`, which is a wildcard wearing a pattern — a new
/// post-filtered variant would have been silently ignored, and an ignored
/// filter shows MORE rows than asked for.
fn row_passes(
    row: &k9rs_proto::row::ResourceRow,
    filter: &protocol::SubscriptionFilter,
) -> bool {
    match filter {
        protocol::SubscriptionFilter::OwnerUid(uid) => {
            row.owner_refs.iter().any(|r| r.uid == *uid)
        }
        protocol::SubscriptionFilter::References { kind, name } => row
            .references
            .iter()
            .any(|r| r.kind == *kind && r.name == *name),
        // Pushed down to the apiserver; the rows never needed filtering.
        protocol::SubscriptionFilter::Labels(_) | protocol::SubscriptionFilter::Field(_) => true,
    }
}

/// Whether a filter is applied HERE rather than by the apiserver.
fn is_post_filter(filter: &Option<protocol::SubscriptionFilter>) -> bool {
    matches!(
        filter,
        Some(
            protocol::SubscriptionFilter::OwnerUid(_)
                | protocol::SubscriptionFilter::References { .. }
        )
    )
}

/// Post-filter, baseline flavor. Some `SubscriptionFilter` variants can't be
/// applied by K8s (no field selector exists for an ownerReference or for a
/// spec's references), so the bridge filters each frame. Exhaustive over
/// frame kinds by construction — one function per frame type, no wildcard
/// arm a future variant can slip past.
fn filter_baseline(
    mut baseline: protocol::TableBaseline,
    filter: &Option<protocol::SubscriptionFilter>,
) -> protocol::TableBaseline {
    let Some(filter) = filter.as_ref().filter(|_| is_post_filter(filter)) else {
        return baseline;
    };
    baseline.rows.retain(|row| row_passes(row, filter));
    baseline
}

/// Post-filter, delta flavor. Stateless: an upsert that fails the filter
/// becomes a Remove — the row may never have been shown, but client removes
/// are idempotent (remove-of-absent is a no-op), so a pod whose ownerRef (or
/// mounted secret) mutates away from the drill correctly disappears.
fn filter_delta<'a>(
    delta: &'a protocol::TableDelta,
    filter: &Option<protocol::SubscriptionFilter>,
) -> std::borrow::Cow<'a, protocol::TableDelta> {
    // Nothing post-filtered → BORROW the delta unchanged (the common case:
    // no clone, no rewrite). Only a post-filter pays for an owned copy.
    let Some(filter) = filter.as_ref().filter(|_| is_post_filter(filter)) else {
        return std::borrow::Cow::Borrowed(delta);
    };
    let changes = delta.changes.iter().map(|change| match change {
        protocol::RowChange::Upsert(row) => {
            if row_passes(row, filter) {
                protocol::RowChange::Upsert(row.clone())
            } else {
                // `change.key()` — the canonical derivation, not a second
                // copy of it. If the two ever diverged, a synthesized
                // `Remove` would stop matching the client's `Upsert` keys,
                // and a non-matching Remove is DEFINED as a no-op — so the
                // filtered-out rows would stay on screen forever with no
                // error anywhere.
                protocol::RowChange::Remove(change.key())
            }
        }
        protocol::RowChange::Remove(key) => protocol::RowChange::Remove(key.clone()),
    }).collect();
    std::borrow::Cow::Owned(protocol::TableDelta { changes })
}

// (`AbortOnDrop` lived here originally; it's now `k9rs_util::AbortOnDrop`,
// shared with the local-operator supervisor.)

// ---------------------------------------------------------------------------
// Log substream handler
// ---------------------------------------------------------------------------

/// Handle a log substream. Streams pod logs via the kube-rs API (reusing
/// the session's authenticated client) as bincode-framed `String`s. Falls
/// back to `kubectl logs` if the API path fails (e.g., exec credentials
/// that kube-rs can't use). When the substream closes (TUI dropped it),
/// the stream is cancelled naturally.
/// Most pods one log view will stream at once.
///
/// A selector can match hundreds; each match is an open watch against the
/// apiserver. kubectl's own `--max-log-requests` defaults to 5, which is
/// stingy for a TUI where "show me this deployment" is the normal case, but
/// unbounded is a way to hold a cluster open by accident. Truncation is
/// REPORTED in-band (see below) rather than silently applied.
const MAX_LOG_FANOUT: usize = 20;

/// How many merged lines one drain pass may take. Only a cap — the drain
/// never waits to fill it.
const LOG_DRAIN_BATCH: usize = 256;

/// Sort key for a kubelet RFC3339 stamp: `(seconds-part, nanoseconds)`.
///
/// The seconds part is fixed-width and zero-padded, so comparing it as text
/// IS chronological. The FRACTION is not: Go's `RFC3339Nano` trims trailing
/// zeros, so it is variable-width, and text comparison then inverts every
/// pair where one fraction is a prefix of the other — `.6Z` vs `.61Z`
/// compares `Z`(0x5A) against `1`(0x31) and puts the EARLIER line second.
/// A stamp with no fraction at all (which `split_timestamp` accepts, at
/// exactly its 20-byte floor) loses to every sub-second stamp in its own
/// second the same way, by up to 999 ms.
///
/// So the fraction is read as a number, padded to nanoseconds. Short is
/// padded with zeros (`.6` is 600 ms, not 6 ns) and anything past nine
/// digits is dropped.
fn time_key(ts: &str) -> (&str, u64) {
    let body = ts.strip_suffix('Z').unwrap_or(ts);
    let (secs, frac) = match body.split_once('.') {
        Some((secs, frac)) => (secs, frac),
        None => (body, ""),
    };
    let mut nanos = 0u64;
    for i in 0..9 {
        let d = frac.as_bytes().get(i).and_then(|b| (*b as char).to_digit(10)).unwrap_or(0);
        nanos = nanos * 10 + d as u64;
    }
    (secs, nanos)
}

/// Put one ready-batch of merged fan-out lines into timestamp order.
///
/// A fan-out merges N independent readers into one channel, so arrival order
/// across sources is scheduling noise: the bottom line of a twenty-replica
/// view is the last line to be *scheduled*, not the newest. This reorders
/// only lines that were ALREADY QUEUED TOGETHER, so nothing is ever held back
/// and no latency is added.
///
/// **It refuses to touch a batch that came from ONE source**, and that is the
/// most important thing it does. A single stream is already in true order —
/// one reader, one FIFO channel — so there is nothing to gain there and a
/// whole line ordering to lose. The test on the batch's actual contents
/// rather than on the stream's shape is deliberate: a fan-out batch in which
/// only one pod happened to speak is also authoritative, and is also left
/// alone.
///
/// It does NOT promise a globally correct ordering and could not: kubelet
/// stamps come from different nodes, so across pods they order by clock skew
/// as much as by time. A batch containing any untimestamped line is left
/// exactly as it arrived — there is no defensible place to sort a line that
/// does not say when it happened.
fn order_batch(batch: &mut [protocol::LogLine]) {
    if batch.len() < 2 || !batch.iter().all(|l| l.timestamp.is_some()) {
        return;
    }
    // One source in this batch ⇒ arrival order IS true order. Never reorder it.
    if batch.iter().all(|l| l.source == batch[0].source) {
        return;
    }
    // Stable, so an exact tie keeps arrival order.
    batch.sort_by(|a, b| {
        time_key(a.timestamp.as_deref().unwrap_or(""))
            .cmp(&time_key(b.timestamp.as_deref().unwrap_or("")))
    });
}

async fn handle_log_substream(
    init: protocol::LogInit,
    mut writer: tokio::io::BufWriter<tokio::io::WriteHalf<k9rs_mux::MuxedStream>>,
    ctx: Arc<SessionContext>,
) {
    let pods = match resolve_log_pods(&init, &ctx).await {
        Ok(p) => p,
        Err(msg) => {
            let _ = protocol::write_bincode(&mut writer, &protocol::LogLine::untagged(msg)).await;
            let _ = writer.flush().await;
            return;
        }
    };
    if pods.is_empty() {
        let msg = format!("No pods match {}", init.target.display());
        let _ = protocol::write_bincode(&mut writer, &protocol::LogLine::untagged(msg)).await;
        let _ = writer.flush().await;
        return;
    }

    let mut work = plan_log_work(&pods, &init.container);
    // The pod guard above doesn't cover this: a `LogContainer::Set(vec![])`
    // has pods and no streams, which would otherwise close the substream with
    // no lines and no reason — the shape this whole function exists to avoid.
    if work.is_empty() {
        let msg = format!("No containers selected for {}", init.target.display());
        let _ = protocol::write_bincode(&mut writer, &protocol::LogLine::untagged(msg)).await;
        let _ = writer.flush().await;
        return;
    }
    let mut notice = None;
    if work.len() > MAX_LOG_FANOUT {
        notice = Some(format!(
            "…streaming the first {} of {} sources (k9rs caps concurrent log streams)",
            MAX_LOG_FANOUT,
            work.len(),
        ));
        work.truncate(MAX_LOG_FANOUT);
    }

    // One task per stream, all feeding ONE channel that this task drains into
    // the single substream. The client therefore still sees one stream over
    // one store under one epoch; nothing about its element model changes.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<protocol::LogLine>(256);
    let mut readers = tokio::task::JoinSet::new();
    for item in work {
        let init = init.clone();
        let ctx = Arc::clone(&ctx);
        let tx = tx.clone();
        readers.spawn(async move {
            let LogWork { pod, container, source } = item;
            // API first (reuses the session's client, no subprocess); kubectl
            // only when the API can't serve this shape (`--all-containers`).
            let args = (pod.as_str(), &container, source.as_deref());
            if stream_logs_via_api(&init, args, &tx, &ctx).await.is_err() {
                stream_logs_via_kubectl(&init, args, &tx, &ctx).await;
            }
        });
    }
    drop(tx); // so the drain below ends when every reader is done

    if let Some(notice) = notice {
        let _ = protocol::write_bincode(&mut writer, &protocol::LogLine::untagged(notice)).await;
    }
    // Drain in READY batches. `recv_many` blocks only when the channel is
    // EMPTY — given anything queued it returns it immediately — so this adds
    // no latency by construction. That is the whole design constraint: a
    // reorder buffer would have to WAIT to know whether an older line is
    // still coming, and paying deliberate lag on a live tail to tidy an
    // ordering nobody can verify is a bad trade.
    //
    // It also collapses the per-line write+flush into one flush per batch
    // (see `write_bincode_no_flush` below), which at twenty pods under load
    // is the dominant cost here.
    let mut batch: Vec<protocol::LogLine> = Vec::with_capacity(LOG_DRAIN_BATCH);
    loop {
        // MUST come first: `recv_many` APPENDS to the buffer, and tokio
        // additionally asserts the buffer is empty when it observes the
        // channel closed-and-empty. Clearing after the writes below would
        // both re-send the previous batch and panic the daemon on shutdown.
        batch.clear();
        if rx.recv_many(&mut batch, LOG_DRAIN_BATCH).await == 0 {
            break; // every reader is done
        }
        order_batch(&mut batch);
        let mut broken = false;
        for line in &batch {
            // `_no_flush`: `write_bincode` flushes the 256 KiB BufWriter on
            // EVERY message, so using it here would have been one flush per
            // line plus a trailing no-op — one more than before, not one
            // total. This is where the batching actually pays.
            if protocol::write_bincode_no_flush(&mut writer, line).await.is_err() {
                broken = true;
                break;
            }
        }
        // Flushed unconditionally: with the writes buffered, short-circuiting
        // past this on `broken` would discard every line that serialized
        // fine ahead of the one that failed.
        let flushed = writer.flush().await.is_ok();
        if broken || !flushed {
            break;
        }
    }
    // `readers` drops here: every still-running per-pod task is aborted, and
    // each kubectl child goes with it (`kill_on_drop`).
    tracing::debug!("log substream ended for {}", init.target.display());
}

/// One stream the daemon opens, and the tag its lines carry.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LogWork {
    pod: String,
    container: protocol::LogContainer,
    /// `None` when there is exactly one source, so a single-pod,
    /// single-container view is byte-identical to the pre-fan-out one.
    source: Option<String>,
}

/// Expand a resolved pod list and a container selector into the streams to
/// open, and decide what each line will be tagged with.
///
/// Both axes can be sets now, so the CROSS PRODUCT is the unit of work —
/// and it's also the thing worth capping, since three marked containers on
/// eight pods is twenty-four apiserver streams, not eight.
///
/// The tag is derived from what actually varies. Naming a pod on every line
/// of a single-pod view is noise, and naming only the container when several
/// pods are in play is ambiguous in a way that's invisible in the output —
/// it just looks like interleaving.
fn plan_log_work(pods: &[String], container: &protocol::LogContainer) -> Vec<LogWork> {
    let multi_pod = pods.len() > 1;
    match container {
        protocol::LogContainer::Set(names) => pods
            .iter()
            .flat_map(|pod| {
                names.iter().map(move |name| LogWork {
                    pod: pod.clone(),
                    container: protocol::LogContainer::Named(name.clone()),
                    source: Some(if multi_pod {
                        format!("{pod}/{name}")
                    } else {
                        name.clone()
                    }),
                })
            })
            .collect(),
        // `All` needs no tag of its own: it runs through kubectl, which
        // prefixes every line with the container it came from, and
        // `tag_kubectl_line` folds the pod into that.
        one => pods
            .iter()
            .map(|pod| LogWork {
                pod: pod.clone(),
                container: one.clone(),
                source: multi_pod.then(|| pod.clone()),
            })
            .collect(),
    }
}

/// Resolve a [`protocol::LogTarget`] to the pods to stream.
///
/// `Selector` is resolved HERE, at stream time, rather than by the client
/// sending a pod list: the client's pod table may be filtered, stale, or
/// absent entirely (you can `:deploy` without ever opening pods), and the
/// daemon is the side holding an authenticated client anyway.
async fn resolve_log_pods(
    init: &protocol::LogInit,
    ctx: &SessionContext,
) -> Result<Vec<String>, String> {
    resolve_target(&init.target, init, ctx).await
}

/// `resolve_log_pods`, recursing through [`protocol::LogTarget::Union`].
/// Boxed because a union resolves its members with the same function.
fn resolve_target<'a>(
    target: &'a protocol::LogTarget,
    init: &'a protocol::LogInit,
    ctx: &'a SessionContext,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<String>, String>> + Send + 'a>> {
    Box::pin(async move {
    match target {
        protocol::LogTarget::One(name) => Ok(vec![name.clone()]),
        protocol::LogTarget::Union { targets, .. } => {
            // Every `Owner` member wants the SAME namespace-wide pod list —
            // ownerReferences have no field selector, so each one lists the
            // whole namespace and filters locally. Resolving them one at a
            // time meant thirty marked Jobs issued thirty sequential LISTs
            // before the first log line. One list serves all of them.
            let owners = owner_pods(targets, init, ctx).await;

            let mut pods = Vec::new();
            let mut seen = std::collections::HashSet::new();
            let mut last_err = None;
            // Walked in TARGET order regardless, because the fan-out cap
            // truncates, and which sources survive that truncation should
            // follow the selection rather than the resolution strategy.
            for t in targets {
                let resolved = match (t, &owners) {
                    (protocol::LogTarget::Owner { uid, .. }, Ok(by_uid)) => {
                        Ok(by_uid.get(uid).cloned().unwrap_or_default())
                    }
                    (protocol::LogTarget::Owner { .. }, Err(e)) => Err(e.clone()),
                    _ => resolve_target(t, init, ctx).await,
                };
                match resolved {
                    // Dedupe: marking a deployment AND one of its pods is a
                    // selection of two things over one pod, and streaming it
                    // twice would double every line.
                    Ok(names) => pods.extend(names.into_iter().filter(|n| seen.insert(n.clone()))),
                    // One unresolvable member must not sink the rest — you
                    // marked five workloads and one was deleted underneath
                    // you. The error is kept only if NOTHING resolved.
                    Err(e) => last_err = Some(e),
                }
            }
            match (pods.is_empty(), last_err) {
                (true, Some(e)) => Err(e),
                _ => Ok(pods),
            }
        }
        protocol::LogTarget::Selector { labels, label } => {
            let selector = protocol::SubscriptionFilter::labels_to_selector(labels);
            let params = kube::api::ListParams::default().labels(&selector);
            list_pod_names(init, ctx, params, |_| true)
                .await
                .map_err(|e| format!("Could not list pods for {label}: {e}"))
        }
        protocol::LogTarget::Owner { uid, label } => {
            // No field selector exists for ownerReferences, so this lists
            // the namespace's pod METADATA (names + owners, no spec/status)
            // and filters client-side. Same cost shape as the selector path.
            list_pod_names(init, ctx, kube::api::ListParams::default(), |meta| {
                meta.owner_references.iter().flatten().any(|o| &o.uid == uid)
            })
            .await
            .map_err(|e| format!("Could not list pods for {label}: {e}"))
        }
    }
    })
}

/// Resolve every `Owner` member of a union with ONE namespace pod list,
/// returning uid → pod names. Empty (and cheap) when the union has no owner
/// members at all.
async fn owner_pods(
    targets: &[protocol::LogTarget],
    init: &protocol::LogInit,
    ctx: &SessionContext,
) -> Result<std::collections::HashMap<String, Vec<String>>, String> {
    let uids: std::collections::HashSet<&str> = targets
        .iter()
        .filter_map(|t| match t {
            protocol::LogTarget::Owner { uid, .. } => Some(uid.as_str()),
            _ => None,
        })
        .collect();
    let mut by_uid: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    if uids.is_empty() {
        return Ok(by_uid);
    }
    let ns = init.namespace.as_option().unwrap_or_default();
    let api: kube::Api<k8s_openapi::api::core::v1::Pod> = namespaced_or_all(ctx, ns);
    let list = api
        .list_metadata(&kube::api::ListParams::default())
        .await
        .map_err(|e| format!("Could not list pods: {e}"))?;
    for p in list.items {
        let Some(name) = p.metadata.name else { continue };
        for owner in p.metadata.owner_references.iter().flatten() {
            if let Some(uid) = uids.get(owner.uid.as_str()) {
                by_uid.entry((*uid).to_string()).or_default().push(name.clone());
            }
        }
    }
    Ok(by_uid)
}

/// The pod API for a log request's namespace. An absent namespace means
/// every namespace — NOT the literal string `"default"`, which is what the
/// two API paths used to substitute while the kubectl path omitted `-n` and
/// got the kubeconfig's current namespace instead. One `LogInit` streaming
/// from two different namespaces depending on which backend served it is not
/// something the wire type should be able to express.
fn namespaced_or_all(
    ctx: &SessionContext,
    ns: &str,
) -> kube::Api<k8s_openapi::api::core::v1::Pod> {
    if ns.is_empty() {
        kube::Api::all(ctx.client.clone())
    } else {
        kube::Api::namespaced(ctx.client.clone(), ns)
    }
}

/// List pod names in the init's namespace, keeping those whose metadata
/// passes `keep`.
async fn list_pod_names(
    init: &protocol::LogInit,
    ctx: &SessionContext,
    params: kube::api::ListParams,
    keep: impl Fn(&kube::core::ObjectMeta) -> bool,
) -> Result<Vec<String>, kube::Error> {
    let api = namespaced_or_all(ctx, init.namespace.as_option().unwrap_or_default());
    let list = api.list_metadata(&params).await?;
    Ok(list
        .items
        .into_iter()
        .filter(|p| keep(&p.metadata))
        .filter_map(|p| p.metadata.name)
        .collect())
}

/// Cap on a single log line's length. A hostile/broken workload can emit a
/// line with no newline for gigabytes; `.lines()`/`.next_line()` buffer the
/// WHOLE line before yielding, so an uncapped read is a daemon OOM (which
/// kills every session). 1 MiB is generous for any real log line; the
/// overflow is discarded (up to the next newline) so memory stays bounded.
const MAX_LOG_LINE_BYTES: usize = 1 << 20;

const LINE_TRUNCATED_MARKER: &str = "…[k9rs: line truncated]";

/// Read one newline-terminated line from a tokio `AsyncBufRead`, capping
/// accumulated bytes at `max` and DISCARDING the rest of an over-long line
/// (bounded memory). `Ok(None)` at EOF with nothing buffered.
async fn read_capped_line_tokio<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    max: usize,
) -> std::io::Result<Option<String>> {
    use tokio::io::{AsyncBufRead, AsyncBufReadExt};
    let mut line: Vec<u8> = Vec::new();
    let mut truncated = false;
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return Ok((!line.is_empty()).then(|| finish_capped_line(line, truncated)));
        }
        let (upto, done) = match chunk.iter().position(|&b| b == b'\n') {
            Some(pos) => (pos, true),
            None => (chunk.len(), false),
        };
        if line.len() < max {
            let take = (max - line.len()).min(upto);
            line.extend_from_slice(&chunk[..take]);
            if take < upto {
                truncated = true;
            }
        } else if upto > 0 {
            truncated = true;
        }
        let advance = if done { upto + 1 } else { upto };
        AsyncBufRead::consume(std::pin::Pin::new(&mut *reader), advance);
        if done {
            return Ok(Some(finish_capped_line(line, truncated)));
        }
    }
}

/// Same as [`read_capped_line_tokio`] for a `futures::AsyncBufRead` (what
/// `Api::log_stream` returns).
async fn read_capped_line_futures<R: futures::AsyncBufRead + Unpin>(
    reader: &mut R,
    max: usize,
) -> std::io::Result<Option<String>> {
    use futures::{AsyncBufRead, AsyncBufReadExt};
    let mut line: Vec<u8> = Vec::new();
    let mut truncated = false;
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return Ok((!line.is_empty()).then(|| finish_capped_line(line, truncated)));
        }
        let (upto, done) = match chunk.iter().position(|&b| b == b'\n') {
            Some(pos) => (pos, true),
            None => (chunk.len(), false),
        };
        if line.len() < max {
            let take = (max - line.len()).min(upto);
            line.extend_from_slice(&chunk[..take]);
            if take < upto {
                truncated = true;
            }
        } else if upto > 0 {
            truncated = true;
        }
        let advance = if done { upto + 1 } else { upto };
        AsyncBufRead::consume(std::pin::Pin::new(&mut *reader), advance);
        if done {
            return Ok(Some(finish_capped_line(line, truncated)));
        }
    }
}

fn finish_capped_line(bytes: Vec<u8>, truncated: bool) -> String {
    let mut s = String::from_utf8_lossy(&bytes).into_owned();
    // Trim a trailing CR (kubectl/CRLF logs) to match `.lines()` behavior.
    if s.ends_with('\r') {
        s.pop();
    }
    if truncated {
        s.push_str(LINE_TRUNCATED_MARKER);
    }
    s
}

/// Stream logs via the kube-rs `Api::log_stream` API.
///
/// `work` is one [`LogWork`] item flattened to `(pod, container, source)` —
/// which pod, which of its containers, and the tag its lines carry (`None`
/// when there is only one source, so a single-pod single-container view is
/// byte-identical to the pre-fan-out one).
async fn stream_logs_via_api(
    init: &protocol::LogInit,
    work: (&str, &protocol::LogContainer, Option<&str>),
    out: &tokio::sync::mpsc::Sender<protocol::LogLine>,
    ctx: &SessionContext,
) -> Result<(), ()> {
    let (pod, container, source) = work;
    let api = namespaced_or_all(ctx, init.namespace.as_option().unwrap_or_default());

    let mut params = kube::api::LogParams {
        follow: init.follow,
        previous: init.previous,
        // Asked for, so the split below is a contract rather than a guess.
        timestamps: true,
        ..Default::default()
    };
    if let Some(tail) = init.tail {
        params.tail_lines = Some(tail as i64);
    }
    if let Some(ref since) = init.since {
        if let Some(secs) = parse_since_to_seconds(since) {
            params.since_seconds = Some(secs);
        }
    }
    match container {
        protocol::LogContainer::Named(name) => {
            params.container = Some(name.clone());
        }
        // kube-rs `LogParams` has no `--all-containers`, so that shape falls
        // back to kubectl. (`Set` never reaches here — `plan_log_work` has
        // already split it into one `Named` stream per container.)
        protocol::LogContainer::All | protocol::LogContainer::Set(_) => return Err(()),
        protocol::LogContainer::Default => {}
    }

    let reader = match api.log_stream(pod, &params).await {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!("log API failed for pod={}: {}, falling back to kubectl", pod, e);
            return Err(());
        }
    };

    // kube-rs log_stream returns `impl futures::AsyncBufRead`. Read
    // length-capped lines so a workload emitting a giant newline-free line
    // can't OOM the daemon.
    let mut reader = reader;
    let mut wrote_any = false;
    loop {
        // NOT `while let Ok(Some(..))`. That swallowed every mid-stream
        // failure — a reset connection, a TLS error, the 300 s idle timeout
        // — into the same shape as EOF, and the caller reads `Ok(())` as
        // success. So a follow whose connection dropped an hour ago looked
        // exactly like a pod that had gone quiet: no fallback, no message,
        // no indication at all. (The failure reporting added alongside the
        // fan-out covers the SPAWN failure and kubectl's stderr; this path
        // had neither.)
        match read_capped_line_futures(&mut reader, MAX_LOG_LINE_BYTES).await {
            Ok(Some(line)) => {
                // The API path only serves Named/Default (All falls back to
                // kubectl), so within one pod every line has a single
                // source: the tag is the pod, or nothing at all when there
                // is only one pod.
                let (timestamp, body) = protocol::LogLine::split_timestamp(&line);
                let log_line = protocol::LogLine {
                    source: source.map(ToOwned::to_owned),
                    timestamp,
                    content: body.to_owned(),
                };
                // A send error means the drain side is gone (client closed
                // the substream), which is the normal end of a follow.
                if out.send(log_line).await.is_err() {
                    return Ok(());
                }
                wrote_any = true;
            }
            // Clean end of stream.
            Ok(None) => return Ok(()),
            // Failed before producing anything: let the caller try kubectl,
            // which is the whole point of this function's `Result`.
            Err(e) if !wrote_any => {
                tracing::debug!("log API failed for pod={} before any line: {}", pod, e);
                return Err(());
            }
            // Failed PART WAY THROUGH. Falling back now would replay the
            // lines already delivered, so say what happened instead — on the
            // stream, where the reader is looking.
            Err(e) => {
                report_log_failure(out, pod, &format!("log stream ended: {e}")).await;
                return Ok(());
            }
        }
    }
}

/// Parse a kubectl-style duration string ("5m", "1h", "30s") into seconds.
fn parse_since_to_seconds(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Some(val) = s.strip_suffix('s') {
        return val.parse().ok();
    }
    if let Some(val) = s.strip_suffix('m') {
        return val.parse::<i64>().ok().map(|v| v * 60);
    }
    if let Some(val) = s.strip_suffix('h') {
        return val.parse::<i64>().ok().map(|v| v * 3600);
    }
    if let Some(val) = s.strip_suffix('d') {
        return val.parse::<i64>().ok().map(|v| v * 86400);
    }
    s.parse().ok()
}

/// Split a kubectl `--prefix` line into `(container, body)`.
///
/// This is only safe because WE pass `--prefix` (see `stream_logs_via_kubectl`),
/// so the first `[...] ` group is always kubectl's and the line's own content —
/// brackets and all — follows it.
///
/// It previously ran on the premise that `--all-containers` enables `--prefix`
/// by itself. It does not; only `--all-pods` does. So no prefix was ever
/// present, and this stripped the first bracketed group of whatever the
/// application actually logged: `[2026-09-17 10:00:00] WARN …` became source
/// `2026-09-17 10:00:00` (coloured by a hash, so a different colour every line)
/// with the timestamp gone from the body, and the container name — the entire
/// point — was never recovered.
///
/// `source` is `pod/container` (or just `container`); the container is its final
/// `/`-separated component. Returns `None` for a line that doesn't open with a
/// recognizable prefix — it then rides the wire untagged rather than guessing.
fn split_all_containers_prefix(line: &str) -> Option<(&str, &str)> {
    let rest = line.strip_prefix('[')?;
    let close = rest.find("] ")?;
    let source = &rest[..close];
    let container = source.rsplit('/').next().unwrap_or(source);
    Some((container, &rest[close + 2..]))
}

/// Put one source's failure ON the stream, tagged with the pod it belongs to.
///
/// In a fan-out this is the only place a per-pod failure can surface: the
/// substream is shared, so there is no per-source error channel and a silent
/// reader is indistinguishable from a quiet one.
async fn report_log_failure(
    out: &tokio::sync::mpsc::Sender<protocol::LogLine>,
    pod: &str,
    message: &str,
) {
    let _ = out
        .send(protocol::LogLine {
            source: Some(pod.to_string()),
            timestamp: None,
            content: format!("k9rs: {message}"),
        })
        .await;
}

/// Attach the `source` tag to one raw `kubectl logs` line.
///
/// Two independent tag axes meet here and both can be live at once:
/// `--all-containers` makes kubectl prefix every line with `[pod/container] `,
/// and a fan-out gives the line a pod. When both apply the tag must be
/// `pod/container` — with several multi-container pods in one view, a bare
/// container name doesn't say which pod emitted it, and that ambiguity is
/// invisible in the rendered output (it just looks like interleaving).
fn tag_kubectl_line(line: String, all_containers: bool, source: Option<&str>) -> protocol::LogLine {
    // Only `--all-containers` carries the prefix; anything else rides through
    // with the fan-out tag, or none.
    // kubectl emits `[prefix] <stamp> body`, so the prefix comes off first.
    let (tag, rest) = match all_containers.then(|| split_all_containers_prefix(&line)).flatten() {
        Some((container, body)) => {
            let tag = Some(match source {
                Some(pod) => format!("{pod}/{container}"),
                None => container.to_owned(),
            });
            (tag, body)
        }
        None => (source.map(ToOwned::to_owned), line.as_str()),
    };
    let (timestamp, content) = protocol::LogLine::split_timestamp(rest);
    protocol::LogLine { source: tag, timestamp, content: content.to_owned() }
}

/// Fallback: stream logs via `kubectl logs` subprocess.
///
/// See [`stream_logs_via_api`] for `work`. When both a fan-out source and
/// kubectl's own per-container prefix are present the tag is `pod/container`,
/// because with several multi-container pods in one view the container name
/// alone doesn't say which pod it came from.
async fn stream_logs_via_kubectl(
    init: &protocol::LogInit,
    work: (&str, &protocol::LogContainer, Option<&str>),
    out: &tokio::sync::mpsc::Sender<protocol::LogLine>,
    ctx: &SessionContext,
) {
    let (pod, container, source) = work;
    let mut cmd = ctx.session_env.kubectl();
    cmd.arg("logs").arg(pod);
    match container {
        protocol::LogContainer::All => {
            // `--prefix` is NOT implied by `--all-containers` — only
            // `--all-pods` sets it. Without it every line arrives bare and
            // `split_all_containers_prefix` strips the application's own
            // leading `[...]` group instead of kubectl's.
            cmd.arg("--all-containers").arg("--prefix");
        }
        protocol::LogContainer::Named(name) => { cmd.arg("-c").arg(name); }
        // Split upstream by `plan_log_work`; a `Set` never reaches a command.
        protocol::LogContainer::Set(_) | protocol::LogContainer::Default => {}
    }
    if let Some(ns) = init.namespace.as_option() {
        cmd.arg("-n").arg(ns);
    }
    cmd.arg("--context").arg(ctx.context.name.as_str());
    if init.follow { cmd.arg("-f"); }
    if let Some(tail) = init.tail {
        cmd.arg("--tail").arg(tail.to_string());
    }
    if let Some(ref since) = init.since {
        cmd.arg("--since").arg(since);
    }
    if init.previous { cmd.arg("--previous"); }
    // Matches the API path, so both backends deliver the same typed line.
    cmd.arg("--timestamps");
    cmd.stdout(std::process::Stdio::piped());
    // Captured, not discarded: kubectl's refusal IS the answer when a pod has
    // no previous container or was deleted mid-fan-out, and it used to go to
    // /dev/null — leaving the view to print "No logs.", an affirmative false
    // statement about a fetch that failed.
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            report_log_failure(out, pod, &format!("could not run kubectl: {e}")).await;
            return;
        }
    };

    let stdout = match child.stdout.take() {
        Some(s) => s,
        None => return,
    };
    let stderr = child.stderr.take();
    let mut reader = tokio::io::BufReader::new(stdout);

    let all_containers = matches!(container, protocol::LogContainer::All);
    let mut wrote_any = false;
    while let Ok(Some(line)) = read_capped_line_tokio(&mut reader, MAX_LOG_LINE_BYTES).await {
        wrote_any = true;
        if out.send(tag_kubectl_line(line, all_containers, source)).await.is_err() {
            return;
        }
    }

    // Only if the stream produced nothing: a successful follow that is simply
    // quiet must stay silent, and a stream that ended after real output has
    // already said what it had to say.
    if !wrote_any {
        if let Some(stderr) = stderr {
            let mut err = tokio::io::BufReader::new(stderr);
            if let Ok(Some(first)) = read_capped_line_tokio(&mut err, MAX_LOG_LINE_BYTES).await {
                report_log_failure(out, pod, first.trim()).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Exec substream handler — kubectl + PTY
// ---------------------------------------------------------------------------

/// Spawn `kubectl exec -it` inside a PTY and bridge terminal bytes over
/// the yamux substream. Uses SPDY (via kubectl) which works on all
/// clusters, including those behind proxies that reject WebSocket.
async fn handle_exec_substream(
    init: protocol::ExecInit,
    mut reader: tokio::io::BufReader<tokio::io::ReadHalf<k9rs_mux::MuxedStream>>,
    mut writer: tokio::io::BufWriter<tokio::io::WriteHalf<k9rs_mux::MuxedStream>>,
    ctx: Arc<SessionContext>,
) {
    // Read-only gate. Exec rides a DATA substream, so it bypasses the
    // control loop's `is_mutating` check — enforce here. `kubectl exec`
    // and `kubectl debug` both mutate cluster state (a debug session can
    // run arbitrary commands; `debug node/...` creates a debug pod), and
    // a non-cooperative client could send `delete`/`apply` args, so a
    // read-only session must not open an exec substream at all.
    if ctx.readonly {
        tracing::warn!(session = ctx.session_id, "exec refused: session is read-only");
        let _ = protocol::write_bincode(
            &mut writer,
            &protocol::ExecFrame::Data(b"k9rs: exec refused (read-only session)\r\n".to_vec()),
        ).await;
        let _ = writer.flush().await;
        return;
    }
    // The TUI sends the full kubectl args. The daemon only adds --context
    // from the session's active context (the TUI doesn't know the daemon's
    // context identity).
    let mut args = Vec::with_capacity(init.kubectl_args.len() + 2);
    args.push("--context".to_string());
    args.push(ctx.context.name.to_string());
    args.extend(init.kubectl_args);

    // Create PTY with initial terminal size.
    let mut pty = match Pty::open(init.term_width, init.term_height) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("PTY open failed: {}", e);
            let msg = format!("exec failed: PTY: {}\r\n", e);
            let _ = protocol::write_bincode(&mut writer, &protocol::ExecFrame::Data(msg.into_bytes())).await;
            return;
        }
    };

    // Spawn kubectl with the slave PTY as its controlling terminal.
    let child = match pty.spawn("kubectl", &args, &ctx.session_env) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("kubectl spawn failed: {}", e);
            let msg = format!("exec failed: {}\r\n", e);
            let _ = protocol::write_bincode(&mut writer, &protocol::ExecFrame::Data(msg.into_bytes())).await;
            return;
        }
    };

    // Held for its Drop: `kill_on_drop` signals the child and tokio reaps it
    // in the background. Cleanup still happens when the task is aborted
    // (daemon shutdown) — without a blocking `waitpid` on the way out.
    let _child = child;

    // Transfer master ownership from the PTY into a File — a plain typed
    // move (`OwnedFd` → `File`), no raw-fd unsafe. Consumes the Pty; the
    // slave was already handed to kubectl by `spawn`.
    let master_file = std::fs::File::from(pty.into_master());
    let master = match tokio::io::unix::AsyncFd::new(master_file) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!("AsyncFd setup failed: {}", e);
            let msg = format!("exec failed: {}\r\n", e);
            let _ = protocol::write_bincode(&mut writer, &protocol::ExecFrame::Data(msg.into_bytes())).await;
            return; // _child_guard drops → kills child
        }
    };

    // Bidirectional bridge: yamux ↔ PTY master.
    let yamux_to_pty = async {
        loop {
            let frame: protocol::ExecFrame = match protocol::read_bincode(&mut reader).await {
                Ok(f) => f,
                Err(_) => break,
            };
            match frame {
                protocol::ExecFrame::Data(bytes) => {
                    // Plain `write` with an offset, NOT `write_all`: on this
                    // O_NONBLOCK fd a partial `write_all` returns WouldBlock
                    // without reporting progress, and retrying it from the
                    // start re-sends the already-written prefix — duplicated
                    // bytes in the remote shell (fast paste, ^S flow-stop).
                    let mut written = 0;
                    while written < bytes.len() {
                        let mut guard = match master.writable().await {
                            Ok(g) => g,
                            Err(_) => return,
                        };
                        match guard.try_io(|inner| {
                            use std::io::Write;
                            inner.get_ref().write(&bytes[written..])
                        }) {
                            Ok(Ok(0)) => return, // PTY closed
                            Ok(Ok(n)) => written += n,
                            Ok(Err(_)) => return,
                            Err(_would_block) => continue,
                        }
                    }
                }
                protocol::ExecFrame::Resize { width, height } if width > 0 && height > 0 => {
                    let ws = libc::winsize {
                        ws_row: height, ws_col: width,
                        ws_xpixel: 0, ws_ypixel: 0,
                    };
                    unsafe { libc::ioctl(master.get_ref().as_raw_fd(), libc::TIOCSWINSZ, &ws); }
                }
                protocol::ExecFrame::Resize { .. } => {} // zero dimensions ignored
            }
        }
    };

    let pty_to_yamux = async {
        let mut buf = [0u8; 4096];
        loop {
            let mut guard = match master.readable().await {
                Ok(g) => g,
                Err(_) => break,
            };
            match guard.try_io(|inner| {
                use std::io::Read;
                inner.get_ref().read(&mut buf)
            }) {
                Ok(Ok(0)) => break,
                Ok(Ok(n)) => {
                    let frame = protocol::ExecFrame::Data(buf[..n].to_vec());
                    if protocol::write_bincode(&mut writer, &frame).await.is_err() { break; }
                }
                Ok(Err(_)) => break,
                Err(_would_block) => continue,
            }
        }
    };

    tokio::select! {
        _ = yamux_to_pty => {}
        _ = pty_to_yamux => {}
    }

    // `kill_on_drop` kills the child and hands the reap to tokio. Whether we
    // get here normally, or the task is aborted (daemon shutdown), or the
    // daemon takes SIGTERM (Drop runs during unwind), the child dies. Only
    // SIGKILL bypasses this — nothing survives SIGKILL on any platform.
    drop(master);
    drop(_child);
    tracing::debug!("exec substream ended");
}

/// Minimal PTY wrapper using raw libc. Fd lifetimes are [`OwnedFd`]s — a
/// consumed or closed fd is UNREPRESENTABLE. (The previous `RawFd` version
/// tracked ownership with `-1` sentinels that three methods had to maintain
/// by hand; one slip there is a double-close, which in a multi-threaded
/// daemon means fd-reuse corruption of an unrelated socket.)
struct Pty {
    master: OwnedFd,
    /// `Some` until `spawn` hands it to the child (the parent's copy closes
    /// on that take-drop); a second spawn is an error, not a double-close.
    slave: Option<OwnedFd>,
}

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

impl Pty {
    fn open(cols: u16, rows: u16) -> std::io::Result<Self> {
        let mut master: libc::c_int = 0;
        let mut slave: libc::c_int = 0;
        let ws = libc::winsize {
            ws_row: rows, ws_col: cols,
            ws_xpixel: 0, ws_ypixel: 0,
        };
        let ret = unsafe {
            libc::openpty(
                &mut master, &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &ws as *const libc::winsize as *mut libc::winsize,
            )
        };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: openpty succeeded, so both fds are freshly opened and held
        // by nobody else — wrapping transfers that sole ownership here. Every
        // error path below closes them by drop.
        let master = unsafe { OwnedFd::from_raw_fd(master) };
        let slave = unsafe { OwnedFd::from_raw_fd(slave) };
        // CLOSE-ON-EXEC on BOTH, immediately.
        //
        // `openpty` does not set it, and the daemon forks other children all
        // the time — `kubectl logs -f`, describe, another exec, the auth
        // credential plugin. Every one of them inherited these fds, and an
        // inherited copy is not released when ours is: open a shell, then
        // follow a log, exit the shell, and that pty pair stays allocated for
        // as long as the log runs. `Drop` cannot reach an fd that has already
        // been copied into another process's table — CLOEXEC is how you stop
        // the copy from happening.
        //
        // Safe for OUR child: `pre_exec` below installs the slave via `dup2`,
        // which clears CLOEXEC on the new descriptor, and closes both
        // originals explicitly.
        for fd in [master.as_raw_fd(), slave.as_raw_fd()] {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            if flags == -1
                || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } == -1
            {
                return Err(std::io::Error::last_os_error());
            }
        }
        // Set master to non-blocking for async I/O.
        let flags = unsafe { libc::fcntl(master.as_raw_fd(), libc::F_GETFL) };
        if flags == -1
            || unsafe { libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                == -1
        {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Pty { master, slave: Some(slave) })
    }

    /// Raw fds for the CLOEXEC pin. Test-only: production code never needs
    /// the numbers, only the `OwnedFd`s.
    #[cfg(test)]
    fn master_fd_for_test(&self) -> std::os::fd::RawFd {
        self.master.as_raw_fd()
    }

    #[cfg(test)]
    fn slave_fd_for_test(&self) -> std::os::fd::RawFd {
        self.slave.as_ref().expect("slave not yet consumed").as_raw_fd()
    }

    /// Hand the master to the caller (typically wrapped into a `File` /
    /// `AsyncFd`), consuming the PTY. By now `spawn` has taken the slave,
    /// so the move IS the complete ownership transfer — nothing is left
    /// behind to leak or double-close.
    fn into_master(self) -> OwnedFd {
        self.master
    }

    /// Spawn a process with the slave PTY as its controlling terminal.
    /// Spawn on TOKIO's process driver with `kill_on_drop`.
    ///
    /// Not `std::process`: the guard that used to own this child killed AND
    /// `wait()`ed in its `Drop`, which is a blocking `waitpid` running on a
    /// runtime worker during task teardown — and two shutdown paths `await`
    /// that teardown. A `kubectl` wedged in uninterruptible sleep therefore
    /// defers its own SIGKILL, `wait()` never returns, the session task never
    /// completes, and `ctl shutdown` hangs forever. It also contradicted the
    /// contract its sibling guard states outright: abort-only, never join,
    /// because a blocking join in `Drop` would deadlock.
    ///
    /// `kill_on_drop(true)` gives the same guarantee without the blocking:
    /// dropping the child signals it and hands the reaping to tokio's orphan
    /// queue.
    fn spawn(&mut self, cmd: &str, args: &[String], env: &SessionEnv) -> std::io::Result<tokio::process::Child> {
        let Some(slave) = self.slave.take() else {
            return Err(std::io::Error::other("PTY slave already consumed"));
        };
        // Raw COPIES for the pre-exec closure (it runs post-fork in the
        // child, where the parent's OwnedFds don't exist as owners).
        let slave_fd = slave.as_raw_fd();
        let master_fd = self.master.as_raw_fd();
        let mut command = tokio::process::Command::new(cmd);
        command.args(args);
        command.kill_on_drop(true);
        // Authenticate as this session, not the daemon's startup env. Applied
        // before TERM below so our forced TERM always wins.
        env.apply_to(&mut command);
        // kubectl forwards TERM to the remote container. Without this,
        // readline disables tab completion and programs disable syntax
        // highlighting because the remote shell thinks it's a dumb terminal.
        command.env("TERM", "xterm-256color");
        unsafe {
            command.pre_exec(move || {
                // New session → detach from parent's controlling terminal.
                libc::setsid();
                // Set the slave PTY as the controlling terminal.
                libc::ioctl(slave_fd, libc::TIOCSCTTY as libc::c_ulong, 0);
                libc::dup2(slave_fd, 0);
                libc::dup2(slave_fd, 1);
                libc::dup2(slave_fd, 2);
                if slave_fd > 2 { libc::close(slave_fd); }
                libc::close(master_fd);
                Ok(())
            });
        }
        let child = command.spawn()?;
        // Parent's copy of the slave closes here — only kubectl holds it now.
        drop(slave);
        Ok(child)
    }
}

fn parse_k8s_minor(git_version: &str) -> Option<u32> {
    let v = git_version.strip_prefix('v').unwrap_or(git_version);
    v.split('.').nth(1)?.parse().ok()
}

#[cfg(test)]
#[path = "../tests/server_session.rs"]
mod tests;
