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
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::live_query::WatcherCache;
use super::protocol::{self, SessionCommand, SessionEvent};
use crate::kube::session_env::SessionEnv;


// ---------------------------------------------------------------------------
// InitParams — extracted from the Init command for ergonomic access
// ---------------------------------------------------------------------------

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
    pub local_registry: Arc<crate::kube::local::LocalRegistry>,
    /// In-memory discovery cache. Per-resource-type storage means a failed
    /// namespace fetch can't erase a prior cached CRD list and vice-versa —
    /// the cache can't be partially poisoned.
    pub discovery_cache: super::cache::DiscoveryCache,
    /// Shared per-`ContextId` metrics + discovery pollers, so N sessions on one
    /// cluster poll it once instead of N times — the same sharing `watcher_cache`
    /// gives resource watches. See [`crate::kube::shared_poller`].
    metrics_pollers: crate::kube::shared_poller::PollerCache<crate::kube::metrics::MetricsSnapshot>,
    discovery_pollers: crate::kube::shared_poller::PollerCache<streaming::DiscoverySnapshot>,
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
    pub fn new() -> Self {
        Self::with_local_grace(Some(std::time::Duration::from_secs(
            crate::kube::GRACE_PERIOD_SECS,
        )))
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
            local_registry: Arc::new(crate::kube::local::LocalRegistry::new(
                crate::kube::daemon_config::daemon_config().exec_resources.clone(),
                local_grace,
            )),
            discovery_cache: super::cache::DiscoveryCache::new(),
            metrics_pollers: crate::kube::shared_poller::PollerCache::new(),
            discovery_pollers: crate::kube::shared_poller::PollerCache::new(),
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
    /// This session's strong hold on the context's local resources
    /// (port-forwards, exec sources). Held here AND on `ServerSession` so
    /// every path that can serve a local op keeps the slice out of grace;
    /// when the session's last holder drops, the context's grace window
    /// begins.
    pub locals: crate::kube::local::ContextKeepalive,
}

// ---------------------------------------------------------------------------
// ServerSession
// ---------------------------------------------------------------------------

pub struct ServerSession {
    writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
    shared: Arc<SessionSharedState>,
    client: kube::Client,
    client_config: Option<kube::Config>,
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
    locals: crate::kube::local::ContextKeepalive,
    metrics_task: Option<JoinHandle<()>>,
    /// Background loop that periodically re-runs discovery (namespaces +
    /// CRDs), so new namespaces / new CRDs land in the cache and reach the
    /// client without the user having to reconnect. Aborted in `cleanup()`.
    discovery_refresher_task: Option<JoinHandle<()>>,
    /// Tracks every `handle_*_async` background task (describe, yaml,
    /// delete, apply, scale, restart, decode, cron-trigger,
    /// cron-toggle-suspend, force-kill, save, etc). On `cleanup()` we
    /// `abort_all` so a session that exits mid-operation doesn't leak
    /// detached tasks holding kube clients and channel senders.
    pending_tasks: tokio::task::JoinSet<()>,
    event_tx: mpsc::Sender<SessionEvent>,
    event_rx: Option<mpsc::Receiver<SessionEvent>>,
}

impl ServerSession {
    pub fn new(
        writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
        shared: Arc<SessionSharedState>,
        client: kube::Client,
        context: protocol::ContextId,
        readonly: bool,
        session_env: SessionEnv,
        locals: crate::kube::local::ContextKeepalive,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            writer,
            shared,
            client,
            client_config: None,
            session_env,
            context,
            readonly,
            locals,
            metrics_task: None,
            discovery_refresher_task: None,
            pending_tasks: tokio::task::JoinSet::new(),
            event_tx,
            event_rx: Some(event_rx),
        }
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
        mut mux: crate::kube::mux::MuxedConnection,
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

    /// Create and run a session from a raw binary connection.
    /// Reads Init command via bincode, creates kube::Client, sends Ready,
    /// then enters the binary command loop.
    /// Used by local mode (--no-daemon) where the client writes Init to the stream.
    ///
    /// NOTE: The protocol version check below is duplicated across the three
    /// `init_and_run*` entry points. It's only 5 lines per site (if/write/return)
    /// and is embedded inside match arms that destructure different input shapes
    /// (read-from-stream vs already-parsed enum), so extracting a helper would
    /// add more ceremony (mutable writer ref, Option<InitParams> return) than
    /// the duplication it removes. If a fourth entry point appears, reconsider.
    pub async fn init_and_run(
        mut reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
        writer: Box<dyn AsyncWrite + Unpin + Send>,
        shared: Arc<SessionSharedState>,
    ) {
        let mut buf_writer = BufWriter::with_capacity(protocol::IO_BUFFER_SIZE, writer);

        let init = match protocol::read_bincode::<_, SessionCommand>(&mut reader).await {
            Ok(SessionCommand::Init {
                protocol_version, context, namespace, readonly,
                kubeconfig_yaml, env_vars, identity,
            }) => {
                if protocol_version != protocol::PROTOCOL_VERSION {
                    let _ = protocol::write_bincode(&mut buf_writer, &SessionEvent::SessionError(
                        format!("Protocol mismatch: TUI v{} != daemon v{}. Restart the daemon.",
                            protocol_version, protocol::PROTOCOL_VERSION),
                    )).await;
                    return;
                }
                InitParams { context, namespace, readonly, kubeconfig_yaml, env_vars, identity }
            }
            Ok(_) => {
                let _ = protocol::write_bincode(&mut buf_writer, &SessionEvent::SessionError(
                    "Expected Init command as first message".to_string(),
                )).await;
                return;
            }
            Err(e) => {
                let _ = protocol::write_bincode(&mut buf_writer, &SessionEvent::SessionError(
                    format!("Failed to read Init command: {}", e),
                )).await;
                return;
            }
        };

        Self::run_session(init, reader, buf_writer, shared).await;
    }

    /// Create and run a session from an already-parsed Init command.
    /// Used by the daemon where the first command was already read for routing.
    pub async fn init_and_run_with_parsed(
        first_cmd: SessionCommand,
        reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
        writer: Box<dyn AsyncWrite + Unpin + Send>,
        shared: Arc<SessionSharedState>,
    ) {
        let mut buf_writer = BufWriter::with_capacity(protocol::IO_BUFFER_SIZE, writer);

        // See NOTE on `init_and_run` re: duplicated protocol version check.
        let init = match first_cmd {
            SessionCommand::Init {
                protocol_version, context, namespace, readonly,
                kubeconfig_yaml, env_vars, identity,
            } => {
                if protocol_version != protocol::PROTOCOL_VERSION {
                    let _ = protocol::write_bincode(&mut buf_writer, &SessionEvent::SessionError(
                        format!("Protocol mismatch: TUI v{} != daemon v{}. Restart the daemon.",
                            protocol_version, protocol::PROTOCOL_VERSION),
                    )).await;
                    return;
                }
                InitParams { context, namespace, readonly, kubeconfig_yaml, env_vars, identity }
            }
            _ => {
                let _ = protocol::write_bincode(&mut buf_writer, &SessionEvent::SessionError(
                    "Expected Init command".to_string(),
                )).await;
                return;
            }
        };

        Self::run_session(init, reader, buf_writer, shared).await;
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
        // See NOTE on `init_and_run` re: duplicated protocol version check.
        let init = match protocol::read_bincode::<_, SessionCommand>(&mut reader).await {
            Ok(SessionCommand::Init {
                protocol_version, context, namespace, readonly,
                kubeconfig_yaml, env_vars, identity,
            }) => {
                if protocol_version != protocol::PROTOCOL_VERSION {
                    let _ = protocol::write_bincode(&mut buf_writer, &SessionEvent::SessionError(
                        format!("Protocol mismatch: TUI v{} != daemon v{}. Restart the daemon.",
                            protocol_version, protocol::PROTOCOL_VERSION),
                    )).await;
                    return;
                }
                InitParams { context, namespace, readonly, kubeconfig_yaml, env_vars, identity }
            }
            Ok(_) => {
                let _ = protocol::write_bincode(&mut buf_writer, &SessionEvent::SessionError(
                    "Expected Init command as first message".to_string(),
                )).await;
                return;
            }
            Err(e) => {
                let _ = protocol::write_bincode(&mut buf_writer, &SessionEvent::SessionError(
                    format!("Failed to read Init command: {}", e),
                )).await;
                return;
            }
        };
        Self::run_session_inner(init, reader, buf_writer, shared, Some(session_ctx_tx), session_id).await;
    }

    /// Common session setup: create client, send Ready, enter command loop.
    async fn run_session(
        init: InitParams,
        reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
        buf_writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
        shared: Arc<SessionSharedState>,
    ) {
        Self::run_session_inner(init, reader, buf_writer, shared, None, 0).await;
    }

    /// Shared implementation.
    async fn run_session_inner(
        init: InitParams,
        reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
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
                let event = SessionEvent::SessionError(
                    "Init command missing context name".to_string(),
                );
                let _ = protocol::write_bincode(&mut buf_writer, &event).await;
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
                warn!("Failed to create client for context {}: {}", context_name, e);
                let event = SessionEvent::SessionError(
                    format!("Failed to create client: {}", e),
                );
                let _ = protocol::write_bincode(&mut buf_writer, &event).await;
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
            identity.k8s_version = info.git_version.clone();
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
        let mut session = ServerSession::new(
            buf_writer,
            shared.clone(),
            client.clone(),
            context_id.clone(),
            init.readonly,
            session_env.clone(),
            locals.clone(),
        );
        session.client_config = Some(client_config.clone());

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
        if let Err(e) = session.run(reader).await {
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
    pub async fn run(mut self, reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>) -> anyhow::Result<()> {
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
        let _reader_guard = crate::util::AbortOnDrop::new(reader_handle.abort_handle());

        let mut event_rx = self
            .event_rx
            .take()
            .expect("ServerSession::run called without event_rx");

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
            other => debug!("session cmd: {:?}", other),
        }

        // Single readonly gate — classified by the typed `is_mutating`
        // method on `SessionCommand` itself. Exhaustive match on the
        // classifier means adding a new mutating command variant without
        // declaring its classification is a compile error. No per-arm
        // `reject_if_readonly` calls; forgetting to call one becomes
        // structurally impossible.
        if cmd.is_mutating() && self.readonly {
            self.reject_async("Session is read-only".to_string());
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
                lines: crate::kube::describe::describe_lines_from_text(&content),
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
        accessor: fn(&dyn crate::kube::local::LocalResourceSource, &str) -> Option<Result<String, String>>,
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
        if let Some(h) = self.metrics_task.take() { h.abort(); }
        if let Some(h) = self.discovery_refresher_task.take() { h.abort(); }
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
    stream: crate::kube::mux::MuxedStream,
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
        protocol::SubstreamInit::Subscribe(sub_init) => {
            handle_subscription_substream_inner(sub_init, writer, ctx, sid, sub_id).await;
        }
        protocol::SubstreamInit::Log(log_init) => {
            tracing::debug!(session = sid, sub = sub_id, "log: pod={} container={:?}", log_init.pod, log_init.container);
            handle_log_substream(log_init, writer, ctx).await;
        }
        protocol::SubstreamInit::Exec(exec_init) => {
            tracing::debug!(session = sid, sub = sub_id, "exec: {:?}", exec_init.kubectl_args);
            handle_exec_substream(exec_init, reader, writer, ctx).await;
        }
    }
}

async fn handle_subscription_substream_inner(
    init: protocol::SubscriptionInit,
    mut writer: tokio::io::BufWriter<tokio::io::WriteHalf<crate::kube::mux::MuxedStream>>,
    ctx: Arc<SessionContext>,
    session_id: u64,
    sub_id: u64,
) {
    use crate::kube::live_query::QueryKey;
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
        let mut sub = crate::kube::local::LocalSubscription::new(
            rid.clone(),
            source.subscribe(),
            source,
        );
        let _ = protocol::write_bincode(&mut writer,
            &protocol::StreamEvent::Snapshot(sub.current())).await;
        let _ = writer.flush().await;
        loop {
            if sub.changed().await.is_err() { break; }
            if protocol::write_bincode(&mut writer,
                &protocol::StreamEvent::Snapshot(sub.current())).await.is_err() { break; }
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
    // OwnerUid is a post-filter applied in the bridge (apply_owner_filter_inline),
    // NOT a K8s API-level filter. Exclude it from the QueryKey so that
    // OwnerUid-filtered subscriptions reuse the same watcher as unfiltered ones,
    // avoiding duplicate API server watches for the same data.
    let cache_filter = match &filter {
        Some(protocol::SubscriptionFilter::OwnerUid(_)) => None,
        other => other.clone(),
    };
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
        BuiltIn(crate::kube::live_query::Subscription),
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
            match crate::kube::describe::api_resource_for(&make_client(), &rid).await {
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

    // Capture re-subscription recipe so the bridge can retry after watcher
    // death. Built-ins need (key, kind); CRDs need the resolved identity.
    enum ResubInfo {
        BuiltIn {
            key: QueryKey,
            kind: crate::kube::resource_def::BuiltInKind,
        },
        Dynamic {
            key: QueryKey,
            gvk: kube::api::GroupVersionKind,
            plural: String,
            scope: protocol::ResourceScope,
            printer_columns: Vec<crate::kube::cache::PrinterColumn>,
        },
    }

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
            let printer_columns = ctx.shared.discovery_cache
                .printer_columns_for(&ctx.context, &gvk.group, &plural)
                .unwrap_or_default();
            if init.force {
                ctx.shared.watcher_cache.remove(&resolved_key);
            }
            let sub = ctx.shared.watcher_cache.subscribe_dynamic(
                resolved_key.clone(), make_client, gvk.clone(),
                plural.clone(), scope, printer_columns.clone(), ctx.streaming_lists,
            );
            (sub, ResubInfo::Dynamic { key: resolved_key, gvk, plural, scope, printer_columns })
        }
    };

    // Bridge: watcher -> StreamEvent frames on the substream.
    //
    // If the watcher dies after having delivered data (transient failure —
    // laptop suspend, VPN drop, token refresh race), re-subscribe with
    // exponential backoff. WatcherCache detects the dead watcher and spawns
    // a fresh one. The TUI keeps the last good snapshot while we retry.
    //
    // If the watcher dies *before* ever delivering data (RBAC, unknown
    // resource), the error is almost certainly permanent — send it
    // immediately and exit.
    let mut ever_received_data = false;
    let mut retry_backoff = std::time::Duration::from_secs(5);

    // Send current snapshot immediately if available.
    if let Some(update) = sub.current() {
        ever_received_data = true;
        let update = apply_owner_filter_inline(update, &filter);
        if let Err(e) = protocol::write_bincode(&mut writer, &protocol::StreamEvent::Snapshot(update)).await {
            tracing::warn!(session = session_id, sub = sub_id, "initial snapshot write failed for {}: {}", rid.plural(), e);
            return;
        }
        if let Err(e) = writer.flush().await {
            tracing::warn!(session = session_id, sub = sub_id, "initial snapshot flush failed for {}: {}", rid.plural(), e);
            return;
        }
        tracing::info!(session = session_id, sub = sub_id,
            "subscribe: {}({}) snapshot ready in {:?}",
            rid.plural(), init.namespace.display(), sub_start.elapsed());
    } else {
        tracing::info!(session = session_id, sub = sub_id,
            "subscribe: {}({}) snapshot pending, waiting for watcher",
            rid.plural(), init.namespace.display());
    }

    loop {
        // Forward snapshots until the watcher dies or the substream breaks.
        let watcher_died = loop {
            if sub.changed().await.is_err() {
                break true;
            }
            let Some(update) = sub.current() else {
                break true;
            };
            ever_received_data = true;
            retry_backoff = std::time::Duration::from_secs(5);
            let update = apply_owner_filter_inline(update, &filter);
            if let Err(e) = protocol::write_bincode(&mut writer, &protocol::StreamEvent::Snapshot(update)).await {
                tracing::warn!(session = session_id, sub = sub_id, "bridge write failed for {}: {}", rid.plural(), e);
                break false;
            }
            if let Err(e) = writer.flush().await {
                tracing::warn!(session = session_id, sub = sub_id, "bridge flush failed for {}: {}", rid.plural(), e);
                break false;
            }
        };

        if !watcher_died {
            return; // Substream error — TUI disconnected, nothing to retry.
        }

        if !ever_received_data {
            // First watcher never produced data — likely permanent (RBAC, unknown resource).
            let detail = sub.last_error().unwrap_or_default();
            let msg = if detail.is_empty() {
                format!("Watcher failed for {}", rid.plural())
            } else {
                format!("Watcher failed for {}: {}", rid.plural(), detail)
            };
            let _ = protocol::write_bincode(&mut writer, &protocol::StreamEvent::Error(msg)).await;
            return;
        }

        // Transient failure after prior success — retry with jittered backoff.
        // ±25% jitter prevents thundering herd when many watchers die
        // simultaneously (e.g., after laptop suspend/resume).
        let detail = sub.last_error().unwrap_or_default();
        let jitter_factor = crate::util::retry_jitter(rid.plural().as_bytes(), retry_backoff.as_millis() as u64);
        let jittered = retry_backoff.mul_f64(jitter_factor);
        tracing::info!(session = session_id, sub = sub_id,
            "watcher died for {}, retrying in {:?}: {}",
            rid.plural(), jittered, detail);
        tokio::time::sleep(jittered).await;
        retry_backoff = (retry_backoff * 2).min(std::time::Duration::from_secs(60));

        // Re-subscribe — WatcherCache sees the dead watcher and creates a fresh one.
        sub = match &resub {
            ResubInfo::BuiltIn { key, kind } => {
                ctx.shared.watcher_cache.subscribe(key.clone(), *kind, make_client, ctx.streaming_lists)
            }
            ResubInfo::Dynamic { key, gvk, plural, scope, printer_columns } => {
                ctx.shared.watcher_cache.subscribe_dynamic(
                    key.clone(), make_client, gvk.clone(),
                    plural.clone(), *scope, printer_columns.clone(), ctx.streaming_lists,
                )
            }
        };

        // Deliver the first snapshot from the new watcher if immediately available.
        if let Some(update) = sub.current() {
            ever_received_data = true;
            let update = apply_owner_filter_inline(update, &filter);
            if protocol::write_bincode(&mut writer, &protocol::StreamEvent::Snapshot(update)).await.is_err() {
                return;
            }
            if writer.flush().await.is_err() {
                return;
            }
        }
    }
}

/// Owner-uid post-filter for subscription substreams. OwnerUid is the one
/// `SubscriptionFilter` variant that K8s can't apply server-side (the
/// field doesn't exist on the resource schema), so we drop rows whose
/// owner chain doesn't include the requested UID after the watcher hands
/// us each snapshot.
fn apply_owner_filter_inline(
    update: crate::event::ResourceUpdate,
    filter: &Option<protocol::SubscriptionFilter>,
) -> crate::event::ResourceUpdate {
    let Some(protocol::SubscriptionFilter::OwnerUid(ref uid)) = filter else {
        return update;
    };
    match update {
        crate::event::ResourceUpdate::Rows { resource, headers, rows } => {
            let filtered: Vec<_> = rows.into_iter().filter(|row| {
                row.owner_refs.iter().any(|r| r.uid == *uid)
            }).collect();
            crate::event::ResourceUpdate::Rows { resource, headers, rows: filtered }
        }
        other => other,
    }
}

// (`AbortOnDrop` lived here originally; it's now `crate::util::AbortOnDrop`,
// shared with the local-operator supervisor.)

// ---------------------------------------------------------------------------
// Log substream handler
// ---------------------------------------------------------------------------

/// Handle a log substream. Streams pod logs via the kube-rs API (reusing
/// the session's authenticated client) as bincode-framed `String`s. Falls
/// back to `kubectl logs` if the API path fails (e.g., exec credentials
/// that kube-rs can't use). When the substream closes (TUI dropped it),
/// the stream is cancelled naturally.
async fn handle_log_substream(
    init: protocol::LogInit,
    mut writer: tokio::io::BufWriter<tokio::io::WriteHalf<crate::kube::mux::MuxedStream>>,
    ctx: Arc<SessionContext>,
) {
    // Try the kube-rs API first — reuses the session's authenticated
    // client, no subprocess spawn, no re-authentication overhead.
    if let Ok(()) = stream_logs_via_api(&init, &mut writer, &ctx).await {
        tracing::debug!("log substream ended (API) for pod={}", init.pod);
        return;
    }

    // Fallback: kubectl logs. Spawns a subprocess with its own auth.
    stream_logs_via_kubectl(&init, &mut writer, &ctx).await;
    tracing::debug!("log substream ended (kubectl) for pod={}", init.pod);
}

/// Stream logs via the kube-rs `Api::log_stream` API.
async fn stream_logs_via_api(
    init: &protocol::LogInit,
    writer: &mut tokio::io::BufWriter<tokio::io::WriteHalf<crate::kube::mux::MuxedStream>>,
    ctx: &SessionContext,
) -> Result<(), ()> {
    let ns = init.namespace.as_option().unwrap_or("default");
    let api: kube::Api<k8s_openapi::api::core::v1::Pod> =
        kube::Api::namespaced(ctx.client.clone(), ns);

    let mut params = kube::api::LogParams {
        follow: init.follow,
        previous: init.previous,
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
    match &init.container {
        protocol::LogContainer::Named(name) => {
            params.container = Some(name.clone());
        }
        protocol::LogContainer::All => {
            // kube-rs LogParams doesn't support --all-containers.
            // Fall back to kubectl for this case.
            return Err(());
        }
        protocol::LogContainer::Default => {}
    }

    let reader = match api.log_stream(&init.pod, &params).await {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!("log API failed for pod={}: {}, falling back to kubectl", init.pod, e);
            return Err(());
        }
    };

    // kube-rs log_stream returns `impl futures::AsyncBufRead`. Convert
    // to lines via the futures crate's async line reader.
    let mut lines = futures::io::AsyncBufReadExt::lines(reader);
    while let Some(line_result) = futures::StreamExt::next(&mut lines).await {
        match line_result {
            Ok(line) => {
                // API path only serves Named/Default (All falls back to
                // kubectl), so every line is single-source → untagged.
                let log_line = protocol::LogLine::untagged(line);
                if protocol::write_bincode(writer, &log_line).await.is_err() { return Ok(()); }
                if writer.flush().await.is_err() { return Ok(()); }
            }
            Err(_) => break,
        }
    }
    Ok(())
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

/// Split a kubectl `--all-containers` line into `(container, body)`. kubectl
/// auto-enables `--prefix` for `--all-containers`, prepending `[source] ` to
/// EVERY streamed line — so the first `[...] ` group is always kubectl's prefix,
/// and the container's own line (brackets and all) follows it. That property is
/// what makes this safe: we strip exactly the first prefix, never log content.
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

/// Fallback: stream logs via `kubectl logs` subprocess.
async fn stream_logs_via_kubectl(
    init: &protocol::LogInit,
    writer: &mut tokio::io::BufWriter<tokio::io::WriteHalf<crate::kube::mux::MuxedStream>>,
    ctx: &SessionContext,
) {
    use tokio::io::AsyncBufReadExt;

    let mut cmd = ctx.session_env.kubectl();
    cmd.arg("logs").arg(&init.pod);
    match &init.container {
        protocol::LogContainer::All => { cmd.arg("--all-containers"); }
        protocol::LogContainer::Named(name) => { cmd.arg("-c").arg(name); }
        protocol::LogContainer::Default => {}
    }
    if let Some(ns) = init.namespace.as_option() {
        cmd.arg("-n").arg(ns);
    }
    if !ctx.context.name.is_empty() {
        cmd.arg("--context").arg(ctx.context.name.as_str());
    }
    if init.follow { cmd.arg("-f"); }
    if let Some(tail) = init.tail {
        cmd.arg("--tail").arg(tail.to_string());
    }
    if let Some(ref since) = init.since {
        cmd.arg("--since").arg(since);
    }
    if init.previous { cmd.arg("--previous"); }
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::null());
    cmd.kill_on_drop(true);

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("log substream: failed to spawn kubectl: {}", e);
            return;
        }
    };

    let stdout = match child.stdout.take() {
        Some(s) => s,
        None => return,
    };
    let mut lines = tokio::io::BufReader::new(stdout).lines();

    // Only `--all-containers` carries kubectl's `[source] ` prefix; tag those
    // lines with the source container and strip the prefix. Single-container
    // streams have no prefix and ride the wire untagged.
    let all_containers = matches!(init.container, protocol::LogContainer::All);
    while let Ok(Some(line)) = lines.next_line().await {
        let parsed = if all_containers { split_all_containers_prefix(&line) } else { None };
        let log_line = match parsed {
            Some((container, body)) => protocol::LogLine {
                container: Some(container.to_owned()),
                content: body.to_owned(),
            },
            None => protocol::LogLine::untagged(line),
        };
        if protocol::write_bincode(writer, &log_line).await.is_err() { break; }
        if writer.flush().await.is_err() { break; }
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
    mut reader: tokio::io::BufReader<tokio::io::ReadHalf<crate::kube::mux::MuxedStream>>,
    mut writer: tokio::io::BufWriter<tokio::io::WriteHalf<crate::kube::mux::MuxedStream>>,
    ctx: Arc<SessionContext>,
) {
    // The TUI sends the full kubectl args. The daemon only adds --context
    // from the session's active context (the TUI doesn't know the daemon's
    // context identity).
    let mut args = Vec::with_capacity(init.kubectl_args.len() + 2);
    if !ctx.context.name.is_empty() {
        args.push("--context".to_string());
        args.push(ctx.context.name.to_string());
    }
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

    // RAII guard: kills and reaps the child on drop. Ensures cleanup
    // even if the tokio task is aborted (daemon shutdown).
    let _child_guard = ChildGuard(Some(child));

    // Transfer master fd ownership from Pty to File. consume_master_fd
    // marks the fd as transferred so Pty::drop skips closing it.
    let master_fd = pty.consume_master_fd();
    let master_file = unsafe { std::fs::File::from_raw_fd(master_fd) };
    drop(pty); // Only closes slave (master already consumed).
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
                    loop {
                        let mut guard = match master.writable().await {
                            Ok(g) => g,
                            Err(_) => return,
                        };
                        match guard.try_io(|inner| {
                            use std::io::Write;
                            inner.get_ref().write_all(&bytes)
                        }) {
                            Ok(Ok(())) => break,
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
                    unsafe { libc::ioctl(master_fd, libc::TIOCSWINSZ, &ws); }
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

    // ChildGuard's Drop kills + reaps the child. Whether we get here
    // normally, or the task is aborted (daemon shutdown), or the daemon
    // is killed with SIGTERM (Drop runs during unwind), the child dies.
    // Only SIGKILL bypasses this — nothing survives SIGKILL on any platform.
    drop(master);
    drop(_child_guard);
    tracing::debug!("exec substream ended");
}

/// RAII guard for a child process. Kills and reaps on drop — ensures
/// cleanup even when a tokio task is aborted (daemon shutdown). Same
/// pattern as `AbortOnDrop`, `SuspendGuard`, `TempFile`. Works on
/// Linux and macOS (uses `kill` + `wait`, no platform-specific APIs).
struct ChildGuard(Option<std::process::Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(ref mut child) = self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Minimal PTY wrapper using raw libc. No external crate needed.
struct Pty {
    master: std::os::unix::io::RawFd,
    slave: std::os::unix::io::RawFd,
}

use std::os::unix::io::{FromRawFd, RawFd};

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
        // Set master to non-blocking for async I/O.
        let flags = unsafe { libc::fcntl(master, libc::F_GETFL) };
        if flags == -1 || unsafe { libc::fcntl(master, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
            let err = std::io::Error::last_os_error();
            unsafe { libc::close(master); libc::close(slave); }
            return Err(err);
        }
        Ok(Pty { master, slave })
    }

    /// Transfer ownership of the master fd. After this call, Drop will
    /// NOT close the master — the caller owns it (typically via File).
    fn consume_master_fd(&mut self) -> RawFd {
        let fd = self.master;
        self.master = -1; // Sentinel: Drop skips -1.
        fd
    }

    /// Spawn a process with the slave PTY as its controlling terminal.
    fn spawn(&mut self, cmd: &str, args: &[String], env: &SessionEnv) -> std::io::Result<std::process::Child> {
        use std::os::unix::process::CommandExt;
        let slave = self.slave;
        let master = self.master;
        let mut command = std::process::Command::new(cmd);
        command.args(args);
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
                libc::ioctl(slave, libc::TIOCSCTTY as libc::c_ulong, 0);
                libc::dup2(slave, 0);
                libc::dup2(slave, 1);
                libc::dup2(slave, 2);
                if slave > 2 { libc::close(slave); }
                libc::close(master);
                Ok(())
            });
        }
        let child = command.spawn()?;
        // Close slave on parent side — only kubectl should hold it.
        unsafe { libc::close(self.slave); }
        self.slave = -1; // Mark as consumed.
        Ok(child)
    }
}

impl Drop for Pty {
    fn drop(&mut self) {
        if self.master >= 0 { unsafe { libc::close(self.master); } }
        if self.slave >= 0 { unsafe { libc::close(self.slave); } }
    }
}

fn parse_k8s_minor(git_version: &str) -> Option<u32> {
    let v = git_version.strip_prefix('v').unwrap_or(git_version);
    v.split('.').nth(1)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::{parse_k8s_minor, split_all_containers_prefix};

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
}

