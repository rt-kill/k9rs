//! Daemon-side session: one per TUI client connection.
//!
//! A `ServerSession` is a tokio task spawned by the daemon for each persistent
//! TUI connection. It reads `SessionCommand`s from the TUI, manages
//! subscriptions via the shared `WatcherCache`, and pushes `SessionEvent`s
//! back over the socket.
//!
//! Wire format: length-prefixed bincode (see protocol.rs).

mod ops;
mod streaming;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::live_query::WatcherCache;
use super::protocol::{self, SessionCommand, SessionEvent};


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

/// A request submitted to the single-owner client-builder task. The builder
/// loop drains these one at a time, mutates process-global env vars in
/// isolation (no concurrent reader), constructs the `kube::Client`, and
/// hands the result back via `reply`. Replaces the `client_creation_lock`
/// `Mutex<()>` that used to fence env-var mutations.
struct ClientBuilderRequest {
    kubeconfig_yaml: String,
    env_vars: HashMap<String, String>,
    context: Option<protocol::ContextName>,
    reply: tokio::sync::oneshot::Sender<anyhow::Result<(kube::Client, kube::Config)>>,
}

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
    /// Channel into the single-owner client-builder task. Sessions submit
    /// `ClientBuilderRequest`s here and await the oneshot reply. The builder
    /// task is the only place that touches process-global env vars, which
    /// removes the need for any lock or `unsafe` synchronization on the
    /// caller side. (`std::env::set_var` is process-global; with one owner,
    /// it can't race.)
    client_builder_tx: mpsc::Sender<ClientBuilderRequest>,
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
        let (tx, rx) = mpsc::channel::<ClientBuilderRequest>(16);
        // The builder task is detached for the daemon's lifetime. It exits
        // when the channel closes, which happens when the last
        // `SessionSharedState` is dropped (i.e. daemon shutdown).
        tokio::spawn(client_builder_loop(rx));
        Self {
            watcher_cache: WatcherCache::new(),
            local_registry: Arc::new(crate::kube::local::LocalRegistry::new()),
            discovery_cache: super::cache::DiscoveryCache::new(),
            client_builder_tx: tx,
            next_session_id: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

/// The single-owner client-builder loop. Owns process-global env vars; no
/// other code in the daemon mutates them. Each request runs to completion
/// before the next is dequeued, so even though we shadow process env vars,
/// we never race with another reader.
async fn client_builder_loop(mut rx: mpsc::Receiver<ClientBuilderRequest>) {
    use kube::config::{Config, KubeConfigOptions, Kubeconfig};

    while let Some(req) = rx.recv().await {
        let result: anyhow::Result<(kube::Client, kube::Config)> = async {
            for (key, value) in &req.env_vars {
                // SAFETY: This task is the only writer of env vars in the
                // daemon — `client_builder_loop` is spawned exactly once
                // from `SessionSharedState::new`, and nothing else calls
                // `std::env::set_var`. So there is no concurrent reader,
                // and the process-global mutation is sequential.
                #[allow(unused_unsafe)]
                unsafe { std::env::set_var(key, value); }
            }

            let kubeconfig: Kubeconfig = serde_yaml::from_str(&req.kubeconfig_yaml)
                .map_err(|e| anyhow::anyhow!("Failed to parse kubeconfig YAML: {}", e))?;

            let context_name: Option<protocol::ContextName> = req.context.clone()
                .or_else(|| kubeconfig.current_context.clone().map(protocol::ContextName::from));

            let options = KubeConfigOptions {
                context: context_name.as_ref().map(|c| c.as_str().to_owned()),
                ..Default::default()
            };

            let mut config = Config::from_custom_kubeconfig(kubeconfig, &options).await
                .map_err(|e| anyhow::anyhow!("Failed to create config from kubeconfig: {}", e))?;

            config.read_timeout = Some(std::time::Duration::from_secs(300));
            config.connect_timeout = Some(std::time::Duration::from_secs(30));

            let client = kube::Client::try_from(config.clone())?;
            Ok((client, config))
        }.await;

        // Receiver may be gone if the calling session bailed; ignore.
        let _ = req.reply.send(result);
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const EVENT_CHANNEL_CAPACITY: usize = 512;

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
    pub client_config: Option<kube::Config>,
    pub context: protocol::ContextId,
    /// Session ID for structured logging in substream tasks.
    pub session_id: u64,
}

// ---------------------------------------------------------------------------
// ServerSession
// ---------------------------------------------------------------------------

pub struct ServerSession {
    writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
    shared: Arc<SessionSharedState>,
    client: kube::Client,
    client_config: Option<kube::Config>,
    context: protocol::ContextId,
    // `namespace` field removed — session doesn't need session-level
    // namespace state. Each subscription carries its own in `SubscriptionInit`.
    readonly: bool,
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
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            writer,
            shared,
            client,
            client_config: None,
            context,
            readonly,
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
    pub async fn init_and_run(
        mut reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
        writer: Box<dyn AsyncWrite + Unpin + Send>,
        shared: Arc<SessionSharedState>,
    ) {
        let mut buf_writer = BufWriter::with_capacity(protocol::IO_BUFFER_SIZE, writer);

        let init = match protocol::read_bincode::<_, SessionCommand>(&mut reader).await {
            Ok(SessionCommand::Init {
                context, namespace, readonly,
                kubeconfig_yaml, env_vars, identity,
            }) => InitParams {
                context, namespace, readonly,
                kubeconfig_yaml, env_vars, identity,
            },
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

        let init = match first_cmd {
            SessionCommand::Init {
                context, namespace, readonly,
                kubeconfig_yaml, env_vars, identity,
            } => InitParams {
                context, namespace, readonly,
                kubeconfig_yaml, env_vars, identity,
            },
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
        let init = match protocol::read_bincode::<_, SessionCommand>(&mut reader).await {
            Ok(SessionCommand::Init {
                context, namespace, readonly,
                kubeconfig_yaml, env_vars, identity,
            }) => InitParams {
                context, namespace, readonly,
                kubeconfig_yaml, env_vars, identity,
            },
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
        info!(
            "Session init: context={:?}, namespace={:?}, readonly={}",
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

        // 3. Create kube::Client from kubeconfig + env vars.
        let (client, client_config) = match Self::create_client_from_init(&init, &shared).await {
            Ok(c) => {
                info!("Created kube::Client for context '{}'", context_name);
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

        // Build ContextId from context name + server URL + user identity.
        let context_id = protocol::ContextId::new(
            context_name.clone(),
            client_config.cluster_url.to_string(),
            init.identity.user.clone(),
        );

        // 4. Send Ready.
        let ready = SessionEvent::Ready {
            context: context_name.clone(),
            identity: init.identity.clone(),
            namespaces: vec![],
        };
        if protocol::write_bincode(&mut buf_writer, &ready).await.is_err() {
            return;
        }

        // 5. Build ServerSession and enter command loop.
        let mut session = ServerSession::new(
            buf_writer,
            shared.clone(),
            client.clone(),
            context_id.clone(),
            init.readonly,
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
                client_config: Some(client_config),
                context: context_id,
                session_id,
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

        info!("Session ready, entering command loop (context={})", session.context.name);
        if let Err(e) = session.run(reader).await {
            info!("Session ended: {}", e);
        } else {
            info!("Session ended cleanly");
        }
    }

    /// Create a `kube::Client` from the kubeconfig YAML + env vars sent by
    /// the TUI. Delegates to the daemon-wide `client_builder_loop` so env
    /// var mutations are serialized through one task — no `Mutex<()>` and
    /// no concurrent process-global writes.
    async fn create_client_from_init(
        init: &InitParams,
        shared: &SessionSharedState,
    ) -> anyhow::Result<(kube::Client, kube::Config)> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let req = ClientBuilderRequest {
            kubeconfig_yaml: init.kubeconfig_yaml.clone(),
            env_vars: init.env_vars.clone(),
            context: init.context.clone(),
            reply: reply_tx,
        };
        shared.client_builder_tx.send(req).await
            .map_err(|_| anyhow::anyhow!("Client builder task is gone"))?;
        reply_rx.await
            .map_err(|_| anyhow::anyhow!("Client builder dropped reply"))?
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
        let _reader_guard = AbortOnDrop(Some(reader_handle.abort_handle()));

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
            SessionCommand::Init { .. } => info!("Session cmd: Init"),
            other => debug!("Session cmd: {:?}", other),
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
                content,
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
    /// owning per-context `LocalResourceSource`. The error path returns a
    /// user-readable string so the TUI can show it in the describe panel
    /// directly.
    fn describe_local(&self, rid: &protocol::ResourceId, name: &str) -> String {
        let Some(source) = self.shared.local_registry.get(&self.context.name, rid) else {
            return format!("Unknown local resource type: {}", rid.plural());
        };
        match source.describe(name) {
            Some(Ok(content)) => content,
            Some(Err(e)) => format!("Error: {}", e),
            None => format!("describe is not supported on {}", rid.plural()),
        }
    }

    /// Render a YAML view for a local-resource row by dispatching to the
    /// owning per-context `LocalResourceSource`. Mirrors `describe_local`.
    fn yaml_local(&self, rid: &protocol::ResourceId, name: &str) -> String {
        let Some(source) = self.shared.local_registry.get(&self.context.name, rid) else {
            return format!("Unknown local resource type: {}", rid.plural());
        };
        match source.yaml(name) {
            Some(Ok(content)) => content,
            Some(Err(e)) => format!("Error: {}", e),
            None => format!("yaml is not supported on {}", rid.plural()),
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
            tracing::info!(session = sid, sub = sub_id, "log: pod={} container={:?}", log_init.pod, log_init.container);
            handle_log_substream(log_init, writer, ctx).await;
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
    tracing::info!(session = session_id, sub = sub_id, "subscribe: {}({}) filter={:?}",
        rid.plural(), init.namespace.display(), filter);

    // Local resources branch out into the LocalResourceSource pipeline.
    if rid.is_local() {
        let Some(source) = ctx.shared.local_registry.get(&ctx.context.name, &rid) else {
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

    // Create a fresh kube::Client per watcher (avoids pool starvation).
    let watcher_client = if let Some(ref cfg) = ctx.client_config {
        kube::Client::try_from(cfg.clone()).unwrap_or_else(|_| ctx.client.clone())
    } else {
        ctx.client.clone()
    };

    let mut sub = match &rid {
        protocol::ResourceId::BuiltIn(kind) => {
            // Built-ins go through the typed registry path — the watcher
            // factory is dispatched on the typed kind, no string match.
            // Hand the destructured `kind` in so the cache doesn't have to
            // re-extract it at runtime.
            if init.force {
                ctx.shared.watcher_cache.subscribe_force(key, *kind, &watcher_client)
            } else {
                ctx.shared.watcher_cache.subscribe(key, *kind, &watcher_client)
            }
        }
        protocol::ResourceId::Crd(crd_ref) => {
            // CRD: resolve GVR via the typed `api_resource_for` helper —
            // it already handles both populated and incomplete (group/version
            // empty) shapes, falling back to discovery for the latter. We
            // only need to notify the TUI of the resolved identity when
            // the original was incomplete.
            let was_incomplete = crd_ref.is_unresolved();
            let (gvk, plural, scope) = match crate::kube::describe::api_resource_for(&watcher_client, &rid).await {
                Ok((ar, resolved_scope)) => {
                    let gvk = kube::api::GroupVersionKind::gvk(&ar.group, &ar.version, &ar.kind);
                    if was_incomplete {
                        let resolved_rid = protocol::ResourceId::crd(
                            ar.group.clone(), ar.version.clone(),
                            ar.kind.clone(), ar.plural.clone(), resolved_scope,
                        );
                        let _ = protocol::write_bincode(&mut writer, &protocol::StreamEvent::Resolved {
                            original: rid.clone(),
                            resolved: resolved_rid,
                        }).await;
                    }
                    (gvk, ar.plural, resolved_scope)
                }
                Err(e) => {
                    tracing::warn!("Failed to resolve resource '{}': {}", crd_ref.plural, e);
                    let _ = protocol::write_bincode(&mut writer, &protocol::StreamEvent::Error(
                        format!("Unknown resource: {}", crd_ref.plural)
                    )).await;
                    return;
                }
            };

            // Rebuild the QueryKey with the resolved GVR so the cache key
            // matches on subsequent navigations (where the TUI sends the
            // resolved ResourceId with populated group/version).
            let resolved_key = QueryKey {
                context: key.context,
                namespace: key.namespace,
                resource: protocol::ResourceId::crd(
                    gvk.group.clone(), gvk.version.clone(),
                    gvk.kind.clone(), plural.clone(), scope,
                ),
                filter: key.filter,
            };

            // Look up printer columns from the discovery cache for CRD-specific columns.
            let printer_columns = ctx.shared.discovery_cache
                .printer_columns_for(&ctx.context, &gvk.group, &plural)
                .unwrap_or_default();

            if init.force {
                ctx.shared.watcher_cache.remove(&resolved_key);
            }
            ctx.shared.watcher_cache.subscribe_dynamic(
                resolved_key, &watcher_client, gvk, plural, scope, printer_columns,
            )
        }
        protocol::ResourceId::Local(_) => {
            // Unreachable — we early-returned above for locals.
            unreachable!("local rids are handled by the early-return branch");
        }
    };

    // Capabilities used to be emitted here as `StreamEvent::Capabilities`.
    // Removed — the client computes the identical manifest from the typed
    // `ResourceId::capabilities()` method, so the wire round-trip and
    // client-side cache were redundant.

    // Bridge: watcher → StreamEvent frames on the substream.
    // Send current snapshot immediately if available.
    if let Some(update) = sub.current() {
        let update = apply_owner_filter_inline(update, &filter);
        let _ = protocol::write_bincode(&mut writer, &protocol::StreamEvent::Snapshot(update)).await;
        let _ = writer.flush().await;
    }

    loop {
        if sub.changed().await.is_err() {
            tracing::debug!(session = session_id, sub = sub_id, "watcher closed for {}", rid.plural());
            break;
        }
        let Some(update) = sub.current() else {
            let detail = sub.last_error().unwrap_or_default();
            let msg = if detail.is_empty() {
                format!("Watcher failed for {}", rid.plural())
            } else {
                format!("Watcher failed for {}: {}", rid.plural(), detail)
            };
            let _ = protocol::write_bincode(&mut writer, &protocol::StreamEvent::Error(msg)).await;
            break;
        };
        let update = apply_owner_filter_inline(update, &filter);
        if let crate::event::ResourceUpdate::Rows { ref resource, ref rows, .. } = update {
            tracing::info!(
                session = session_id, sub = sub_id,
                "server bridge: writing snapshot for {} ({} rows)",
                resource.plural(), rows.len(),
            );
        }
        if protocol::write_bincode(&mut writer, &protocol::StreamEvent::Snapshot(update)).await.is_err() {
            break;
        }
        if writer.flush().await.is_err() { break; }
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

/// RAII guard that aborts a tokio task when dropped. Used by
/// `ServerSession::run` to make sure the binary reader is reaped on every
/// exit path, including unwind. Holding the abort handle in an `Option`
/// lets the guard be `Drop`'d explicitly without `Drop` running twice.
struct AbortOnDrop(Option<tokio::task::AbortHandle>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Log substream handler
// ---------------------------------------------------------------------------

/// Handle a log substream. Spawns `kubectl logs` and streams lines back
/// as bincode-framed `String`s. When the substream closes (TUI dropped it),
/// the subprocess is killed via `kill_on_drop`.
async fn handle_log_substream(
    init: protocol::LogInit,
    mut writer: tokio::io::BufWriter<tokio::io::WriteHalf<crate::kube::mux::MuxedStream>>,
    ctx: Arc<SessionContext>,
) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

    // The dispatch site `handle_substream` already logged
    // `pod={} container={:?}` at substream open — we don't need to
    // duplicate it here. The typed `LogContainer` enum's Debug repr
    // is already self-explanatory.

    let ns = match init.namespace.as_option() {
        Some(n) => n.to_string(),
        None => String::new(),
    };

    let mut cmd = tokio::process::Command::new("kubectl");
    cmd.arg("logs").arg(&init.pod);
    match &init.container {
        protocol::LogContainer::All => {
            cmd.arg("--all-containers");
        }
        protocol::LogContainer::Named(name) => {
            cmd.arg("-c").arg(name);
        }
        protocol::LogContainer::Default => {}
    }
    if !ns.is_empty() {
        cmd.arg("-n").arg(&ns);
    }
    if !ctx.context.name.is_empty() {
        cmd.arg("--context").arg(ctx.context.name.as_str());
    }
    if init.follow {
        cmd.arg("-f");
    }
    if let Some(tail) = init.tail {
        cmd.arg("--tail").arg(tail.to_string());
    }
    if let Some(ref since) = init.since {
        cmd.arg("--since").arg(since);
    }
    if init.previous {
        cmd.arg("--previous");
    }
    cmd.stdout(std::process::Stdio::piped());
    // stderr → null. We don't surface kubectl's own warnings to the TUI,
    // and an unread piped stderr can fill the OS pipe buffer (kubectl emits
    // a line per pod restart / auth refresh / TLS handshake on long-running
    // follow streams) and stall the subprocess after ~64 KiB.
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

    while let Ok(Some(line)) = lines.next_line().await {
        if protocol::write_bincode(&mut writer, &line).await.is_err() {
            break;
        }
        if writer.flush().await.is_err() {
            break;
        }
    }
    // Substream closes here → TUI sees EOF → LogStreamEnded.
    tracing::debug!("log substream ended for pod={}", init.pod);
}
