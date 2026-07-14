//! TUI-side session: connects to a `ServerSession` over a Unix socket (daemon
//! mode) or an in-memory duplex stream (`--no-daemon` mode).
//!
//! `ClientSession` is the counterpart of `ServerSession`. It sends
//! `SessionCommand`s and converts incoming `SessionEvent`s into `AppEvent`s
//! that the existing TUI event loop can process.
//!
//! ## Lifecycle
//!
//! `ClientSession::new` returns immediately with a usable handle whose command
//! methods queue locally on an unbounded channel. A background "connection
//! manager" task reads the kubeconfig, opens the socket, performs the Init/
//! Ready handshake, and only then spawns the reader/writer tasks that drain
//! the queue and feed the wire. The TUI is interactive (local actions like
//! `:`, scrolling, sort, help all work) from frame zero — daemon-bound
//! commands simply wait in the queue until the handshake completes. The
//! handshake outcome is delivered as `AppEvent::ConnectionEstablished` /
//! `AppEvent::ConnectionFailed`.
//!
//! Wire format: length-prefixed bincode (see protocol.rs).

use std::collections::HashMap;
use std::sync::Arc;

use kube::config::Kubeconfig;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, warn};

use super::daemon::socket_path;
use super::protocol::{self, SessionCommand, SessionEvent};
use super::server_session::{ServerSession, SessionSharedState};
use crate::app::{FlashMessage, KubeContext};
use crate::event::{AppEvent, ResourceUpdate};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Parameters needed to bring up a daemon connection.
#[derive(Debug, Clone)]
pub struct ConnectionParams {
    pub context: Option<crate::kube::protocol::ContextName>,
    /// Initial namespace selection for the session. Typed so we don't
    /// flatten-and-reparse at the boundary.
    pub namespace: protocol::Namespace,
    pub readonly: bool,
    pub no_daemon: bool,
}

/// Output of a single kubeconfig read. Built once by the manager task and
/// then split into the daemon Init payload (`prepared`) and the TUI's
/// `KubeconfigLoaded` event (`contexts`, `current_*`).
struct KubeconfigBundle {
    prepared: PreparedKubeconfig,
    contexts: Vec<KubeContext>,
}

/// Kubeconfig + env vars prepared by the TUI for sending to the session.
struct PreparedKubeconfig {
    kubeconfig_yaml: String,
    env_vars: HashMap<String, String>,
    context_name: crate::kube::protocol::ContextName,
    identity: crate::kube::protocol::ClusterIdentity,
}

/// Result of a successful Init/Ready handshake. Returned from `do_handshake`
/// and consumed by `connection_manager` to spin up the I/O loops and notify
/// the TUI.
struct HandshakeOutcome {
    context: crate::kube::protocol::ContextName,
    identity: crate::kube::protocol::ClusterIdentity,
    namespaces: Vec<String>,
    reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
}

/// TUI-side session handle. Three pieces, no shared mutable state, no locks:
/// - `cmd_tx`: the only way to send commands. Bounded at
///   [`CMD_CHANNEL_CAPACITY`] so a stalled writer (slow daemon, network
///   pause) can't let the TUI queue commands without backpressure. The
///   capacity is generous — commands are small, user-triggered, and rare
///   compared to snapshot traffic. Apply payloads (multi-MB) are the only
///   large commands and even a handful of them is bounded in practice.
/// - `_shutdown`: held only for its `Drop` side effect. When this
///   `oneshot::Sender` drops, the manager task's matching `Receiver`
///   resolves with `Err`, signalling cleanup. The manager handles aborting
///   its own children — there are no abort handles to track on this side.
pub struct ClientSession {
    cmd_tx: mpsc::Sender<SessionCommand>,
    _shutdown: oneshot::Sender<()>,
    mux_handle_rx: watch::Receiver<Option<crate::kube::mux::MuxHandle>>,
    event_tx: mpsc::Sender<AppEvent>,
    no_daemon: bool,
    /// Tracks fire-and-forget background tasks launched by TUI action
    /// handlers (copy-to-clipboard, save-table). Mirrors
    /// `ServerSession::pending_tasks` on the server side. On
    /// `ClientSession::drop` these are aborted via the JoinSet's built-in
    /// abort-on-drop behavior, preventing clipboard tools from outliving
    /// the TUI process.
    pending_tasks: tokio::task::JoinSet<()>,
}

/// An active subscription backed by its own yamux substream. Drop aborts
/// the bridge task which closes the substream (sending RST to the daemon).
/// The daemon's bridge for this substream exits on EOF; the underlying
/// watcher enters its grace period via `Subscription::Drop`.
///
/// From the application's perspective: this IS the subscription. Hold it
/// to keep the data flowing; drop it to stop. No ids, no routing tags.
pub struct SubscriptionStream {
    _bridge: tokio::task::AbortHandle,
}

impl std::fmt::Debug for SubscriptionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionStream").finish_non_exhaustive()
    }
}

impl Drop for SubscriptionStream {
    fn drop(&mut self) {
        self._bridge.abort();
    }
}

impl SubscriptionStream {
    /// Whether the bridge task is still running. Goes `false` once the bridge
    /// has terminated by any path — retry give-up, EOF, or panic.
    ///
    /// Every such terminal path also emits `SubscriptionFailed`, which clears
    /// the nav stack's handle reactively; this method lets the nav layer's
    /// re-subscribe decision observe liveness *directly* instead of trusting
    /// that convention, so a present-but-dead handle can never masquerade as a
    /// live subscription owner.
    pub fn is_alive(&self) -> bool {
        !self._bridge.is_finished()
    }

    /// Build a handle from a raw abort handle. Test-only — production code gets
    /// a `SubscriptionStream` from `subscribe_stream`, never by hand.
    #[cfg(test)]
    pub(crate) fn from_abort_handle(bridge: tokio::task::AbortHandle) -> Self {
        Self { _bridge: bridge }
    }
}

/// An active exec session backed by a yamux substream. The daemon
/// spawns kubectl in a PTY; raw terminal bytes flow over yamux.
/// Drop aborts the reader bridge and closes the substream
/// (RST → daemon kills kubectl).
pub struct ExecStream {
    pub writer: tokio::io::BufWriter<tokio::io::WriteHalf<crate::kube::mux::MuxedStream>>,
    _bridge: tokio::task::AbortHandle,
}

impl Drop for ExecStream {
    fn drop(&mut self) {
        self._bridge.abort();
    }
}

/// An active log stream backed by its own yamux substream. Same shape as
/// `SubscriptionStream` — drop aborts the bridge, closes the substream,
/// the daemon's kubectl subprocess dies via `kill_on_drop`. Lines land in
/// the `LineStore` the stream was handed at open (destination rides the
/// event) — no ids, no routing tags.
pub struct LogStream {
    _bridge: tokio::task::AbortHandle,
}

impl std::fmt::Debug for LogStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogStream").finish_non_exhaustive()
    }
}

impl LogStream {
    /// Whether the bridge task behind this stream is still running.
    pub fn is_alive(&self) -> bool {
        !self._bridge.is_finished()
    }

    /// Build a handle from a raw abort handle. Test-only.
    #[cfg(test)]
    pub(crate) fn from_abort_handle(bridge: tokio::task::AbortHandle) -> Self {
        Self { _bridge: bridge }
    }
}

impl Drop for LogStream {
    fn drop(&mut self) {
        self._bridge.abort();
    }
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

/// Capacity of the client-side command channel. Commands are small and
/// user-triggered; 128 is generous for the worst-case burst (confirm
/// dialog approves a batch of actions). A stalled writer blocks beyond
/// this many pending commands, which is the right behavior — better than
/// unbounded memory growth.
const CMD_CHANNEL_CAPACITY: usize = 128;

impl ClientSession {
    /// Construct a session and kick off the daemon handshake in the background.
    /// The returned session is immediately usable: command methods queue
    /// locally on a bounded channel (see [`CMD_CHANNEL_CAPACITY`]) and are
    /// flushed to the wire as soon as the manager task spawns the writer
    /// (after a successful handshake).
    pub fn new(params: ConnectionParams, event_tx: mpsc::Sender<AppEvent>) -> Self {
        let no_daemon = params.no_daemon;
        let (cmd_tx, cmd_rx) = mpsc::channel::<SessionCommand>(CMD_CHANNEL_CAPACITY);
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let (mux_handle_tx, mux_handle_rx) = watch::channel(None);

        tokio::spawn(connection_manager(
            params,
            cmd_rx,
            event_tx.clone(),
            shutdown_rx,
            mux_handle_tx,
        ));

        Self {
            cmd_tx,
            _shutdown: shutdown_tx,
            mux_handle_rx,
            event_tx,
            no_daemon,
            pending_tasks: tokio::task::JoinSet::new(),
        }
    }

    /// Spawn a background task tracked by the session's `JoinSet`. Drops
    /// finished tasks opportunistically so the set doesn't grow. Equivalent
    /// to `ServerSession::track_task` on the server side — use this
    /// instead of bare `tokio::spawn` for any handler-side fire-and-forget
    /// work so the tasks are aborted when the session drops.
    pub fn track_task<F>(&mut self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        while self.pending_tasks.try_join_next().is_some() {}
        self.pending_tasks.spawn(fut);
    }

    /// Whether this session is running in no-daemon mode (in-process).
    pub fn is_no_daemon(&self) -> bool {
        self.no_daemon
    }

    /// Read the on-disk kubeconfig and produce both the daemon Init payload
    /// (`PreparedKubeconfig`) and the full contexts list for the TUI's
    /// contexts panel — from a single disk read and a single parse.
    fn read_kubeconfig_bundle(cli_context: Option<&crate::kube::protocol::ContextName>) -> anyhow::Result<KubeconfigBundle> {
        let kubeconfig = Kubeconfig::read()
            .map_err(|e| anyhow::anyhow!("Failed to read kubeconfig: {}", e))?;

        let context_name: crate::kube::protocol::ContextName = cli_context
            .cloned()
            .or_else(|| kubeconfig.current_context.clone().map(Into::into))
            .ok_or_else(|| anyhow::anyhow!("No context specified and no current-context in kubeconfig"))?;

        let identity = lookup_cluster_user(&kubeconfig, &context_name);

        let contexts: Vec<KubeContext> = kubeconfig.contexts.iter().map(|nc| {
            let identity = nc.context.as_ref()
                .map(|c| crate::kube::protocol::ClusterIdentity::new(
                    c.cluster.clone(),
                    c.user.clone().unwrap_or_default(),
                ))
                .unwrap_or_default();
            KubeContext {
                name: nc.name.as_str().into(),
                identity,
                is_current: nc.name.as_str() == context_name.as_str(),
            }
        }).collect();

        let kubeconfig_yaml = serde_yaml::to_string(&kubeconfig)
            .map_err(|e| anyhow::anyhow!("Failed to serialize kubeconfig: {}", e))?;

        Ok(KubeconfigBundle {
            prepared: PreparedKubeconfig {
                kubeconfig_yaml,
                env_vars: collect_env_vars(),
                context_name,
                identity,
            },
            contexts,
        })
    }

    fn build_init_command(
        params: &ConnectionParams,
        prepared: &PreparedKubeconfig,
    ) -> SessionCommand {
        SessionCommand::Init {
            context: Some(prepared.context_name.clone()),
            namespace: params.namespace.clone(),
            readonly: params.readonly,
            kubeconfig_yaml: prepared.kubeconfig_yaml.clone(),
            env_vars: prepared.env_vars.clone(),
            identity: prepared.identity.clone(),
        }
    }

}


/// Monotonic epoch source for table streams: each subscription bridge
/// instance takes one, stamping its Baseline/Delta events so a successor
/// subscription's table can reject this stream's late-queued events
/// (accepted iff `event.epoch >= table.epoch`; a Baseline sets it).
static NEXT_STREAM_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

// ---------------------------------------------------------------------------
// Connection manager
// ---------------------------------------------------------------------------

/// Background task that brings up the daemon connection through a small,
/// linear pipeline of named stages: read the kubeconfig, open the transport,
/// exchange Init/Ready, spawn the I/O loops, then await shutdown. The whole
/// thing is raced against `shutdown_rx` so dropping the `ClientSession`
/// promptly tears everything down — no shared mutable state required.
async fn connection_manager(
    params: ConnectionParams,
    cmd_rx: mpsc::Receiver<SessionCommand>,
    event_tx: mpsc::Sender<AppEvent>,
    mut shutdown_rx: oneshot::Receiver<()>,
    mux_handle_tx: watch::Sender<Option<crate::kube::mux::MuxHandle>>,
) {
    let pipeline = run_connection_pipeline(params, cmd_rx, event_tx, mux_handle_tx);
    tokio::pin!(pipeline);
    tokio::select! {
        biased;
        _ = &mut shutdown_rx => {
            // ClientSession dropped — `pipeline` is dropped here, which
            // cancels any pending await and aborts any I/O tasks the
            // pipeline spawned (see `run_connection_pipeline`).
        }
        _ = &mut pipeline => {
            // Pipeline ran to completion (either failed early and reported
            // ConnectionFailed, or the I/O loops exited naturally).
        }
    }
}

/// The connection pipeline. Owns its I/O tasks via local
/// `JoinHandle`s — when this future is dropped (via `tokio::select!` losing
/// to `shutdown_rx`), the JoinHandles drop with it. Aborting on drop is wired
/// up by spawning the children with explicit `AbortHandle`s captured into a
/// scope guard.
async fn run_connection_pipeline(
    params: ConnectionParams,
    cmd_rx: mpsc::Receiver<SessionCommand>,
    event_tx: mpsc::Sender<AppEvent>,
    mux_handle_tx: watch::Sender<Option<crate::kube::mux::MuxHandle>>,
) {
    macro_rules! fail {
        ($($arg:tt)*) => {{
            let _ = event_tx.send(AppEvent::ConnectionFailed(format!($($arg)*))).await;
            return;
        }};
    }

    // Stage 1: read the kubeconfig once. Splits into the daemon Init payload
    // and the contexts list — both delivered from a single disk read. The
    // YAML and env vars are passed through to the TUI in the same event so
    // App can cache them for context switches without going back to disk.
    let bundle = match load_kubeconfig(params.context.clone()).await {
        Ok(b) => b,
        Err(e) => fail!("kubeconfig: {}", e),
    };
    let _ = event_tx.send(AppEvent::KubeconfigLoaded {
        current_context: bundle.prepared.context_name.clone(),
        current_identity: bundle.prepared.identity.clone(),
        contexts: bundle.contexts,
    }).await;

    // Stage 2: open the transport and establish the yamux multiplexed
    // connection. The first substream we open is the CONTROL stream, which
    // carries the Init/Ready handshake plus one-shot commands (Apply,
    // Delete, Yaml, etc.) and global events (Flash, PodMetrics, etc.).
    let mux = match open_transport(params.no_daemon).await {
        Ok(m) => m,
        Err(e) => fail!("{}", e),
    };
    // Publish the MuxHandle so subscribe_stream() tasks can open their own
    // substreams. They'll await the handshake completion before actually
    // subscribing, but having the handle early lets them start their await
    // immediately rather than polling.
    let _ = mux_handle_tx.send(Some(mux.handle()));
    let control_stream = match mux.open().await {
        Ok(s) => s,
        Err(e) => fail!("failed to open control stream: {}", e),
    };

    // Split the control substream into reader/writer halves for the
    // existing handshake + I/O loop code. Inside, it's still plain bincode
    // framing — the yamux layer is invisible at this level.
    let (ctrl_read, ctrl_write) = tokio::io::split(control_stream);
    let reader: BufReader<Box<dyn AsyncRead + Unpin + Send>> = BufReader::with_capacity(
        protocol::IO_BUFFER_SIZE,
        Box::new(ctrl_read),
    );
    let writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>> = BufWriter::with_capacity(
        protocol::IO_BUFFER_SIZE,
        Box::new(ctrl_write),
    );

    // Stage 3: Init/Ready handshake over the control substream.
    let outcome = match do_handshake(
        reader, writer,
        &params,
        &bundle.prepared,
    ).await {
        Ok(o) => o,
        Err(e) => fail!("{}", e),
    };
    // Stage 4: spawn the reader/writer loops (on the control substream)
    // and notify the TUI. The `_io_guard` captures both abort handles; if
    // this future is dropped (e.g. shutdown raced ahead), the guard fires
    // in its `Drop` and the tasks are aborted.
    let writer_handle = tokio::spawn(writer_loop(cmd_rx, outcome.writer));
    let reader_handle = tokio::spawn(reader_loop(
        outcome.reader,
        event_tx.clone(),
        outcome.context.clone(),
    ));
    let _io_guard = AbortOnDrop::new([
        writer_handle.abort_handle(),
        reader_handle.abort_handle(),
    ]);

    let _ = event_tx.send(AppEvent::ConnectionEstablished {
        context: outcome.context,
        identity: outcome.identity,
        namespaces: outcome.namespaces,
    }).await;

    // Stage 5: keep the I/O loops alive for as long as both are running.
    // When either dies (cmd_tx dropped → writer exits; daemon disconnects
    // → reader exits) the other is aborted by `_io_guard` going out of scope.
    // The `mux` connection stays alive here on the stack so the yamux
    // driver task continues running for the session's lifetime.
    tokio::select! {
        _ = writer_handle => {}
        _ = reader_handle => {}
    }
    drop(mux); // explicit for clarity — driver task dies here
}

/// RAII guard: aborts a fixed set of tokio tasks when dropped. Used by the
/// connection pipeline to guarantee its child I/O loops die when the pipeline
/// future is cancelled (e.g. by `connection_manager`'s shutdown select).
struct AbortOnDrop<const N: usize> {
    handles: [tokio::task::AbortHandle; N],
}

impl<const N: usize> AbortOnDrop<N> {
    fn new(handles: [tokio::task::AbortHandle; N]) -> Self {
        Self { handles }
    }
}

impl<const N: usize> Drop for AbortOnDrop<N> {
    fn drop(&mut self) {
        for h in &self.handles {
            h.abort();
        }
    }
}

/// Stage 1: read and parse the kubeconfig on a blocking thread.
async fn load_kubeconfig(cli_context: Option<crate::kube::protocol::ContextName>) -> anyhow::Result<KubeconfigBundle> {
    tokio::task::spawn_blocking(move || ClientSession::read_kubeconfig_bundle(cli_context.as_ref()))
        .await
        .map_err(|e| anyhow::anyhow!("kubeconfig task panicked: {}", e))?
}

/// Stage 2: open the transport and establish a multiplexed connection.
///
/// Returns a `MuxedConnection` (the yamux session wrapped in our
/// abstraction) plus its `MuxHandle` for opening subscription substreams
/// from arbitrary tasks. The caller opens the first substream (control)
/// from the connection to run the Init/Ready handshake over it.
///
/// In `--no-daemon` mode, both ends of an in-memory duplex are wrapped
/// in yamux sessions; the server side is spawned as a background task.
async fn open_transport(
    no_daemon: bool,
) -> anyhow::Result<crate::kube::mux::MuxedConnection> {
    use crate::kube::mux::MuxedConnection;
    if no_daemon {
        // Single-session state: no local-resource grace window — this
        // registry dies with the connection, so recovery is impossible and
        // grace would only keep kubectl children alive pointlessly.
        let server_state = Arc::new(SessionSharedState::new_single_session());
        let (client_stream, server_stream) = tokio::io::duplex(protocol::DUPLEX_BUFFER_SIZE);

        // Server side of the duplex — wrapped in a yamux session and
        // handed to the existing ServerSession entry point.
        tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(
                ServerSession::init_and_run_muxed(
                    MuxedConnection::server(server_stream),
                    server_state,
                )
            );
            if let Err(e) = futures::FutureExt::catch_unwind(result).await {
                tracing::error!("ServerSession panicked: {:?}", e);
            }
        });

        Ok(MuxedConnection::client(client_stream))
    } else {
        use tokio::io::AsyncWriteExt;
        let path = socket_path();
        let mut stream = UnixStream::connect(&path).await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to connect to daemon at {}: {}. Is the daemon running?",
                path.display(),
                e
            )
        })?;
        // Write the connection-type discriminator so the daemon's accept
        // loop knows to wrap this connection in yamux (not plain bincode).
        stream.write_all(&[crate::kube::daemon::CONN_TYPE_SESSION]).await
            .map_err(|e| anyhow::anyhow!("Failed to write session marker: {}", e))?;
        Ok(MuxedConnection::client(stream))
    }
}

/// Stage 3: send `SessionCommand::Init`, read the first `SessionEvent` and
/// require it to be `Ready`. Returns the negotiated session info plus the
/// (now-positioned) reader and writer for the I/O loops.
async fn do_handshake(
    mut reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    mut writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
    params: &ConnectionParams,
    prepared: &PreparedKubeconfig,
) -> anyhow::Result<HandshakeOutcome> {
    // Version preamble BEFORE any framed message (see PROTOCOL_MAGIC):
    // a pre-8 daemon can never mis-parse us, and we detect it deterministically.
    protocol::write_handshake(&mut writer).await
        .map_err(|e| anyhow::anyhow!("Failed to send protocol handshake: {}", e))?;
    protocol::read_handshake(&mut reader).await?;

    let init_cmd = ClientSession::build_init_command(params, prepared);
    protocol::write_bincode(&mut writer, &init_cmd).await
        .map_err(|e| anyhow::anyhow!("Failed to send Init: {}", e))?;

    let first: SessionEvent = protocol::read_bincode(&mut reader).await
        .map_err(|e| anyhow::anyhow!("Failed to read Ready: {}", e))?;

    match first {
        SessionEvent::Ready { context, identity, namespaces } => {
            Ok(HandshakeOutcome { context, identity, namespaces, reader, writer })
        }
        SessionEvent::SessionError(msg) => {
            Err(anyhow::anyhow!("Server rejected session: {}", msg))
        }
        other => {
            Err(anyhow::anyhow!("Unexpected first event from server: {:?}", other))
        }
    }
}

/// The writer half of the I/O pair: drains the unbounded command channel
/// (which the TUI fills via `ClientSession::send_command`) and serializes
/// each `SessionCommand` onto the wire as length-prefixed bincode. Exits on
/// any write error or when all senders on `cmd_rx` are dropped.
async fn writer_loop(
    mut cmd_rx: mpsc::Receiver<SessionCommand>,
    mut writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        if protocol::write_bincode(&mut writer, &cmd).await.is_err() {
            break;
        }
    }
}

/// Look up the cluster + user names for a given context inside a parsed
/// `Kubeconfig`. Returns an empty [`ClusterIdentity`] if the context isn't
/// found — matches what the rest of the code expects (rendered as `n/a` in
/// the UI).
fn lookup_cluster_user(
    kubeconfig: &Kubeconfig,
    context_name: &crate::kube::protocol::ContextName,
) -> crate::kube::protocol::ClusterIdentity {
    for named_ctx in &kubeconfig.contexts {
        if named_ctx.name == context_name.as_str() {
            if let Some(ref ctx) = named_ctx.context {
                return crate::kube::protocol::ClusterIdentity::new(
                    ctx.cluster.clone(),
                    ctx.user.clone().unwrap_or_default(),
                );
            }
            break;
        }
    }
    crate::kube::protocol::ClusterIdentity::default()
}

// ---------------------------------------------------------------------------
// Command methods
// ---------------------------------------------------------------------------

impl ClientSession {
    // `switch_namespace` used to send a server-side command; it was only
    // informational and the server never used the value for subscription
    // scoping. Deleted in favor of fully client-side namespace switching —
    // `do_switch_namespace` in session_nav.rs handles everything locally.

    pub fn describe(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(SessionCommand::Describe(target.clone()))
    }

    pub fn yaml(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(SessionCommand::Yaml(target.clone()))
    }

    /// Submit edited YAML for `target`. The server routes by
    /// `target.resource.is_local()` — same wire command for K8s and local
    /// resources, no client-side branching. Used by the unified edit flow:
    /// `Action::Edit` fetches via `yaml()`, the user edits in `$EDITOR`,
    /// and the result is sent back via `apply()`.
    pub fn apply(&mut self, target: &protocol::ObjectRef, yaml: String) -> anyhow::Result<()> {
        self.send_command(SessionCommand::Apply { target: target.clone(), yaml })
    }

    /// Open an exec substream. Spawns a reader bridge that delivers
    /// `ExecData`/`ExecEnded` events. Drop aborts the bridge and
    /// closes the substream.
    pub async fn open_exec_stream(
        &self,
        exec_init: protocol::ExecInit,
        event_tx: mpsc::Sender<AppEvent>,
    ) -> anyhow::Result<ExecStream> {
        let mux_handle = self.mux_handle_rx.borrow().clone()
            .ok_or_else(|| anyhow::anyhow!("not connected"))?;
        let stream = mux_handle.open().await
            .map_err(|e| anyhow::anyhow!("substream open failed: {}", e))?;
        let (read_half, mut write_half) = tokio::io::split(stream);
        let mut reader = tokio::io::BufReader::with_capacity(
            protocol::IO_BUFFER_SIZE,
            read_half,
        );
        let init_msg = protocol::SubstreamInit::Exec(exec_init);
        protocol::write_bincode(&mut write_half, &init_msg).await?;
        let writer = tokio::io::BufWriter::with_capacity(
            protocol::IO_BUFFER_SIZE,
            write_half,
        );
        let bridge = tokio::spawn(async move {
            let panic_tx = event_tx.clone();
            let result = std::panic::AssertUnwindSafe(async {
                loop {
                    match protocol::read_bincode::<_, protocol::ExecFrame>(&mut reader).await {
                        Ok(protocol::ExecFrame::Data(bytes)) => {
                            if event_tx.send(AppEvent::ExecData(bytes)).await.is_err() { break; }
                        }
                        Ok(protocol::ExecFrame::Resize { .. }) => {}
                        Err(_) => {
                            let _ = event_tx.send(AppEvent::ExecEnded).await;
                            break;
                        }
                    }
                }
            });
            if let Err(_panic) = futures::FutureExt::catch_unwind(result).await {
                let _ = panic_tx.send(AppEvent::ExecEnded).await;
                let _ = panic_tx.send(AppEvent::Flash(FlashMessage::error(
                    "exec bridge panicked".to_string(),
                ))).await;
            }
        });
        Ok(ExecStream { writer, _bridge: bridge.abort_handle() })
    }

    pub fn delete(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(SessionCommand::Delete(target.clone()))
    }

    pub fn force_kill(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(SessionCommand::ForceKill(target.clone()))
    }

    pub fn scale(&mut self, target: &protocol::ObjectRef, replicas: u32) -> anyhow::Result<()> {
        self.send_command(SessionCommand::Scale {
            target: target.clone(),
            replicas,
        })
    }

    pub fn restart(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(SessionCommand::Restart(target.clone()))
    }

    /// Open a log substream. Same shape as `subscribe_stream` — spawns a
    /// bridge task that opens a yamux substream, writes `LogInit`, reads
    /// log lines, and forwards them as `AppEvent`s. Drop the `LogStream`
    /// to stop (the substream RSTs, the daemon kills kubectl).
    ///
    /// Takes a fully-built `LogInit` so callers can't mistakenly pass an
    /// untyped namespace string and round-trip it through `Namespace::from_row`
    /// (the parameter list used to be 7 strings/options long).
    pub fn stream_log_substream(
        &self,
        log_init: protocol::LogInit,
        store: std::sync::Arc<crate::app::store::LineStore>,
    ) -> LogStream {
        // Same succession rule as table subscriptions: the store's epoch
        // floor is raised BEFORE this returns, so a replaced stream's
        // queued lines are stale on arrival (range restarts reuse the
        // store so LogFilter children keep their handles).
        let epoch = NEXT_STREAM_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        store.expect_epoch(epoch);
        let mut mux_rx = self.mux_handle_rx.clone();
        let event_tx = self.event_tx.clone();
        let init = protocol::SubstreamInit::Log(log_init);
        let handle = tokio::spawn(async move {
            let panic_tx = event_tx.clone();
            let panic_store = store.clone();
            let result = std::panic::AssertUnwindSafe(async {
            // Yield the MuxHandle out of the wait loop so the "is Some"
            // check and the binding are one step — no post-loop `.unwrap()`.
            let mux_handle = loop {
                if let Some(v) = mux_rx.borrow_and_update().clone() { break v; }
                if mux_rx.changed().await.is_err() { return; }
            };

            let stream = match mux_handle.open().await {
                Ok(s) => s,
                Err(e) => {
                    let _ = event_tx.send(AppEvent::Log(crate::event::LogEvent {
                        store: store.clone(),
                        epoch,
                        payload: crate::event::LogPayload::Ended,
                    })).await;
                    let _ = event_tx.send(AppEvent::Flash(FlashMessage::error(
                        format!("log substream open failed: {}", e),
                    ))).await;
                    return;
                }
            };
            let (read_half, mut write_half) = tokio::io::split(stream);
            let mut reader = tokio::io::BufReader::with_capacity(
                protocol::IO_BUFFER_SIZE,
                read_half,
            );

            if protocol::write_bincode(&mut write_half, &init).await.is_err() {
                let _ = event_tx.send(AppEvent::Log(crate::event::LogEvent {
                    store: store.clone(),
                    epoch,
                    payload: crate::event::LogPayload::Ended,
                })).await;
                let _ = event_tx.send(AppEvent::Flash(FlashMessage::error(
                    "Log init handshake failed".to_string(),
                ))).await;
                return;
            }

            // Each frame is a single typed `LogLine` (bincode-framed): content
            // plus an optional source container (set only for `--all-containers`
            // streams, where the daemon already stripped kubectl's prefix).
            // EOF = log stream ended.
            loop {
                match protocol::read_bincode::<_, protocol::LogLine>(&mut reader).await {
                    Ok(line) => {
                        // Pass ANSI codes through — the log renderer parses
                        // SGR sequences into ratatui Styles so application-
                        // embedded colors are preserved faithfully.
                        let event = AppEvent::Log(crate::event::LogEvent {
                            store: store.clone(),
                            epoch,
                            payload: crate::event::LogPayload::Line(line),
                        });
                        if event_tx.send(event).await.is_err() { break; }
                    }
                    Err(_) => {
                        // EOF — log stream ended.
                        let _ = event_tx.send(AppEvent::Log(crate::event::LogEvent {
                            store: store.clone(),
                            epoch,
                            payload: crate::event::LogPayload::Ended,
                        })).await;
                        let _ = event_tx.send(AppEvent::Flash(
                            FlashMessage::info("Log stream ended"),
                        )).await;
                        break;
                    }
                }
            }
            });
            if let Err(panic) = futures::FutureExt::catch_unwind(result).await {
                let msg = if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = panic.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "unknown panic".to_string()
                };
                let _ = panic_tx.send(AppEvent::Log(crate::event::LogEvent {
                    store: panic_store,
                    epoch,
                    payload: crate::event::LogPayload::Ended,
                })).await;
                let _ = panic_tx.send(AppEvent::Flash(FlashMessage::error(
                    format!("log bridge panicked: {}", msg),
                ))).await;
            }
        });
        LogStream {
            _bridge: handle.abort_handle(),
        }
    }

    pub fn get_discovery(&mut self) -> anyhow::Result<()> {
        self.send_command(SessionCommand::GetDiscovery)
    }

    pub fn decode_secret(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(SessionCommand::DecodeSecret(target.clone()))
    }

    pub fn trigger_cronjob(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(SessionCommand::TriggerCronJob(target.clone()))
    }

    pub fn toggle_suspend_cronjob(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(SessionCommand::ToggleSuspendCronJob(target.clone()))
    }

    pub fn port_forward(
        &mut self,
        target: &protocol::ObjectRef,
        local_port: u16,
        container_port: u16,
    ) -> anyhow::Result<()> {
        self.send_command(SessionCommand::PortForward {
            target: target.clone(),
            local_port,
            container_port,
        })
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    pub fn send_command(&mut self, cmd: SessionCommand) -> anyhow::Result<()> {
        // try_send never blocks: if the channel is full (writer stalled),
        // surface an error so the caller sees "daemon is behind" rather
        // than silently buffering multi-MB Apply payloads. Previously the
        // channel was unbounded and queued without backpressure.
        self.cmd_tx.try_send(cmd)
            .map_err(|e| match e {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    anyhow::anyhow!("Daemon writer task is stalled — command queue full")
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    anyhow::anyhow!("Daemon writer task closed")
                }
            })
    }
}

// ---------------------------------------------------------------------------
// High-level operations
// ---------------------------------------------------------------------------

impl ClientSession {
    /// Open a per-subscription substream feeding `store`. Spawns a
    /// self-healing bridge task that:
    ///
    /// 1. Awaits the `MuxHandle` (blocks until the handshake completes —
    ///    same timing as the old `cmd_tx` queue-until-handshake pattern).
    /// 2. Opens a fresh yamux substream and writes `SubscriptionInit`.
    /// 3. Reads `StreamEvent`s and forwards them as [`AppEvent::Store`]
    ///    events CARRYING the destination store — no routing exists, and
    ///    the bridge itself never touches the store's lock (the event
    ///    loop is the sole applier).
    ///
    /// The store's epoch floor is raised with this stream's first epoch
    /// BEFORE this returns: from that instant, every event of a
    /// superseded stream targeting the same store is stale on arrival
    /// (Ctrl-R / reconnect succession can't flicker old data back).
    /// Bridge-internal retries allocate fresh (higher) epochs; their
    /// baselines set the floor on acceptance.
    ///
    /// `force` asks the server to bypass its watcher cache (Ctrl-R).
    ///
    /// Returns a `SubscriptionStream` handle. Drop it to unsubscribe —
    /// the bridge task is aborted, the substream is closed (RST sent to
    /// the daemon), and the daemon's bridge exits on EOF.
    pub fn subscribe_stream(
        &self,
        resource: protocol::ResourceId,
        namespace: protocol::Namespace,
        filter: Option<protocol::SubscriptionFilter>,
        store: std::sync::Arc<crate::app::store::RowStore>,
        force: bool,
    ) -> SubscriptionStream {
        let first_epoch = NEXT_STREAM_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        store.expect_epoch(first_epoch);
        let mut mux_rx = self.mux_handle_rx.clone();
        let event_tx = self.event_tx.clone();
        let rid = resource.clone();
        let handle = tokio::spawn(async move {
            let panic_tx = event_tx.clone();
            let panic_rid = rid.clone();
            let result = std::panic::AssertUnwindSafe(async {
            // Self-healing bridge: open substream → read events → on
            // failure, retry with backoff. Same pattern as the daemon
            // bridge's watcher retry. Natural exit: session death
            // (open fails) or event channel dropped (reconnection).
            // Give up re-probing a permanently-failing subscription after this
            // many consecutive server-side errors with no healthy snapshot in
            // between. Each retry backs off, so this bounds the self-heal window
            // (~30s) instead of churning forever on a forbidden / missing-CRD
            // view. A Snapshot resets the count.
            const SUBSCRIPTION_ERROR_CEILING: u32 = 5;

            let mut ever_received_data = false;
            let mut retry_backoff = std::time::Duration::from_secs(2);
            let bridge_start = std::time::Instant::now();
            // Consecutive `StreamEvent::Error`s since the last healthy snapshot,
            // plus the last error we actually flashed — together they drive the
            // give-up ceiling and de-dup the flash so a stable error surfaces
            // once, not once per retry.
            let mut consecutive_errors: u32 = 0;
            let mut last_flashed_error: Option<String> = None;
            // First attempt rides the epoch floored synchronously by
            // `subscribe_stream`; every retry attempt mints a fresh one.
            let mut first_epoch = Some(first_epoch);

            loop {
                // Epoch for THIS attempt — allocated up front so failure
                // paths can stamp their `Failed` payload with it (the
                // store's floor gates a superseded stream's failure away
                // from its successor's state).
                let epoch = first_epoch.take().unwrap_or_else(|| {
                    NEXT_STREAM_EPOCH.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                });
                // Re-check the watch channel each iteration so we pick up
                // a fresh MuxHandle after session reconnection.
                let mux_handle = loop {
                    if let Some(v) = mux_rx.borrow_and_update().clone() { break v; }
                    if mux_rx.changed().await.is_err() { return; }
                };

                let stream = match mux_handle.open().await {
                    Ok(s) => s,
                    Err(e) => {
                        if !ever_received_data {
                            let message = format!("substream open failed: {}", e);
                            let _ = event_tx.send(AppEvent::Store(crate::event::StoreEvent {
                                store: store.clone(),
                                epoch,
                                payload: crate::app::store::StorePayload::Failed(message.clone()),
                            })).await;
                            let _ = event_tx.send(AppEvent::SubscriptionFailed {
                                resource: rid,
                                message,
                            }).await;
                            return;
                        }
                        tracing::warn!("subscription bridge reopen failed for {}: {}", rid.plural(), e);
                        let jitter = crate::util::retry_jitter(rid.plural().as_bytes(), retry_backoff.as_millis() as u64);
                        let jittered = retry_backoff.mul_f64(jitter);
                        tokio::time::sleep(jittered).await;
                        retry_backoff = (retry_backoff * 2).min(std::time::Duration::from_secs(30));
                        continue;
                    }
                };
                let (read_half, mut write_half) = tokio::io::split(stream);
                let mut reader = tokio::io::BufReader::with_capacity(
                    protocol::IO_BUFFER_SIZE,
                    read_half,
                );

                let init = protocol::SubstreamInit::Subscribe(protocol::SubscriptionInit {
                    resource: rid.clone(),
                    namespace: namespace.clone(),
                    filter: filter.clone(),
                    force,
                });
                if let Err(e) = protocol::write_bincode(&mut write_half, &init).await {
                    if !ever_received_data {
                        let message = format!("subscription init failed: {}", e);
                        let _ = event_tx.send(AppEvent::Store(crate::event::StoreEvent {
                            store: store.clone(),
                            epoch,
                            payload: crate::app::store::StorePayload::Failed(message.clone()),
                        })).await;
                        let _ = event_tx.send(AppEvent::SubscriptionFailed {
                            resource: rid,
                            message,
                        }).await;
                        return;
                    }
                    tracing::warn!("subscription bridge init failed for {}: {}, retrying", rid.plural(), e);
                    continue;
                }

                // Bridge: read StreamEvents from the substream, convert to
                // AppEvents, forward into the main event channel.
                //
                // `current_rid` tracks the rid as the server resolved it.
                // Reset on each retry — the daemon re-resolves CRDs.
                let mut current_rid = rid.clone();
                loop {
                    let event = match protocol::read_bincode::<_, protocol::StreamEvent>(&mut reader).await {
                        Ok(event) => event,
                        Err(e) => {
                            tracing::warn!("subscription bridge read failed for {}: {}", current_rid.plural(), e);
                            break;
                        }
                    };
                    if !ever_received_data {
                        tracing::info!("subscription bridge: {} first data in {:?}",
                            rid.plural(), bridge_start.elapsed());
                    }
                    ever_received_data = true;

                    let app_event = match event {
                        protocol::StreamEvent::Baseline(baseline) => {
                            // Healthy data: the subscription works. Clear the
                            // failure ceiling + flash de-dup and reset the backoff
                            // so a *later* failure surfaces fresh.
                            consecutive_errors = 0;
                            last_flashed_error = None;
                            retry_backoff = std::time::Duration::from_secs(2);
                            AppEvent::Store(crate::event::StoreEvent {
                                store: store.clone(),
                                epoch,
                                payload: crate::app::store::StorePayload::Baseline(baseline),
                            })
                        }
                        protocol::StreamEvent::Delta(delta) => {
                            consecutive_errors = 0;
                            last_flashed_error = None;
                            retry_backoff = std::time::Duration::from_secs(2);
                            AppEvent::Store(crate::event::StoreEvent {
                                store: store.clone(),
                                epoch,
                                payload: crate::app::store::StorePayload::Delta(delta),
                            })
                        }
                        protocol::StreamEvent::Resolved { original, resolved } => {
                            // The substream is alive and the server resolved the
                            // rid — progress for backoff purposes, but not a
                            // healthy snapshot, so it does NOT clear the ceiling.
                            current_rid = resolved.clone();
                            retry_backoff = std::time::Duration::from_secs(2);
                            AppEvent::ResourceResolved { original, resolved }
                        }
                        protocol::StreamEvent::Error(msg) => {
                            // Server-side subscription error (RBAC denied, CRD
                            // missing, ...). The bridge re-probes on retry, so it
                            // self-heals once the cause is fixed — but a PERMANENT
                            // failure must not retry-and-reflash forever. Note we
                            // do NOT reset the backoff here (errors back off), and
                            // we count consecutive failures.
                            consecutive_errors += 1;
                            if consecutive_errors >= SUBSCRIPTION_ERROR_CEILING {
                                // Terminal: stop retrying this subscription, with
                                // one final flash. A fresh subscribe (Ctrl-R /
                                // nav) starts a new bridge with the count reset.
                                let message = format!(
                                    "{msg} (stopped retrying after {consecutive_errors} failures — press Ctrl-R to retry)"
                                );
                                let _ = event_tx.send(AppEvent::Store(crate::event::StoreEvent {
                                    store: store.clone(),
                                    epoch,
                                    payload: crate::app::store::StorePayload::Failed(message.clone()),
                                })).await;
                                let _ = event_tx.send(AppEvent::SubscriptionFailed {
                                    resource: current_rid.clone(),
                                    message,
                                }).await;
                                return;
                            }
                            if last_flashed_error.as_deref() == Some(msg.as_str()) {
                                // Same error as the last flash — suppress the
                                // duplicate, keep quietly retrying within the
                                // ceiling.
                                continue;
                            }
                            last_flashed_error = Some(msg.clone());
                            // The element's state flips through its OWN store
                            // (epoch-gated); the flash below is display-only.
                            let _ = event_tx.send(AppEvent::Store(crate::event::StoreEvent {
                                store: store.clone(),
                                epoch,
                                payload: crate::app::store::StorePayload::Failed(msg.clone()),
                            })).await;
                            AppEvent::SubscriptionFailed {
                                resource: current_rid.clone(),
                                message: msg,
                            }
                        }
                    };
                    if event_tx.send(app_event).await.is_err() {
                        return;
                    }
                }

                if !ever_received_data {
                    let message = String::from("substream closed before any data");
                    let _ = event_tx.send(AppEvent::Store(crate::event::StoreEvent {
                        store: store.clone(),
                        epoch,
                        payload: crate::app::store::StorePayload::Failed(message.clone()),
                    })).await;
                    let _ = event_tx.send(AppEvent::SubscriptionFailed {
                        resource: rid,
                        message,
                    }).await;
                    return;
                }

                // Transient failure after prior success — retry with
                // jittered backoff. Session death naturally exits on the
                // next open() or event_tx.send().
                let jitter = crate::util::retry_jitter(rid.plural().as_bytes(), retry_backoff.as_millis() as u64);
                let jittered = retry_backoff.mul_f64(jitter);
                tracing::info!("subscription bridge retrying for {} in {:?}", rid.plural(), jittered);
                tokio::time::sleep(jittered).await;
                retry_backoff = (retry_backoff * 2).min(std::time::Duration::from_secs(30));
            }
            });
            let result: Result<(), Box<dyn std::any::Any + Send>> = futures::FutureExt::catch_unwind(result).await;
            if let Err(panic) = result {
                let msg = if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = panic.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "unknown panic".to_string()
                };
                let _ = panic_tx.send(AppEvent::SubscriptionFailed {
                    resource: panic_rid,
                    message: format!("bridge panicked: {}", msg),
                }).await;
            }
        });
        SubscriptionStream {
            _bridge: handle.abort_handle(),
        }
    }
}

/// Collect relevant environment variables from the TUI process.
fn collect_env_vars() -> HashMap<String, String> {
    [
        "PATH",
        "HOME",
        "USER",
        "KUBECONFIG",
        "AWS_PROFILE",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_CONFIG_FILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "CLOUDSDK_CONFIG",
        "AZURE_CONFIG_DIR",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
    ]
    .iter()
    .filter_map(|k| std::env::var(k).ok().map(|v| (k.to_string(), v)))
    .collect()
}

// ---------------------------------------------------------------------------
// Background reader (binary)
// ---------------------------------------------------------------------------

/// Reads binary `SessionEvent`s from the server and converts them to
/// `AppEvent`s on the TUI event channel.
async fn reader_loop(
    mut reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    event_tx: mpsc::Sender<AppEvent>,
    context: crate::kube::protocol::ContextName,
) {
    loop {
        match protocol::read_bincode::<_, SessionEvent>(&mut reader).await {
            Ok(event) => {
                for app_event in convert_session_event(event, &context) {
                    if event_tx.send(app_event).await.is_err() {
                        debug!("ClientSession: TUI event channel closed, stopping reader");
                        return;
                    }
                }
            }
            Err(e) => {
                // EOF or deserialization error — daemon closed or crashed.
                debug!("ClientSession: read error: {}", e);
                let _ = event_tx
                    .send(AppEvent::Flash(FlashMessage::error(
                        "Lost connection to daemon",
                    )))
                    .await;
                let _ = event_tx.send(AppEvent::DaemonDisconnected).await;
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Event conversion
// ---------------------------------------------------------------------------

/// Convert a `SessionEvent` from the daemon into `AppEvent`s for the TUI.
/// With the binary protocol, Snapshot events carry typed ResourceUpdate
/// directly — no JSON parsing needed.
/// Convert a `SessionEvent` from the daemon's control substream into
/// `AppEvent`s for the TUI. Subscription-specific events (Snapshot,
/// Capabilities, Resolved, SubscriptionError) no longer arrive here —
/// they flow on per-subscription yamux substreams via `StreamEvent` and
/// are converted inside the per-subscription bridge task in
/// `subscribe_stream`.
fn convert_session_event(event: SessionEvent, current_context: &crate::kube::protocol::ContextName) -> Vec<AppEvent> {
    match event {
        SessionEvent::DescribeResult { target, lines } => {
            vec![AppEvent::ResourceUpdate(ResourceUpdate::Describe { target, lines })]
        }

        SessionEvent::YamlResult { target, content } => {
            vec![AppEvent::ResourceUpdate(ResourceUpdate::Yaml { target, content })]
        }

        // LogLine and LogEnd are gone — logs now flow on yamux substreams.
        // The bridge task in stream_log_substream forwards them.

        SessionEvent::CommandResult(result) => {
            // Surface as a typed `CommandResult` event, not `Flash` —
            // lets the edit flow react only to its own response and
            // distinguishes server responses from locally-originated
            // flash messages (batch delete, force-kill, save, etc).
            vec![AppEvent::CommandResult(result)]
        }

        SessionEvent::Discovery { context: ctx, namespaces, crds } => {
            if ctx != *current_context {
                debug!("ClientSession: discarding stale Discovery for context '{}' (current: '{}')", ctx, current_context);
                return vec![];
            }
            // Typed pass-through: the EVENT LOOP converts to rows and
            // seeds the app-level core stores — the reader task holds no
            // store Arcs (single-writer rule) and does no row work.
            vec![AppEvent::Discovery { namespaces, crds }]
        }

        SessionEvent::PodMetrics(metrics) => {
            vec![AppEvent::PodMetrics(metrics)]
        }

        SessionEvent::NodeMetrics(metrics) => {
            vec![AppEvent::NodeMetrics(metrics)]
        }

        SessionEvent::SessionError(message) => {
            vec![AppEvent::Flash(FlashMessage::error(message))]
        }

        // Ready and DaemonStatus are handled during handshake / by ctl, not in the reader loop.
        SessionEvent::Ready { .. } | SessionEvent::DaemonStatus(_) => {
            warn!("ClientSession: unexpected event after handshake");
            vec![]
        }
    }
}

#[cfg(test)]
mod stream_liveness_tests {
    use super::*;

    #[tokio::test]
    async fn is_alive_tracks_the_bridge_task() {
        // A running bridge → alive.
        let running = tokio::spawn(std::future::pending::<()>());
        let live = SubscriptionStream::from_abort_handle(running.abort_handle());
        assert!(live.is_alive());

        // A finished bridge → not alive.
        let done = tokio::spawn(async {});
        let abort = done.abort_handle();
        let _ = done.await; // ensure the task has actually completed
        let dead = SubscriptionStream::from_abort_handle(abort);
        assert!(!dead.is_alive());
    }
}

