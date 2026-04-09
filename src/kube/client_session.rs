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
    pub context: Option<String>,
    pub namespace: Option<String>,
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
    context_name: String,
    cluster_name: String,
    user_name: String,
}

/// Result of a successful Init/Ready handshake. Returned from `do_handshake`
/// and consumed by `connection_manager` to spin up the I/O loops and notify
/// the TUI.
struct HandshakeOutcome {
    context: String,
    cluster: String,
    user: String,
    namespaces: Vec<String>,
    reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
}

/// TUI-side session handle. Three pieces, no shared mutable state, no locks:
///   - `cmd_tx`     : the only way to send commands. Unbounded so calls never
///                    block; they queue here until the writer task spawns.
///   - `context`    : a `watch` channel carrying the daemon's currently bound
///                    context. The connection manager publishes the initial
///                    value after handshake; `set_context_info` publishes
///                    new values on context switch.
///   - `_shutdown`  : held only for its `Drop` side effect. When this
///                    `oneshot::Sender` drops, the manager task's matching
///                    `Receiver` resolves with `Err`, signalling cleanup.
///                    The manager handles aborting its own children — there
///                    are no abort handles to track on this side.
pub struct ClientSession {
    cmd_tx: mpsc::UnboundedSender<SessionCommand>,
    context: watch::Sender<String>,
    _shutdown: oneshot::Sender<()>,
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

impl ClientSession {
    /// Construct a session and kick off the daemon handshake in the background.
    /// The returned session is immediately usable: command methods queue
    /// locally on an unbounded channel and are flushed to the wire as soon as
    /// the manager task spawns the writer (after a successful handshake).
    pub fn new(params: ConnectionParams, event_tx: mpsc::Sender<AppEvent>) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<SessionCommand>();
        let (context, _) = watch::channel(String::new());
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

        tokio::spawn(connection_manager(
            params,
            cmd_rx,
            event_tx,
            context.clone(),
            shutdown_rx,
        ));

        Self {
            cmd_tx,
            context,
            _shutdown: shutdown_tx,
        }
    }

    /// Read the on-disk kubeconfig and produce both the daemon Init payload
    /// (`PreparedKubeconfig`) and the full contexts list for the TUI's
    /// contexts panel — from a single disk read and a single parse.
    fn read_kubeconfig_bundle(cli_context: Option<&str>) -> anyhow::Result<KubeconfigBundle> {
        let kubeconfig = Kubeconfig::read()
            .map_err(|e| anyhow::anyhow!("Failed to read kubeconfig: {}", e))?;

        let context_name = cli_context
            .map(|s| s.to_string())
            .or_else(|| kubeconfig.current_context.clone())
            .ok_or_else(|| anyhow::anyhow!("No context specified and no current-context in kubeconfig"))?;

        let (cluster_name, user_name) = lookup_cluster_user(&kubeconfig, &context_name);

        let contexts: Vec<KubeContext> = kubeconfig.contexts.iter().map(|nc| {
            let (cluster, user) = nc.context.as_ref()
                .map(|c| (c.cluster.clone(), c.user.clone().unwrap_or_default()))
                .unwrap_or_default();
            KubeContext {
                name: nc.name.clone(),
                cluster,
                user,
                is_current: nc.name == context_name,
            }
        }).collect();

        let kubeconfig_yaml = serde_yaml::to_string(&kubeconfig)
            .map_err(|e| anyhow::anyhow!("Failed to serialize kubeconfig: {}", e))?;

        Ok(KubeconfigBundle {
            prepared: PreparedKubeconfig {
                kubeconfig_yaml,
                env_vars: collect_env_vars(),
                context_name,
                cluster_name,
                user_name,
            },
            contexts,
        })
    }

    fn prepare_kubeconfig_from_cached(
        yaml: &str,
        env: &HashMap<String, String>,
        context: Option<&str>,
    ) -> PreparedKubeconfig {
        let kubeconfig: Kubeconfig = serde_yaml::from_str(yaml).unwrap_or_default();
        let context_name = context
            .map(|s| s.to_string())
            .or_else(|| kubeconfig.current_context.clone())
            .unwrap_or_default();
        let (cluster_name, user_name) = lookup_cluster_user(&kubeconfig, &context_name);
        PreparedKubeconfig {
            kubeconfig_yaml: yaml.to_string(),
            env_vars: env.clone(),
            context_name,
            cluster_name,
            user_name,
        }
    }

    fn build_init_command(
        namespace: Option<&str>,
        readonly: bool,
        prepared: &PreparedKubeconfig,
    ) -> SessionCommand {
        SessionCommand::Init {
            context: Some(prepared.context_name.clone()),
            namespace: namespace.map(|s| s.to_string()).unwrap_or_default().into(),
            readonly,
            kubeconfig_yaml: prepared.kubeconfig_yaml.clone(),
            env_vars: prepared.env_vars.clone(),
            cluster_name: prepared.cluster_name.clone(),
            user_name: prepared.user_name.clone(),
        }
    }

}

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
    cmd_rx: mpsc::UnboundedReceiver<SessionCommand>,
    event_tx: mpsc::Sender<AppEvent>,
    context: watch::Sender<String>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let pipeline = run_connection_pipeline(params, cmd_rx, event_tx, context);
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
    cmd_rx: mpsc::UnboundedReceiver<SessionCommand>,
    event_tx: mpsc::Sender<AppEvent>,
    context: watch::Sender<String>,
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
        current_cluster: bundle.prepared.cluster_name.clone(),
        current_user: bundle.prepared.user_name.clone(),
        contexts: bundle.contexts,
        kubeconfig_yaml: bundle.prepared.kubeconfig_yaml.clone(),
        env_vars: bundle.prepared.env_vars.clone(),
    }).await;

    // Stage 2: open the transport. Real Unix socket, or in-memory duplex
    // running an in-process `ServerSession` when `--no-daemon`.
    let (reader_io, writer_io) = match open_transport(params.no_daemon).await {
        Ok(t) => t,
        Err(e) => fail!("{}", e),
    };
    let reader = BufReader::with_capacity(protocol::IO_BUFFER_SIZE, reader_io);
    let writer = BufWriter::with_capacity(protocol::IO_BUFFER_SIZE, writer_io);

    // Stage 3: Init/Ready handshake.
    let outcome = match do_handshake(
        reader, writer,
        params.namespace.as_deref(),
        params.readonly,
        &bundle.prepared,
    ).await {
        Ok(o) => o,
        Err(e) => fail!("{}", e),
    };
    // Publish the resolved context name now that the daemon has confirmed it.
    let _ = context.send(outcome.context.clone());

    // Stage 4: spawn the reader/writer loops and notify the TUI. The
    // `_io_guard` captures both abort handles; if this future is dropped
    // (e.g. shutdown raced ahead), the guard fires in its `Drop` and the
    // tasks are aborted. Otherwise we await the guarded `select!` below.
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
        cluster: outcome.cluster,
        user: outcome.user,
        namespaces: outcome.namespaces,
    }).await;

    // Stage 5: keep the I/O loops alive for as long as both are running.
    // When either dies (cmd_tx dropped → writer exits; daemon disconnects
    // → reader exits) the other is aborted by `_io_guard` going out of scope.
    tokio::select! {
        _ = writer_handle => {}
        _ = reader_handle => {}
    }
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
async fn load_kubeconfig(cli_context: Option<String>) -> anyhow::Result<KubeconfigBundle> {
    tokio::task::spawn_blocking(move || ClientSession::read_kubeconfig_bundle(cli_context.as_deref()))
        .await
        .map_err(|e| anyhow::anyhow!("kubeconfig task panicked: {}", e))?
}

/// Stage 2: open either the daemon Unix socket or an in-process duplex.
async fn open_transport(
    no_daemon: bool,
) -> anyhow::Result<(Box<dyn AsyncRead + Unpin + Send>, Box<dyn AsyncWrite + Unpin + Send>)> {
    if no_daemon {
        let server_state = Arc::new(SessionSharedState::new());
        let (client_stream, server_stream) = tokio::io::duplex(protocol::DUPLEX_BUFFER_SIZE);

        let (server_read, server_write) = tokio::io::split(server_stream);
        let server_boxed_read: Box<dyn AsyncRead + Unpin + Send> = Box::new(server_read);
        let server_boxed_write: Box<dyn AsyncWrite + Unpin + Send> = Box::new(server_write);
        tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(
                ServerSession::init_and_run(
                    BufReader::with_capacity(protocol::IO_BUFFER_SIZE, server_boxed_read),
                    server_boxed_write,
                    server_state,
                )
            );
            if let Err(e) = futures::FutureExt::catch_unwind(result).await {
                tracing::error!("ServerSession panicked: {:?}", e);
            }
        });

        let (client_read, client_write) = tokio::io::split(client_stream);
        Ok((Box::new(client_read), Box::new(client_write)))
    } else {
        let path = socket_path();
        let stream = UnixStream::connect(&path).await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to connect to daemon at {}: {}. Is the daemon running?",
                path.display(),
                e
            )
        })?;
        let (read_half, write_half) = stream.into_split();
        Ok((Box::new(read_half), Box::new(write_half)))
    }
}

/// Stage 3: send `SessionCommand::Init`, read the first `SessionEvent` and
/// require it to be `Ready`. Returns the negotiated session info plus the
/// (now-positioned) reader and writer for the I/O loops.
async fn do_handshake(
    mut reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
    mut writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
    namespace: Option<&str>,
    readonly: bool,
    prepared: &PreparedKubeconfig,
) -> anyhow::Result<HandshakeOutcome> {
    let init_cmd = ClientSession::build_init_command(namespace, readonly, prepared);
    protocol::write_bincode(&mut writer, &init_cmd).await
        .map_err(|e| anyhow::anyhow!("Failed to send Init: {}", e))?;

    let first: SessionEvent = protocol::read_bincode(&mut reader).await
        .map_err(|e| anyhow::anyhow!("Failed to read Ready: {}", e))?;

    match first {
        SessionEvent::Ready { context, cluster, user, namespaces } => {
            Ok(HandshakeOutcome { context, cluster, user, namespaces, reader, writer })
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
    mut cmd_rx: mpsc::UnboundedReceiver<SessionCommand>,
    mut writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        if protocol::write_bincode(&mut writer, &cmd).await.is_err() {
            break;
        }
    }
}

/// Look up the cluster + user names for a given context inside a parsed
/// `Kubeconfig`. Returns empty strings if the context isn't found — matches
/// what the rest of the code expects (rendered as `n/a` in the UI).
fn lookup_cluster_user(kubeconfig: &Kubeconfig, context_name: &str) -> (String, String) {
    for named_ctx in &kubeconfig.contexts {
        if named_ctx.name == context_name {
            if let Some(ref ctx) = named_ctx.context {
                return (ctx.cluster.clone(), ctx.user.clone().unwrap_or_default());
            }
            break;
        }
    }
    (String::new(), String::new())
}

// ---------------------------------------------------------------------------
// Command methods
// ---------------------------------------------------------------------------

impl ClientSession {
    pub fn switch_namespace(&mut self, namespace: &str) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::SwitchNamespace {
            namespace: protocol::Namespace::from(namespace),
        })
    }

    /// Build a `SwitchContext` command and queue it on the wire. The cached
    /// kubeconfig YAML and env vars come from the caller (App owns them, set
    /// by the `KubeconfigLoaded` event) so this method does no I/O and needs
    /// no shared state.
    pub fn switch_context(
        &mut self,
        context: &str,
        cached_yaml: &str,
        cached_env: &HashMap<String, String>,
    ) -> anyhow::Result<()> {
        let prepared = Self::prepare_kubeconfig_from_cached(cached_yaml, cached_env, Some(context));
        self.send_command(&SessionCommand::SwitchContext {
            context: context.to_string(),
            kubeconfig_yaml: prepared.kubeconfig_yaml,
            env_vars: prepared.env_vars,
            cluster_name: prepared.cluster_name,
            user_name: prepared.user_name,
        })
    }

    pub fn describe(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Describe(target.clone()))
    }

    pub fn yaml(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Yaml(target.clone()))
    }

    pub fn delete(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Delete(target.clone()))
    }

    pub fn scale(&mut self, target: &protocol::ObjectRef, replicas: u32) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Scale {
            target: target.clone(),
            replicas,
        })
    }

    pub fn restart(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Restart(target.clone()))
    }

    pub fn stream_logs(
        &mut self,
        pod: &str,
        namespace: &str,
        container: &str,
        follow: bool,
        tail: Option<u64>,
        since: Option<String>,
        previous: bool,
    ) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::StreamLogs {
            pod: pod.to_string(),
            namespace: protocol::Namespace::from(namespace),
            container: container.to_string(),
            follow,
            tail,
            since,
            previous,
        })
    }

    pub fn stop_logs(&mut self) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::StopLogs)
    }

    pub fn get_discovery(&mut self) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::GetDiscovery)
    }

    pub fn decode_secret(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::DecodeSecret(target.clone()))
    }

    pub fn trigger_cronjob(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::TriggerCronJob(target.clone()))
    }

    pub fn toggle_suspend_cronjob(&mut self, target: &protocol::ObjectRef) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::ToggleSuspendCronJob(target.clone()))
    }

    pub fn port_forward(
        &mut self,
        target: &protocol::ObjectRef,
        local_port: u16,
        container_port: u16,
    ) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::PortForward {
            target: target.clone(),
            local_port,
            container_port,
        })
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    fn send_command(&mut self, cmd: &SessionCommand) -> anyhow::Result<()> {
        // Send to the background writer task via channel — instant, never blocks.
        self.cmd_tx.send(cmd.clone())
            .map_err(|_| anyhow::anyhow!("Daemon writer task closed"))
    }
}

// ---------------------------------------------------------------------------
// High-level operations
// ---------------------------------------------------------------------------

impl ClientSession {
    /// Subscribe to a resource by its ResourceId, optionally with a server-side filter.
    pub fn subscribe_resource(&mut self, rid: &protocol::ResourceId, filter: Option<protocol::SubscriptionFilter>) {
        if let Err(e) = self.send_command(&SessionCommand::Subscribe { resource: rid.clone(), filter }) {
            tracing::warn!("ClientSession::subscribe_resource failed: {}", e);
        }
    }

    /// Unsubscribe from a resource by its ResourceId.
    pub fn unsubscribe_resource(&mut self, rid: &protocol::ResourceId) {
        if let Err(e) = self.send_command(&SessionCommand::Unsubscribe(rid.clone())) {
            tracing::warn!("ClientSession::unsubscribe_resource failed: {}", e);
        }
    }

    pub fn refresh_resource(&mut self, rid: &protocol::ResourceId) {
        if let Err(e) = self.send_command(&SessionCommand::Refresh(rid.clone())) {
            tracing::warn!("ClientSession::refresh_resource failed: {}", e);
        }
    }

    pub fn set_context_info(&self, new_context: &str) {
        let _ = self.context.send(new_context.to_string());
    }

    /// The context the daemon is currently bound to. Empty string if the
    /// initial handshake hasn't completed yet.
    pub fn context_name(&self) -> String {
        self.context.borrow().clone()
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
    initial_context: String,
) {
    let mut current_context = initial_context;

    loop {
        match protocol::read_bincode::<_, SessionEvent>(&mut reader).await {
            Ok(event) => {
                // Track context switches so we can filter stale events.
                if let SessionEvent::ContextSwitched { ref context, ok: true, .. } = event {
                    current_context = context.clone();
                }

                for app_event in convert_session_event(event, &current_context) {
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
fn convert_session_event(event: SessionEvent, current_context: &str) -> Vec<AppEvent> {
    match event {
        SessionEvent::Snapshot(update) => {
            vec![AppEvent::ResourceUpdate(update)]
        }

        SessionEvent::DescribeResult(content) => {
            vec![AppEvent::ResourceUpdate(ResourceUpdate::Describe(content))]
        }

        SessionEvent::YamlResult(content) => {
            vec![AppEvent::ResourceUpdate(ResourceUpdate::Yaml(content))]
        }

        SessionEvent::LogLine(line) => {
            let clean = crate::util::strip_ansi(&line);
            vec![AppEvent::ResourceUpdate(ResourceUpdate::LogLine(clean))]
        }

        SessionEvent::LogEnd => {
            vec![
                AppEvent::LogStreamEnded,
                AppEvent::Flash(FlashMessage::info("Log stream ended")),
            ]
        }

        SessionEvent::CommandResult { ok, message } => {
            let flash = if ok {
                FlashMessage::info(message)
            } else {
                FlashMessage::error(message)
            };
            vec![AppEvent::Flash(flash)]
        }

        SessionEvent::Discovery { context: ctx, namespaces, crds } => {
            if ctx != current_context {
                debug!("ClientSession: discarding stale Discovery for context '{}' (current: '{}')", ctx, current_context);
                return vec![];
            }
            let mut events = Vec::new();
            if !namespaces.is_empty() {
                let rows = crate::kube::cache::cached_namespaces_to_rows(&namespaces);
                let resource_id = crate::kube::protocol::ResourceId::from_alias("namespaces").unwrap();
                events.push(AppEvent::ResourceUpdate(ResourceUpdate::Rows {
                    resource: resource_id,
                    headers: vec!["NAME".into(), "STATUS".into(), "AGE".into()],
                    rows,
                }));
            }
            if !crds.is_empty() {
                let rows = crate::kube::cache::cached_crds_to_rows(&crds);
                let resource_id = crate::kube::protocol::ResourceId::from_alias("customresourcedefinitions").unwrap();
                events.push(AppEvent::ResourceUpdate(ResourceUpdate::Rows {
                    resource: resource_id,
                    headers: vec![
                        "NAME".into(), "GROUP".into(), "VERSION".into(), "KIND".into(), "SCOPE".into(), "AGE".into(),
                    ],
                    rows,
                }));
            }
            events
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

        SessionEvent::SubscriptionError { resource, message } => {
            vec![
                AppEvent::SubscriptionFailed { resource, message: message.clone() },
                AppEvent::Flash(FlashMessage::error(message)),
            ]
        }

        SessionEvent::ResourceResolved { original, resolved } => {
            vec![AppEvent::ResourceResolved { original, resolved }]
        }

        SessionEvent::ResourceCapabilities { resource, capabilities } => {
            vec![AppEvent::ResourceCapabilities { resource, capabilities }]
        }

        SessionEvent::ContextSwitched { context, ok, message } => {
            let result = if ok { Ok(()) } else { Err(message) };
            vec![AppEvent::ContextSwitchResult { context, result }]
        }

        // Ready and DaemonStatus are handled during handshake / by ctl, not in the reader loop.
        SessionEvent::Ready { .. } | SessionEvent::DaemonStatus(_) => {
            warn!("ClientSession: unexpected event after handshake");
            vec![]
        }
    }
}

