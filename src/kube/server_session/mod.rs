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

use dashmap::DashMap;
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::Config;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::live_query::{Subscription, WatcherCache};
use super::protocol::{self, ResourceId, SessionCommand, SessionEvent};


// ---------------------------------------------------------------------------
// InitParams — extracted from the Init command for ergonomic access
// ---------------------------------------------------------------------------

/// Holds the fields from `SessionCommand::Init` in a flat struct so
/// `init_and_run` doesn't have to juggle a dozen local variables.
struct InitParams {
    context: Option<String>,
    namespace: protocol::Namespace,
    readonly: bool,
    kubeconfig_yaml: String,
    env_vars: HashMap<String, String>,
    cluster_name: String,
    user_name: String,
}

// ---------------------------------------------------------------------------
// SessionSharedState — opaque to the daemon, holds kube-aware shared state
// ---------------------------------------------------------------------------

/// Shared state for all `ServerSession`s, created once at daemon startup.
pub struct SessionSharedState {
    pub(super) watcher_cache: WatcherCache,
    /// In-memory discovery cache: keyed by server_url for correct sharing.
    /// In-memory discovery cache: keyed by ContextId (server_url + user).
    pub discovery_cache: DashMap<protocol::ContextId, (Vec<String>, Vec<super::cache::CachedCrd>)>,
    /// Cached server-provided column headers per resource type.
    /// Populated on first subscription via the K8s Table API.
    pub column_cache: DashMap<protocol::ResourceId, Vec<String>>,
    /// Serializes client creation so concurrent sessions don't corrupt
    /// each other's process-global environment variables.
    pub client_creation_lock: tokio::sync::Mutex<()>,
}

impl SessionSharedState {
    pub fn new() -> Self {
        Self {
            watcher_cache: WatcherCache::new(),
            discovery_cache: DashMap::new(),
            column_cache: DashMap::new(),
            client_creation_lock: tokio::sync::Mutex::new(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const EVENT_CHANNEL_CAPACITY: usize = 512;

/// Result from a background client creation task (context switch Phase 2+3).
struct PendingClientResult {
    context_name: String,
    user_name: String,
    client: anyhow::Result<(kube::Client, kube::Config)>,
    /// Resource types that had cache misses and need subscribing with the new client.
    cache_misses: Vec<protocol::ResourceId>,
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
    namespace: protocol::Namespace,
    readonly: bool,
    subscriptions: HashMap<protocol::ResourceId, (Subscription, JoinHandle<()>)>,
    /// ResourceIds with CRD discovery tasks in flight. If the user unsubscribes
    /// before discovery completes, the ID is removed so the main loop discards
    /// the subscription when it arrives via sub_ready_rx.
    pub(super) pending_discovery: std::collections::HashSet<protocol::ResourceId>,
    log_task: Option<JoinHandle<()>>,
    metrics_task: Option<JoinHandle<()>>,
    pending_client: Option<tokio::sync::oneshot::Receiver<PendingClientResult>>,
    pending_client_task: Option<JoinHandle<()>>,
    event_tx: mpsc::Sender<SessionEvent>,
    event_rx: Option<mpsc::Receiver<SessionEvent>>,
    /// Channel for CRD discovery tasks to send back their subscriptions
    /// so the main loop can spawn bridges and track them.
    pub(super) sub_ready_tx: mpsc::Sender<(ResourceId, Subscription)>,
    sub_ready_rx: Option<mpsc::Receiver<(ResourceId, Subscription)>>,
}

impl ServerSession {
    pub fn new(
        writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
        shared: Arc<SessionSharedState>,
        client: kube::Client,
        context: protocol::ContextId,
        namespace: protocol::Namespace,
        readonly: bool,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let (sub_ready_tx, sub_ready_rx) = mpsc::channel(32);
        Self {
            writer,
            shared,
            client,
            client_config: None,
            context,
            namespace,
            readonly,
            subscriptions: HashMap::new(),
            pending_discovery: std::collections::HashSet::new(),
            log_task: None,
            metrics_task: None,
            pending_client: None,
            pending_client_task: None,
            event_tx,
            event_rx: Some(event_rx),
            sub_ready_tx,
            sub_ready_rx: Some(sub_ready_rx),
        }
    }

    // -----------------------------------------------------------------------
    // Init + Run (entry point called by daemon)
    // -----------------------------------------------------------------------

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
                kubeconfig_yaml, env_vars, cluster_name, user_name,
            }) => InitParams {
                context, namespace, readonly,
                kubeconfig_yaml, env_vars, cluster_name, user_name,
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
                kubeconfig_yaml, env_vars, cluster_name, user_name,
            } => InitParams {
                context, namespace, readonly,
                kubeconfig_yaml, env_vars, cluster_name, user_name,
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

    /// Common session setup: create client, send Ready, enter command loop.
    async fn run_session(
        init: InitParams,
        reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
        mut buf_writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
        shared: Arc<SessionSharedState>,
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
            init.user_name.clone(),
        );

        // 4. Send Ready.
        let ns = init.namespace.clone();
        let ready = SessionEvent::Ready {
            context: context_name.clone(),
            cluster: init.cluster_name.clone(),
            user: init.user_name.clone(),
            namespaces: vec![],
        };
        if protocol::write_bincode(&mut buf_writer, &ready).await.is_err() {
            return;
        }

        // 5. Build ServerSession and enter command loop.
        let mut session = ServerSession::new(
            buf_writer,
            shared,
            client,
            context_id,
            ns,
            init.readonly,
        );
        session.client_config = Some(client_config);

        // Auto-subscribe to core resources.
        session.handle_subscribe_resource(&protocol::ResourceId::from_alias("namespaces").unwrap(), false);
        session.handle_subscribe_resource(&protocol::ResourceId::from_alias("nodes").unwrap(), false);

        // Eagerly fetch discovery.
        session.handle_get_discovery_async();

        // Spawn metrics polling.
        session.spawn_metrics_poller();

        info!("Session ready, entering command loop (context={}, ns={})", session.context.name, session.namespace.display());
        if let Err(e) = session.run(reader).await {
            info!("Session ended: {}", e);
        } else {
            info!("Session ended cleanly");
        }
    }

    /// Create a `kube::Client` from the kubeconfig YAML + env vars sent by the TUI.
    async fn create_client_from_init(
        init: &InitParams,
        shared: &SessionSharedState,
    ) -> anyhow::Result<(kube::Client, kube::Config)> {
        let _guard = shared.client_creation_lock.lock().await;

        for (key, value) in &init.env_vars {
            #[allow(unused_unsafe)]
            unsafe {
                std::env::set_var(key, value);
            }
        }

        let kubeconfig: Kubeconfig = serde_yaml::from_str(&init.kubeconfig_yaml)
            .map_err(|e| anyhow::anyhow!("Failed to parse kubeconfig YAML: {}", e))?;

        let context_name = init.context.clone()
            .or_else(|| kubeconfig.current_context.clone());

        let options = KubeConfigOptions {
            context: context_name,
            ..Default::default()
        };

        let mut config = Config::from_custom_kubeconfig(kubeconfig, &options).await
            .map_err(|e| anyhow::anyhow!("Failed to create config from kubeconfig: {}", e))?;

        config.read_timeout = Some(std::time::Duration::from_secs(300));
        config.connect_timeout = Some(std::time::Duration::from_secs(30));

        let client = kube::Client::try_from(config.clone())?;
        Ok((client, config))
    }

    // -----------------------------------------------------------------------
    // Main loop
    // -----------------------------------------------------------------------

    /// Run the session until the TUI disconnects or an error occurs.
    /// Uses a spawned reader task to avoid partial-read corruption from
    /// select! cancellation.
    pub async fn run(mut self, reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>) -> anyhow::Result<()> {
        debug!(
            "ServerSession started: context={}, namespace={}",
            self.context.name, self.namespace
        );

        // Spawn a reader task that reads binary commands and sends them
        // through an mpsc channel. This avoids partial-read issues with select!.
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<SessionCommand>(32);
        let reader_handle = tokio::spawn(async move {
            binary_reader_loop(reader, cmd_tx).await;
        });

        let mut event_rx = self
            .event_rx
            .take()
            .expect("ServerSession::run called without event_rx");

        let mut sub_ready_rx = self
            .sub_ready_rx
            .take()
            .expect("ServerSession::run called without sub_ready_rx");

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

                // Branch 3: background client creation completed (context switch)
                result = async {
                    match self.pending_client.as_mut() {
                        Some(rx) => rx.await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.handle_pending_client_result(result).await;
                }

                // Branch 4: CRD discovery task completed — bridge and track if still wanted
                Some((id, sub)) = sub_ready_rx.recv() => {
                    if self.pending_discovery.remove(&id) {
                        // User still wants this subscription — bridge and track it.
                        info!("CRD subscription ready for '{}', spawning bridge", id.plural);
                        let bridge = self.spawn_bridge(sub.clone(), id.plural.clone(), id.clone());
                        self.subscriptions.insert(id, (sub, bridge));
                    } else {
                        // User unsubscribed while discovery was in flight — discard.
                        info!("Discarding stale CRD subscription for '{}' (unsubscribed during discovery)", id.plural);
                        // sub is dropped here, starting the watcher's grace period.
                    }
                }
            }
        }

        reader_handle.abort();
        self.cleanup().await;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Command dispatch
    // -----------------------------------------------------------------------

    async fn handle_command(&mut self, cmd: SessionCommand) -> anyhow::Result<()> {
        match &cmd {
            SessionCommand::Init { .. } => info!("Session cmd: Init"),
            SessionCommand::SwitchContext { context, .. } => info!("Session cmd: SwitchContext({})", context),
            other => debug!("Session cmd: {:?}", other),
        }
        match cmd {
            SessionCommand::Init { .. } => {
                debug!("ServerSession: ignoring duplicate Init");
            }

            SessionCommand::Subscribe(ref resource_id) => {
                if self.pending_client.is_none() {
                    info!("Subscribing to '{}' (context={}, ns={})", resource_id.plural, self.context.name, self.namespace);
                    self.handle_subscribe_resource(resource_id, false);
                }
            }

            SessionCommand::Unsubscribe(ref resource_id) => {
                self.handle_unsubscribe(resource_id);
            }

            SessionCommand::SwitchNamespace { namespace } => {
                self.handle_switch_namespace(namespace).await;
            }

            SessionCommand::SwitchContext {
                context, kubeconfig_yaml, env_vars,
                cluster_name, user_name,
            } => {
                let init = InitParams {
                    context: Some(context.clone()),
                    namespace: self.namespace.clone(),
                    readonly: self.readonly,
                    kubeconfig_yaml, env_vars,
                    cluster_name, user_name,
                };
                self.handle_switch_context_resolved(&context, init).await?;
            }

            SessionCommand::Describe(ref obj) => {
                if self.pending_client.is_some() {
                    self.send_event(&SessionEvent::DescribeResult(
                        "Context switch in progress...".to_string(),
                    )).await?;
                } else {
                    self.handle_describe_async(&obj.resource.plural, &obj.name, obj.namespace.display());
                }
            }

            SessionCommand::Yaml(ref obj) => {
                if self.pending_client.is_some() {
                    self.send_event(&SessionEvent::YamlResult(
                        "Context switch in progress...".to_string(),
                    )).await?;
                } else {
                    self.handle_yaml_async(&obj.resource.plural, &obj.name, obj.namespace.display());
                }
            }

            SessionCommand::Delete(ref obj) => {
                if self.pending_client.is_some() {
                    let _ = self.send_event(&SessionEvent::CommandResult {
                        ok: false,
                        message: "Context switch in progress".to_string(),
                    }).await;
                } else {
                    self.handle_delete_async(&obj.resource.plural, &obj.name, obj.namespace.display());
                }
            }

            SessionCommand::Scale { ref target, replicas } => {
                if self.pending_client.is_some() {
                    let _ = self.send_event(&SessionEvent::CommandResult {
                        ok: false,
                        message: "Context switch in progress".to_string(),
                    }).await;
                } else {
                    self.handle_scale_async(&target.resource.plural, &target.name, target.namespace.display(), replicas);
                }
            }

            SessionCommand::Restart(ref obj) => {
                if self.pending_client.is_some() {
                    let _ = self.send_event(&SessionEvent::CommandResult {
                        ok: false,
                        message: "Context switch in progress".to_string(),
                    }).await;
                } else {
                    self.handle_restart_async(&obj.resource.plural, &obj.name, obj.namespace.display());
                }
            }

            SessionCommand::StreamLogs {
                pod,
                namespace,
                container,
                follow,
                tail,
                since,
                previous,
            } => {
                self.handle_stream_logs(
                    &pod, namespace.display(), &container, follow, tail, since, previous,
                )
                .await;
            }

            SessionCommand::Refresh(ref resource_id) => {
                if self.pending_client.is_none() {
                    self.handle_subscribe_resource(resource_id, true);
                }
            }

            SessionCommand::StopLogs => {
                self.handle_stop_logs();
            }

            SessionCommand::GetDiscovery => {
                self.handle_get_discovery_async();
            }

            SessionCommand::DecodeSecret { name, namespace } => {
                if self.pending_client.is_some() {
                    let _ = self.send_event(&SessionEvent::DescribeResult(
                        "Context switch in progress...".to_string(),
                    )).await;
                } else {
                    self.handle_decode_secret_async(&name, namespace.display());
                }
            }

            SessionCommand::TriggerCronJob { name, namespace } => {
                if self.reject_if_readonly() { return Ok(()); }
                self.handle_trigger_cronjob_async(&name, namespace.display());
            }

            SessionCommand::SuspendCronJob { name, namespace, suspend } => {
                if self.reject_if_readonly() { return Ok(()); }
                self.handle_suspend_cronjob_async(&name, namespace.display(), suspend);
            }

            // Management commands should not arrive on a session connection.
            SessionCommand::Ping | SessionCommand::Status
            | SessionCommand::Shutdown | SessionCommand::Clear { .. } => {
                debug!("ServerSession: ignoring management command on session connection");
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Bridge: Subscription -> SessionEvent::Snapshot
    // -----------------------------------------------------------------------

    fn spawn_bridge(&self, sub: Subscription, resource_type: String, resource_id: protocol::ResourceId) -> JoinHandle<()> {
        let tx = self.event_tx.clone();
        tokio::spawn(bridge_subscription_to_events(sub, resource_type, resource_id, tx))
    }

    // -----------------------------------------------------------------------
    // Socket I/O (binary)
    // -----------------------------------------------------------------------

    async fn send_event(&mut self, event: &SessionEvent) -> anyhow::Result<()> {
        protocol::write_bincode(&mut self.writer, event).await
    }

    async fn send_event_no_flush(&mut self, event: &SessionEvent) -> anyhow::Result<()> {
        protocol::write_bincode_no_flush(&mut self.writer, event).await
    }

    /// Create a fresh kube::Client for a watcher. Each watcher gets its own
    /// HTTP connection so large LIST responses don't starve other watchers.
    fn watcher_client(&self) -> kube::Client {
        if let Some(ref cfg) = self.client_config {
            kube::Client::try_from(cfg.clone()).unwrap_or_else(|_| self.client.clone())
        } else {
            self.client.clone()
        }
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    fn stop_all(&mut self) {
        for (_, (_sub, handle)) in self.subscriptions.drain() {
            handle.abort();
        }
    }

    async fn cleanup(&mut self) {
        self.handle_stop_logs();
        self.stop_all();
        if let Some(h) = self.metrics_task.take() { h.abort(); }
        if let Some(h) = self.pending_client_task.take() { h.abort(); }
        debug!("ServerSession: cleaned up");
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

// ---------------------------------------------------------------------------
// Bridge task: Subscription snapshots -> typed events
// ---------------------------------------------------------------------------

/// Bridge loop: watches a Subscription for typed snapshot changes and sends
/// them directly as `SessionEvent::Snapshot(ResourceUpdate)`. No JSON
/// serialization — the ResourceUpdate enum crosses the wire via bincode.
async fn bridge_subscription_to_events(
    mut sub: Subscription,
    resource_type: String,
    resource_id: protocol::ResourceId,
    tx: mpsc::Sender<SessionEvent>,
) {
    // Send the current snapshot immediately if the watcher already has data.
    let current = sub.current();
    if current.is_some() {
        info!("Bridge '{}': sending cached snapshot immediately", resource_type);
    } else {
        info!("Bridge '{}': no cached data yet, waiting for first update", resource_type);
    }
    if let Some(update) = current {
        let event = SessionEvent::Snapshot(update);
        if tx.send(event).await.is_err() {
            return;
        }
    }

    loop {
        if sub.changed().await.is_err() {
            debug!(
                "ServerSession bridge: subscription closed for {}",
                resource_type
            );
            break;
        }
        let Some(update) = sub.current() else {
            warn!(
                "ServerSession bridge: watcher died for {}",
                resource_type
            );
            let _ = tx.send(SessionEvent::SubscriptionError {
                resource: resource_id,
                message: format!("Watcher failed for {}", resource_type),
            }).await;
            break;
        };
        debug!("Bridge sending snapshot for '{}'", resource_type);
        let event = SessionEvent::Snapshot(update);
        if tx.send(event).await.is_err() {
            debug!(
                "ServerSession bridge: event channel closed for {}",
                resource_type
            );
            break;
        }
    }
}
