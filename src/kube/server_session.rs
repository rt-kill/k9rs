//! Daemon-side session: one per TUI client connection.
//!
//! A `ServerSession` is a tokio task spawned by the daemon for each persistent
//! TUI connection. It reads `SessionCommand`s from the TUI, manages
//! subscriptions via the shared `WatcherCache`, and pushes `SessionEvent`s
//! back over the socket.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use k8s_openapi::api::core::v1::Namespace;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, ListParams};
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Api, Config};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::cache::CachedCrd;
use super::live_query::{QueryKey, Subscription, WatcherCache};
use super::metrics::{parse_node_metrics_usage, parse_pod_metrics_usage};
use super::protocol::{SessionCommand, SessionEvent};
use crate::event::ResourceUpdate;

// ---------------------------------------------------------------------------
// InitParams — extracted from the Init command for ergonomic access
// ---------------------------------------------------------------------------

/// Holds the fields from `SessionCommand::Init` in a flat struct so
/// `init_and_run` doesn't have to juggle a dozen local variables.
struct InitParams {
    context: Option<String>,
    namespace: Option<String>,
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
///
/// The daemon holds an `Arc<SessionSharedState>` but never accesses its
/// fields — it just passes the Arc through to each `ServerSession`.  This
/// keeps all kube-crate types out of `daemon.rs`.
pub struct SessionSharedState {
    pub(super) watcher_cache: WatcherCache,
    /// In-memory discovery cache: context name -> (namespaces, CRDs).
    /// Prevents re-LISTing namespaces and CRDs from the API on every call.
    pub discovery_cache: DashMap<String, (Vec<String>, Vec<CachedCrd>)>,
    /// Serializes client creation so concurrent sessions don't corrupt
    /// each other's process-global environment variables.
    pub client_creation_lock: tokio::sync::Mutex<()>,
}

impl SessionSharedState {
    /// Create a new, empty shared state.
    pub fn new() -> Self {
        Self {
            watcher_cache: WatcherCache::new(),
            discovery_cache: DashMap::new(),
            client_creation_lock: tokio::sync::Mutex::new(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Capacity for the per-session event channel (bridge/log tasks -> run loop).
const EVENT_CHANNEL_CAPACITY: usize = 512;

// ---------------------------------------------------------------------------
// ServerSession
// ---------------------------------------------------------------------------

/// Daemon-side session: one per persistent TUI connection.
pub struct ServerSession {
    // Socket I/O — reader is Option so `run()` can take ownership via `.take()`.
    reader: Option<BufReader<Box<dyn AsyncRead + Unpin + Send>>>,
    writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
    // Shared state from daemon (opaque to daemon.rs)
    shared: Arc<SessionSharedState>,
    // Per-session state
    client: kube::Client,
    context: String,
    namespace: String,
    readonly: bool,
    // Active subscriptions: resource_type -> (Subscription, bridge_task)
    subscriptions: HashMap<String, (Subscription, JoinHandle<()>)>,
    // Log streaming task
    log_task: Option<JoinHandle<()>>,
    // Metrics polling task
    metrics_task: Option<JoinHandle<()>>,
    // Pending client creation from a background context switch.
    // The run loop polls this and subscribes cache misses when ready.
    pending_client: Option<tokio::sync::oneshot::Receiver<(String, anyhow::Result<kube::Client>, Vec<String>)>>,
    // JoinHandle for the pending client creation task, so we can abort it on cleanup or rapid re-switch.
    pending_client_task: Option<JoinHandle<()>>,
    // Channel for bridge/log tasks to send events back to the run loop,
    // which then writes them to the TUI socket.
    event_tx: mpsc::Sender<SessionEvent>,
    event_rx: Option<mpsc::Receiver<SessionEvent>>,
}

impl ServerSession {
    /// Create a new server session from a TUI connection.
    ///
    /// The `init` command has already been parsed from the first line. The
    /// caller provides the kube::Client matching the requested context.
    pub fn new(
        reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
        writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
        shared: Arc<SessionSharedState>,
        client: kube::Client,
        context: String,
        namespace: String,
        readonly: bool,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            reader: Some(reader),
            writer,
            shared,
            client,
            context,
            namespace,
            readonly,
            subscriptions: HashMap::new(),
            log_task: None,
            metrics_task: None,
            pending_client: None,
            pending_client_task: None,
            event_tx,
            event_rx: Some(event_rx),
        }
    }

    // -----------------------------------------------------------------------
    // Init + Run (entry point called by daemon)
    // -----------------------------------------------------------------------

    /// Create and run a session from a raw connection. Handles the entire
    /// lifecycle: parses the already-read Init line, creates kube::Client
    /// from the TUI-resolved connection parameters, sends Ready, then enters
    /// the command loop.
    ///
    /// The daemon calls this after determining that the connection is a
    /// session (first line contains `"cmd":`). The daemon does NOT parse
    /// the session protocol — all protocol knowledge lives here.
    pub async fn init_and_run(
        first_line: String,
        reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
        writer: Box<dyn AsyncWrite + Unpin + Send>,
        shared: Arc<SessionSharedState>,
    ) {
        let mut buf_writer = BufWriter::with_capacity(256 * 1024, writer);

        // 1. Parse the first line as SessionCommand::Init.
        let init = match serde_json::from_str::<SessionCommand>(&first_line) {
            Ok(SessionCommand::Init {
                context,
                namespace,
                readonly,
                kubeconfig_yaml,
                env_vars,
                cluster_name,
                user_name,
            }) => InitParams {
                context,
                namespace,
                readonly,
                kubeconfig_yaml,
                env_vars,
                cluster_name,
                user_name,
            },
            Ok(_) => {
                let event = SessionEvent::SessionError {
                    message: "Expected Init command as first message".to_string(),
                };
                let _ = Self::write_event(&mut buf_writer, &event).await;
                return;
            }
            Err(e) => {
                let event = SessionEvent::SessionError {
                    message: format!("Invalid Init command: {}", e),
                };
                let _ = Self::write_event(&mut buf_writer, &event).await;
                return;
            }
        };

        info!(
            "Session init: context={:?}, namespace={:?}, readonly={}",
            init.context, init.namespace, init.readonly
        );

        // 2. Resolve context name — the TUI always sends an explicit context
        //    (resolved from kubeconfig or --context flag), so None here is an error.
        let context_name = match init.context.clone() {
            Some(c) => c,
            None => {
                let event = SessionEvent::SessionError {
                    message: "Init command missing context name".to_string(),
                };
                let _ = Self::write_event(&mut buf_writer, &event).await;
                return;
            }
        };

        // 3. Create a fresh kube::Client from the kubeconfig + env vars.
        //    Each session gets its own client — kube-rs handles token refresh.
        let client = match Self::create_client_from_init(&init, &shared).await {
            Ok(c) => {
                info!("Created kube::Client for context '{}'", context_name);
                c
            }
            Err(e) => {
                warn!("Failed to create client for context {}: {}", context_name, e);
                let event = SessionEvent::SessionError {
                    message: format!("Failed to create client: {}", e),
                };
                let _ = Self::write_event(&mut buf_writer, &event).await;
                return;
            }
        };

        // 4. Send Ready with cluster/user info from the TUI-provided fields.
        let ns = init.namespace.clone().unwrap_or_else(|| "default".to_string());
        let cluster = init.cluster_name.clone();
        let user = init.user_name.clone();

        let ready = SessionEvent::Ready {
            context: context_name.clone(),
            cluster: cluster.clone(),
            user,
            namespaces: vec![], // Discovery will populate via GetDiscovery.
        };
        if Self::write_event(&mut buf_writer, &ready).await.is_err() {
            return;
        }

        // 5. Build ServerSession and enter command loop.
        let mut session = ServerSession::new(
            reader,
            buf_writer,
            shared,
            client,
            context_name,
            ns,
            init.readonly,
        );

        // Auto-subscribe to core resources so namespace/node data flows
        // to the TUI immediately without waiting for explicit Subscribe commands.
        session.handle_subscribe("namespaces").await;
        session.handle_subscribe("nodes").await;

        // Eagerly fetch discovery (CRDs + namespaces) so autocomplete works
        // immediately, don't wait for the user to request it.
        session.handle_get_discovery_async();

        // Spawn a metrics polling task that sends pod and node metrics
        // to the TUI via SessionEvent::Metrics.
        session.spawn_metrics_poller();

        info!("Session ready, entering command loop (context={}, ns={})", session.context, session.namespace);
        if let Err(e) = session.run().await {
            info!("Session ended: {}", e);
        } else {
            info!("Session ended cleanly");
        }
    }

    /// Create a `kube::Client` from the kubeconfig YAML + env vars sent by the TUI.
    ///
    /// Sets the forwarded environment variables so exec plugins (e.g.
    /// aws-iam-authenticator, gke-gcloud-auth-plugin) can find credentials,
    /// then uses `Config::from_custom_kubeconfig()` which runs exec plugins
    /// and wraps the result with `RefreshableToken` for automatic token renewal.
    async fn create_client_from_init(
        init: &InitParams,
        shared: &SessionSharedState,
    ) -> anyhow::Result<kube::Client> {
        // Acquire the lock to serialize env var mutations across concurrent
        // sessions — std::env::set_var is process-global.
        let _guard = shared.client_creation_lock.lock().await;

        // Set forwarded env vars so exec plugins have the correct environment.
        for (key, value) in &init.env_vars {
            #[allow(unused_unsafe)]
            unsafe {
                std::env::set_var(key, value);
            }
        }

        // Parse the kubeconfig YAML back into a Kubeconfig struct.
        let kubeconfig: Kubeconfig = serde_yaml::from_str(&init.kubeconfig_yaml)
            .map_err(|e| anyhow::anyhow!("Failed to parse kubeconfig YAML: {}", e))?;

        // Determine which context to use.
        let context_name = init.context.clone()
            .or_else(|| kubeconfig.current_context.clone());

        let options = KubeConfigOptions {
            context: context_name,
            ..Default::default()
        };

        // This runs exec plugins in the current env and creates a Config with
        // RefreshableToken for automatic token renewal.
        let mut config = Config::from_custom_kubeconfig(kubeconfig, &options).await
            .map_err(|e| anyhow::anyhow!("Failed to create config from kubeconfig: {}", e))?;

        // Set generous timeouts for large clusters where LIST responses can take
        // minutes to transfer. The default is often too short and causes
        // "error reading a body from connection" on large pod/node lists.
        config.read_timeout = Some(std::time::Duration::from_secs(300));
        config.connect_timeout = Some(std::time::Duration::from_secs(30));

        let client = kube::Client::try_from(config)?;
        // _guard drops here, releasing the lock
        Ok(client)
    }

    /// Write a single SessionEvent to a BufWriter (helper for init_and_run).
    async fn write_event(
        writer: &mut BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
        event: &SessionEvent,
    ) -> anyhow::Result<()> {
        let mut buf = serde_json::to_string(event)?;
        buf.push('\n');
        writer.write_all(buf.as_bytes()).await?;
        writer.flush().await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Main loop
    // -----------------------------------------------------------------------

    /// Run the session until the TUI disconnects or an error occurs.
    ///
    /// Uses `tokio::select!` to multiplex between:
    /// - Commands arriving from the TUI socket (reader)
    /// - Events from bridge/log tasks (event_rx)
    pub async fn run(mut self) -> anyhow::Result<()> {
        debug!(
            "ServerSession started: context={}, namespace={}",
            self.context, self.namespace
        );

        // Take the reader out of self so `lines()` can consume it while
        // we still have mutable access to the rest of self for handling
        // commands and writing responses.
        let reader = self
            .reader
            .take()
            .expect("ServerSession::run called without reader");
        let mut lines = reader.lines();

        // Take the event receiver out of self so we can poll it in select.
        let mut event_rx = self
            .event_rx
            .take()
            .expect("ServerSession::run called without event_rx");

        loop {
            tokio::select! {
                // Branch 1: command from TUI socket
                line_result = lines.next_line() => {
                    let line = match line_result {
                        Ok(Some(l)) => l,
                        Ok(None) => {
                            debug!("ServerSession: TUI disconnected");
                            break;
                        }
                        Err(e) => {
                            debug!("ServerSession: socket read error: {}", e);
                            break;
                        }
                    };

                    let cmd: SessionCommand = match serde_json::from_str(&line) {
                        Ok(c) => c,
                        Err(e) => {
                            if self.send_event(&SessionEvent::SessionError {
                                message: format!("Invalid command: {}", e),
                            })
                            .await
                            .is_err() {
                                break;
                            }
                            continue;
                        }
                    };

                    if let Err(e) = self.handle_command(cmd).await {
                        warn!("ServerSession: command error: {}", e);
                        if self
                            .send_event(&SessionEvent::SessionError {
                                message: e.to_string(),
                            })
                            .await
                            .is_err()
                        {
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

                // Branch 3: background client creation completed (context switch Phase 2+3)
                result = async {
                    match self.pending_client.as_mut() {
                        Some(rx) => rx.await,
                        None => std::future::pending().await,
                    }
                } => {
                    self.pending_client = None;
                    self.pending_client_task = None;
                    match result {
                        Ok((ctx, client_result, cache_misses)) => {
                            // Guard: discard stale result if context changed since spawn
                            if ctx != self.context {
                                info!("Discarding stale client for context '{}' (current: '{}')", ctx, self.context);
                            } else {
                                match client_result {
                                    Ok(new_client) => {
                                        info!("Background client ready for context '{}'", ctx);
                                        self.client = new_client;
                                        // Subscribe to cache misses now that we have a client.
                                        for rt in cache_misses {
                                            self.handle_subscribe(&rt).await;
                                        }
                                        // Restart metrics with new client.
                                        if let Some(h) = self.metrics_task.take() { h.abort(); }
                                        self.spawn_metrics_poller();
                                        self.handle_get_discovery_async();
                                    }
                                    Err(e) => {
                                        warn!("Background client creation failed: {}", e);
                                        let _ = self.send_event(&SessionEvent::ContextSwitched {
                                            context: ctx.clone(),
                                            ok: false,
                                            message: format!("Client creation failed: {}", e),
                                        }).await;
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            // Oneshot sender dropped (task panicked or was cancelled).
                            warn!("Background client task dropped without result");
                            let _ = self.send_event(&SessionEvent::ContextSwitched {
                                context: self.context.clone(),
                                ok: false,
                                message: "Client creation task failed unexpectedly".to_string(),
                            }).await;
                        }
                    }
                }
            }
        }

        self.cleanup().await;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Command dispatch
    // -----------------------------------------------------------------------

    async fn handle_command(&mut self, cmd: SessionCommand) -> anyhow::Result<()> {
        // Log command type only — Init/SwitchContext contain credentials
        match &cmd {
            SessionCommand::Init { .. } => info!("Session cmd: Init"),
            SessionCommand::SwitchContext { context, .. } => info!("Session cmd: SwitchContext({})", context),
            other => debug!("Session cmd: {:?}", other),
        }
        match cmd {
            SessionCommand::Init { .. } => {
                // Init is handled before entering the run loop; if we get a
                // second one, ignore it.
                debug!("ServerSession: ignoring duplicate Init");
            }

            SessionCommand::Subscribe { resource_type } => {
                // During context switch, self.client is stale — defer until new client arrives.
                if self.pending_client.is_none() {
                    info!("Subscribing to '{}' (context={}, ns={})", resource_type, self.context, self.namespace);
                    self.handle_subscribe(&resource_type).await;
                }
            }

            SessionCommand::Unsubscribe { resource_type } => {
                self.handle_unsubscribe(&resource_type);
            }

            SessionCommand::SwitchNamespace { namespace } => {
                self.handle_switch_namespace(&namespace).await;
            }

            SessionCommand::SwitchContext {
                context, kubeconfig_yaml, env_vars,
                cluster_name, user_name,
            } => {
                let init = InitParams {
                    context: Some(context.clone()),
                    namespace: Some(self.namespace.clone()),
                    readonly: self.readonly,
                    kubeconfig_yaml, env_vars,
                    cluster_name: cluster_name.clone(), user_name: user_name.clone(),
                };
                self.handle_switch_context_resolved(&context, init).await?;
            }

            SessionCommand::Describe {
                resource_type,
                name,
                namespace,
            } => {
                if self.pending_client.is_some() {
                    self.send_event(&SessionEvent::DescribeResult {
                        content: "Context switch in progress...".to_string(),
                    }).await?;
                } else {
                    self.handle_describe_async(&resource_type, &name, &namespace);
                }
            }

            SessionCommand::Yaml {
                resource_type,
                name,
                namespace,
            } => {
                if self.pending_client.is_some() {
                    self.send_event(&SessionEvent::YamlResult {
                        content: "Context switch in progress...".to_string(),
                    }).await?;
                } else {
                    self.handle_yaml_async(&resource_type, &name, &namespace);
                }
            }

            SessionCommand::Delete {
                resource_type,
                name,
                namespace,
            } => {
                if self.pending_client.is_some() {
                    let tx = self.event_tx.clone();
                    tokio::spawn(async move {
                        let _ = tx.send(SessionEvent::CommandResult {
                            ok: false,
                            message: "Context switch in progress".to_string(),
                        }).await;
                    });
                } else {
                    self.handle_delete_async(&resource_type, &name, &namespace);
                }
            }

            SessionCommand::Scale {
                resource_type,
                name,
                namespace,
                replicas,
            } => {
                if self.pending_client.is_some() {
                    let tx = self.event_tx.clone();
                    tokio::spawn(async move {
                        let _ = tx.send(SessionEvent::CommandResult {
                            ok: false,
                            message: "Context switch in progress".to_string(),
                        }).await;
                    });
                } else {
                    self.handle_scale_async(&resource_type, &name, &namespace, replicas);
                }
            }

            SessionCommand::Restart {
                resource_type,
                name,
                namespace,
            } => {
                if self.pending_client.is_some() {
                    let tx = self.event_tx.clone();
                    tokio::spawn(async move {
                        let _ = tx.send(SessionEvent::CommandResult {
                            ok: false,
                            message: "Context switch in progress".to_string(),
                        }).await;
                    });
                } else {
                    self.handle_restart_async(&resource_type, &name, &namespace);
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
                    &pod, &namespace, &container, follow, tail, since, previous,
                )
                .await;
            }

            SessionCommand::Refresh { resource_type } => {
                if self.pending_client.is_none() {
                    self.handle_refresh(&resource_type).await;
                }
            }

            SessionCommand::StopLogs => {
                self.handle_stop_logs();
            }

            SessionCommand::GetDiscovery => {
                self.handle_get_discovery_async();
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Subscribe / Unsubscribe
    // -----------------------------------------------------------------------

    async fn handle_subscribe(&mut self, resource_type: &str) {
        // Already active? Check if the bridge is still running — if it finished
        // (watcher died after 30 errors), remove the stale entry so we can re-subscribe.
        if let Some((_sub, bridge)) = self.subscriptions.get(resource_type) {
            if bridge.is_finished() {
                info!("Removing dead subscription for '{}'", resource_type);
                self.subscriptions.remove(resource_type);
            } else {
                return;
            }
        }

        let key = QueryKey {
            context: self.context.clone(),
            namespace: self.namespace.clone(),
            resource_type: resource_type.to_string(),
        };

        // Dynamic CRD resources use the format "dynamic:{group}|{version}|{kind}|{plural}|{scope}"
        let sub = if let Some(dynamic_spec) = resource_type.strip_prefix("dynamic:") {
            let parts: Vec<&str> = dynamic_spec.split('|').collect();
            if parts.len() >= 5 {
                let group = parts[0];
                let version = parts[1];
                let kind = parts[2];
                let plural = parts[3];
                let scope = crate::kube::protocol::ResourceScope::from_scope_str(parts[4]);
                let gvk = kube::api::GroupVersionKind::gvk(group, version, kind);
                self.shared.watcher_cache.subscribe_dynamic(
                    key,
                    &self.client,
                    gvk,
                    plural.to_string(),
                    scope,
                )
            } else {
                warn!("ServerSession: invalid dynamic resource format: {}", resource_type);
                return;
            }
        } else {
            // Subscribe via shared WatcherCache. If the watcher is in its grace
            // period (Subscription::Drop spawned a hold task), Weak::upgrade()
            // succeeds and the existing watcher is reused.
            self.shared.watcher_cache.subscribe(key, &self.client)
        };

        let bridge = self.spawn_bridge(sub.clone(), resource_type.to_string());
        self.subscriptions
            .insert(resource_type.to_string(), (sub, bridge));
    }

    fn handle_unsubscribe(&mut self, resource_type: &str) {
        if let Some((_sub, handle)) = self.subscriptions.remove(resource_type) {
            handle.abort();
            // _sub (Subscription) is dropped here. Its Drop impl spawns a
            // hold task that keeps the watcher alive for the grace period.
        }
    }

    /// Force-refresh a resource type: kill the current watcher and start a
    /// new one that re-LISTs from the API server.
    async fn handle_refresh(&mut self, resource_type: &str) {
        // Remove the existing subscription (aborts bridge task, drops sub).
        if let Some((_sub, handle)) = self.subscriptions.remove(resource_type) {
            handle.abort();
            // _sub dropped — Drop impl spawns grace hold task.
        }

        let key = QueryKey {
            context: self.context.clone(),
            namespace: self.namespace.clone(),
            resource_type: resource_type.to_string(),
        };

        // Use subscribe_force to bypass grace-period reuse and create a fresh watcher.
        let sub = if let Some(dynamic_spec) = resource_type.strip_prefix("dynamic:") {
            // Dynamic CRDs: for now, unsubscribe + subscribe_dynamic (no force variant needed
            // because dynamic watchers are per-session and won't be grace-held).
            let parts: Vec<&str> = dynamic_spec.split('|').collect();
            if parts.len() >= 5 {
                let group = parts[0];
                let version = parts[1];
                let kind = parts[2];
                let plural = parts[3];
                let scope = crate::kube::protocol::ResourceScope::from_scope_str(parts[4]);
                let gvk = kube::api::GroupVersionKind::gvk(group, version, kind);
                // Remove existing entry so subscribe_dynamic creates a fresh watcher.
                self.shared.watcher_cache.remove(&key);
                self.shared.watcher_cache.subscribe_dynamic(
                    key,
                    &self.client,
                    gvk,
                    plural.to_string(),
                    scope,
                )
            } else {
                warn!("ServerSession: invalid dynamic resource format for refresh: {}", resource_type);
                return;
            }
        } else {
            self.shared.watcher_cache.subscribe_force(key, &self.client)
        };

        let bridge = self.spawn_bridge(sub.clone(), resource_type.to_string());
        self.subscriptions
            .insert(resource_type.to_string(), (sub, bridge));
    }

    // -----------------------------------------------------------------------
    // Namespace / context switching
    // -----------------------------------------------------------------------

    async fn handle_switch_namespace(&mut self, namespace: &str) {
        self.namespace = namespace.to_string();

        // Collect active resource types before clearing.
        let active_types: Vec<String> = self.subscriptions.keys().cloned().collect();

        // Stop any active log stream before clearing subscriptions.
        self.handle_stop_logs();

        // Drop all subscriptions (they're bound to the old namespace).
        self.stop_all();

        // Re-subscribe for the new namespace.
        for rt in active_types {
            self.handle_subscribe(&rt).await;
        }
    }

    async fn handle_switch_context_resolved(
        &mut self,
        context: &str,
        init: InitParams,
    ) -> anyhow::Result<()> {
        // Collect active resource types before clearing.
        let active_types: Vec<String> = self.subscriptions.keys().cloned().collect();

        // Stop any active log stream before clearing subscriptions.
        self.handle_stop_logs();

        // Stop metrics poller — it's bound to the old context's client.
        if let Some(h) = self.metrics_task.take() { h.abort(); }

        // Drop all subscriptions.
        self.stop_all();

        // Invalidate discovery cache for both old and new contexts.
        let old_context = self.context.clone();
        self.shared.discovery_cache.remove(&old_context);
        self.shared.discovery_cache.remove(context);

        // Update session state BEFORE subscribing — so QueryKeys use the new context.
        self.context = context.to_string();
        self.namespace = "all".to_string();

        // Phase 1: Try cache hits IMMEDIATELY (no client needed).
        // If another session already has watchers for this context, we get
        // instant data without waiting for client creation.
        let mut cache_misses: Vec<String> = Vec::new();
        let all_types: Vec<String> = {
            let mut v = vec!["namespaces".to_string(), "nodes".to_string()];
            for rt in &active_types {
                if rt != "namespaces" && rt != "nodes" {
                    v.push(rt.clone());
                }
            }
            v
        };

        for rt in &all_types {
            let key = QueryKey {
                context: self.context.clone(),
                namespace: self.namespace.clone(),
                resource_type: rt.clone(),
            };
            if let Some(sub) = self.shared.watcher_cache.try_get(&key) {
                let bridge = self.spawn_bridge(sub.clone(), rt.clone());
                self.subscriptions.insert(rt.clone(), (sub, bridge));
            } else {
                cache_misses.push(rt.clone());
            }
        }

        // Phase 2+3: Create client in background, then subscribe cache misses.
        // This avoids blocking the command loop during exec plugin execution.
        // The result is sent via a oneshot channel that the run loop polls.
        // Abort any in-flight client creation from a prior context switch.
        if let Some(h) = self.pending_client_task.take() { h.abort(); }
        let (client_tx, client_rx) = tokio::sync::oneshot::channel();
        let shared = self.shared.clone();
        let ctx = context.to_string();
        let task = tokio::spawn(async move {
            let result = Self::create_client_from_init(&init, &shared).await;
            let _ = client_tx.send((ctx, result, cache_misses));
        });
        self.pending_client = Some(client_rx);
        self.pending_client_task = Some(task);

        // Acknowledge context switch immediately — data arrives as watchers start.
        self.send_event(&SessionEvent::ContextSwitched {
            context: context.to_string(),
            ok: true,
            message: format!("Switching to context: {}", context),
        })
        .await?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Describe / YAML
    // -----------------------------------------------------------------------

    fn handle_describe_async(&self, resource_type: &str, name: &str, namespace: &str) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.clone();
        let rt = resource_type.to_string();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            let content = crate::kube::describe::fetch_describe_native(&client, &rt, &n, &ns, &context).await;
            let _ = tx.send(SessionEvent::DescribeResult { content }).await;
        });
    }

    fn handle_yaml_async(&self, resource_type: &str, name: &str, namespace: &str) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.clone();
        let rt = resource_type.to_string();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            let content = crate::kube::describe::fetch_yaml_native(&client, &rt, &n, &ns, &context).await;
            let _ = tx.send(SessionEvent::YamlResult { content }).await;
        });
    }

    // -----------------------------------------------------------------------
    // Mutating operations (delete, scale, restart)
    // -----------------------------------------------------------------------

    /// Returns true (and sends an error event) if the session is read-only.
    fn reject_if_readonly(&self) -> bool {
        if !self.readonly { return false; }
        let tx = self.event_tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(SessionEvent::CommandResult {
                ok: false,
                message: "Session is read-only".to_string(),
            }).await;
        });
        true
    }

    fn handle_delete_async(&self, resource_type: &str, name: &str, namespace: &str) {
        if self.reject_if_readonly() { return; }

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let context = self.context.clone();
        let rt = resource_type.to_string();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            let result = crate::kube::ops::execute_delete(&client, &rt, &n, &ns, &context).await;
            let event = match result {
                Ok(()) => SessionEvent::CommandResult {
                    ok: true,
                    message: format!("Deleted {}/{}", rt, n),
                },
                Err(e) => SessionEvent::CommandResult {
                    ok: false,
                    message: format!("Delete failed: {}", e),
                },
            };
            let _ = tx.send(event).await;
        });
    }

    fn handle_scale_async(&self, resource_type: &str, name: &str, namespace: &str, replicas: u32) {
        if self.reject_if_readonly() {
            return;
        }

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let rt = resource_type.to_string();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            let result = async {
                use ::kube::api::{Api, DynamicObject, Patch, PatchParams};
                use crate::kube::protocol::ResourceScope;

                let (ar, scope) =
                    crate::kube::describe::resolve_api_resource(&client, &rt).await?;
                let api: Api<DynamicObject> = match scope {
                    ResourceScope::Cluster => Api::all_with(client.clone(), &ar),
                    ResourceScope::Namespaced if ns.is_empty() => {
                        Api::all_with(client.clone(), &ar)
                    }
                    ResourceScope::Namespaced => {
                        Api::namespaced_with(client.clone(), &ns, &ar)
                    }
                };
                let patch = serde_json::json!({"spec": {"replicas": replicas}});
                api.patch(&n, &PatchParams::apply("k9rs"), &Patch::Merge(&patch))
                    .await?;
                Ok::<(), anyhow::Error>(())
            }
            .await;

            let event = match result {
                Ok(()) => SessionEvent::CommandResult {
                    ok: true,
                    message: format!("Scaled {}/{} to {} replicas", rt, n, replicas),
                },
                Err(e) => SessionEvent::CommandResult {
                    ok: false,
                    message: format!("Scale failed: {}", e),
                },
            };
            let _ = tx.send(event).await;
        });
    }

    fn handle_restart_async(&self, resource_type: &str, name: &str, namespace: &str) {
        if self.reject_if_readonly() { return; }

        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let rt = resource_type.to_string();
        let n = name.to_string();
        let ns = namespace.to_string();
        tokio::spawn(async move {
            let result = crate::kube::ops::restart_via_patch(&client, &rt, &n, &ns).await;
            let event = match result {
                Ok(()) => SessionEvent::CommandResult {
                    ok: true,
                    message: format!("Restarted {}/{}", rt, n),
                },
                Err(e) => SessionEvent::CommandResult {
                    ok: false,
                    message: format!("Restart failed: {}", e),
                },
            };
            let _ = tx.send(event).await;
        });
    }

    // -----------------------------------------------------------------------
    // Log streaming
    // -----------------------------------------------------------------------

    async fn handle_stream_logs(
        &mut self,
        pod: &str,
        namespace: &str,
        container: &str,
        follow: bool,
        tail: Option<u64>,
        since: Option<String>,
        previous: bool,
    ) {
        // Stop any existing log stream.
        self.handle_stop_logs();

        // The log task sends LogLine/LogEnd events through the shared event_tx
        // channel. The main run() loop's select! will pick them up from
        // event_rx and write them to the TUI socket.
        let log_tx = self.event_tx.clone();

        let pod = pod.to_string();
        let namespace = namespace.to_string();
        let container = container.to_string();
        let context = self.context.clone();

        // Spawn the kubectl logs process.
        let log_handle = tokio::spawn(async move {
            use tokio::io::BufReader as TokioBufReader;
            use tokio::process::Command;

            let mut cmd = Command::new("kubectl");
            cmd.arg("logs");

            if follow {
                cmd.arg("-f");
            }
            if previous {
                cmd.arg("--previous");
            }

            cmd.arg(&pod);

            if !namespace.is_empty() {
                cmd.arg("-n").arg(&namespace);
            }

            if !container.is_empty() && container != "all" {
                cmd.arg("-c").arg(&container);
            } else if container == "all" {
                cmd.arg("--all-containers=true");
            }

            if let Some(ref s) = since {
                cmd.arg(format!("--since={}", s));
            } else if let Some(t) = tail {
                cmd.arg("--tail").arg(t.to_string());
            }

            if !context.is_empty() {
                cmd.arg("--context").arg(&context);
            }

            cmd.stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .kill_on_drop(true);

            let mut child = match cmd.spawn() {
                Ok(c) => c,
                Err(e) => {
                    let _ = log_tx
                        .send(SessionEvent::SessionError {
                            message: format!("Failed to spawn kubectl logs: {}", e),
                        })
                        .await;
                    let _ = log_tx.send(SessionEvent::LogEnd).await;
                    return;
                }
            };

            if let Some(stdout) = child.stdout.take() {
                let mut lines = TokioBufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if log_tx.send(SessionEvent::LogLine { line }).await.is_err() {
                        break;
                    }
                }
            }

            let _ = log_tx.send(SessionEvent::LogEnd).await;
        });

        self.log_task = Some(log_handle);
    }

    fn handle_stop_logs(&mut self) {
        if let Some(handle) = self.log_task.take() {
            handle.abort();
        }
    }

    // -----------------------------------------------------------------------
    // Discovery
    // -----------------------------------------------------------------------

    /// Spawn discovery as a background task so it doesn't block the command loop.
    fn handle_get_discovery_async(&self) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();
        let shared = self.shared.clone();
        let context = self.context.clone();
        tokio::spawn(async move {
            // Check in-memory cache first
            if let Some(entry) = shared.discovery_cache.get(&context) {
                let (ns, crds) = entry.value().clone();
                let _ = tx.send(SessionEvent::Discovery { context: context.clone(), namespaces: ns, crds }).await;
                return; // Don't hit the API
            }

            // List namespaces.
            let ns_api: Api<Namespace> = Api::all(client.clone());
            let mut ns_ok = false;
            let namespaces: Vec<String> = match ns_api.list(&ListParams::default()).await {
                Ok(list) => { ns_ok = true; list.items.iter().filter_map(|ns| ns.metadata.name.clone()).collect() }
                Err(e) => { warn!("Discovery: failed to list namespaces: {}", e); vec![] }
            };

            // List CRDs.
            let crd_api: Api<CustomResourceDefinition> = Api::all(client);
            let mut crd_ok = false;
            let crds: Vec<CachedCrd> = match crd_api.list(&ListParams::default()).await {
                Ok(list) => { crd_ok = true; list.items.iter().filter_map(|crd| {
                    let name = crd.metadata.name.clone()?;
                    let spec = &crd.spec;
                    let version = spec.versions.iter()
                        .find(|v| v.served)
                        .or(spec.versions.first())
                        .map(|v| v.name.clone())
                        .unwrap_or_default();
                    Some(CachedCrd {
                        name,
                        kind: spec.names.kind.clone(),
                        plural: spec.names.plural.clone(),
                        group: spec.group.clone(),
                        version,
                        scope: spec.scope.clone(),
                    })
                }).collect() }
                Err(e) => { warn!("Discovery: failed to list CRDs: {}", e); vec![] }
            };

            // Only cache if at least one API call succeeded.
            // Failed calls return empty vecs — we don't want to permanently cache
            // an API failure as "no resources exist".
            if ns_ok || crd_ok {
                shared.discovery_cache.insert(context.clone(), (namespaces.clone(), crds.clone()));
            }

            let _ = tx.send(SessionEvent::Discovery { context, namespaces, crds }).await;
        });
    }

    // -----------------------------------------------------------------------
    // Metrics polling
    // -----------------------------------------------------------------------

    /// Spawn a background task that polls the metrics API every 30 seconds
    /// and sends `SessionEvent::Metrics` events through `event_tx`.
    fn spawn_metrics_poller(&mut self) {
        let client = self.client.clone();
        let tx = self.event_tx.clone();

        let handle = tokio::spawn(async move {
            // Small initial delay so core watchers can establish first.
            tokio::time::sleep(Duration::from_secs(2)).await;

            loop {
                // Fetch pod metrics.
                let pod_ar = ApiResource::from_gvk_with_plural(
                    &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics"),
                    "pods",
                );
                let pod_api: Api<DynamicObject> = Api::all_with(client.clone(), &pod_ar);
                if let Ok(list) = pod_api.list(&ListParams::default()).await {
                    let mut metrics: HashMap<String, (String, String)> = HashMap::new();
                    for item in &list.items {
                        let ns = item.metadata.namespace.clone().unwrap_or_default();
                        let name = item.metadata.name.clone().unwrap_or_default();
                        let (cpu, mem) = parse_pod_metrics_usage(&item.data);
                        metrics.insert(format!("{}/{}", ns, name), (cpu, mem));
                    }
                    if let Ok(data) = serde_json::to_string(&metrics) {
                        if tx.send(SessionEvent::PodMetrics { data }).await.is_err() {
                            break;
                        }
                    }
                }

                // Fetch node metrics.
                let node_ar = ApiResource::from_gvk_with_plural(
                    &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "NodeMetrics"),
                    "nodes",
                );
                let node_api: Api<DynamicObject> = Api::all_with(client.clone(), &node_ar);
                if let Ok(list) = node_api.list(&ListParams::default()).await {
                    let mut metrics: HashMap<String, (String, String)> = HashMap::new();
                    for item in &list.items {
                        let name = item.metadata.name.clone().unwrap_or_default();
                        let (cpu, mem) = parse_node_metrics_usage(&item.data);
                        metrics.insert(name, (cpu, mem));
                    }
                    if let Ok(data) = serde_json::to_string(&metrics) {
                        if tx.send(SessionEvent::NodeMetrics { data }).await.is_err() {
                            break;
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
        self.metrics_task = Some(handle);
    }

    // -----------------------------------------------------------------------
    // Bridge: Subscription -> SessionEvent::Snapshot
    // -----------------------------------------------------------------------

    /// Spawn a bridge task that watches a Subscription for changes, serializes
    /// each ResourceUpdate snapshot to JSON, and sends `SessionEvent::Snapshot`
    /// events through `event_tx`. The main `run()` loop receives these via
    /// `event_rx` and writes them to the TUI socket.
    fn spawn_bridge(&self, sub: Subscription, resource_type: String) -> JoinHandle<()> {
        let tx = self.event_tx.clone();
        tokio::spawn(bridge_subscription_to_events(sub, resource_type, tx))
    }

    // -----------------------------------------------------------------------
    // Socket I/O
    // -----------------------------------------------------------------------

    /// Send a SessionEvent to the TUI as a newline-delimited JSON message.
    async fn send_event(&mut self, event: &SessionEvent) -> anyhow::Result<()> {
        let mut buf = serde_json::to_string(event)?;
        buf.push('\n');
        self.writer.write_all(buf.as_bytes()).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Write a SessionEvent without flushing (for batched writes).
    async fn send_event_no_flush(&mut self, event: &SessionEvent) -> anyhow::Result<()> {
        let mut buf = serde_json::to_string(event)?;
        buf.push('\n');
        self.writer.write_all(buf.as_bytes()).await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Cleanup
    // -----------------------------------------------------------------------

    fn stop_all(&mut self) {
        for (_, (_sub, handle)) in self.subscriptions.drain() {
            handle.abort();
            // _sub (Subscription) dropped here — Drop impl handles grace.
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
// Bridge task: Subscription snapshots -> serialized events
// ---------------------------------------------------------------------------

/// Serialize a `ResourceUpdate` into a `(resource_type, json_data)` pair
/// suitable for `SessionEvent::Snapshot`.
///
/// **IMPORTANT**: This function must stay in sync with
/// `client_session::deserialize_snapshot` — the resource type strings and
/// variant names must match exactly. A roundtrip test in
/// `client_session::tests::test_serialize_deserialize_roundtrip` verifies this.
pub fn serialize_resource_update(update: &ResourceUpdate) -> Option<(String, String)> {
    // Match each variant and serialize the inner Vec to JSON.
    // The resource_type string matches the watcher key names.
    macro_rules! serialize_variant {
        ($($variant:ident => $rt:expr),+ $(,)?) => {
            match update {
                $(
                    ResourceUpdate::$variant(items) => {
                        serde_json::to_string(items).ok().map(|data| ($rt.to_string(), data))
                    }
                )+
                // Content variants are sent as dedicated SessionEvent types
                // (DescribeResult, YamlResult, LogLine), not as Snapshots.
                ResourceUpdate::Yaml(_) | ResourceUpdate::Describe(_) | ResourceUpdate::LogLine(_) => None,
            }
        };
    }

    serialize_variant!(
        Pods => "pods",
        Deployments => "deployments",
        Services => "services",
        Nodes => "nodes",
        Namespaces => "namespaces",
        ConfigMaps => "configmaps",
        Secrets => "secrets",
        StatefulSets => "statefulsets",
        DaemonSets => "daemonsets",
        Jobs => "jobs",
        CronJobs => "cronjobs",
        ReplicaSets => "replicasets",
        Ingresses => "ingresses",
        NetworkPolicies => "networkpolicies",
        ServiceAccounts => "serviceaccounts",
        StorageClasses => "storageclasses",
        Pvs => "persistentvolumes",
        Pvcs => "persistentvolumeclaims",
        Events => "events",
        Roles => "roles",
        ClusterRoles => "clusterroles",
        RoleBindings => "rolebindings",
        ClusterRoleBindings => "clusterrolebindings",
        Hpa => "horizontalpodautoscalers",
        Endpoints => "endpoints",
        LimitRanges => "limitranges",
        ResourceQuotas => "resourcequotas",
        Pdb => "poddisruptionbudgets",
        Crds => "customresourcedefinitions",
        DynamicResources => "dynamic",
    )
}

/// Bridge loop: watches a Subscription for typed snapshot changes, serializes
/// them to JSON, and sends `SessionEvent::Snapshot` events through the mpsc
/// channel. The main `run()` loop receives these and writes them to the TUI
/// socket.
async fn bridge_subscription_to_events(
    mut sub: Subscription,
    resource_type: String,
    tx: mpsc::Sender<SessionEvent>,
) {
    // Send the current snapshot immediately — don't wait for the first change.
    // This is critical when reusing an existing watcher: the data is already
    // available, and waiting for `changed()` would block until the next update.
    if let Some(update) = sub.current() {
        if let Some((rt, data)) = serialize_resource_update(&update) {
            let event = SessionEvent::Snapshot {
                resource_type: rt,
                data,
            };
            if tx.send(event).await.is_err() {
                return;
            }
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
        // None means the watcher died — stop the bridge.
        let Some(update) = sub.current() else {
            debug!(
                "ServerSession bridge: watcher died for {}",
                resource_type
            );
            break;
        };
        if let Some((rt, data)) = serialize_resource_update(&update) {
            debug!("Bridge sending snapshot for '{}' ({} bytes)", rt, data.len());
            let event = SessionEvent::Snapshot {
                resource_type: rt,
                data,
            };
            if tx.send(event).await.is_err() {
                debug!(
                    "ServerSession bridge: event channel closed for {}",
                    resource_type
                );
                break;
            }
        }
    }
}
