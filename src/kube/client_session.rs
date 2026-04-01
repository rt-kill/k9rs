//! TUI-side session: connects to a `ServerSession` over a Unix socket (daemon
//! mode) or an in-memory duplex stream (`--no-daemon` mode).
//!
//! `ClientSession` is the counterpart of `ServerSession`. It sends
//! `SessionCommand`s and converts incoming `SessionEvent`s into `AppEvent`s
//! that the existing TUI event loop can process.

use std::collections::HashMap;
use std::sync::Arc;

use kube::config::Kubeconfig;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::daemon::socket_path;
use super::protocol::{SessionCommand, SessionEvent};
use super::server_session::{ServerSession, SessionSharedState};
use crate::app::{FlashMessage, ResourceTab};
use crate::event::{AppEvent, ResourceUpdate};
use crate::kube::resources::{
    configmaps::KubeConfigMap,
    crds::{DynamicKubeResource, KubeCrd},
    cronjobs::KubeCronJob,
    daemonsets::KubeDaemonSet,
    deployments::KubeDeployment,
    endpoints::KubeEndpoints,
    events::KubeEvent,
    hpa::KubeHpa,
    ingress::KubeIngress,
    jobs::KubeJob,
    limitranges::KubeLimitRange,
    namespaces::KubeNamespace,
    networkpolicies::KubeNetworkPolicy,
    nodes::KubeNode,
    pdb::KubePdb,
    pods::KubePod,
    pvcs::KubePvc,
    pvs::KubePv,
    rbac::{KubeClusterRole, KubeClusterRoleBinding, KubeRole, KubeRoleBinding},
    replicasets::KubeReplicaSet,
    resourcequotas::KubeResourceQuota,
    secrets::KubeSecret,
    serviceaccounts::KubeServiceAccount,
    services::KubeService,
    statefulsets::KubeStatefulSet,
    storageclasses::KubeStorageClass,
};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Information returned by the daemon when a session is established.
#[derive(Debug, Clone)]
pub struct SessionReadyInfo {
    pub context: String,
    pub cluster: String,
    pub user: String,
    pub namespaces: Vec<String>,
}

/// Kubeconfig + env vars prepared by the TUI for sending to the session.
///
/// The TUI reads the kubeconfig from disk, serializes it to YAML, and
/// collects relevant environment variables. The session creates the
/// kube::Client from this, running exec plugins itself.
struct PreparedKubeconfig {
    kubeconfig_yaml: String,
    env_vars: HashMap<String, String>,
    cluster_name: String,
    user_name: String,
}

/// TUI-side session handle for communicating with a `ServerSession`.
///
/// Owns the write half of the transport (Unix socket or in-memory duplex) and
/// a background reader task that converts `SessionEvent`s into `AppEvent`s on
/// the TUI event channel.
pub struct ClientSession {
    writer: BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
    _reader_task: JoinHandle<()>,
    /// Tracks the current tab subscription so we can unsubscribe on switch.
    current_tab_rt: Option<String>,
    /// Context name received from the server's Ready event.
    context: String,
    /// Cached kubeconfig YAML + env vars from initial startup read.
    /// Avoids re-reading from disk on every context switch.
    cached_kubeconfig: Option<(String, HashMap<String, String>)>,
}

impl Drop for ClientSession {
    fn drop(&mut self) {
        self._reader_task.abort();
    }
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

impl ClientSession {
    /// Connect to the daemon, perform the Init handshake, and spawn the
    /// background reader loop.
    ///
    /// Returns the session handle together with `SessionReadyInfo` containing
    /// the resolved context, cluster, user, and initial namespace list.
    pub async fn connect(
        context: Option<&str>,
        namespace: Option<&str>,
        readonly: bool,
        event_tx: mpsc::Sender<AppEvent>,
    ) -> anyhow::Result<(Self, SessionReadyInfo)> {
        // 1. Connect to daemon socket.
        let path = socket_path();
        let stream = UnixStream::connect(&path).await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to connect to daemon at {}: {}. Is the daemon running?",
                path.display(),
                e
            )
        })?;

        let (read_half, write_half) = stream.into_split();
        let boxed_read: Box<dyn AsyncRead + Unpin + Send> = Box::new(read_half);
        let boxed_write: Box<dyn AsyncWrite + Unpin + Send> = Box::new(write_half);

        let prepared = Self::prepare_kubeconfig(context)?;
        let cached = (prepared.kubeconfig_yaml.clone(), prepared.env_vars.clone());
        let (mut cs, info) = Self::handshake(boxed_read, boxed_write, context, namespace, readonly, prepared, event_tx).await?;
        cs.cached_kubeconfig = Some(cached);
        Ok((cs, info))
    }

    /// Connect locally without a daemon: creates an in-memory duplex stream
    /// and spawns a `ServerSession` as a local tokio task.
    ///
    /// The TUI gets the exact same session protocol as daemon mode — the only
    /// difference is the transport (in-memory bytes vs Unix socket).
    pub async fn connect_local(
        context: Option<&str>,
        namespace: Option<&str>,
        readonly: bool,
        event_tx: mpsc::Sender<AppEvent>,
    ) -> anyhow::Result<(Self, SessionReadyInfo)> {
        // 1. Create SessionSharedState (same as daemon startup).
        let shared = Arc::new(SessionSharedState::new());

        // 2. Read kubeconfig from disk and collect env vars.
        let prepared = Self::prepare_kubeconfig(context)?;

        // 3. Create an in-memory bidirectional byte stream.
        let (client_stream, server_stream) = tokio::io::duplex(4 * 1024 * 1024);

        // 4. Prepare the Init command to send as the first line (ServerSession
        //    expects to parse it from `first_line`).
        let init_cmd = Self::build_init_command(context, namespace, readonly, &prepared);
        let first_line = serde_json::to_string(&init_cmd)?;

        // 5. Spawn ServerSession on the server end.
        let (server_read, server_write) = tokio::io::split(server_stream);
        let server_boxed_read: Box<dyn AsyncRead + Unpin + Send> = Box::new(server_read);
        let server_boxed_write: Box<dyn AsyncWrite + Unpin + Send> = Box::new(server_write);
        tokio::spawn(async move {
            ServerSession::init_and_run(
                first_line,
                BufReader::with_capacity(256 * 1024, server_boxed_read),
                server_boxed_write,
                shared,
            )
            .await;
        });

        // 6. Create ClientSession on the client end.
        let (client_read, client_write) = tokio::io::split(client_stream);
        let boxed_read: Box<dyn AsyncRead + Unpin + Send> = Box::new(client_read);
        let boxed_write: Box<dyn AsyncWrite + Unpin + Send> = Box::new(client_write);

        // Don't send Init again — ServerSession already has it via first_line.
        // Just read the Ready response and spawn the reader loop.
        let cached = (prepared.kubeconfig_yaml.clone(), prepared.env_vars.clone());
        let (mut cs, info) = Self::handshake_no_init(boxed_read, boxed_write, event_tx).await?;
        cs.cached_kubeconfig = Some(cached);
        Ok((cs, info))
    }

    /// Read the kubeconfig from disk, serialize to YAML, and collect env vars.
    ///
    /// The session (daemon-side) will create the kube::Client from this,
    /// running exec plugins itself with the forwarded env vars.
    fn prepare_kubeconfig(
        context: Option<&str>,
    ) -> anyhow::Result<PreparedKubeconfig> {
        let kubeconfig = Kubeconfig::read()
            .map_err(|e| anyhow::anyhow!("Failed to read kubeconfig: {}", e))?;

        // Determine context name.
        let context_name = context
            .map(|s| s.to_string())
            .or_else(|| kubeconfig.current_context.clone())
            .ok_or_else(|| anyhow::anyhow!("No context specified and no current-context in kubeconfig"))?;

        // Look up cluster/user display names from the kubeconfig contexts.
        let (cluster_name, user_name) = {
            let mut found = (String::new(), String::new());
            for named_ctx in &kubeconfig.contexts {
                if named_ctx.name == context_name {
                    if let Some(ref ctx) = named_ctx.context {
                        found = (
                            ctx.cluster.clone(),
                            ctx.user.clone().unwrap_or_default(),
                        );
                    }
                    break;
                }
            }
            found
        };

        // Serialize the kubeconfig to YAML for sending to the session.
        let kubeconfig_yaml = serde_yaml::to_string(&kubeconfig)
            .map_err(|e| anyhow::anyhow!("Failed to serialize kubeconfig: {}", e))?;

        // Collect relevant environment variables.
        let env_vars = collect_env_vars();

        Ok(PreparedKubeconfig {
            kubeconfig_yaml,
            env_vars,
            cluster_name,
            user_name,
        })
    }

    /// Build a PreparedKubeconfig from already-cached YAML + env vars.
    /// Avoids re-reading from disk — only parses the YAML for context lookup.
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
        let (cluster_name, user_name) = {
            let mut found = (String::new(), String::new());
            for named_ctx in &kubeconfig.contexts {
                if named_ctx.name == context_name {
                    if let Some(ref ctx) = named_ctx.context {
                        found = (ctx.cluster.clone(), ctx.user.clone().unwrap_or_default());
                    }
                    break;
                }
            }
            found
        };
        PreparedKubeconfig {
            kubeconfig_yaml: yaml.to_string(),
            env_vars: env.clone(),
            cluster_name,
            user_name,
        }
    }

    /// Build the Init command from prepared kubeconfig fields.
    fn build_init_command(
        context: Option<&str>,
        namespace: Option<&str>,
        readonly: bool,
        prepared: &PreparedKubeconfig,
    ) -> SessionCommand {
        SessionCommand::Init {
            context: context.map(|s| s.to_string()),
            namespace: namespace.map(|s| s.to_string()),
            readonly,
            kubeconfig_yaml: prepared.kubeconfig_yaml.clone(),
            env_vars: prepared.env_vars.clone(),
            cluster_name: prepared.cluster_name.clone(),
            user_name: prepared.user_name.clone(),
        }
    }

    /// Common handshake: send Init, read Ready, spawn reader loop.
    async fn handshake(
        read: Box<dyn AsyncRead + Unpin + Send>,
        write: Box<dyn AsyncWrite + Unpin + Send>,
        context: Option<&str>,
        namespace: Option<&str>,
        readonly: bool,
        prepared: PreparedKubeconfig,
        event_tx: mpsc::Sender<AppEvent>,
    ) -> anyhow::Result<(Self, SessionReadyInfo)> {
        let reader = BufReader::with_capacity(256 * 1024, read);
        let mut writer = BufWriter::with_capacity(256 * 1024, write);

        // Send SessionCommand::Init with the kubeconfig + env vars.
        let init_cmd = Self::build_init_command(context, namespace, readonly, &prepared);
        send_command_raw(&mut writer, &init_cmd).await?;

        // Read the first line — must be SessionEvent::Ready (or SessionError).
        let (ready_info, reader_task) = Self::read_ready_and_spawn(reader, event_tx.clone()).await?;

        Ok((
            Self {
                writer,
                _reader_task: reader_task,
                current_tab_rt: None,
                context: ready_info.context.clone(),
                cached_kubeconfig: None,
            },
            ready_info,
        ))
    }

    /// Handshake for local mode: Init was already passed to ServerSession,
    /// so just read the Ready response and spawn the reader loop.
    async fn handshake_no_init(
        read: Box<dyn AsyncRead + Unpin + Send>,
        write: Box<dyn AsyncWrite + Unpin + Send>,
        event_tx: mpsc::Sender<AppEvent>,
    ) -> anyhow::Result<(Self, SessionReadyInfo)> {
        let reader = BufReader::with_capacity(256 * 1024, read);
        let writer = BufWriter::with_capacity(256 * 1024, write);

        let (ready_info, reader_task) = Self::read_ready_and_spawn(reader, event_tx.clone()).await?;

        Ok((
            Self {
                writer,
                _reader_task: reader_task,
                current_tab_rt: None,
                context: ready_info.context.clone(),
                cached_kubeconfig: None,
            },
            ready_info,
        ))
    }

    /// Read the Ready event from the server and spawn the background reader loop.
    async fn read_ready_and_spawn(
        mut reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>,
        event_tx: mpsc::Sender<AppEvent>,
    ) -> anyhow::Result<(SessionReadyInfo, JoinHandle<()>)> {
        let mut first_line = String::new();
        let n = reader.read_line(&mut first_line).await?;
        if n == 0 {
            anyhow::bail!("Server closed connection before sending Ready event");
        }

        let first_event: SessionEvent = serde_json::from_str(first_line.trim()).map_err(|e| {
            anyhow::anyhow!("Failed to parse server response: {} — raw: {}", e, first_line.trim())
        })?;

        let ready_info = match first_event {
            SessionEvent::Ready {
                context: ctx,
                cluster,
                user,
                namespaces,
            } => SessionReadyInfo {
                context: ctx,
                cluster,
                user,
                namespaces,
            },
            SessionEvent::SessionError { message } => {
                anyhow::bail!("Server rejected session: {}", message);
            }
            other => {
                anyhow::bail!(
                    "Expected Ready or SessionError from server, got: {:?}",
                    other
                );
            }
        };

        let reader_tx = event_tx;
        let initial_ctx = ready_info.context.clone();
        let reader_task = tokio::spawn(async move {
            reader_loop(reader, reader_tx, initial_ctx).await;
        });

        Ok((ready_info, reader_task))
    }
}

// ---------------------------------------------------------------------------
// Command methods
// ---------------------------------------------------------------------------

impl ClientSession {
    /// Subscribe to a resource type (starts/reuses a watcher on the daemon).
    pub async fn subscribe(&mut self, resource_type: &str) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Subscribe {
            resource_type: resource_type.to_string(),
        })
        .await
    }

    /// Unsubscribe from a resource type.
    pub async fn unsubscribe(&mut self, resource_type: &str) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Unsubscribe {
            resource_type: resource_type.to_string(),
        })
        .await
    }

    /// Force-refresh a resource type (kills watcher, re-LISTs from API server).
    pub async fn refresh(&mut self, resource_type: &str) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Refresh {
            resource_type: resource_type.to_string(),
        })
        .await
    }

    /// Switch the active namespace (daemon re-subscribes all active watchers).
    pub async fn switch_namespace(&mut self, namespace: &str) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::SwitchNamespace {
            namespace: namespace.to_string(),
        })
        .await
    }

    /// Switch the active context (daemon creates a new kube::Client).
    pub async fn switch_context(&mut self, context: &str) -> anyhow::Result<()> {
        // Use cached kubeconfig if available (avoids sync file I/O on the TUI thread).
        let prepared = if let Some((ref yaml, ref env)) = self.cached_kubeconfig {
            Self::prepare_kubeconfig_from_cached(yaml, env, Some(context))
        } else {
            let p = Self::prepare_kubeconfig(Some(context))?;
            self.cached_kubeconfig = Some((p.kubeconfig_yaml.clone(), p.env_vars.clone()));
            p
        };
        self.send_command(&SessionCommand::SwitchContext {
            context: context.to_string(),
            kubeconfig_yaml: prepared.kubeconfig_yaml,
            env_vars: prepared.env_vars,
            cluster_name: prepared.cluster_name,
            user_name: prepared.user_name,
        })
        .await
    }

    /// Request describe output for a resource.
    pub async fn describe(
        &mut self,
        resource_type: &str,
        name: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Describe {
            resource_type: resource_type.to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
        })
        .await
    }

    /// Request YAML output for a resource.
    pub async fn yaml(
        &mut self,
        resource_type: &str,
        name: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Yaml {
            resource_type: resource_type.to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
        })
        .await
    }

    /// Delete a resource.
    pub async fn delete(
        &mut self,
        resource_type: &str,
        name: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Delete {
            resource_type: resource_type.to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
        })
        .await
    }

    /// Scale a resource.
    pub async fn scale(
        &mut self,
        resource_type: &str,
        name: &str,
        namespace: &str,
        replicas: u32,
    ) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Scale {
            resource_type: resource_type.to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
            replicas,
        })
        .await
    }

    /// Restart a resource (rolling restart via annotation patch).
    pub async fn restart(
        &mut self,
        resource_type: &str,
        name: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Restart {
            resource_type: resource_type.to_string(),
            name: name.to_string(),
            namespace: namespace.to_string(),
        })
        .await
    }

    /// Start streaming logs for a pod.
    pub async fn stream_logs(
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
            namespace: namespace.to_string(),
            container: container.to_string(),
            follow,
            tail,
            since,
            previous,
        })
        .await
    }

    /// Stop log streaming.
    pub async fn stop_logs(&mut self) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::StopLogs).await
    }

    /// Request discovery data (namespaces + CRDs).
    pub async fn get_discovery(&mut self) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::GetDiscovery).await
    }



    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    async fn send_command(&mut self, cmd: &SessionCommand) -> anyhow::Result<()> {
        send_command_raw(&mut self.writer, cmd).await
    }
}

// ---------------------------------------------------------------------------
// High-level operations (replaces DataSource)
// ---------------------------------------------------------------------------

use crate::kube::protocol::ResourceScope;

impl ClientSession {
    /// Switch the active resource tab.
    pub async fn switch_tab(&mut self, tab: ResourceTab) {
        // For DynamicResource, the actual resource key is stored in current_tab_rt
        // from a prior watch_dynamic() call. Don't overwrite it with "dynamic".
        if tab == ResourceTab::DynamicResource {
            if let Some(ref existing) = self.current_tab_rt {
                if existing.starts_with("dynamic:") {
                    return; // Already watching the correct dynamic resource
                }
            }
            return; // No dynamic spec stored — nothing to subscribe to
        }

        let rt = tab_resource_type(tab).to_string();
        // Unsubscribe from the previous tab's resource type (if any)
        // but never unsubscribe from core resources.
        let old_rt = self.current_tab_rt.take();
        if let Some(ref old) = old_rt {
            if old != &rt && old != "namespaces" && old != "nodes" {
                let _ = self.unsubscribe(old).await;
            }
        }
        if let Err(e) = self.subscribe(&rt).await {
            tracing::warn!("ClientSession::switch_tab subscribe failed: {}", e);
        }
        self.current_tab_rt = Some(rt);
    }

    /// Force-refresh the active tab.
    pub async fn refresh_tab(&mut self, tab: ResourceTab) {
        let rt = tab_resource_type(tab);
        let _ = self.refresh(rt).await;
    }

    /// Start watching a dynamic CRD resource type.
    pub async fn watch_dynamic(
        &mut self,
        group: String,
        version: String,
        kind: String,
        plural: String,
        scope: ResourceScope,
    ) {
        let scope_str = match scope {
            ResourceScope::Namespaced => "Namespaced",
            ResourceScope::Cluster => "Cluster",
        };
        let rt = format!("dynamic:{}|{}|{}|{}|{}", group, version, kind, plural, scope_str);
        // Unsubscribe from the previous tab's resource type (if any).
        let old_rt = self.current_tab_rt.take();
        if let Some(ref old) = old_rt {
            if old != &rt && old != "namespaces" && old != "nodes" {
                let _ = self.unsubscribe(old).await;
            }
        }
        if let Err(e) = self.subscribe(&rt).await {
            tracing::warn!("ClientSession::watch_dynamic subscribe failed: {}", e);
        }
        self.current_tab_rt = Some(rt);
    }

    /// Update stored context name.
    pub fn set_context_info(&mut self, new_context: &str) {
        self.context = new_context.to_string();
    }

    /// Persist discovery data. The server manages its own cache.
    pub fn put_discovery(
        &self,
        namespaces: &[String],
        crds: &[crate::kube::cache::CachedCrd],
    ) {
        // Write to disk cache so namespace/CRD autocomplete persists across restarts.
        let cache_key = crate::kube::cache::cache_key(&self.context, "");
        let dc = crate::kube::cache::build_cache(&cache_key, namespaces, crds);
        tokio::spawn(async move {
            crate::kube::cache::save_cache(&dc).await;
        });
    }

    /// Returns the current context name.
    pub fn context_name(&self) -> &str {
        &self.context
    }


}

/// Collect relevant environment variables from the TUI process to forward
/// to the session so exec plugins (aws-iam-authenticator, gke-gcloud-auth-plugin,
/// etc.) can run correctly.
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

/// Serialize and write a `SessionCommand` as a newline-delimited JSON line.
async fn send_command_raw(
    writer: &mut BufWriter<Box<dyn AsyncWrite + Unpin + Send>>,
    cmd: &SessionCommand,
) -> anyhow::Result<()> {
    let mut buf = serde_json::to_string(cmd)?;
    buf.push('\n');
    writer.write_all(buf.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Background reader
// ---------------------------------------------------------------------------

/// Reads `SessionEvent`s from the server and converts them to `AppEvent`s
/// on the TUI event channel.
async fn reader_loop(mut reader: BufReader<Box<dyn AsyncRead + Unpin + Send>>, event_tx: mpsc::Sender<AppEvent>, initial_context: String) {
    let mut line_buf = String::with_capacity(256 * 1024);
    let mut current_context = initial_context;

    loop {
        line_buf.clear();
        match reader.read_line(&mut line_buf).await {
            Ok(0) => {
                // EOF — daemon closed the connection.
                debug!("ClientSession: daemon closed connection");
                let _ = event_tx
                    .send(AppEvent::Flash(FlashMessage::error(
                        "Lost connection to daemon",
                    )))
                    .await;
                let _ = event_tx.send(AppEvent::DaemonDisconnected).await;
                break;
            }
            Ok(_) => {
                let trimmed = line_buf.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let event: SessionEvent = match serde_json::from_str(trimmed) {
                    Ok(e) => e,
                    Err(e) => {
                        warn!(
                            "ClientSession: failed to parse event: {} — raw: {}",
                            e, trimmed
                        );
                        continue;
                    }
                };

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
                warn!("ClientSession: read error: {}", e);
                let _ = event_tx
                    .send(AppEvent::Flash(FlashMessage::error(format!(
                        "Daemon read error: {}",
                        e
                    ))))
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

/// Convert a `SessionEvent` from the daemon into an `AppEvent` for the TUI.
fn convert_session_event(event: SessionEvent, current_context: &str) -> Vec<AppEvent> {
    match event {
        SessionEvent::Snapshot {
            resource_type,
            data,
        } => deserialize_snapshot(&resource_type, &data)
            .map(|u| vec![AppEvent::ResourceUpdate(u)])
            .unwrap_or_default(),

        SessionEvent::DescribeResult { content } => {
            vec![AppEvent::ResourceUpdate(ResourceUpdate::Describe(content))]
        }

        SessionEvent::YamlResult { content } => {
            vec![AppEvent::ResourceUpdate(ResourceUpdate::Yaml(content))]
        }

        SessionEvent::LogLine { line } => {
            vec![AppEvent::ResourceUpdate(ResourceUpdate::LogLine(line))]
        }

        SessionEvent::LogEnd => {
            // Signal the TUI that the log stream has ended and reset streaming.
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
            // Discard stale discovery from a previous context.
            if ctx != current_context {
                debug!("ClientSession: discarding stale Discovery for context '{}' (current: '{}')", ctx, current_context);
                return vec![];
            }
            let mut events = Vec::new();
            if !namespaces.is_empty() {
                let ns_items: Vec<KubeNamespace> = namespaces
                    .into_iter()
                    .map(|n| KubeNamespace {
                        name: n,
                        status: "Active".to_string(),
                        age: None,
                        labels: Default::default(),
                    })
                    .collect();
                events.push(AppEvent::ResourceUpdate(ResourceUpdate::Namespaces(ns_items)));
            }
            if !crds.is_empty() {
                let domain_crds: Vec<KubeCrd> = crds
                    .into_iter()
                    .map(|c| KubeCrd {
                        name: c.name.clone(),
                        group: c.group.clone(),
                        version: c.version.clone(),
                        kind: c.kind.clone(),
                        plural: c.plural.clone(),
                        scope: c.scope.to_string(),
                        age: None,
                    })
                    .collect();
                events.push(AppEvent::ResourceUpdate(ResourceUpdate::Crds(domain_crds)));
            }
            events
        }

        SessionEvent::PodMetrics { data } => {
            if let Ok(raw) = serde_json::from_str::<std::collections::HashMap<String, (String, String)>>(&data) {
                let pod_metrics: std::collections::HashMap<(String, String), (String, String)> =
                    raw.into_iter()
                        .filter_map(|(key, val)| {
                            let (ns, name) = key.split_once('/')?;
                            Some(((ns.to_string(), name.to_string()), val))
                        })
                        .collect();
                vec![AppEvent::PodMetrics(pod_metrics)]
            } else {
                vec![]
            }
        }
        SessionEvent::NodeMetrics { data } => {
            if let Ok(node_metrics) = serde_json::from_str::<std::collections::HashMap<String, (String, String)>>(&data) {
                vec![AppEvent::NodeMetrics(node_metrics)]
            } else {
                vec![]
            }
        }

        SessionEvent::SessionError { message } => {
            vec![AppEvent::Flash(FlashMessage::error(message))]
        }

        SessionEvent::ContextSwitched { context, ok, message } => {
            let result = if ok { Ok(()) } else { Err(message) };
            vec![AppEvent::ContextSwitchResult { context, result }]
        }

        // Ready is handled during the connect handshake, not in the reader loop.
        SessionEvent::Ready { .. } => {
            warn!("ClientSession: unexpected Ready event after handshake");
            vec![]
        }
    }
}

// ---------------------------------------------------------------------------
// Snapshot deserialization
// ---------------------------------------------------------------------------

/// Deserialize a JSON snapshot string into the appropriate `ResourceUpdate`
/// variant based on the resource type key.
///
/// This is the ONE place we deserialize typed resource data on the TUI side —
/// it crosses a process boundary from the daemon.
///
/// **IMPORTANT**: This function must stay in sync with
/// `server_session::serialize_resource_update` — the resource type strings and
/// variant names must match exactly. A roundtrip test in
/// `tests::test_serialize_deserialize_roundtrip` below verifies this.
pub fn deserialize_snapshot(resource_type: &str, data: &str) -> Option<ResourceUpdate> {
    // Macro to reduce boilerplate: try to deserialize as Vec<T> and wrap in
    // the given ResourceUpdate variant.
    macro_rules! deser {
        ($variant:ident, $ty:ty) => {
            match serde_json::from_str::<Vec<$ty>>(data) {
                Ok(items) => Some(ResourceUpdate::$variant(items)),
                Err(e) => {
                    warn!(
                        "ClientSession: failed to deserialize {} snapshot: {}",
                        resource_type, e
                    );
                    None
                }
            }
        };
    }

    match resource_type {
        "pods" => deser!(Pods, KubePod),
        "deployments" => deser!(Deployments, KubeDeployment),
        "services" => deser!(Services, KubeService),
        "nodes" => deser!(Nodes, KubeNode),
        "namespaces" => deser!(Namespaces, KubeNamespace),
        "configmaps" => deser!(ConfigMaps, KubeConfigMap),
        "secrets" => deser!(Secrets, KubeSecret),
        "statefulsets" => deser!(StatefulSets, KubeStatefulSet),
        "daemonsets" => deser!(DaemonSets, KubeDaemonSet),
        "jobs" => deser!(Jobs, KubeJob),
        "cronjobs" => deser!(CronJobs, KubeCronJob),
        "replicasets" => deser!(ReplicaSets, KubeReplicaSet),
        "ingresses" => deser!(Ingresses, KubeIngress),
        "networkpolicies" => deser!(NetworkPolicies, KubeNetworkPolicy),
        "serviceaccounts" => deser!(ServiceAccounts, KubeServiceAccount),
        "storageclasses" => deser!(StorageClasses, KubeStorageClass),
        "persistentvolumes" => deser!(Pvs, KubePv),
        "persistentvolumeclaims" => deser!(Pvcs, KubePvc),
        "events" => deser!(Events, KubeEvent),
        "roles" => deser!(Roles, KubeRole),
        "clusterroles" => deser!(ClusterRoles, KubeClusterRole),
        "rolebindings" => deser!(RoleBindings, KubeRoleBinding),
        "clusterrolebindings" => deser!(ClusterRoleBindings, KubeClusterRoleBinding),
        "horizontalpodautoscalers" => deser!(Hpa, KubeHpa),
        "endpoints" => deser!(Endpoints, KubeEndpoints),
        "limitranges" => deser!(LimitRanges, KubeLimitRange),
        "resourcequotas" => deser!(ResourceQuotas, KubeResourceQuota),
        "poddisruptionbudgets" => deser!(Pdb, KubePdb),
        "customresourcedefinitions" => deser!(Crds, KubeCrd),
        "dynamic" => deser!(DynamicResources, DynamicKubeResource),
        _ => {
            warn!(
                "ClientSession: unknown resource type in snapshot: {}",
                resource_type
            );
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tab -> resource type string mapping
// ---------------------------------------------------------------------------

/// Maps a `ResourceTab` to the resource type string used in protocol commands.
pub fn tab_resource_type(tab: ResourceTab) -> &'static str {
    match tab {
        ResourceTab::Pods => "pods",
        ResourceTab::Deployments => "deployments",
        ResourceTab::Services => "services",
        ResourceTab::StatefulSets => "statefulsets",
        ResourceTab::DaemonSets => "daemonsets",
        ResourceTab::Jobs => "jobs",
        ResourceTab::CronJobs => "cronjobs",
        ResourceTab::ConfigMaps => "configmaps",
        ResourceTab::Secrets => "secrets",
        ResourceTab::Nodes => "nodes",
        ResourceTab::Namespaces => "namespaces",
        ResourceTab::Ingresses => "ingresses",
        ResourceTab::ReplicaSets => "replicasets",
        ResourceTab::Pvs => "persistentvolumes",
        ResourceTab::Pvcs => "persistentvolumeclaims",
        ResourceTab::StorageClasses => "storageclasses",
        ResourceTab::ServiceAccounts => "serviceaccounts",
        ResourceTab::NetworkPolicies => "networkpolicies",
        ResourceTab::Events => "events",
        ResourceTab::Roles => "roles",
        ResourceTab::ClusterRoles => "clusterroles",
        ResourceTab::RoleBindings => "rolebindings",
        ResourceTab::ClusterRoleBindings => "clusterrolebindings",
        ResourceTab::Hpa => "horizontalpodautoscalers",
        ResourceTab::Endpoints => "endpoints",
        ResourceTab::LimitRanges => "limitranges",
        ResourceTab::ResourceQuotas => "resourcequotas",
        ResourceTab::Pdb => "poddisruptionbudgets",
        ResourceTab::Crds => "customresourcedefinitions",
        ResourceTab::DynamicResource => "dynamic",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kube::server_session::serialize_resource_update;

    /// Verify that every list-type `ResourceUpdate` variant can survive a
    /// serialize -> deserialize roundtrip. This catches drift between
    /// `serialize_resource_update` and `deserialize_snapshot`.
    #[test]
    fn test_serialize_deserialize_roundtrip() {
        // Build one empty-vec ResourceUpdate per list variant.
        let updates: Vec<ResourceUpdate> = vec![
            ResourceUpdate::Pods(vec![]),
            ResourceUpdate::Deployments(vec![]),
            ResourceUpdate::Services(vec![]),
            ResourceUpdate::Nodes(vec![]),
            ResourceUpdate::Namespaces(vec![]),
            ResourceUpdate::ConfigMaps(vec![]),
            ResourceUpdate::Secrets(vec![]),
            ResourceUpdate::StatefulSets(vec![]),
            ResourceUpdate::DaemonSets(vec![]),
            ResourceUpdate::Jobs(vec![]),
            ResourceUpdate::CronJobs(vec![]),
            ResourceUpdate::ReplicaSets(vec![]),
            ResourceUpdate::Ingresses(vec![]),
            ResourceUpdate::NetworkPolicies(vec![]),
            ResourceUpdate::ServiceAccounts(vec![]),
            ResourceUpdate::StorageClasses(vec![]),
            ResourceUpdate::Pvs(vec![]),
            ResourceUpdate::Pvcs(vec![]),
            ResourceUpdate::Events(vec![]),
            ResourceUpdate::Roles(vec![]),
            ResourceUpdate::ClusterRoles(vec![]),
            ResourceUpdate::RoleBindings(vec![]),
            ResourceUpdate::ClusterRoleBindings(vec![]),
            ResourceUpdate::Hpa(vec![]),
            ResourceUpdate::Endpoints(vec![]),
            ResourceUpdate::LimitRanges(vec![]),
            ResourceUpdate::ResourceQuotas(vec![]),
            ResourceUpdate::Pdb(vec![]),
            ResourceUpdate::Crds(vec![]),
            ResourceUpdate::DynamicResources(vec![]),
        ];

        for update in &updates {
            let (rt, json) = serialize_resource_update(update)
                .unwrap_or_else(|| panic!("serialize failed for {:?}", std::mem::discriminant(update)));
            let result = deserialize_snapshot(&rt, &json);
            assert!(
                result.is_some(),
                "deserialize_snapshot returned None for resource_type '{}' (serialized from {:?})",
                rt,
                std::mem::discriminant(update),
            );
        }
    }
}
