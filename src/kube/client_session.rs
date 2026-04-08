//! TUI-side session: connects to a `ServerSession` over a Unix socket (daemon
//! mode) or an in-memory duplex stream (`--no-daemon` mode).
//!
//! `ClientSession` is the counterpart of `ServerSession`. It sends
//! `SessionCommand`s and converts incoming `SessionEvent`s into `AppEvent`s
//! that the existing TUI event loop can process.
//!
//! Wire format: length-prefixed bincode (see protocol.rs).

use std::collections::HashMap;
use std::sync::Arc;

use kube::config::Kubeconfig;
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use super::daemon::socket_path;
use super::protocol::{self, SessionCommand, SessionEvent};
use super::server_session::{ServerSession, SessionSharedState};
use crate::app::FlashMessage;
use crate::event::{AppEvent, ResourceUpdate};

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

/// Cached kubeconfig data for re-use across context switches.
#[derive(Debug, Clone)]
struct CachedKubeconfig {
    yaml: String,
    env_vars: HashMap<String, String>,
}

/// Kubeconfig + env vars prepared by the TUI for sending to the session.
struct PreparedKubeconfig {
    kubeconfig_yaml: String,
    env_vars: HashMap<String, String>,
    cluster_name: String,
    user_name: String,
}

/// TUI-side session handle for communicating with a `ServerSession`.
/// Commands are sent via an mpsc channel to a background writer task,
/// so the TUI event loop NEVER blocks on socket I/O.
pub struct ClientSession {
    cmd_tx: mpsc::UnboundedSender<SessionCommand>,
    _writer_task: JoinHandle<()>,
    _reader_task: JoinHandle<()>,
    context: String,
    cached_kubeconfig: Option<CachedKubeconfig>,
}

impl Drop for ClientSession {
    fn drop(&mut self) {
        self._reader_task.abort();
        self._writer_task.abort();
    }
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

impl ClientSession {
    /// Connect to the daemon via Unix socket (binary protocol).
    pub async fn connect(
        context: Option<&str>,
        namespace: Option<&str>,
        readonly: bool,
        event_tx: mpsc::Sender<AppEvent>,
    ) -> anyhow::Result<(Self, SessionReadyInfo)> {
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
        let cached = CachedKubeconfig {
            yaml: prepared.kubeconfig_yaml.clone(),
            env_vars: prepared.env_vars.clone(),
        };
        let (mut cs, info) = Self::handshake(boxed_read, boxed_write, context, namespace, readonly, prepared, event_tx).await?;
        cs.cached_kubeconfig = Some(cached);
        Ok((cs, info))
    }

    /// Connect locally without a daemon: in-memory duplex + local ServerSession.
    pub async fn connect_local(
        context: Option<&str>,
        namespace: Option<&str>,
        readonly: bool,
        event_tx: mpsc::Sender<AppEvent>,
    ) -> anyhow::Result<(Self, SessionReadyInfo)> {
        let shared = Arc::new(SessionSharedState::new());
        let prepared = Self::prepare_kubeconfig(context)?;

        let (client_stream, server_stream) = tokio::io::duplex(protocol::DUPLEX_BUFFER_SIZE);

        // Spawn ServerSession on the server end.
        let (server_read, server_write) = tokio::io::split(server_stream);
        let server_boxed_read: Box<dyn AsyncRead + Unpin + Send> = Box::new(server_read);
        let server_boxed_write: Box<dyn AsyncWrite + Unpin + Send> = Box::new(server_write);
        tokio::spawn(async move {
            let result = std::panic::AssertUnwindSafe(
                ServerSession::init_and_run(
                    BufReader::with_capacity(protocol::IO_BUFFER_SIZE, server_boxed_read),
                    server_boxed_write,
                    shared,
                )
            );
            if let Err(e) = futures::FutureExt::catch_unwind(result).await {
                tracing::error!("ServerSession panicked: {:?}", e);
            }
        });

        // Create ClientSession on the client end — no magic byte needed
        // for local mode (the server reads Init directly).
        let (client_read, client_write) = tokio::io::split(client_stream);
        let boxed_read: Box<dyn AsyncRead + Unpin + Send> = Box::new(client_read);
        let boxed_write: Box<dyn AsyncWrite + Unpin + Send> = Box::new(client_write);

        let cached = CachedKubeconfig {
            yaml: prepared.kubeconfig_yaml.clone(),
            env_vars: prepared.env_vars.clone(),
        };
        let (mut cs, info) = Self::handshake(boxed_read, boxed_write, context, namespace, readonly, prepared, event_tx).await?;
        cs.cached_kubeconfig = Some(cached);
        Ok((cs, info))
    }

    fn prepare_kubeconfig(context: Option<&str>) -> anyhow::Result<PreparedKubeconfig> {
        let kubeconfig = Kubeconfig::read()
            .map_err(|e| anyhow::anyhow!("Failed to read kubeconfig: {}", e))?;

        let context_name = context
            .map(|s| s.to_string())
            .or_else(|| kubeconfig.current_context.clone())
            .ok_or_else(|| anyhow::anyhow!("No context specified and no current-context in kubeconfig"))?;

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

        let kubeconfig_yaml = serde_yaml::to_string(&kubeconfig)
            .map_err(|e| anyhow::anyhow!("Failed to serialize kubeconfig: {}", e))?;

        let env_vars = collect_env_vars();

        Ok(PreparedKubeconfig {
            kubeconfig_yaml,
            env_vars,
            cluster_name,
            user_name,
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

    fn build_init_command(
        context: Option<&str>,
        namespace: Option<&str>,
        readonly: bool,
        prepared: &PreparedKubeconfig,
    ) -> SessionCommand {
        SessionCommand::Init {
            context: context.map(|s| s.to_string()),
            namespace: namespace.map(|s| s.to_string()).unwrap_or_default().into(),
            readonly,
            kubeconfig_yaml: prepared.kubeconfig_yaml.clone(),
            env_vars: prepared.env_vars.clone(),
            cluster_name: prepared.cluster_name.clone(),
            user_name: prepared.user_name.clone(),
        }
    }

    /// Common handshake: send Init (binary), read Ready (binary), spawn reader + writer loops.
    async fn handshake(
        read: Box<dyn AsyncRead + Unpin + Send>,
        write: Box<dyn AsyncWrite + Unpin + Send>,
        context: Option<&str>,
        namespace: Option<&str>,
        readonly: bool,
        prepared: PreparedKubeconfig,
        event_tx: mpsc::Sender<AppEvent>,
    ) -> anyhow::Result<(Self, SessionReadyInfo)> {
        let mut reader = BufReader::with_capacity(protocol::IO_BUFFER_SIZE, read);
        let mut writer = BufWriter::with_capacity(protocol::IO_BUFFER_SIZE, write);

        // Send Init command (binary) — before spawning the writer task.
        let init_cmd = Self::build_init_command(context, namespace, readonly, &prepared);
        protocol::write_bincode(&mut writer, &init_cmd).await?;

        // Read Ready response (binary) — before spawning tasks.
        let first_event: SessionEvent = protocol::read_bincode(&mut reader).await
            .map_err(|e| anyhow::anyhow!("Failed to read server response: {}", e))?;

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
            SessionEvent::SessionError(message) => {
                anyhow::bail!("Server rejected session: {}", message);
            }
            other => {
                anyhow::bail!(
                    "Expected Ready or SessionError from server, got: {:?}",
                    other
                );
            }
        };

        // Spawn the writer task — owns the BufWriter, drains the channel.
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<SessionCommand>();
        let writer_task = tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                if protocol::write_bincode(&mut writer, &cmd).await.is_err() {
                    break;
                }
            }
        });

        let initial_ctx = ready_info.context.clone();
        let reader_task = tokio::spawn(async move {
            reader_loop(reader, event_tx, initial_ctx).await;
        });

        Ok((
            Self {
                cmd_tx,
                _writer_task: writer_task,
                _reader_task: reader_task,
                context: ready_info.context.clone(),
                cached_kubeconfig: None,
            },
            ready_info,
        ))
    }
}

// ---------------------------------------------------------------------------
// Command methods
// ---------------------------------------------------------------------------

impl ClientSession {
    pub fn subscribe(&mut self, resource_type: &str) -> anyhow::Result<()> {
        let rid = protocol::ResourceId::from_alias(resource_type).unwrap_or_else(|| {
            protocol::ResourceId::new("", "", resource_type, resource_type, protocol::ResourceScope::Namespaced)
        });
        self.send_command(&SessionCommand::Subscribe(rid))
    }

    pub fn unsubscribe(&mut self, resource_type: &str) -> anyhow::Result<()> {
        let rid = protocol::ResourceId::from_alias(resource_type).unwrap_or_else(|| {
            protocol::ResourceId::new("", "", resource_type, resource_type, protocol::ResourceScope::Namespaced)
        });
        self.send_command(&SessionCommand::Unsubscribe(rid))
    }

    pub fn refresh(&mut self, resource_type: &str) -> anyhow::Result<()> {
        let rid = protocol::ResourceId::from_alias(resource_type).unwrap_or_else(|| {
            protocol::ResourceId::new("", "", resource_type, resource_type, protocol::ResourceScope::Namespaced)
        });
        self.send_command(&SessionCommand::Refresh(rid))
    }

    pub fn switch_namespace(&mut self, namespace: &str) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::SwitchNamespace {
            namespace: protocol::Namespace::from(namespace),
        })
    }

    pub fn switch_context(&mut self, context: &str) -> anyhow::Result<()> {
        let prepared = if let Some(ref cached) = self.cached_kubeconfig {
            Self::prepare_kubeconfig_from_cached(&cached.yaml, &cached.env_vars, Some(context))
        } else {
            let p = Self::prepare_kubeconfig(Some(context))?;
            self.cached_kubeconfig = Some(CachedKubeconfig {
                yaml: p.kubeconfig_yaml.clone(),
                env_vars: p.env_vars.clone(),
            });
            p
        };
        self.send_command(&SessionCommand::SwitchContext {
            context: context.to_string(),
            kubeconfig_yaml: prepared.kubeconfig_yaml,
            env_vars: prepared.env_vars,
            cluster_name: prepared.cluster_name,
            user_name: prepared.user_name,
        })
    }

    pub fn describe(
        &mut self,
        resource_type: &str,
        name: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Describe(protocol::ObjectRef::from_parts(
            resource_type, name, protocol::Namespace::from(namespace),
        )))
    }

    pub fn yaml(
        &mut self,
        resource_type: &str,
        name: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Yaml(protocol::ObjectRef::from_parts(
            resource_type, name, protocol::Namespace::from(namespace),
        )))
    }

    pub fn delete(
        &mut self,
        resource_type: &str,
        name: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Delete(protocol::ObjectRef::from_parts(
            resource_type, name, protocol::Namespace::from(namespace),
        )))
    }

    pub fn scale(
        &mut self,
        resource_type: &str,
        name: &str,
        namespace: &str,
        replicas: u32,
    ) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Scale {
            target: protocol::ObjectRef::from_parts(
                resource_type, name, protocol::Namespace::from(namespace),
            ),
            replicas,
        })
    }

    pub fn restart(
        &mut self,
        resource_type: &str,
        name: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::Restart(protocol::ObjectRef::from_parts(
            resource_type, name, protocol::Namespace::from(namespace),
        )))
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

    pub fn decode_secret(&mut self, name: &str, namespace: &str) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::DecodeSecret {
            name: name.to_string(),
            namespace: protocol::Namespace::from(namespace),
        })
    }

    pub fn trigger_cronjob(&mut self, name: &str, namespace: &str) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::TriggerCronJob {
            name: name.to_string(),
            namespace: protocol::Namespace::from(namespace),
        })
    }

    pub fn suspend_cronjob(&mut self, name: &str, namespace: &str, suspend: bool) -> anyhow::Result<()> {
        self.send_command(&SessionCommand::SuspendCronJob {
            name: name.to_string(),
            namespace: protocol::Namespace::from(namespace),
            suspend,
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
    /// Subscribe to a resource by its ResourceId.
    pub fn subscribe_resource(&mut self, rid: &protocol::ResourceId) {
        if let Err(e) = self.send_command(&SessionCommand::Subscribe(rid.clone())) {
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

    pub fn set_context_info(&mut self, new_context: &str) {
        self.context = new_context.to_string();
    }

    pub fn context_name(&self) -> &str {
        &self.context
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

