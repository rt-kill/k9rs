//! Port-forward source — the first implementation of [`LocalResourceSource`].
//!
//! Owns a set of `kubectl port-forward` subprocesses, each with a proper state
//! machine ([`PortForwardState`]), and publishes snapshot updates via a
//! `watch::Sender` whenever any entry transitions states.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};

use crate::event::ResourceUpdate;
use crate::kube::protocol::{
    InputSchema, ObjectRef, OperationDescriptor, OperationKind,
    ResourceCapabilities, ResourceId,
};
use crate::kube::resources::row::ResourceRow;

use super::LocalResourceSource;

/// The state of a single port-forward.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PortForwardState {
    /// Subprocess has been spawned, waiting for tunnel readiness.
    Starting,
    /// Tunnel is up and accepting connections.
    Active,
    /// Subprocess exited with an error, or the port-bind probe failed, or
    /// kubectl couldn't be spawned. See `last_message` for details.
    Failed,
    /// User explicitly stopped the forward.
    Stopped,
}

impl PortForwardState {
    pub fn as_str(self) -> &'static str {
        match self {
            PortForwardState::Starting => "Starting",
            PortForwardState::Active => "Active",
            PortForwardState::Failed => "Failed",
            PortForwardState::Stopped => "Stopped",
        }
    }
}

/// A single active port-forward, as known to the daemon.
#[derive(Debug, Clone, Serialize)]
pub struct PortForwardEntry {
    pub id: u64,
    /// The original K8s `ObjectRef` this forward was created for. Purely
    /// informational — the kubectl command uses `kubectl_target` below.
    pub target: ObjectRef,
    /// The kubectl short-form target string (e.g. `"svc/nginx"`, `"pod/foo"`).
    pub kubectl_target: String,
    /// Namespace the target lives in (for `kubectl -n`).
    pub namespace: String,
    /// Context the forward was created under (for `kubectl --context`).
    pub context: String,
    pub local_port: u16,
    pub remote_port: u16,
    pub state: PortForwardState,
    /// Monotonic clock — never serialized; the YAML/describe formatters emit
    /// the derived `age` instead.
    #[serde(skip)]
    pub started_at: Instant,
    /// Human-readable detail, populated on failure or on state transitions.
    pub last_message: String,
}

/// Arguments for creating a new forward. The source's bound context is
/// used implicitly — there's no per-request context override since each
/// `PortForwardSource` instance is per-context already.
pub struct PortForwardRequest {
    pub target: ObjectRef,
    pub kubectl_target: String,
    pub namespace: String,
    pub local_port: u16,
    pub remote_port: u16,
}

struct EntrySlot {
    entry: PortForwardEntry,
    /// Abort handle for the subprocess monitor task (owned by the source).
    abort: Option<AbortHandle>,
}

/// Per-context port-forward source. Each context the daemon serves gets
/// its own instance via [`crate::kube::local::LocalRegistry::port_forwards_for`].
/// All `kubectl port-forward` subprocesses spawned by this instance run
/// against the bound context — switching context in the TUI hands you a
/// different `PortForwardSource` and the old one's subprocesses go with it
/// after the registry's grace period.
pub struct PortForwardSource {
    id: ResourceId,
    /// Context name this source is bound to. Used as the `--context` arg on
    /// every `kubectl port-forward` subprocess this source spawns.
    bound_context: String,
    entries: DashMap<u64, EntrySlot>,
    next_id: AtomicU64,
    tx: watch::Sender<ResourceUpdate>,
    /// Keeps the `watch::Sender` alive regardless of subscriber count.
    _keep_rx: watch::Receiver<ResourceUpdate>,
}

impl PortForwardSource {
    /// Construct a fresh source bound to a single context. Called by the
    /// registry the first time any session subscribes for that context.
    pub fn for_context(bound_context: String) -> Arc<Self> {
        let id = super::types::find_by_plural("portforwards")
            .expect("port-forward type metadata missing")
            .to_resource_id();
        let empty = ResourceUpdate::Rows {
            resource: id.clone(),
            headers: headers(),
            rows: Vec::new(),
        };
        let (tx, rx) = watch::channel(empty);
        Arc::new(Self {
            id,
            bound_context,
            entries: DashMap::new(),
            next_id: AtomicU64::new(1),
            tx,
            _keep_rx: rx,
        })
    }

    /// The context this source is bound to.
    pub fn bound_context(&self) -> &str {
        &self.bound_context
    }

    /// Create a new port-forward. Returns the assigned id immediately; the
    /// subprocess is spawned in the background and state transitions are
    /// published via the watch channel. The kubectl `--context` arg comes
    /// from `self.bound_context`, not the request.
    pub fn create(self: &Arc<Self>, req: PortForwardRequest) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let entry = PortForwardEntry {
            id,
            target: req.target.clone(),
            kubectl_target: req.kubectl_target.clone(),
            namespace: req.namespace.clone(),
            context: self.bound_context.clone(),
            local_port: req.local_port,
            remote_port: req.remote_port,
            state: PortForwardState::Starting,
            started_at: Instant::now(),
            last_message: String::new(),
        };
        self.entries.insert(id, EntrySlot { entry, abort: None });
        self.publish();

        let this = Arc::clone(self);
        let handle: JoinHandle<()> = tokio::spawn(async move {
            this.run_forward(id, req).await;
        });
        if let Some(mut slot) = self.entries.get_mut(&id) {
            slot.abort = Some(handle.abort_handle());
        }
        id
    }

    /// Stop a port-forward by id. Aborts the monitor task (which kills the
    /// subprocess via `kill_on_drop`) and removes the entry.
    pub fn stop(&self, id: u64) -> Result<(), String> {
        let Some((_, slot)) = self.entries.remove(&id) else {
            return Err(format!("no port-forward with id {id}"));
        };
        if let Some(abort) = slot.abort {
            abort.abort();
        }
        self.publish();
        Ok(())
    }

    /// The full lifecycle: port probe → spawn kubectl → grace + probe → wait.
    async fn run_forward(self: Arc<Self>, id: u64, req: PortForwardRequest) {
        // 1. Local port bind probe.
        match tokio::net::TcpListener::bind(("127.0.0.1", req.local_port)).await {
            Ok(listener) => drop(listener),
            Err(e) => {
                self.set_state(
                    id,
                    PortForwardState::Failed,
                    format!("local port {} unavailable: {}", req.local_port, e),
                );
                return;
            }
        }

        // 2. Spawn kubectl. The context comes from the source's binding,
        // not the request — every PF this source spawns runs against the
        // same cluster.
        let mut cmd = tokio::process::Command::new("kubectl");
        cmd.arg("port-forward")
            .arg(&req.kubectl_target)
            .arg(format!("{}:{}", req.local_port, req.remote_port));
        if !req.namespace.is_empty() && req.namespace != "all" {
            cmd.arg("-n").arg(&req.namespace);
        }
        if !self.bound_context.is_empty() {
            cmd.arg("--context").arg(&self.bound_context);
        }
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::piped());
        let mut child = match cmd.kill_on_drop(true).spawn() {
            Ok(c) => c,
            Err(e) => {
                self.set_state(id, PortForwardState::Failed, format!("spawn failed: {e}"));
                return;
            }
        };

        // 3. Grace period + TCP probe to confirm tunnel is up.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let probed = TcpStream::connect(("127.0.0.1", req.local_port)).await.is_ok();
        if probed {
            self.set_state(id, PortForwardState::Active, String::new());
        }
        // If the probe failed but the subprocess is still running, we leave
        // the entry in `Starting` — the child.wait() below will eventually
        // transition it to Failed or Stopped.

        // 4. Wait for subprocess exit.
        match child.wait().await {
            Ok(status) if status.success() => {
                self.set_state(id, PortForwardState::Stopped, String::new());
            }
            Ok(status) => {
                self.set_state(
                    id,
                    PortForwardState::Failed,
                    format!("kubectl exited: {status}"),
                );
            }
            Err(e) => {
                self.set_state(id, PortForwardState::Failed, format!("wait error: {e}"));
            }
        }
    }

    fn set_state(&self, id: u64, state: PortForwardState, message: String) {
        if let Some(mut slot) = self.entries.get_mut(&id) {
            slot.entry.state = state;
            if !message.is_empty() {
                slot.entry.last_message = message;
            }
        }
        self.publish();
    }

    fn publish(&self) {
        let mut rows: Vec<ResourceRow> = self
            .entries
            .iter()
            .map(|e| pf_to_row(&e.entry))
            .collect();
        rows.sort_by(|a, b| a.name.cmp(&b.name));
        let _ = self.tx.send(ResourceUpdate::Rows {
            resource: self.id.clone(),
            headers: headers(),
            rows,
        });
    }
}

impl LocalResourceSource for PortForwardSource {
    fn resource_id(&self) -> &ResourceId {
        &self.id
    }

    fn headers(&self) -> Vec<String> {
        headers()
    }

    /// Port-forwards declare three operations: describe, yaml, and delete
    /// (where "delete" stops the forward). Everything else (logs, scale,
    /// shell, restart, …) is meaningless on a local resource.
    fn capabilities(&self) -> ResourceCapabilities {
        ResourceCapabilities {
            operations: vec![
                OperationDescriptor {
                    kind: OperationKind::Describe,
                    label: "Describe".into(),
                    input: InputSchema::None,
                    requires_confirm: false,
                },
                OperationDescriptor {
                    kind: OperationKind::Yaml,
                    label: "YAML".into(),
                    input: InputSchema::None,
                    requires_confirm: false,
                },
                OperationDescriptor {
                    kind: OperationKind::Delete,
                    label: "Stop".into(),
                    input: InputSchema::None,
                    requires_confirm: true,
                },
            ],
        }
    }

    fn subscribe(&self) -> watch::Receiver<ResourceUpdate> {
        self.tx.subscribe()
    }

    fn delete(&self, name: &str) -> Result<(), String> {
        let id = parse_pf_row_name(name)?;
        self.stop(id)
    }

    fn describe(&self, name: &str) -> Option<Result<String, String>> {
        let id = match parse_pf_row_name(name) {
            Ok(id) => id,
            Err(e) => return Some(Err(e)),
        };
        let slot = match self.entries.get(&id) {
            Some(slot) => slot,
            None => return Some(Err(format!("no port-forward with id {id}"))),
        };
        Some(Ok(format_describe(&slot.entry)))
    }

    fn yaml(&self, name: &str) -> Option<Result<String, String>> {
        let id = match parse_pf_row_name(name) {
            Ok(id) => id,
            Err(e) => return Some(Err(e)),
        };
        let slot = match self.entries.get(&id) {
            Some(slot) => slot,
            None => return Some(Err(format!("no port-forward with id {id}"))),
        };
        Some(format_yaml(&slot.entry))
    }
}

/// Parse a row name (`"pf-42"`) into the underlying entry id. Centralized so
/// `delete`/`describe`/`yaml` all error identically on bad input.
fn parse_pf_row_name(name: &str) -> Result<u64, String> {
    name.strip_prefix("pf-")
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| format!("invalid port-forward id: {name}"))
}

/// Format a `PortForwardEntry` as a multi-line human-readable describe view,
/// loosely matching the visual shape of `kubectl describe`.
fn format_describe(entry: &PortForwardEntry) -> String {
    let mut out = String::new();
    let row_name = format!("pf-{}", entry.id);
    let age = format_age(entry.started_at.elapsed());
    let ns_display = if entry.namespace.is_empty() { "-" } else { entry.namespace.as_str() };
    let ctx_display = if entry.context.is_empty() { "-" } else { entry.context.as_str() };
    let msg_display = if entry.last_message.is_empty() { "-" } else { entry.last_message.as_str() };

    out.push_str(&format!("Name:          {}\n", row_name));
    out.push_str(&format!("Kind:          PortForward\n"));
    out.push_str(&format!("Target:        {}\n", entry.kubectl_target));
    out.push_str(&format!("Namespace:     {}\n", ns_display));
    out.push_str(&format!("Context:       {}\n", ctx_display));
    out.push_str(&format!("Local Port:    {}\n", entry.local_port));
    out.push_str(&format!("Remote Port:   {}\n", entry.remote_port));
    out.push_str(&format!("State:         {}\n", entry.state.as_str()));
    out.push_str(&format!("Age:           {}\n", age));
    out.push_str(&format!("Last Message:  {}\n", msg_display));
    out
}

/// Format a `PortForwardEntry` as YAML. Wraps the entry in a small view
/// struct so we can flatten its fields and inject the derived `age` (the
/// raw `started_at: Instant` is `#[serde(skip)]`'d).
fn format_yaml(entry: &PortForwardEntry) -> Result<String, String> {
    #[derive(Serialize)]
    struct Yaml<'a> {
        #[serde(flatten)]
        entry: &'a PortForwardEntry,
        age: String,
    }
    let view = Yaml {
        entry,
        age: format_age(entry.started_at.elapsed()),
    };
    serde_yaml::to_string(&view).map_err(|e| format!("yaml serialize error: {e}"))
}

/// Column headers for the port-forward table.
pub fn headers() -> Vec<String> {
    vec![
        "NAME".into(),
        "TARGET".into(),
        "NAMESPACE".into(),
        "LOCAL".into(),
        "REMOTE".into(),
        "STATE".into(),
        "AGE".into(),
        "MESSAGE".into(),
    ]
}

/// Convert a [`PortForwardEntry`] to a [`ResourceRow`]. Pure function, easy to test.
pub fn pf_to_row(entry: &PortForwardEntry) -> ResourceRow {
    let age = format_age(entry.started_at.elapsed());
    let row_name = format!("pf-{}", entry.id);
    ResourceRow {
        cells: vec![
            row_name.clone(),
            entry.kubectl_target.clone(),
            if entry.namespace.is_empty() { "-".into() } else { entry.namespace.clone() },
            entry.local_port.to_string(),
            entry.remote_port.to_string(),
            entry.state.as_str().to_string(),
            age,
            entry.last_message.clone(),
        ],
        name: row_name,
        namespace: String::new(),
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        crd_info: None,
        drill_target: None,
    }
}

fn format_age(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{secs}s")
    }
}
