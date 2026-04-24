//! Port-forward source — the first implementation of [`LocalResourceSource`].
//!
//! Owns a set of `kubectl port-forward` subprocesses, each with a proper state
//! machine ([`PortForwardState`]), and publishes snapshot updates via a
//! `watch::Sender` whenever any entry transitions states.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;

use dashmap::DashMap;
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};

use crate::event::ResourceUpdate;
use crate::kube::protocol::{
    Namespace, ObjectRef, ResourceId,
};
use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

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
    pub context: crate::kube::protocol::ContextName,
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
    /// The namespace to pass to `kubectl -n`. Typed [`Namespace`] —
    /// `Namespace::All` is rejected at create time because port-forward
    /// has no meaning across all namespaces.
    pub namespace: Namespace,
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
    bound_context: crate::kube::protocol::ContextName,
    entries: DashMap<u64, EntrySlot>,
    next_id: AtomicU64,
    tx: watch::Sender<ResourceUpdate>,
    /// Keeps the `watch::Sender` alive regardless of subscriber count.
    _keep_rx: watch::Receiver<ResourceUpdate>,
    /// Self-reference set by `Arc::new_cyclic` during construction. Lets
    /// `&self` methods (e.g. `apply_yaml` from the trait) reach the `Arc`
    /// they live inside without interior mutability — `create()` needs the
    /// `Arc` to spawn a monitor task that outlives the call.
    self_weak: Weak<Self>,
    /// Coalesces grace-period tasks: at most ONE in flight per source.
    /// CAS-set to true in `try_begin_grace`, cleared by the grace task
    /// before it drops its Arc. Without this, rapid context-switch churn
    /// stacked one detached 5-minute task per drop.
    grace_in_flight: std::sync::atomic::AtomicBool,
}

impl Drop for PortForwardSource {
    fn drop(&mut self) {
        // Abort every monitor task. This drops their Arc<Self> references
        // (breaking the circular ref) AND drops the `kill_on_drop` child
        // process handle inside each task, which kills the kubectl subprocess.
        for entry in self.entries.iter() {
            if let Some(ref abort) = entry.abort {
                abort.abort();
            }
        }
    }
}

impl PortForwardSource {
    /// Construct a fresh source bound to a single context. Called by the
    /// registry the first time any session subscribes for that context.
    pub fn for_context(bound_context: crate::kube::protocol::ContextName) -> Arc<Self> {
        let id = super::types::LocalResourceKind::PortForward.to_resource_id();
        let empty = ResourceUpdate::Rows {
            resource: id.clone(),
            headers: headers(),
            rows: Vec::new(),
        };
        let (tx, rx) = watch::channel(empty);
        Arc::new_cyclic(|weak: &Weak<Self>| Self {
            id,
            bound_context,
            entries: DashMap::new(),
            next_id: AtomicU64::new(1),
            tx,
            _keep_rx: rx,
            self_weak: weak.clone(),
            grace_in_flight: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Recover the live `Arc<Self>` from the cyclic `Weak`. Always succeeds
    /// while at least one external strong ref is held — which is true any
    /// time a method is being called, since the caller had to upgrade the
    /// registry's `Weak` to land here.
    fn arc_self(&self) -> Arc<Self> {
        self.self_weak
            .upgrade()
            .expect("self_weak must upgrade while a method is running on this source")
    }

    /// The context this source is bound to.
    pub fn bound_context(&self) -> &crate::kube::protocol::ContextName {
        &self.bound_context
    }

    /// Create a new port-forward. Returns the assigned id immediately; the
    /// subprocess is spawned in the background and state transitions are
    /// published via the watch channel. The kubectl `--context` arg comes
    /// from `self.bound_context`, not the request.
    pub fn create(self: &Arc<Self>, req: PortForwardRequest) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // Stored as a String (serialized into the YAML describe view).
        // The typed Namespace gets flattened here at the boundary.
        let ns_string = req.namespace.as_option().unwrap_or("").to_string();
        let entry = PortForwardEntry {
            id,
            target: req.target.clone(),
            kubectl_target: req.kubectl_target.clone(),
            namespace: ns_string,
            context: self.bound_context.clone(),
            local_port: req.local_port,
            remote_port: req.remote_port,
            state: PortForwardState::Starting,
            started_at: Instant::now(),
            last_message: String::new(),
        };

        // CRITICAL: the monitor task holds a `Weak<Self>`, NOT a strong Arc.
        // Otherwise the task's Arc would keep the source alive forever
        // (kubectl port-forward never exits on its own), the source's `Drop`
        // would never run, and kubectl subprocesses would leak across
        // context switches. With `Weak`, when external refs drop, `Drop`
        // fires, aborts every monitor task, and `kill_on_drop` reaps each
        // child process.
        //
        // RACE-FREE HANDOFF: Spawn the task in a "waiting" state (blocked on
        // `start_rx`), insert the slot with the abort handle already set,
        // then signal via `start_tx.send(())`. This guarantees:
        //   1. When run_forward calls `set_state(id, ...)`, the entry
        //      exists in the DashMap (the previous race where Failed state
        //      from a fast-failing port bind was silently dropped).
        //   2. A concurrent `stop(id)` called between insert and start-send
        //      still wins: it removes the slot + aborts the task, which
        //      drops the oneshot future and run_forward never runs — no
        //      kubectl subprocess to reap.
        let weak = Arc::downgrade(self);
        let (start_tx, start_rx) = tokio::sync::oneshot::channel::<()>();
        let handle: JoinHandle<()> = tokio::spawn(async move {
            // Wait for the caller to finish inserting the slot. An `Err`
            // here means the caller dropped the sender — treated as a
            // cancellation, so we exit without running run_forward.
            if start_rx.await.is_err() {
                return;
            }
            Self::run_forward(weak, id, req).await;
        });
        let abort = handle.abort_handle();
        self.entries.insert(id, EntrySlot { entry, abort: Some(abort) });
        self.publish();
        let _ = start_tx.send(());
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
    /// Holds only a `Weak<Self>` and upgrades on demand; drops the upgrade
    /// before any `await` so the source can be dropped while kubectl runs.
    async fn run_forward(weak: Weak<Self>, id: u64, req: PortForwardRequest) {
        // 1. Local port bind probe.
        match tokio::net::TcpListener::bind(("127.0.0.1", req.local_port)).await {
            Ok(listener) => drop(listener),
            Err(e) => {
                if let Some(this) = weak.upgrade() {
                    this.set_state(
                        id,
                        PortForwardState::Failed,
                        format!("local port {} unavailable: {}", req.local_port, e),
                    );
                }
                return;
            }
        }

        // Read the bound context out of the source while we still have a
        // strong ref — we drop the upgrade before spawning so the kubectl
        // wait() below doesn't keep the source alive.
        let bound_context = match weak.upgrade() {
            Some(this) => this.bound_context.clone(),
            None => return, // source dropped during port probe
        };

        // 2. Spawn kubectl.
        let mut cmd = tokio::process::Command::new("kubectl");
        cmd.arg("port-forward")
            .arg(&req.kubectl_target)
            .arg(format!("{}:{}", req.local_port, req.remote_port));
        // Typed `Namespace` — `as_option()` returns `Some(name)` for
        // Named and None for All. The `Namespace::All` case is silently
        // dropped because port-forward against "all namespaces" makes no
        // sense; the upstream caller should never construct one.
        if let Some(ns) = req.namespace.as_option() {
            cmd.arg("-n").arg(ns);
        }
        if !bound_context.is_empty() {
            cmd.arg("--context").arg(bound_context.as_str());
        }
        cmd.stdout(std::process::Stdio::null());
        // stderr → null, not piped: an unread piped stderr can fill the OS
        // pipe buffer over a long-running forward (kubectl port-forward
        // logs "Handling connection" per accept) and block the subprocess.
        cmd.stderr(std::process::Stdio::null());
        let mut child = match cmd.kill_on_drop(true).spawn() {
            Ok(c) => c,
            Err(e) => {
                if let Some(this) = weak.upgrade() {
                    this.set_state(id, PortForwardState::Failed, format!("spawn failed: {e}"));
                }
                return;
            }
        };

        // 3. Probe the local port with backoff until it binds, or give up.
        // kubectl usually binds in <500ms, but slow auth / busy clusters can
        // push it out several seconds. Probing a single time 500ms in (the
        // prior behavior) left the state stuck in `Starting` whenever bind
        // was slower than that — the child.wait() below wouldn't advance
        // until kubectl itself exited, which it doesn't while still trying.
        // Deltas between probes, cumulative ~7s:
        const PROBE_DELAYS_MS: [u64; 6] = [200, 300, 500, 1000, 2000, 3000];
        let mut probed = false;
        for delay_ms in PROBE_DELAYS_MS {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            if TcpStream::connect(("127.0.0.1", req.local_port)).await.is_ok() {
                probed = true;
                break;
            }
        }
        if probed {
            if let Some(this) = weak.upgrade() {
                this.set_state(id, PortForwardState::Active, String::new());
            } else {
                return; // source dropped — `child` drops next, kill_on_drop reaps
            }
        } else {
            // kubectl is still running but never bound the port. Return
            // early — dropping `child` triggers `kill_on_drop` to reap the
            // subprocess, then set state to Failed with a clear reason.
            if let Some(this) = weak.upgrade() {
                this.set_state(
                    id,
                    PortForwardState::Failed,
                    "port-forward did not bind within probe timeout".to_string(),
                );
            }
            return;
        }

        // 4. Wait for subprocess exit.
        match child.wait().await {
            Ok(status) if status.success() => {
                if let Some(this) = weak.upgrade() {
                    this.set_state(id, PortForwardState::Stopped, String::new());
                }
            }
            Ok(status) => {
                if let Some(this) = weak.upgrade() {
                    this.set_state(
                        id,
                        PortForwardState::Failed,
                        format!("kubectl exited: {status}"),
                    );
                }
            }
            Err(e) => {
                if let Some(this) = weak.upgrade() {
                    this.set_state(id, PortForwardState::Failed, format!("wait error: {e}"));
                }
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

    fn try_begin_grace(&self) -> bool {
        use std::sync::atomic::Ordering;
        // CAS false→true. Only the first drop in a run wins; subsequent
        // drops see true and skip spawning their own grace tasks.
        self.grace_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn end_grace(&self) {
        self.grace_in_flight.store(false, std::sync::atomic::Ordering::Release);
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

    fn apply_yaml(&self, name: &str, yaml: &str) -> Result<String, String> {
        let id = parse_pf_row_name(name)?;

        // Typed view of the user-editable fields. Serde does the field
        // validation, type coercion, and missing-field reporting for us
        // — no `read_string`/`read_u16` plumbing against a raw Mapping.
        // System-managed fields (`id`, `state`, `age`, `last_message`)
        // are absent here so they round-trip untouched.
        #[derive(serde::Deserialize)]
        struct PortForwardEditView {
            local_port: u16,
            remote_port: u16,
            #[serde(default)]
            namespace: String,
            kubectl_target: String,
        }
        let edit: PortForwardEditView = serde_yaml::from_str(yaml)
            .map_err(|e| format!("yaml parse error: {e}"))?;

        // Snapshot the current entry so we can decide what changed and
        // build a fresh request from it.
        let current = self
            .entries
            .get(&id)
            .map(|slot| slot.entry.clone())
            .ok_or_else(|| format!("no port-forward with id {id}"))?;

        let unchanged = current.local_port == edit.local_port
            && current.remote_port == edit.remote_port
            && current.namespace == edit.namespace
            && current.kubectl_target == edit.kubectl_target;
        if unchanged {
            return Ok(format!("pf-{id} unchanged"));
        }

        // Refuse edits that would produce an empty namespace — a
        // port-forward against "all namespaces" is meaningless (kubectl
        // just drops the `-n` flag and the forward either fails or
        // silently targets the default namespace).
        if edit.namespace.is_empty() {
            return Err("namespace field is required for port-forward".into());
        }

        // Reconcile by stop + recreate. The new entry gets a fresh id; the
        // old row disappears from the next snapshot. `create` requires
        // `Arc<Self>` because it spawns a long-lived monitor task — we
        // recover the Arc via the cyclic `self_weak`.
        self.stop(id)?;
        let arc = self.arc_self();
        let new_id = arc.create(PortForwardRequest {
            target: current.target,
            kubectl_target: edit.kubectl_target,
            namespace: Namespace::Named(edit.namespace),
            local_port: edit.local_port,
            remote_port: edit.remote_port,
        });
        Ok(format!("pf-{id} → pf-{new_id}"))
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
    let age = crate::util::format_age_duration(entry.started_at.elapsed());
    let ns_display = if entry.namespace.is_empty() { "-" } else { entry.namespace.as_str() };
    let ctx_display = if entry.context.is_empty() { "-" } else { entry.context.as_str() };
    let msg_display = if entry.last_message.is_empty() { "-" } else { entry.last_message.as_str() };

    out.push_str(&format!("Name:          {}\n", row_name));
    out.push_str("Kind:          PortForward\n");
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
        age: crate::util::format_age_duration(entry.started_at.elapsed()),
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
    let age = crate::util::format_age_duration(entry.started_at.elapsed());
    let row_name = format!("pf-{}", entry.id);
    let health = match entry.state {
        PortForwardState::Active => RowHealth::Normal,
        PortForwardState::Starting => RowHealth::Pending,
        PortForwardState::Failed => RowHealth::Failed,
        _ => RowHealth::Pending,
    };
    let state_health = match entry.state {
        PortForwardState::Active => RowHealth::Normal,
        PortForwardState::Starting => RowHealth::Pending,
        PortForwardState::Failed => RowHealth::Failed,
        _ => RowHealth::Pending,
    };
    let cells: Vec<CellValue> = vec![
        CellValue::Text(row_name.clone()),
        CellValue::Text(entry.kubectl_target.clone()),
        CellValue::Text(if entry.namespace.is_empty() { "-".into() } else { entry.namespace.clone() }),
        CellValue::Count(entry.local_port as i64),
        CellValue::Count(entry.remote_port as i64),
        CellValue::Status { text: entry.state.as_str().to_string(), health: state_health },
        CellValue::Text(age),
        CellValue::Text(entry.last_message.clone()),
    ];    ResourceRow {
        name: row_name,
        namespace: Some(entry.namespace.clone()),
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        health,
        crd_info: None,
        drill_target: None,
        cells,
        ..Default::default()
    }
}

