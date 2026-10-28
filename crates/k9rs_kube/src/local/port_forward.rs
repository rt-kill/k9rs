//! Port-forward source — the first implementation of [`LocalResourceSource`],
//! and the first [`LocalOperator`].
//!
//! Each forward is one supervised **native websocket tunnel**
//! (`Api::portforward` — no kubectl subprocess): an attempt resolves the
//! target to a ready pod (see [`super::pf_resolve`]), binds the local
//! listener, opens a canary stream to the pod as the liveness signal, and
//! serves each accepted connection over its own tunnel. When the canary
//! drops (suspend/resume, network blip, pod death), the supervisor
//! re-establishes with exponential backoff — and because resolution reruns
//! per attempt, the reconnect lands on a healthy replacement pod, which a
//! pinned `kubectl port-forward` never could. The supervisor is the sole
//! writer of [`PortForwardState`]; snapshots publish via a `watch::Sender`
//! on every transition.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use k8s_openapi::api::core::v1::Pod;
use kube::api::Api;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::sync::watch;

use k9rs_proto::protocol::{
    Namespace, ObjectRef, ResourceId,
};
use k9rs_proto::row::{CellValue, ResourceRow, RowHealth};

use super::pf_resolve::{self, ResolveError};
use super::supervise::{
    supervise, AttemptHandle, Backoff, LocalOperator, OperatorEvent, OperatorExit, OperatorGuard,
    RunDelay,
};
use super::LocalResourceSource;

/// Backoff between re-establish attempts after a forward dies. Reaching
/// Active resets it, so a long-lived tunnel that drops reconnects fast.
const PF_BACKOFF_MIN: Duration = Duration::from_millis(500);
const PF_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// The state of a single port-forward, written exclusively by its
/// supervisor's event sink (see `create`). There is no `Stopped` variant:
/// an explicit stop *removes the entry* (row gone), and a tunnel that
/// drops — cleanly or not — gets re-established, so no resting state ever
/// meant "stopped".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PortForwardState {
    /// First attempt is underway, waiting for tunnel readiness.
    Starting,
    /// Tunnel is up and accepting connections.
    Active,
    /// The tunnel dropped; the supervisor is re-establishing it (backoff in
    /// progress or a retry attempt probing). `last_message` has the detail.
    Reconnecting,
    /// Permanent failure — the supervisor parked (e.g. unsupported
    /// target kind, port not on the service). No further attempts until
    /// the forward is recreated.
    Failed,
}

impl PortForwardState {
    pub fn as_str(self) -> &'static str {
        match self {
            PortForwardState::Starting => "Starting",
            PortForwardState::Active => "Active",
            PortForwardState::Reconnecting => "Reconnecting",
            PortForwardState::Failed => "Failed",
        }
    }
}

/// A single active port-forward, as known to the daemon.
#[derive(Debug, Clone, Serialize)]
pub struct PortForwardEntry {
    pub id: u64,
    /// The original K8s `ObjectRef` this forward was created for. Purely
    /// informational — resolution parses `kubectl_target` below.
    pub target: ObjectRef,
    /// The kubectl-style short-form target (e.g. `"services/nginx"`,
    /// `"pods/foo"`) — parsed by `pf_resolve` into a concrete pod.
    pub kubectl_target: String,
    /// Namespace the target lives in (resolution + tunnels scope to it).
    pub namespace: String,
    /// Context the forward was created under (display metadata; the
    /// request's client carries the actual identity).
    pub context: k9rs_proto::protocol::ContextName,
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

/// Arguments for creating a new forward.
/// A namespace that is definitely ONE namespace.
///
/// A port-forward resolves one pod in one namespace, so `Namespace::All` is
/// not something it can mean. This field used to be a plain `Namespace` with
/// a doc saying `All` was "rejected at create time", and it was not: `create`
/// flattened it with `.unwrap_or("")` and returned an id, so the row existed
/// first and displayed its namespace as `-` until the operator's first
/// attempt failed it. The real refusals were at the two CALL SITES, and the
/// one inside `run_once` was a third copy that nothing could reach.
///
/// Now the question is asked once, by whoever builds the request, before
/// anything is created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodNamespace(String);

impl PodNamespace {
    /// `None` for `Namespace::All`. The caller has to say what that means for
    /// it, which is the point — there is no sensible default to fall into.
    pub fn of(ns: &Namespace) -> Option<Self> {
        ns.as_option().map(|s| PodNamespace(s.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub struct PortForwardRequest {
    pub target: ObjectRef,
    pub kubectl_target: String,
    /// The context name the CREATING SESSION knows itself by — display
    /// metadata, recorded per entry.
    ///
    /// It has to ride the request rather than being read off the source,
    /// because a source is shared by every context that resolves to the same
    /// `ContextId` (same cluster, same credentials — the name is deliberately
    /// not part of that identity). Reading it from the source stamped every
    /// forward with whichever context happened to create the source FIRST, and
    /// `PortForwardEntry` is `Serialize` — so that wrong name was on the
    /// describe/yaml view of forwards created from the other context.
    pub context: k9rs_proto::protocol::ContextName,
    /// The namespace the target lives in — see [`PodNamespace`].
    pub namespace: PodNamespace,
    pub local_port: u16,
    pub remote_port: u16,
    /// The creating session's `kube::Client`, used for target resolution
    /// and the websocket tunnels so the forward authenticates as that
    /// session (with its refreshable credentials) rather than anything
    /// daemon-global. A `PortForwardSource` is shared per context, but each
    /// forward is created by one session — its client rides on the request
    /// and outlives the session (a `kube::Client` is a self-contained
    /// handle; its auth layer refreshes in-process).
    pub client: kube::Client,
}

struct EntrySlot {
    entry: PortForwardEntry,
    /// RAII handle to this entry's supervised loop. Dropping the slot
    /// (explicit `stop`, or the whole source tearing down) drops the guard,
    /// which aborts the loop, tearing down the in-flight attempt
    /// (listener, canary, connection pumps). Drop IS the cleanup — there
    /// is no separate stop path.
    _guard: OperatorGuard,
    /// The client the forward was created with — reused when an edit
    /// (`apply_yaml`) reconciles by stop + recreate, so the new tunnel
    /// authenticates as the original creating session.
    client: kube::Client,
}

/// Per-context port-forward source, owned strongly by its context's
/// [`ContextLocals`](super::context_locals::ContextLocals). All tunnels
/// this instance opens authenticate via their creating request's client
/// (the bound context). Forwards are side-effect resources: they run while the context is
/// attached (or within its grace window), independent of whether anyone is
/// viewing the `:pf` list, and die only on explicit stop or context teardown.
pub struct PortForwardSource {
    id: ResourceId,
    // NO context name is stored here, deliberately.
    //
    // A source belongs to a `ContextId`, whose identity is
    // `(server_url, credential fingerprint)` — the context NAME is excluded so
    // watchers are shared by actual cluster endpoint rather than by a label
    // that collides across kubeconfig files. Two context entries pointing at
    // one cluster with one set of credentials therefore share this source,
    // which is correct: the forwards are equally real and equally reachable
    // from either. Naming the source after whichever context happened to
    // create it would just be a label that is wrong half the time, so the
    // right number of context names to keep here is zero. Authentication
    // rides on each request's `kube::Client`, which IS the context.
    entries: DashMap<u64, EntrySlot>,
    next_id: AtomicU64,
    tx: watch::Sender<k9rs_proto::protocol::TableBaseline>,
    /// Keeps the `watch::Sender` alive regardless of subscriber count.
    _keep_rx: watch::Receiver<k9rs_proto::protocol::TableBaseline>,
    /// Self-reference set by `Arc::new_cyclic` during construction. Lets
    /// `&self` methods (e.g. `apply_yaml` from the trait) reach the `Arc`
    /// they live inside without interior mutability — `create()` needs the
    /// `Arc` to downgrade into the supervised operator it registers (the
    /// operator holds only a `Weak` back, per the `supervise` contract).
    self_weak: Weak<Self>,
}

// No `Drop` impl: teardown is the field-drop cascade. Dropping the source
// drops `entries`, which drops every `EntrySlot`, whose `OperatorGuard`
// aborts its supervised loop, whose in-flight attempt drops its listener,
// canary, and pumps. The old manual abort-every-task loop is subsumed.

impl PortForwardSource {
    /// Construct a fresh source for one `ContextId`'s local slice. Called by
    /// the registry the first time any session attaches to that context.
    pub fn for_context() -> Arc<Self> {
        let id = super::types::LocalResourceKind::PortForward.to_resource_id();
        let empty = k9rs_proto::protocol::TableBaseline {
            resource: id.clone(),
            headers: headers(),
            rows: Vec::new(),
        };
        let (tx, rx) = watch::channel(empty);
        Arc::new_cyclic(|weak: &Weak<Self>| Self {
            id,
            entries: DashMap::new(),
            next_id: AtomicU64::new(1),
            tx,
            _keep_rx: rx,
            self_weak: weak.clone(),
        })
    }

    /// Recover the live `Arc<Self>` from the cyclic `Weak`. Always succeeds
    /// while at least one external strong ref is held — which is true any
    /// time a method is being called, since the caller had to upgrade the
    /// context's handle to land here.
    ///
    /// This deliberately hands back an *owned* strong `Arc` — its only
    /// caller, the synchronous `apply_yaml`, needs it to call `create`,
    /// which downgrades it for the supervised task. Keep callers
    /// synchronous: holding this `Arc` across an `.await` would pin the
    /// source open and defeat the `kill_on_drop` reaping the operator
    /// contract exists to protect (see `supervise` module docs).
    fn arc_self(&self) -> Arc<Self> {
        self.self_weak
            .upgrade()
            .expect("self_weak must upgrade while a method is running on this source")
    }

    /// How many forwards this source is currently running. Forwards outlive
    /// any view of them, so `ctl status` is the only way to count them
    /// without opening the `:pf` table.
    pub fn live_count(&self) -> usize {
        self.entries.len()
    }

    /// Create a new port-forward. Returns the assigned id immediately; the
    /// supervised tunnel runs in the background and every state transition
    /// is published via the watch channel. The request carries both the
    /// `client` (the actual identity every tunnel authenticates with) and the
    /// `context` name (display metadata for this entry).
    pub fn create(self: &Arc<Self>, req: PortForwardRequest) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // Stored as a String (serialized into the YAML describe view). Never
        // empty: `PodNamespace` cannot be built from `Namespace::All`.
        let ns_string = req.namespace.as_str().to_string();
        let entry = PortForwardEntry {
            id,
            target: req.target.clone(),
            kubectl_target: req.kubectl_target.clone(),
            namespace: ns_string,
            context: req.context.clone(),
            local_port: req.local_port,
            remote_port: req.remote_port,
            state: PortForwardState::Starting,
            started_at: Instant::now(),
            last_message: String::new(),
        };
        // Remember the creating session's client so an edit (`apply_yaml`)
        // can recreate this forward authenticating as that same session.
        let entry_client = req.client.clone();

        // CRITICAL: both the operator and the event sink hold `Weak<Self>`,
        // never a strong Arc. A strong ref inside the supervised task would
        // keep the source alive forever (the loop never exits on its own),
        // so the source could never drop and its tunnels would leak.
        // The sink is a synchronous closure — its upgrade-use-drop cannot
        // straddle an `.await`, which is the old `with_live` guarantee held
        // structurally.
        let op = PortForwardOp {
            weak: Arc::downgrade(self),
            req,
        };
        // Single state writer: the supervisor narrates the lifecycle through
        // this sink; `run_once` itself never touches entry state.
        let weak = Arc::downgrade(self);
        let sink = move |ev: OperatorEvent| {
            let Some(this) = weak.upgrade() else { return };
            match ev {
                OperatorEvent::Starting => {
                    this.set_state(id, PortForwardState::Starting, String::new())
                }
                OperatorEvent::Active => {
                    this.set_state(id, PortForwardState::Active, String::new())
                }
                OperatorEvent::Retrying { error, .. } => {
                    this.set_state(id, PortForwardState::Reconnecting, error)
                }
                OperatorEvent::Fatal { error } => {
                    this.set_state(id, PortForwardState::Failed, error)
                }
            }
        };
        let (guard, gate) = supervise(
            op,
            RunDelay::Backoff(Backoff::new(PF_BACKOFF_MIN, PF_BACKOFF_MAX)),
            sink,
        );

        // RACE-FREE HANDOFF (preserved from the pre-supervision code): the
        // loop is spawned parked on the gate; insert the slot with the guard
        // already set, then arm. This guarantees:
        //   1. When the sink calls `set_state(id, ...)`, the entry exists in
        //      the DashMap (the previous race where a fast-failing bind's
        //      state was silently dropped).
        //   2. A concurrent `stop(id)` between insert and arm still wins: it
        //      removes the slot, dropping the guard, which aborts the parked
        //      task — the operator never runs, no tunnel to tear down.
        self.entries.insert(id, EntrySlot { entry, _guard: guard, client: entry_client });
        self.publish();
        gate.arm();
        id
    }

    /// Stop a port-forward by id: remove the entry. Dropping the slot drops
    /// its `OperatorGuard`, aborting the supervised loop and closing the
    /// tunnel — removal IS the stop.
    pub fn stop(&self, id: u64) -> Result<(), String> {
        let Some((_, _slot)) = self.entries.remove(&id) else {
            return Err(format!("no port-forward with id {id}"));
        };
        self.publish();
        Ok(())
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
        let _ = self.tx.send(k9rs_proto::protocol::TableBaseline {
            resource: self.id.clone(),
            headers: headers(),
            rows,
        });
    }
}

/// One supervised forward: a single attempt is resolve-target → bind the
/// local listener → open a canary tunnel to the pod (the liveness signal) →
/// serve accepted connections, each over its own tunnel. The supervisor
/// around it owns retry, backoff, and all state narration; this only
/// classifies how the attempt ended. Holds `Weak<PortForwardSource>` purely
/// for the gone-check — per the operator contract it never touches source
/// state (the sink does). Everything an attempt owns (listener, canary,
/// pump tasks in the `JoinSet`) lives inside `run_once`'s future, so an
/// abort tears the whole attempt down.
struct PortForwardOp {
    weak: Weak<PortForwardSource>,
    req: PortForwardRequest,
}

impl LocalOperator for PortForwardOp {
    async fn run_once(&self, attempt: AttemptHandle) -> OperatorExit {
        // No namespace check: `PodNamespace` cannot hold "all namespaces",
        // so there is nothing here to refuse. This was a `Fatal` branch that
        // nothing could reach, and it was the third copy of the rule.
        let ns = self.req.namespace.as_str();

        // 1. Resolve the target to a concrete (pod, port). Re-runs every
        // attempt, so a reconnect after pod death lands on a healthy
        // replacement.
        let target = match pf_resolve::resolve(
            &self.req.client,
            ns,
            &self.req.kubectl_target,
            self.req.remote_port,
        )
        .await
        {
            Ok(t) => t,
            Err(ResolveError::Retry(m)) => return OperatorExit::Continue(m),
            Err(ResolveError::Fatal(m)) => return OperatorExit::Fatal(m),
        };

        // Source gone during resolution? Nothing left to forward for.
        if self.weak.strong_count() == 0 {
            return OperatorExit::Gone;
        }

        // 2. Bind the local listener. Continue (retry), not Fatal: the port
        // may be held by a dying previous incarnation — `apply_yaml`'s
        // stop→recreate races the old attempt's teardown, which used to
        // strand an edited forward in terminal Failed. Backoff self-heals
        // it; a port held permanently by another app keeps retrying cheaply.
        let listener = match tokio::net::TcpListener::bind(("127.0.0.1", self.req.local_port)).await
        {
            Ok(l) => l,
            Err(e) => {
                return OperatorExit::Continue(format!(
                    "local port {} unavailable: {}",
                    self.req.local_port, e
                ));
            }
        };

        // 3. Canary tunnel — one idle stream to the pod. Its closure is a
        // liveness HINT (pod death, kubelet restart, a suspended laptop's
        // dead connection), but NOT a reliable one: many servers close an
        // idle connection on their own keepalive timeout while the pod is
        // perfectly healthy. So a canary close is repaired IN PLACE by
        // re-opening the canary — only a re-open FAILURE (pod really gone)
        // tears the attempt down. Critically, the canary is decoupled from
        // the user's connections: closing it must never abort live pumps.
        let api: Api<Pod> = Api::namespaced(self.req.client.clone(), ns);
        let mut canary_pf = match api.portforward(&target.pod, &[target.port]).await {
            Ok(p) => p,
            Err(e) => {
                return OperatorExit::Continue(format!("tunnel to {}: {}", target.pod, e));
            }
        };
        let Some(mut canary) = canary_pf.take_stream(target.port) else {
            return OperatorExit::Continue("tunnel stream unavailable".into());
        };
        // Healthy steady-state — the one signal an operator may send. Also
        // resets the supervisor's backoff so a long-lived tunnel that later
        // drops reconnects from the minimum delay.
        attempt.active();
        // When the canary last (re)opened. A canary that closes and re-opens
        // in QUICK succession is flapping (a dead-but-endpoint-exists pod, or
        // a proxy that resets on connect) — re-opening it in place with no
        // throttle would hammer the apiserver with websocket upgrades at
        // network RTT. So a re-open sooner than PF_BACKOFF_MIN is treated as
        // pod-death: tear down and Continue, which re-engages the
        // supervisor's escalating backoff AND re-resolves to a replacement
        // pod. An OCCASIONAL idle close (>= PF_BACKOFF_MIN apart, e.g. an
        // application keepalive timeout every ~65s) still re-opens in place
        // with live pumps untouched.
        let mut last_canary_open = Instant::now();

        // 4. Serve. Each accepted connection gets its own tunnel to the
        // resolved pod, pumped by a task in this JoinSet — dropping the
        // set (attempt ends or is aborted) aborts every in-flight pump.
        // `canary_pf` is reassigned on canary re-open and held ONLY for its
        // Drop (dropping the Portforwarder closes the tunnel) — its value
        // is never read back, hence the allow.
        let mut pumps: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        let mut sink = [0u8; 64];
        #[allow(unused_assignments)]
        loop {
            tokio::select! {
                read = canary.read(&mut sink) => match read {
                    Ok(n) if n > 0 => {} // stray bytes on the idle canary — ignore
                    _ => {
                        // Ok(0) = idle close; Err = tunnel error.
                        if last_canary_open.elapsed() < PF_BACKOFF_MIN {
                            return OperatorExit::Continue(format!(
                                "tunnel to {} flapping — re-resolving", target.pod,
                            ));
                        }
                        // Re-open IN PLACE — success means the pod is alive and
                        // this was just an idle timeout (listener and every
                        // live pump stay untouched); failure means the pod is
                        // likely gone, so tear down and Continue → the next
                        // attempt re-resolves to a replacement pod.
                        match api.portforward(&target.pod, &[target.port]).await {
                            Ok(mut pf) => match pf.take_stream(target.port) {
                                Some(stream) => {
                                    canary_pf = pf;
                                    canary = stream;
                                    last_canary_open = Instant::now();
                                }
                                None => return OperatorExit::Continue(format!(
                                    "tunnel to {} lost (no stream on re-open)", target.pod,
                                )),
                            },
                            Err(e) => return OperatorExit::Continue(format!(
                                "tunnel to {} lost: {}", target.pod, e,
                            )),
                        }
                    }
                },
                accepted = listener.accept() => match accepted {
                    Ok((tcp, _addr)) => {
                        pumps.spawn(pump(
                            self.req.client.clone(),
                            ns.to_string(),
                            target.pod.clone(),
                            target.port,
                            tcp,
                        ));
                    }
                    Err(e) => {
                        return OperatorExit::Continue(format!("accept failed: {e}"));
                    }
                },
                // Reap finished pumps so the set doesn't grow unboundedly.
                Some(_) = pumps.join_next(), if !pumps.is_empty() => {}
            }
        }
    }
}

/// Pump one local TCP connection over its own websocket tunnel to the pod.
/// The `Portforwarder` stays in scope for the pump's lifetime — dropping it
/// (connection done, or the pump aborted with its attempt) closes the
/// websocket.
async fn pump(
    client: kube::Client,
    ns: String,
    pod: String,
    port: u16,
    mut tcp: tokio::net::TcpStream,
) {
    let api: Api<Pod> = Api::namespaced(client, &ns);
    let mut pf = match api.portforward(&pod, &[port]).await {
        Ok(p) => p,
        Err(e) => {
            tracing::debug!("port-forward pump to {pod}:{port}: {e}");
            return;
        }
    };
    let Some(mut stream) = pf.take_stream(port) else { return };
    let _ = tokio::io::copy_bidirectional(&mut tcp, &mut stream).await;
}

impl LocalResourceSource for PortForwardSource {
    fn resource_id(&self) -> &ResourceId {
        &self.id
    }

    fn headers(&self) -> Vec<String> {
        headers()
    }

    fn grace_gate(&self) -> Option<&crate::grace::GraceGate> {
        // Port-forward sources are held strongly by their `ContextLocals` —
        // a forward's lifetime is the CONTEXT's, not any view of it — so
        // subscription-level grace doesn't apply. `None` tells
        // `LocalSubscription::Drop` to just drop its Arc.
        None
    }

    fn subscribe(&self) -> watch::Receiver<k9rs_proto::protocol::TableBaseline> {
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
        let (current, current_client) = self
            .entries
            .get(&id)
            .map(|slot| (slot.entry.clone(), slot.client.clone()))
            .ok_or_else(|| format!("no port-forward with id {id}"))?;

        let unchanged = current.local_port == edit.local_port
            && current.remote_port == edit.remote_port
            && current.namespace == edit.namespace
            && current.kubectl_target == edit.kubectl_target;
        if unchanged {
            return Ok(format!("pf-{id} unchanged"));
        }

        // Resolve the edited namespace BEFORE tearing anything down: a
        // blanked field must leave the working forward alone, and this is
        // the one place the rule is stated now (`PodNamespace` carries it
        // from here on).
        let namespace = PodNamespace::of(&Namespace::from_row(&edit.namespace))
            .ok_or("namespace field is required for port-forward")?;

        // Reconcile by stop + recreate. The new entry gets a fresh id; the
        // old row disappears from the next snapshot. `create` requires
        // `Arc<Self>` because it downgrades it for the supervised operator
        // it registers — we recover the Arc via the cyclic `self_weak`.
        self.stop(id)?;
        let arc = self.arc_self();
        let new_id = arc.create(PortForwardRequest {
            target: current.target,
            // The edit reconciles by stop-and-recreate, so the replacement
            // inherits the original's context label along with its client —
            // an edit does not move a forward to whoever is editing it.
            context: current.context,
            kubectl_target: edit.kubectl_target,
            namespace,
            local_port: edit.local_port,
            remote_port: edit.remote_port,
            client: current_client,
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
    let age = k9rs_util::units::format_age_duration(entry.started_at.elapsed());
    // Always populated: the entry was built from a `PodNamespace`. The `-`
    // placeholder this used to carry conflated "no namespace" with a
    // cluster-scoped row, and neither is reachable here.
    let ns_display = entry.namespace.as_str();
    let ctx_display = entry.context.as_str();
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
        age: k9rs_util::units::format_age_duration(entry.started_at.elapsed()),
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
    let age = k9rs_util::units::format_age_duration(entry.started_at.elapsed());
    let row_name = format!("pf-{}", entry.id);
    // Exhaustive on purpose (no `_` arm): adding a `PortForwardState`
    // variant must force a decision here, same closed-enum discipline as
    // `LocalResourceKind`. `Reconnecting` maps to the EXISTING
    // `RowHealth::Pending` — `RowHealth` crosses the wire, so a new variant
    // there would be a protocol change; a new state string is not.
    let health = match entry.state {
        PortForwardState::Active => RowHealth::Normal,
        PortForwardState::Starting => RowHealth::Pending,
        PortForwardState::Reconnecting => RowHealth::Pending,
        PortForwardState::Failed => RowHealth::Failed,
    };
    let cells: Vec<CellValue> = vec![
        CellValue::Text(row_name.clone()),
        CellValue::Text(entry.kubectl_target.clone()),
        // Always populated — see `format_describe`.
        CellValue::Text(entry.namespace.clone()),
        CellValue::Count(entry.local_port as i64),
        CellValue::Count(entry.remote_port as i64),
        CellValue::Status { text: entry.state.as_str().to_string(), health },
        CellValue::Text(age),
        CellValue::Text(entry.last_message.clone()),
    ];

    ResourceRow {
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


#[cfg(test)]
#[path = "../tests/local/port_forward.rs"]
mod tests;
