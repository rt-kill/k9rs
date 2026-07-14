pub mod handler;

use std::collections::HashMap;


use crate::app::FlashMessage;

/// Top-level event type for the application event loop.
///
/// Input events (keystrokes and ticks — k9rs is keyboard-only) are handled
/// directly by the main event loop via crossterm's `EventStream`. Only
/// resource updates, errors, and flash messages flow through this channel.
pub enum AppEvent {
    /// A table-stream event, carrying its DESTINATION: the bridge that
    /// produced it holds the `Arc<RowStore>` it was handed at subscribe
    /// time, so there is no routing and nothing to mis-route — an event
    /// for a popped element lands in an unreferenced store and frees it
    /// when the queue drains. The event loop is the sole applier (the
    /// store's single-writer convention).
    Store(StoreEvent),
    /// A content-view update (yaml / describe / log line).
    ResourceUpdate(ResourceUpdate),
    /// A server-sent `CommandResult` — the response to a mutating wire
    /// command (Apply, Delete, Scale, Restart, Decode, Trigger, Toggle).
    /// Previously these were collapsed into `Flash`, but that made the
    /// edit-flow terminal state ambiguous: any unrelated flash popped the
    /// edit route out of `EditState::Applying` prematurely. Keeping them
    /// distinct lets the apply path react only to its own response.
    ///
    /// `Result<String, String>` rather than `{ ok, message }` so readers
    /// have to branch on success/failure before touching the message —
    /// the struct shape let convention-based code leak.
    CommandResult(Result<String, String>),
    /// A temporary flash message shown in the status bar. Produced by
    /// purely local TUI events (key bindings, nav, info messages).
    Flash(FlashMessage),
    /// Pod metrics from the metrics-server.
    PodMetrics(HashMap<crate::kube::protocol::ObjectKey, crate::kube::protocol::MetricsUsage>),
    /// Node metrics from the metrics-server.
    NodeMetrics(HashMap<crate::kube::protocol::NodeName, crate::kube::protocol::MetricsUsage>),
    /// A log-stream event for its destination line store — same
    /// destination-rides-the-event shape as [`AppEvent::Store`].
    Log(LogEvent),
    /// The server resolved an unknown resource to its true identity.
    /// The TUI should update its nav and table keys.
    ResourceResolved {
        original: crate::kube::protocol::ResourceId,
        resolved: crate::kube::protocol::ResourceId,
    },
    /// A subscription failed for a specific resource. FLASH-ONLY: the
    /// failing bridge separately delivers `StorePayload::Failed` to its
    /// own store (epoch-gated), which is what flips the element's state —
    /// this event just surfaces the message regardless of which view is
    /// active.
    SubscriptionFailed {
        resource: crate::kube::protocol::ResourceId,
        message: String,
    },
    /// Cached discovery data from the daemon (namespace names + CRDs),
    /// typed — the event loop converts to rows and seeds the app-level
    /// core stores (completion / picker sources).
    Discovery {
        namespaces: Vec<String>,
        crds: Vec<crate::kube::cache::CachedCrd>,
    },
    /// Raw terminal bytes from an exec session (daemon → TUI).
    /// Buffered during Connecting, written directly to stdout during
    /// bridge mode.
    ExecData(Vec<u8>),
    /// The exec stream ended (daemon closed the substream).
    ExecEnded,
    /// The daemon connection was lost. TUI should exit gracefully.
    DaemonDisconnected,
    /// The daemon handshake completed and the session is ready. The TUI should
    /// populate context/cluster/user info and trigger any initial subscriptions.
    ConnectionEstablished {
        context: crate::kube::protocol::ContextName,
        identity: crate::kube::protocol::ClusterIdentity,
        namespaces: Vec<String>,
    },
    /// The daemon handshake failed. The TUI should exit with this error.
    ConnectionFailed(String),
    /// The kubeconfig was read in the background. Lets the TUI populate the
    /// contexts panel and `:ctx <tab>` completion before the daemon answers.
    KubeconfigLoaded {
        contexts: Vec<crate::app::KubeContext>,
        current_context: crate::kube::protocol::ContextName,
        current_identity: crate::kube::protocol::ClusterIdentity,
    },
}

/// A table-stream event plus its destination store. CLIENT-INTERNAL:
/// deliberately NOT serde-derived (the wire carries
/// [`crate::kube::protocol::StreamEvent`]; the bridge maps into this).
/// The `epoch` stamps which stream instance produced it — the store's
/// epoch floor drops events from superseded streams targeting the same
/// store (refresh / reconnect succession).
pub struct StoreEvent {
    pub store: std::sync::Arc<crate::app::store::RowStore>,
    pub epoch: u64,
    pub payload: crate::app::store::StorePayload,
}

impl std::fmt::Debug for StoreEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreEvent").field("epoch", &self.epoch).finish_non_exhaustive()
    }
}

/// A log-stream event plus its destination line store. CLIENT-INTERNAL.
pub struct LogEvent {
    pub store: std::sync::Arc<crate::app::store::LineStore>,
    pub epoch: u64,
    pub payload: LogPayload,
}

pub enum LogPayload {
    /// One typed log line (ANSI passthrough; the renderer parses SGR).
    Line(crate::kube::protocol::LogLine),
    /// The daemon closed the log substream.
    Ended,
}

impl std::fmt::Debug for LogEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogEvent").field("epoch", &self.epoch).finish_non_exhaustive()
    }
}

/// A content-view update (yaml / describe / log line).
///
/// CLIENT-INTERNAL: deliberately NOT serde-derived. The missing serde
/// derives are the compile-time guarantee this enum can never sneak back
/// onto the wire.
#[derive(Debug, Clone)]
pub enum ResourceUpdate {
    /// Response to `SessionCommand::Yaml(target)`. Carries the originating
    /// `ObjectRef` so the apply path can gate on "does the current route
    /// target match". Without this, rapid navigation A→B can deliver A's
    /// slower yaml fetch to B's edit state machine and write A's content
    /// to B's temp file.
    Yaml { target: crate::kube::protocol::ObjectRef, content: String },
    /// Response to `SessionCommand::Describe(target)`. Same gating rules
    /// as `Yaml` above. Typed lines (not a flat `String`) so the UI renders by
    /// the producer's role tags instead of re-inferring structure from text.
    Describe { target: crate::kube::protocol::ObjectRef, lines: Vec<crate::kube::protocol::DescribeLine> },
}

