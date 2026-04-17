pub mod handler;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::app::FlashMessage;

/// Top-level event type for the application event loop.
///
/// Input events (keystrokes and ticks — k9rs is keyboard-only) are handled
/// directly by the main event loop via crossterm's `EventStream`. Only
/// resource updates, errors, and flash messages flow through this channel.
pub enum AppEvent {
    /// An update to a Kubernetes resource list or content view.
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
    /// The log stream has ended (daemon mode). Resets the streaming flag.
    LogStreamEnded,
    /// The server resolved an unknown resource to its true identity.
    /// The TUI should update its nav and table keys.
    ResourceResolved {
        original: crate::kube::protocol::ResourceId,
        resolved: crate::kube::protocol::ResourceId,
    },
    /// A subscription failed for a specific resource (e.g., resource doesn't exist).
    SubscriptionFailed {
        resource: crate::kube::protocol::ResourceId,
        message: String,
    },
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

/// An update to a particular Kubernetes resource type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResourceUpdate {
    /// Resource table snapshot — all resource types use this unified format.
    Rows {
        resource: crate::kube::protocol::ResourceId,
        headers: Vec<String>,
        rows: Vec<crate::kube::resources::row::ResourceRow>,
    },
    /// Response to `SessionCommand::Yaml(target)`. Carries the originating
    /// `ObjectRef` so the apply path can gate on "does the current route
    /// target match". Without this, rapid navigation A→B can deliver A's
    /// slower yaml fetch to B's edit state machine and write A's content
    /// to B's temp file.
    Yaml { target: crate::kube::protocol::ObjectRef, content: String },
    /// Response to `SessionCommand::Describe(target)`. Same gating rules
    /// as `Yaml` above.
    Describe { target: crate::kube::protocol::ObjectRef, content: String },
    /// A single log line tagged with the producing stream's generation id.
    /// The TUI's log view stamps its own generation id on the route state
    /// when it opens a new stream and only applies lines whose id matches —
    /// otherwise lines already in flight from a *previous* stream (whose
    /// bridge has been aborted but whose queued events haven't been drained
    /// yet) would bleed into the new view.
    LogLine { generation: u64, line: String },
}

