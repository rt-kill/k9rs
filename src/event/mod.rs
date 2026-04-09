pub mod handler;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::app::FlashMessage;

/// Top-level event type for the application event loop.
///
/// Input events (key, mouse, tick) are handled directly by the main event loop
/// via crossterm's `EventStream`. Only resource updates, errors, and flash
/// messages flow through this channel.
pub enum AppEvent {
    /// An update to a Kubernetes resource list or content view.
    ResourceUpdate(ResourceUpdate),
    /// A temporary flash message shown in the status bar.
    Flash(FlashMessage),
    /// Result of a background context switch. Contains the context name and
    /// success/failure. On success the server has already switched contexts
    /// and the `ClientSession` will update its stored context info.
    ContextSwitchResult {
        context: String,
        result: Result<(), String>,
    },
    /// Pod metrics from the metrics-server.
    PodMetrics(HashMap<crate::kube::protocol::ObjectKey, crate::kube::protocol::MetricsUsage>),
    /// Node metrics from the metrics-server.
    NodeMetrics(HashMap<String, crate::kube::protocol::MetricsUsage>),
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
    /// Resource capabilities from the server (which operations are supported).
    ResourceCapabilities {
        resource: crate::kube::protocol::ResourceId,
        capabilities: crate::kube::protocol::ResourceCapabilities,
    },
    /// The daemon connection was lost. TUI should exit gracefully.
    DaemonDisconnected,
    /// The daemon handshake completed and the session is ready. The TUI should
    /// populate context/cluster/user info and trigger any initial subscriptions.
    ConnectionEstablished {
        context: String,
        cluster: String,
        user: String,
        namespaces: Vec<String>,
    },
    /// The daemon handshake failed. The TUI should exit with this error.
    ConnectionFailed(String),
    /// The kubeconfig was read in the background. Lets the TUI populate the
    /// contexts panel and `:ctx <tab>` completion before the daemon answers,
    /// and gives the context-switch path the cached YAML/env it needs to
    /// build a new `SwitchContext` payload without going back to disk.
    KubeconfigLoaded {
        contexts: Vec<crate::app::KubeContext>,
        current_context: String,
        current_cluster: String,
        current_user: String,
        kubeconfig_yaml: String,
        env_vars: HashMap<String, String>,
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
    Yaml(String),
    Describe(String),
    LogLine(String),
}

