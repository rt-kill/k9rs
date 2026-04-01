//! Wire protocol types for k9rs daemon communication.
//!
//! All messages are newline-delimited JSON over a Unix socket.
//! Used by both the daemon server and the client library.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use super::cache::CachedCrd;

/// Whether a Kubernetes resource is cluster-scoped or namespace-scoped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceScope {
    Cluster,
    Namespaced,
}

impl ResourceScope {
    /// Parse a scope string ("Cluster" or "Namespaced") into the enum.
    /// Unknown values default to `Namespaced`.
    pub fn from_scope_str(s: &str) -> Self {
        match s {
            "Cluster" => ResourceScope::Cluster,
            _ => ResourceScope::Namespaced,
        }
    }
}



// ---------------------------------------------------------------------------
// Client -> Daemon
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum Request {
    // --- Daemon management ---
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "status")]
    Status,
    #[serde(rename = "shutdown")]
    Shutdown,
    #[serde(rename = "clear")]
    Clear { context: Option<String> },
}

// ---------------------------------------------------------------------------
// Daemon -> Client
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Response {
    #[serde(rename = "ok")]
    Ok,
    #[serde(rename = "error")]
    Error { message: String },
    #[serde(rename = "status")]
    Status(DaemonStatus),
}

// ---------------------------------------------------------------------------
// Payload types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DaemonStatus {
    pub pid: u32,
    pub uptime_secs: u64,
    pub socket_path: String,
}

// ---------------------------------------------------------------------------
// TUI <-> Session (persistent, bidirectional)
// ---------------------------------------------------------------------------

/// TUI -> Session (commands from the TUI over a persistent connection)
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd")]
pub enum SessionCommand {
    /// Start a session with raw kubeconfig + environment variables.
    ///
    /// The TUI reads the kubeconfig from disk, serializes it to YAML, and
    /// collects relevant environment variables. The session (daemon-side)
    /// creates the kube::Client from the kubeconfig, running exec plugins
    /// itself with the forwarded env vars. kube-rs handles token refresh
    /// natively via `RefreshableToken`.
    #[serde(rename = "init")]
    Init {
        context: Option<String>,
        namespace: Option<String>,
        readonly: bool,
        kubeconfig_yaml: String,
        env_vars: HashMap<String, String>,
        cluster_name: String,
        user_name: String,
    },
    /// Subscribe to a resource type (starts/reuses watcher)
    #[serde(rename = "subscribe")]
    Subscribe { resource_type: String },
    /// Unsubscribe from a resource type
    #[serde(rename = "unsubscribe")]
    Unsubscribe { resource_type: String },
    /// Switch namespace (re-subscribes all active watchers)
    #[serde(rename = "switch_namespace")]
    SwitchNamespace { namespace: String },
    /// Switch context (new kube::Client, re-subscribe everything).
    /// Carries raw kubeconfig + env vars — the session creates the client.
    #[serde(rename = "switch_context")]
    SwitchContext {
        context: String,
        kubeconfig_yaml: String,
        env_vars: HashMap<String, String>,
        cluster_name: String,
        user_name: String,
    },
    /// Fetch describe output for a resource
    #[serde(rename = "describe")]
    Describe {
        resource_type: String,
        name: String,
        namespace: String,
    },
    /// Fetch YAML for a resource
    #[serde(rename = "yaml")]
    Yaml {
        resource_type: String,
        name: String,
        namespace: String,
    },
    /// Delete a resource
    #[serde(rename = "delete")]
    Delete {
        resource_type: String,
        name: String,
        namespace: String,
    },
    /// Scale a resource
    #[serde(rename = "scale")]
    Scale {
        resource_type: String,
        name: String,
        namespace: String,
        replicas: u32,
    },
    /// Restart a resource (rolling restart via annotation patch)
    #[serde(rename = "restart")]
    Restart {
        resource_type: String,
        name: String,
        namespace: String,
    },
    /// Start streaming logs for a pod
    #[serde(rename = "stream_logs")]
    StreamLogs {
        pod: String,
        namespace: String,
        container: String,
        follow: bool,
        tail: Option<u64>,
        since: Option<String>,
        previous: bool,
    },
    /// Force-refresh a resource type (kills watcher, re-LISTs from API server)
    #[serde(rename = "refresh")]
    Refresh { resource_type: String },
    /// Stop log streaming
    #[serde(rename = "stop_logs")]
    StopLogs,
    /// Get discovery data (namespaces + CRDs)
    #[serde(rename = "get_discovery")]
    GetDiscovery,
}

/// Session -> TUI (data pushed to the TUI over a persistent connection)
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event")]
pub enum SessionEvent {
    /// Session initialized successfully
    #[serde(rename = "ready")]
    Ready {
        context: String,
        cluster: String,
        user: String,
        namespaces: Vec<String>,
    },
    /// A resource snapshot (full replacement of table data)
    #[serde(rename = "snapshot")]
    Snapshot {
        resource_type: String,
        data: String,
    },
    /// Describe output
    #[serde(rename = "describe_result")]
    DescribeResult { content: String },
    /// YAML output
    #[serde(rename = "yaml_result")]
    YamlResult { content: String },
    /// Command result (delete, scale, restart)
    #[serde(rename = "command_result")]
    CommandResult { ok: bool, message: String },
    /// A log line
    #[serde(rename = "log_line")]
    LogLine { line: String },
    /// Log stream ended
    #[serde(rename = "log_end")]
    LogEnd,
    /// Discovery data
    #[serde(rename = "discovery")]
    Discovery {
        context: String,
        namespaces: Vec<String>,
        crds: Vec<CachedCrd>,
    },
    /// Context switch completed (daemon -> TUI acknowledgment)
    #[serde(rename = "context_switched")]
    ContextSwitched {
        context: String,
        ok: bool,
        message: String,
    },
    /// Error
    #[serde(rename = "error")]
    SessionError { message: String },
    /// Pod metrics: HashMap<"ns/name", (cpu, mem)> serialized as JSON
    #[serde(rename = "pod_metrics")]
    PodMetrics { data: String },
    /// Node metrics: HashMap<name, (cpu, mem)> serialized as JSON
    #[serde(rename = "node_metrics")]
    NodeMetrics { data: String },
}

