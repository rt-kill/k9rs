//! Wire protocol types for k9rs daemon communication.
//!
//! All messages are newline-delimited JSON over a Unix socket.
//! Used by both the daemon server and the client library.

use serde::{Deserialize, Serialize};

use super::cache::CachedCrd;

// ---------------------------------------------------------------------------
// Client -> Daemon
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum Request {
    // --- Discovery cache ---
    #[serde(rename = "ping")]
    Ping,
    #[serde(rename = "get")]
    Get { context: String },
    #[serde(rename = "put")]
    Put {
        context: String,
        #[serde(default)]
        namespaces: Vec<String>,
        #[serde(default)]
        crds: Vec<CachedCrd>,
    },

    // --- Resource list cache ---
    #[serde(rename = "get_resources")]
    GetResources {
        context: String,
        namespace: String,
        resource_type: String,
    },
    #[serde(rename = "put_resources")]
    PutResources {
        context: String,
        namespace: String,
        resource_type: String,
        data: String,
    },

    // --- Daemon management ---
    #[serde(rename = "status")]
    Status,
    #[serde(rename = "stats")]
    Stats,
    #[serde(rename = "shutdown")]
    Shutdown,
    #[serde(rename = "clear")]
    Clear { context: Option<String> },

    // --- Session tracking ---
    #[serde(rename = "register_session")]
    RegisterSession {
        pid: u32,
        context: String,
        namespace: String,
    },
    #[serde(rename = "deregister_session")]
    DeregisterSession { session_id: String },
    #[serde(rename = "update_session")]
    UpdateSession {
        session_id: String,
        context: String,
        namespace: String,
    },
    #[serde(rename = "list_sessions")]
    ListSessions,
    #[serde(rename = "list_watchers")]
    ListWatchers,
}

// ---------------------------------------------------------------------------
// Daemon -> Client
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<CachePayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<DaemonStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sessions: Option<Vec<SessionInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<CacheStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

impl Response {
    pub fn ok() -> Self {
        Self { ok: true, data: None, error: None, resource_data: None, status: None, sessions: None, stats: None, session_id: None }
    }
    pub fn error(msg: impl Into<String>) -> Self {
        Self { ok: false, data: None, error: Some(msg.into()), resource_data: None, status: None, sessions: None, stats: None, session_id: None }
    }
}

// ---------------------------------------------------------------------------
// Payload types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CachePayload {
    pub namespaces: Vec<String>,
    pub crds: Vec<CachedCrd>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DaemonStatus {
    pub pid: u32,
    pub uptime_secs: u64,
    pub socket_path: String,
    pub session_count: usize,
    pub discovery_entries: u64,
    pub resource_entries: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub pid: u32,
    pub context: String,
    pub namespace: String,
    pub connected_at: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CacheStats {
    pub discovery_entry_count: u64,
    pub discovery_max_capacity: u64,
    pub resource_entry_count: u64,
    pub resource_max_capacity: u64,
}
