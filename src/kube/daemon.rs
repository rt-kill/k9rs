//! Lock-free cache daemon for k9rs, powered by moka.
//!
//! moka::Cache is a concurrent, lock-free cache with TTL and eviction.
//! Connection handlers read/write the cache directly — no actor, no channels,
//! no mutexes. Idle tracking via atomic counter.
//!
//! - `k9rs --daemon` starts the daemon (auto-exits after idle timeout)
//! - Normal k9rs instances connect as clients
//! - Falls back to direct file cache if daemon is unavailable

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use moka::future::Cache;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, info};

use super::cache::{self, CachedCrd, DiscoveryCache};

// ---------------------------------------------------------------------------
// Protocol
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op")]
enum Request {
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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Response {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<CachePayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// Serialized resource list data (for get_resources/put_resources).
    #[serde(skip_serializing_if = "Option::is_none")]
    resource_data: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct CachePayload {
    namespaces: Vec<String>,
    crds: Vec<CachedCrd>,
}

// ---------------------------------------------------------------------------
// Socket path
// ---------------------------------------------------------------------------

pub fn socket_path() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(dir).join("k9rs.sock");
    }
    #[cfg(unix)]
    {
        let uid = unsafe { libc::getuid() };
        PathBuf::from(format!("/tmp/k9rs-{}.sock", uid))
    }
    #[cfg(not(unix))]
    {
        PathBuf::from("/tmp/k9rs.sock")
    }
}

// ---------------------------------------------------------------------------
// Daemon server
// ---------------------------------------------------------------------------

const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Run the cache daemon. moka handles all concurrency — no locks.
pub async fn run_daemon() -> anyhow::Result<()> {
    let path = socket_path();

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let listener = match UnixListener::bind(&path) {
        Ok(l) => l,
        Err(_) => {
            if UnixStream::connect(&path).await.is_ok() {
                anyhow::bail!("Daemon already running");
            }
            let _ = std::fs::remove_file(&path);
            UnixListener::bind(&path)?
        }
    };
    info!("k9rs cache daemon listening on {:?}", path);

    // Signal handler for clean shutdown
    let signal_path = path.clone();
    tokio::spawn(async move {
        let mut sigterm = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        ).expect("SIGTERM handler");
        let mut sigint = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::interrupt(),
        ).expect("SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
        info!("Daemon signal — cleaning up");
        let _ = std::fs::remove_file(&signal_path);
        std::process::exit(0);
    });

    // moka cache: lock-free, concurrent, TTL-based eviction
    // 1 hour TTL per entry, max 1000 contexts
    let store: Cache<String, DiscoveryCache> = Cache::builder()
        .max_capacity(1000)
        .time_to_idle(Duration::from_secs(3600))
        .build();

    // Resource list cache: stores serialized resource data for instant tab switches
    // Key: "context:namespace:resource_type", Value: JSON string
    let resource_store: Cache<String, String> = Cache::builder()
        .max_capacity(500)
        .time_to_idle(Duration::from_secs(600)) // 10 min TTL
        .build();

    // Seed cache from disk
    if let Some(cache_dir) = cache::cache_dir() {
        if let Ok(entries) = std::fs::read_dir(&cache_dir) {
            for entry in entries.flatten() {
                if entry.path().extension().map_or(false, |e| e == "json") {
                    if let Ok(data) = std::fs::read_to_string(entry.path()) {
                        if let Ok(dc) = serde_json::from_str::<DiscoveryCache>(&data) {
                            store.insert(dc.context.clone(), dc).await;
                        }
                    }
                }
            }
        }
    }

    let last_activity = Arc::new(AtomicU64::new(now_millis()));

    // Idle timeout
    let idle_activity = last_activity.clone();
    let idle_path = path.clone();
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_listen = shutdown.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let last = idle_activity.load(Ordering::Relaxed);
            let now = now_millis();
            if now.saturating_sub(last) > IDLE_TIMEOUT.as_millis() as u64 {
                info!("Daemon idle timeout — shutting down");
                let _ = std::fs::remove_file(&idle_path);
                shutdown.notify_one();
                return;
            }
        }
    });

    // Accept loop — each connection handler reads/writes moka directly
    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, _) = accept_result?;
                last_activity.store(now_millis(), Ordering::Relaxed);
                let conn_store = store.clone();
                let conn_res_store = resource_store.clone();
                let conn_activity = last_activity.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, conn_store, conn_res_store, conn_activity).await {
                        debug!("Connection error: {}", e);
                    }
                });
            }
            _ = shutdown_listen.notified() => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let _ = std::fs::remove_file(&path);
                return Ok(());
            }
        }
    }
}

/// Handle a single connection. Reads/writes moka cache directly — no
/// indirection, no channels, no locks.
async fn handle_connection(
    stream: UnixStream,
    store: Cache<String, DiscoveryCache>,
    resource_store: Cache<String, String>,
    last_activity: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Some(line) = lines.next_line().await? {
        let request: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let resp = Response {
                    ok: false,
                    data: None,
                    error: Some(format!("Invalid request: {}", e)),
                    resource_data: None,
                };
                write_response(&mut writer, &resp).await?;
                continue;
            }
        };

        last_activity.store(now_millis(), Ordering::Relaxed);

        let resp = match request {
            Request::Ping => Response { ok: true, data: None, error: None, resource_data: None },

            Request::Get { context } => {
                let payload = store.get(&context).await.map(|dc| CachePayload {
                    namespaces: dc.namespaces.clone(),
                    crds: dc.crds.clone(),
                }).unwrap_or(CachePayload {
                    namespaces: vec![],
                    crds: vec![],
                });
                Response { ok: true, data: Some(payload), error: None, resource_data: None }
            }

            Request::Put { context, namespaces, crds } => {
                let dc = cache::build_cache(&context, &namespaces, &crds);
                let dc_for_disk = dc.clone();
                store.insert(context, dc).await;
                // Persist to disk in background (atomic write)
                tokio::spawn(async move {
                    cache::save_cache(&dc_for_disk).await;
                });
                Response { ok: true, data: None, error: None, resource_data: None }
            }

            Request::GetResources { context, namespace, resource_type } => {
                let key = format!("{}:{}:{}", context, namespace, resource_type);
                Response {
                    ok: true,
                    data: None,
                    error: None,
                    resource_data: resource_store.get(&key).await,
                }
            }

            Request::PutResources { context, namespace, resource_type, data } => {
                let key = format!("{}:{}:{}", context, namespace, resource_type);
                resource_store.insert(key, data).await;
                Response { ok: true, data: None, error: None, resource_data: None }
            }
        };

        write_response(&mut writer, &resp).await?;
    }

    Ok(())
}

async fn write_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    resp: &Response,
) -> anyhow::Result<()> {
    let mut buf = serde_json::to_string(resp)?;
    buf.push('\n');
    writer.write_all(buf.as_bytes()).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct DaemonClient {
    stream: Option<UnixStream>,
}

impl DaemonClient {
    pub async fn connect() -> Option<Self> {
        let path = socket_path();
        match UnixStream::connect(&path).await {
            Ok(stream) => Some(Self { stream: Some(stream) }),
            Err(_) => None,
        }
    }

    pub async fn connect_or_spawn() -> Option<Self> {
        if let Some(client) = Self::connect().await {
            return Some(client);
        }

        let exe = std::env::current_exe().ok()?;
        let child = std::process::Command::new(&exe)
            .arg("--daemon")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();

        if child.is_err() {
            debug!("Failed to spawn daemon");
            return None;
        }

        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            if let Some(client) = Self::connect().await {
                return Some(client);
            }
        }
        debug!("Daemon started but connection timed out");
        None
    }

    pub async fn get(&mut self, context: &str) -> Option<(Vec<String>, Vec<CachedCrd>)> {
        let req = serde_json::json!({"op": "get", "context": context});
        let resp = self.send_request(&req).await?;
        if resp.ok {
            resp.data.map(|d| (d.namespaces, d.crds))
        } else {
            None
        }
    }

    /// Send a raw JSON request and get the raw response. Used by CLI.
    pub async fn send_raw(&mut self, req: &serde_json::Value) -> Option<serde_json::Value> {
        let stream = self.stream.as_mut()?;
        let mut buf = serde_json::to_string(req).ok()?;
        buf.push('\n');
        stream.write_all(buf.as_bytes()).await.ok()?;

        let mut resp_buf = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            match stream.try_read(&mut byte) {
                Ok(0) => return None,
                Ok(1) => {
                    if byte[0] == b'\n' { break; }
                    resp_buf.push(byte[0]);
                }
                _ => { stream.readable().await.ok()?; }
            }
        }
        serde_json::from_slice(&resp_buf).ok()
    }

    pub async fn put(
        &mut self,
        context: &str,
        namespaces: &[String],
        crds: &[CachedCrd],
    ) -> bool {
        let req = serde_json::json!({
            "op": "put",
            "context": context,
            "namespaces": namespaces,
            "crds": crds,
        });
        self.send_request(&req).await.map_or(false, |r| r.ok)
    }

    pub async fn get_resources(
        &mut self,
        context: &str,
        namespace: &str,
        resource_type: &str,
    ) -> Option<String> {
        let req = serde_json::json!({
            "op": "get_resources",
            "context": context,
            "namespace": namespace,
            "resource_type": resource_type,
        });
        let resp = self.send_request(&req).await?;
        if resp.ok {
            resp.resource_data
        } else {
            None
        }
    }

    pub async fn put_resources(
        &mut self,
        context: &str,
        namespace: &str,
        resource_type: &str,
        data: &str,
    ) -> bool {
        let req = serde_json::json!({
            "op": "put_resources",
            "context": context,
            "namespace": namespace,
            "resource_type": resource_type,
            "data": data,
        });
        self.send_request(&req).await.map_or(false, |r| r.ok)
    }

    async fn send_request(&mut self, req: &serde_json::Value) -> Option<Response> {
        let stream = self.stream.as_mut()?;
        let mut buf = serde_json::to_string(req).ok()?;
        buf.push('\n');
        stream.write_all(buf.as_bytes()).await.ok()?;

        let mut resp_buf = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            match stream.try_read(&mut byte) {
                Ok(0) => return None,
                Ok(1) => {
                    if byte[0] == b'\n' {
                        break;
                    }
                    resp_buf.push(byte[0]);
                }
                _ => {
                    stream.readable().await.ok()?;
                }
            }
        }
        serde_json::from_slice(&resp_buf).ok()
    }
}
