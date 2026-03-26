//! Lock-free cache daemon for k9rs, powered by moka.
//!
//! moka::Cache is a concurrent, lock-free cache with TTL and eviction.
//! Connection handlers read/write the cache directly — no actor, no channels,
//! no mutexes.
//!
//! - Start with `k9rs --daemon` (stays running until killed / Ctrl-C / SIGTERM)
//! - Normal k9rs instances connect as clients
//! - Falls back to direct file cache if daemon is unavailable

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use moka::future::Cache;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::RwLock;
use tracing::{debug, info};

use super::cache::{self, CachedCrd, DiscoveryCache};
use super::protocol::*;

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

/// Run the cache daemon. Stays alive until killed (Ctrl-C / SIGTERM).
/// moka handles all concurrency — no locks.
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

    // Signal handlers for clean shutdown
    let mut sigterm = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::terminate(),
    )?;
    let mut sigint = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::interrupt(),
    )?;

    // Daemon state: caches + session tracking
    let state = Arc::new(DaemonState {
        started_at: Instant::now(),
        discovery_store: Cache::builder()
            .max_capacity(1000)
            .time_to_idle(Duration::from_secs(3600))
            .build(),
        resource_store: Cache::builder()
            .max_capacity(500)
            .time_to_idle(Duration::from_secs(600))
            .support_invalidation_closures()
            .build(),
        sessions: RwLock::new(HashMap::new()),
        socket_path: path.display().to_string(),
        shutdown: tokio::sync::Notify::new(),
    });

    // Seed discovery cache from disk
    if let Some(cache_dir) = cache::cache_dir() {
        if let Ok(entries) = std::fs::read_dir(&cache_dir) {
            for entry in entries.flatten() {
                if entry.path().extension().map_or(false, |e| e == "json") {
                    if let Ok(data) = std::fs::read_to_string(entry.path()) {
                        if let Ok(dc) = serde_json::from_str::<DiscoveryCache>(&data) {
                            state.discovery_store.insert(dc.context.clone(), dc).await;
                        }
                    }
                }
            }
        }
    }

    // Accept loop — exits on signal or shutdown request
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result?;
                let conn_state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, conn_state).await {
                        debug!("Connection error: {}", e);
                    }
                });
            }
            _ = sigterm.recv() => {
                info!("Daemon received SIGTERM — shutting down");
                break;
            }
            _ = sigint.recv() => {
                info!("Daemon received SIGINT — shutting down");
                break;
            }
            _ = state.shutdown.notified() => {
                info!("Daemon received shutdown request — shutting down");
                break;
            }
        }
    }

    // Clean shutdown: remove socket, let tokio drain in-flight tasks
    let _ = std::fs::remove_file(&path);
    info!("Daemon stopped");
    Ok(())
}

// ---------------------------------------------------------------------------
// Daemon state
// ---------------------------------------------------------------------------

pub struct DaemonState {
    pub started_at: Instant,
    pub discovery_store: Cache<String, DiscoveryCache>,
    pub resource_store: Cache<String, String>,
    pub sessions: RwLock<HashMap<String, SessionInfo>>,
    pub socket_path: String,
    pub shutdown: tokio::sync::Notify,
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

async fn handle_connection(
    stream: UnixStream,
    state: Arc<DaemonState>,
) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Some(line) = lines.next_line().await? {
        let request: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                write_response(&mut writer, &Response::error(format!("Invalid request: {}", e))).await?;
                continue;
            }
        };

        let resp = handle_request(&state, request).await;
        write_response(&mut writer, &resp).await?;
    }

    Ok(())
}

async fn handle_request(state: &DaemonState, request: Request) -> Response {
    match request {
        Request::Ping => Response::ok(),

        Request::Get { context } => {
            let payload = state.discovery_store.get(&context).await.map(|dc| CachePayload {
                namespaces: dc.namespaces.clone(),
                crds: dc.crds.clone(),
            }).unwrap_or(CachePayload {
                namespaces: vec![],
                crds: vec![],
            });
            let mut resp = Response::ok();
            resp.data = Some(payload);
            resp
        }

        Request::Put { context, namespaces, crds } => {
            let dc = cache::build_cache(&context, &namespaces, &crds);
            let dc_for_disk = dc.clone();
            state.discovery_store.insert(context, dc).await;
            tokio::spawn(async move {
                cache::save_cache(&dc_for_disk).await;
            });
            Response::ok()
        }

        Request::GetResources { context, namespace, resource_type } => {
            let key = format!("{}:{}:{}", context, namespace, resource_type);
            let mut resp = Response::ok();
            resp.resource_data = state.resource_store.get(&key).await;
            resp
        }

        Request::PutResources { context, namespace, resource_type, data } => {
            let key = format!("{}:{}:{}", context, namespace, resource_type);
            state.resource_store.insert(key, data).await;
            Response::ok()
        }

        Request::Status => {
            let sessions = state.sessions.read().await;
            let mut resp = Response::ok();
            resp.status = Some(DaemonStatus {
                pid: std::process::id(),
                uptime_secs: state.started_at.elapsed().as_secs(),
                socket_path: state.socket_path.clone(),
                session_count: sessions.len(),
                discovery_entries: state.discovery_store.entry_count(),
                resource_entries: state.resource_store.entry_count(),
            });
            resp
        }

        Request::Stats => {
            let mut resp = Response::ok();
            resp.stats = Some(CacheStats {
                discovery_entry_count: state.discovery_store.entry_count(),
                discovery_max_capacity: state.discovery_store.policy().max_capacity().unwrap_or(0),
                resource_entry_count: state.resource_store.entry_count(),
                resource_max_capacity: state.resource_store.policy().max_capacity().unwrap_or(0),
            });
            resp
        }

        Request::Shutdown => {
            state.shutdown.notify_one();
            Response::ok()
        }

        Request::Clear { context } => {
            match context {
                Some(ctx) => {
                    state.discovery_store.remove(&ctx).await;
                    // Clear all resource entries for this context (prefix match)
                    // moka doesn't support prefix delete, so we invalidate matching entries
                    state.resource_store.invalidate_entries_if(move |key, _| {
                        key.starts_with(&format!("{}:", ctx))
                    }).ok();
                }
                None => {
                    state.discovery_store.invalidate_all();
                    state.resource_store.invalidate_all();
                }
            }
            Response::ok()
        }

        Request::RegisterSession { pid, context, namespace } => {
            let session_id = format!("{:x}", rand::random::<u64>());
            let info = SessionInfo {
                session_id: session_id.clone(),
                pid,
                context,
                namespace,
                connected_at: chrono::Utc::now().to_rfc3339(),
            };
            state.sessions.write().await.insert(session_id.clone(), info);
            let mut resp = Response::ok();
            resp.session_id = Some(session_id);
            resp
        }

        Request::DeregisterSession { session_id } => {
            state.sessions.write().await.remove(&session_id);
            Response::ok()
        }

        Request::UpdateSession { session_id, context, namespace } => {
            let mut sessions = state.sessions.write().await;
            if let Some(session) = sessions.get_mut(&session_id) {
                session.context = context;
                session.namespace = namespace;
            }
            Response::ok()
        }

        Request::ListSessions => {
            let sessions = state.sessions.read().await;
            let mut resp = Response::ok();
            resp.sessions = Some(sessions.values().cloned().collect());
            resp
        }

        Request::ListWatchers => {
            // Placeholder — watchers will be daemon-managed in a future phase
            Response::ok()
        }
    }
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

use tokio::io::BufWriter;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};

pub struct DaemonClient {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
}

impl DaemonClient {
    /// Connect to the daemon. Returns None if the daemon isn't running.
    pub async fn connect() -> Option<Self> {
        let path = socket_path();
        let stream = UnixStream::connect(&path).await.ok()?;
        let (read_half, write_half) = stream.into_split();
        Some(Self {
            reader: BufReader::new(read_half),
            writer: BufWriter::new(write_half),
        })
    }

    pub async fn get(&mut self, context: &str) -> Option<(Vec<String>, Vec<CachedCrd>)> {
        let req = Request::Get { context: context.to_string() };
        let resp = self.request(&req).await?;
        if resp.ok {
            resp.data.map(|d| (d.namespaces, d.crds))
        } else {
            None
        }
    }

    /// Send a typed `Request` and get a typed `Response`.
    pub async fn request(&mut self, req: &Request) -> Option<Response> {
        let line = self.send_and_read(req).await?;
        serde_json::from_str(&line).ok()
    }

    pub async fn put(
        &mut self,
        context: &str,
        namespaces: &[String],
        crds: &[CachedCrd],
    ) -> bool {
        let req = Request::Put {
            context: context.to_string(),
            namespaces: namespaces.to_vec(),
            crds: crds.to_vec(),
        };
        self.request(&req).await.map_or(false, |r| r.ok)
    }

    pub async fn get_resources(
        &mut self,
        context: &str,
        namespace: &str,
        resource_type: &str,
    ) -> Option<String> {
        let req = Request::GetResources {
            context: context.to_string(),
            namespace: namespace.to_string(),
            resource_type: resource_type.to_string(),
        };
        let resp = self.request(&req).await?;
        if resp.ok { resp.resource_data } else { None }
    }

    pub async fn put_resources(
        &mut self,
        context: &str,
        namespace: &str,
        resource_type: &str,
        data: &str,
    ) -> bool {
        let req = Request::PutResources {
            context: context.to_string(),
            namespace: namespace.to_string(),
            resource_type: resource_type.to_string(),
            data: data.to_string(),
        };
        self.request(&req).await.map_or(false, |r| r.ok)
    }

    /// Core I/O: write a JSON request line, read a JSON response line.
    /// Uses buffered I/O (not byte-at-a-time) and a 5-second timeout
    /// to prevent hanging on a dead daemon.
    async fn send_and_read(&mut self, req: &impl serde::Serialize) -> Option<String> {
        use tokio::io::AsyncWriteExt as _;

        let mut buf = serde_json::to_string(req).ok()?;
        buf.push('\n');
        self.writer.write_all(buf.as_bytes()).await.ok()?;
        self.writer.flush().await.ok()?;

        let mut resp_line = String::new();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            self.reader.read_line(&mut resp_line),
        ).await;

        match result {
            Ok(Ok(0)) => None,          // EOF
            Ok(Ok(_)) => {
                if resp_line.ends_with('\n') {
                    resp_line.pop();
                }
                Some(resp_line)
            }
            Ok(Err(_)) => None,         // I/O error
            Err(_) => None,             // timeout
        }
    }
}
