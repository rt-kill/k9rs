//! Daemon process for k9rs.
//!
//! Accepts TUI connections and spawns a ServerSession per client. Holds shared
//! state (WatcherCache via DashMap) that sessions interact with for watcher
//! sharing. The daemon itself has no k8s knowledge — all k8s logic lives in
//! ServerSession.
//!
//! - Start with `k9rs daemon` (stays running until killed / Ctrl-C / SIGTERM)
//! - TUI clients connect via Unix socket, send kubeconfig + env vars
//! - Management CLI (`k9rs ctl`) connects for status/ping/shutdown

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{UnixListener, UnixStream};
use tracing::info;

use super::protocol::{DaemonStatus, Request, Response};
use super::server_session::{ServerSession, SessionSharedState};

// ---------------------------------------------------------------------------
// Daemon configuration constants
// ---------------------------------------------------------------------------

/// Timeout for reading a response from the daemon (seconds).
const CLIENT_READ_TIMEOUT_SECS: u64 = 5;

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

    // Daemon state
    let state = Arc::new(DaemonState {
        started_at: Instant::now(),
        socket_path: path.display().to_string(),
        shutdown: tokio::sync::Notify::new(),
        session_shared: Arc::new(SessionSharedState::new()),
    });

    // Accept loop — exits on signal or shutdown request
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result?;
                info!("New connection accepted");
                let conn_state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, conn_state).await {
                        info!("Connection ended: {}", e);
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

struct DaemonState {
    started_at: Instant,
    socket_path: String,
    shutdown: tokio::sync::Notify,
    /// Shared state for all ServerSessions (watcher cache + client pool).
    /// The daemon holds this opaquely — it never accesses the fields.
    session_shared: Arc<SessionSharedState>,
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

async fn handle_connection(
    stream: UnixStream,
    state: Arc<DaemonState>,
) -> anyhow::Result<()> {
    let (reader, writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);

    // Read the first line to determine connection type:
    //   - `"cmd":` => session connection (hand off to ServerSession)
    //   - `"op":`  => management connection (handle here)
    let mut first_line = String::new();
    let n = buf_reader.read_line(&mut first_line).await?;
    if n == 0 {
        return Ok(()); // EOF immediately
    }
    let first_line = first_line.trim_end().to_string();

    if !first_line.contains("\"op\"") {
        info!("Routing connection as TUI session");
        // Session connection — hand off entirely to ServerSession.
        // The daemon does NOT parse SessionCommand or construct SessionEvent.
        // Box the buf_reader directly (it already has an internal buffer and
        // implements AsyncRead, preserving any read-ahead data after first_line).
        let boxed_reader: Box<dyn tokio::io::AsyncRead + Unpin + Send> = Box::new(buf_reader);
        let boxed_reader = BufReader::with_capacity(256 * 1024, boxed_reader);
        let boxed_writer: Box<dyn tokio::io::AsyncWrite + Unpin + Send> = Box::new(writer);
        ServerSession::init_and_run(
            first_line,
            boxed_reader,
            boxed_writer,
            state.session_shared.clone(),
        )
        .await;
        return Ok(());
    }

    // Management connection — process with Request/Response protocol.
    let mut writer = writer;

    // Process the first line as a Request.
    match serde_json::from_str::<Request>(&first_line) {
        Ok(request) => {
            let resp = handle_request(&state, request).await;
            write_response(&mut writer, &resp).await?;
        }
        Err(e) => {
            write_response(
                &mut writer,
                &Response::Error {
                    message: format!("Invalid request: {}", e),
                },
            )
            .await?;
        }
    }

    // Continue processing management requests on this connection.
    let mut lines = buf_reader.lines();
    while let Some(line) = lines.next_line().await? {
        let request: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                write_response(&mut writer, &Response::Error { message: format!("Invalid request: {}", e) }).await?;
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
        Request::Ping => Response::Ok,

        Request::Status => {
            Response::Status(DaemonStatus {
                pid: std::process::id(),
                uptime_secs: state.started_at.elapsed().as_secs(),
                socket_path: state.socket_path.clone(),
            })
        }

        Request::Shutdown => {
            state.shutdown.notify_one();
            Response::Ok
        }

        Request::Clear { context } => {
            match context {
                Some(ctx) => {
                    state.session_shared.discovery_cache.remove(&ctx);
                }
                None => {
                    state.session_shared.discovery_cache.clear();
                }
            }
            Response::Ok
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

    /// Send a typed `Request` and get a typed `Response`.
    pub async fn request(&mut self, req: &Request) -> Option<Response> {
        let line = self.send_and_read(req).await?;
        serde_json::from_str(&line).ok()
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
            Duration::from_secs(CLIENT_READ_TIMEOUT_SECS),
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

