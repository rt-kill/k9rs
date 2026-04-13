//! Daemon process for k9rs.
//!
//! All connections use the same binary protocol (length-prefixed bincode).
//! The first command determines connection type:
//! - `Init` → long-lived TUI session (handed off to ServerSession)
//! - `Ping`/`Status`/`Shutdown`/`Clear` → one-shot management request

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use tokio::io::{BufReader, BufWriter};
use tokio::net::{UnixListener, UnixStream};
use tracing::info;

use super::protocol::{self, DaemonStatus, SessionCommand, SessionEvent};
use super::server_session::{ServerSession, SessionSharedState};

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
                anyhow::bail!("Daemon already running at {}", path.display());
            }
            let _ = std::fs::remove_file(&path);
            UnixListener::bind(&path)?
        }
    };
    info!("k9rs cache daemon listening on {:?}", path);

    let mut sigterm = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::terminate(),
    )?;
    let mut sigint = tokio::signal::unix::signal(
        tokio::signal::unix::SignalKind::interrupt(),
    )?;

    let state = Arc::new(DaemonState {
        started_at: Instant::now(),
        socket_path: path.display().to_string(),
        shutdown: tokio::sync::Notify::new(),
        session_shared: Arc::new(SessionSharedState::new()),
    });

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
    session_shared: Arc<SessionSharedState>,
}

// ---------------------------------------------------------------------------
// Connection handler (unified binary protocol)
// ---------------------------------------------------------------------------

/// Connection-type discriminator written by every client as the very
/// first byte after connecting. The daemon reads it to decide whether
/// to wrap the socket in yamux (TUI session) or speak plain bincode
/// (management CLI).
pub(crate) const CONN_TYPE_SESSION: u8 = 0x01;
pub(crate) const CONN_TYPE_MANAGEMENT: u8 = 0x02;

async fn handle_connection(
    stream: UnixStream,
    state: Arc<DaemonState>,
) -> anyhow::Result<()> {
    use tokio::io::AsyncReadExt;

    // First byte: connection type discriminator.
    let mut conn_type = [0u8; 1];
    let mut peek_stream = stream;
    peek_stream.read_exact(&mut conn_type).await?;

    match conn_type[0] {
        // Yamux-multiplexed TUI session.
        CONN_TYPE_SESSION => {
            info!("Routing connection as yamux TUI session");
            let mux = crate::kube::mux::MuxedConnection::server(peek_stream);
            ServerSession::init_and_run_muxed(mux, state.session_shared.clone()).await;
        }

        // Plain bincode management request (k9rs ctl).
        CONN_TYPE_MANAGEMENT => {
            let (reader, writer) = peek_stream.into_split();
            let mut reader = BufReader::with_capacity(protocol::IO_BUFFER_SIZE, reader);
            let mut writer = BufWriter::with_capacity(protocol::IO_BUFFER_SIZE, writer);
            let first_cmd: SessionCommand = protocol::read_bincode(&mut reader).await?;
            handle_management_command(first_cmd, &mut writer, &state).await?;
        }

        other => {
            tracing::warn!("Unknown connection type byte: 0x{:02x}", other);
        }
    }
    Ok(())
}

/// Handle a one-shot management command from `k9rs ctl`.
async fn handle_management_command(
    cmd: SessionCommand,
    writer: &mut BufWriter<tokio::net::unix::OwnedWriteHalf>,
    state: &Arc<DaemonState>,
) -> anyhow::Result<()> {
    match cmd {

        // Management: one-shot commands
        SessionCommand::Ping => {
            protocol::write_bincode(writer, &SessionEvent::CommandResult {
                ok: true,
                message: "pong".to_string(),
            }).await?;
        }

        SessionCommand::Status => {
            protocol::write_bincode(writer, &SessionEvent::DaemonStatus(DaemonStatus {
                pid: std::process::id(),
                uptime_secs: state.started_at.elapsed().as_secs(),
                socket_path: state.socket_path.clone(),
            })).await?;
        }

        SessionCommand::Shutdown => {
            protocol::write_bincode(writer, &SessionEvent::CommandResult {
                ok: true,
                message: "shutting down".to_string(),
            }).await?;
            state.shutdown.notify_one();
        }

        SessionCommand::Clear { context } => {
            match &context {
                Some(_) => { state.session_shared.discovery_cache.clear(); }
                None => { state.session_shared.discovery_cache.clear(); }
            }
            protocol::write_bincode(writer, &SessionEvent::CommandResult {
                ok: true,
                message: "cache cleared".to_string(),
            }).await?;
        }

        other => {
            protocol::write_bincode(writer, &SessionEvent::SessionError(
                format!("Expected Init or management command, got: {:?}", other),
            )).await?;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Client (unified binary protocol)
// ---------------------------------------------------------------------------

pub struct DaemonClient {
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    writer: BufWriter<tokio::net::unix::OwnedWriteHalf>,
}

impl DaemonClient {
    /// Connect to the daemon. Returns None if the daemon isn't running.
    /// Writes the `CONN_TYPE_MANAGEMENT` discriminator byte immediately so
    /// the daemon's accept loop routes this connection to the plain-bincode
    /// management handler (not the yamux session path).
    pub async fn connect() -> Option<Self> {
        use tokio::io::AsyncWriteExt;
        let path = socket_path();
        let mut stream = UnixStream::connect(&path).await.ok()?;
        stream.write_all(&[CONN_TYPE_MANAGEMENT]).await.ok()?;
        let (read_half, write_half) = stream.into_split();
        Some(Self {
            reader: BufReader::new(read_half),
            writer: BufWriter::new(write_half),
        })
    }

    /// Send a command and read the response (binary).
    pub async fn request(&mut self, cmd: &SessionCommand) -> Option<SessionEvent> {
        protocol::write_bincode(&mut self.writer, cmd).await.ok()?;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            protocol::read_bincode::<_, SessionEvent>(&mut self.reader),
        ).await;
        match result {
            Ok(Ok(event)) => Some(event),
            _ => None,
        }
    }
}
