//! The accept path's handshake deadline.
//!
//! `UnixStream::pair()` is the whole fixture: no socket file, no listener,
//! no daemon process. Time is paused, so the deadline these tests wait on
//! costs nothing — the runtime advances the clock once every task is idle,
//! which is exactly the state a stalled peer puts us in.

use super::*;
use tokio::io::AsyncWriteExt;

fn state() -> Arc<DaemonState> {
    Arc::new(DaemonState {
        started_at: Instant::now(),
        socket_path: "/nonexistent/k9rs-test.sock".to_string(),
        shutdown: tokio::sync::Notify::new(),
        session_shared: Arc::new(SessionSharedState::new_single_session()),
    })
}

/// A peer that connects and then says nothing is dropped.
///
/// It used to be held forever: the connection-type byte was a bare
/// `read_exact`, so the handler task and the socket's fd stayed parked with
/// no error, no log line and nothing to see but the daemon's fd count.
#[tokio::test(start_paused = true)]
async fn a_peer_that_never_declares_itself_is_dropped() {
    let (daemon_side, _peer) = tokio::net::UnixStream::pair().expect("socketpair");
    // `_peer` is held: an fd that is still open is the whole point. Dropping
    // it would end the read with EOF, which was never the problem.

    let err = handle_connection(daemon_side, state())
        .await
        .expect_err("a silent peer must not be served forever");
    assert!(
        err.to_string().contains("did not send a connection-type byte"),
        "got {err}",
    );
}

/// A peer that declares itself and then stalls mid-handshake is dropped too.
///
/// The management door writes its version preamble first and then reads the
/// peer's — so a peer that sends the type byte and stops leaves us blocked
/// on a read that will never complete.
#[tokio::test(start_paused = true)]
async fn a_management_peer_that_stalls_mid_handshake_is_dropped() {
    let (daemon_side, mut peer) = tokio::net::UnixStream::pair().expect("socketpair");
    peer.write_all(&[CONN_TYPE_MANAGEMENT]).await.expect("declare, then stall");

    let err = handle_connection(daemon_side, state())
        .await
        .expect_err("a peer that stops mid-handshake must not be served forever");
    assert!(
        err.to_string().contains("did not finish its handshake"),
        "got {err}",
    );
}

/// A well-behaved peer is NOT caught by the deadline: the type byte arrives,
/// and the handshake completes. Without this the two tests above would pass
/// against a deadline of zero.
#[tokio::test]
async fn a_peer_that_speaks_promptly_is_served() {
    let (daemon_side, peer) = tokio::net::UnixStream::pair().expect("socketpair");
    let served = tokio::spawn(handle_connection(daemon_side, state()));

    let (reader, writer) = peer.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);
    writer.write_all(&[CONN_TYPE_MANAGEMENT]).await.expect("declare");
    protocol::write_handshake(&mut writer).await.expect("preamble");
    protocol::read_handshake(&mut reader).await.expect("the daemon's preamble");
    protocol::write_bincode(&mut writer, &SessionCommand::Ping).await.expect("a command");

    let reply: SessionEvent = protocol::read_bincode(&mut reader).await.expect("a reply");
    assert!(
        matches!(reply, SessionEvent::CommandResult(Ok(ref s)) if s == "pong"),
        "got {reply:?}",
    );
    served.await.expect("handler").expect("served cleanly");
}

// ---------------------------------------------------------------------------
// The socket path's planted-file refusal
// ---------------------------------------------------------------------------

/// A directory that removes itself. No `tempfile` dependency for four
/// tests, and nothing here leaves the fixture directory.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("k9rs-sock-test-{tag}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("fixture dir");
        Self(dir)
    }
    fn join(&self, name: &str) -> std::path::PathBuf {
        self.0.join(name)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A stale socket we own is what the recovery path exists for.
#[test]
fn a_stale_socket_of_ours_may_be_removed() {
    let dir = TempDir::new("ours");
    let path = dir.join("k9rs.sock");
    let listener = std::os::unix::net::UnixListener::bind(&path).expect("bind");
    // Dropping the listener leaves the FILE behind — exactly the stale state.
    drop(listener);

    let our_uid = unsafe { libc::getuid() };
    assert!(
        check_stale_socket_is_ours(&path, our_uid).is_ok(),
        "our own leftover socket is removable",
    );
}

/// A regular file at the socket path is not ours to delete. This is the
/// crude version of the attack and the one a wrong `remove_file` would
/// happily carry out.
#[test]
fn a_regular_file_at_the_socket_path_is_refused() {
    let dir = TempDir::new("file");
    let path = dir.join("k9rs.sock");
    std::fs::write(&path, b"not a socket").expect("write");

    let err = check_stale_socket_is_ours(&path, unsafe { libc::getuid() })
        .expect_err("a regular file must not be unlinked");
    assert!(err.to_string().contains("not a Unix socket"), "got {err}");
}

/// THE attack: a symlink pointing at something else, at a path whose parent
/// is world-traversable. `symlink_metadata` does not follow it, so the file
/// type is a symlink and not the socket it points at — which is why the
/// check must not use plain `metadata`.
#[test]
fn a_symlink_to_a_real_socket_is_still_refused() {
    let dir = TempDir::new("symlink");
    let real = dir.join("real.sock");
    let planted = dir.join("k9rs.sock");
    let listener = std::os::unix::net::UnixListener::bind(&real).expect("bind");
    drop(listener);
    std::os::unix::fs::symlink(&real, &planted).expect("symlink");

    let err = check_stale_socket_is_ours(&planted, unsafe { libc::getuid() })
        .expect_err("a symlink must not be unlinked, even pointing at a socket");
    assert!(
        err.to_string().contains("not a Unix socket"),
        "the refusal names the file type it actually saw: {err}",
    );
}

/// A socket owned by another uid is refused by owner, with both uids named
/// so the operator can see whose it is.
#[test]
fn a_socket_owned_by_someone_else_is_refused() {
    let dir = TempDir::new("otheruid");
    let path = dir.join("k9rs.sock");
    let listener = std::os::unix::net::UnixListener::bind(&path).expect("bind");
    drop(listener);

    // We cannot chown without privileges, so ask the question as a
    // DIFFERENT uid — which is also the only way to reach this arm at all.
    let our_uid = unsafe { libc::getuid() };
    let err = check_stale_socket_is_ours(&path, our_uid.wrapping_add(1))
        .expect_err("another user's socket must not be unlinked");
    let msg = err.to_string();
    assert!(msg.contains("owned by uid"), "got {msg}");
    assert!(msg.contains(&our_uid.to_string()), "names the owner: {msg}");
}

/// Nothing there at all is an error, not a silent pass — the caller only
/// reaches this after `bind` said the address was in use, so an empty path
/// means the world changed underneath it.
#[test]
fn an_absent_path_is_an_error_rather_than_a_pass() {
    let dir = TempDir::new("absent");
    assert!(
        check_stale_socket_is_ours(&dir.join("nothing.sock"), unsafe { libc::getuid() }).is_err(),
    );
}
