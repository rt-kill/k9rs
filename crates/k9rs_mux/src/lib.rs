//! Multiplexer abstraction over the TUI ↔ daemon Unix socket.
//!
//! # Why this module exists
//!
//! The TUI talks to the daemon over a single `UnixStream`. We want each
//! subscription (and each log stream, and the control channel) to be its
//! own bidirectional substream so that:
//!
//! - Subscribing twice to the same resource with different filters does
//!   not require a routing tag in the protocol — each subscription is its
//!   own substream and reads only its own bytes.
//! - Dropping a `MuxedStream` cleanly closes its substream (the underlying
//!   muxer sends a close frame, the peer's read returns EOF). Late events
//!   from the daemon are dropped at the muxer layer; the TUI's consumer
//!   never sees them.
//! - The wire grammar inside each substream stays plain bincode — the
//!   muxer is invisible to `read_bincode`/`write_bincode`.
//!
//! # Why hide the implementation behind this module
//!
//! Right now we use [`tokio_yamux`] (a tokio-native port of the Hashicorp
//! Yamux spec). The rest of the codebase imports nothing from
//! `tokio_yamux` directly — only [`MuxedConnection`] and [`MuxedStream`]
//! from this module. Swapping to a different muxer (libp2p's `yamux`,
//! a hand-rolled stream-id frame format, anything else) means rewriting
//! exactly this file. The public surface stays identical, the rest of the
//! project is untouched.
//!
//! # Design
//!
//! Construction wraps the socket in a [`tokio_yamux::Session`] and spawns
//! a **driver task** that polls the session forward. The driver:
//!
//! - Drives the underlying yamux state machine (handshake, keepalive,
//!   window updates, frame I/O).
//! - Forwards inbound substreams to an `mpsc::Receiver` so [`accept`]
//!   can hand them out one at a time.
//! - Stays alive until the underlying `UnixStream` closes or the
//!   [`MuxedConnection`] is dropped (whichever happens first).
//!
//! Outbound substream opens go through `tokio_yamux::Control`, which is
//! cloneable and submits commands to the driver task via an internal
//! channel. The driver task processes those alongside the inbound flow.
//!
//! Drop semantics:
//! - [`MuxedConnection`]'s `Drop` aborts the driver task → the underlying
//!   socket closes → the peer sees EOF.
//! - [`MuxedStream`]'s `Drop` (via `tokio_yamux::StreamHandle`'s own Drop)
//!   sends an RST frame → the peer's read returns EOF / `BrokenPipe`.
//!
//! [`accept`]: MuxedConnection::accept

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;
use tokio_stream::StreamExt as _;

/// Bidirectional multiplexed transport between two peers. Owns the
/// underlying `UnixStream` (consumed at construction) and a driver task
/// that pumps the multiplexer state machine.
///
/// Cloneable across the codebase via [`MuxedConnection::handle`] which
/// returns a cheap [`MuxHandle`] for opening outbound streams from
/// arbitrary tasks. The connection itself is not `Clone` because it owns
/// the inbound-stream receiver.
pub struct MuxedConnection {
    /// Async outbound-open handle. Cloneable; clones can be passed to
    /// arbitrary tasks that need to open substreams.
    control: tokio_yamux::Control,
    /// Inbound substreams forwarded by the driver task.
    inbound_rx: mpsc::Receiver<MuxedStream>,
    /// Aborts the driver task on drop. Closing the driver in turn closes
    /// the underlying socket, sending an EOF to the peer.
    _driver: k9rs_util::AbortOnDrop,
}

/// A cheap clone-able handle for opening outbound substreams from any task.
/// Constructed via [`MuxedConnection::handle`]. Holds a clone of the
/// muxer's `Control`, which sends commands to the driver task; the actual
/// muxer state lives in the driver.
#[derive(Clone)]
pub struct MuxHandle {
    control: tokio_yamux::Control,
}

/// One bidirectional substream. Implements tokio's [`AsyncRead`] +
/// [`AsyncWrite`] by delegating to the underlying yamux substream, so
/// `protocol::read_bincode` / `write_bincode` work on it without changes.
///
/// Drop sends an RST frame on the substream automatically (handled inside
/// `tokio_yamux::StreamHandle::Drop`), and the peer's next read fails with
/// `ConnectionReset`.
///
/// NOT EOF — this said "returns EOF" until a test measured it. The
/// difference does not reach any caller today, because every reader goes
/// through `read_exact`, which answers `Err` for a clean end too, and every
/// call site treats `Err` as "the stream is over". But an RST also discards
/// whatever is still in flight, so `write_all` + `flush` + drop is not a
/// delivery: the teardown path is fine with that because it drops when it
/// has stopped caring, and nothing else should lean on it.
/// See `dropping_a_substream_resets_it_rather_than_closing_it`.
pub struct MuxedStream {
    inner: tokio_yamux::StreamHandle,
}

/// How much one substream may have in flight before yamux's flow control
/// makes the sender wait.
///
/// At least `k9rs_proto::protocol::MAX_MESSAGE_SIZE`, or a message that the
/// protocol considers legitimate — exactly at its cap — stalls here instead
/// of moving, and the write timeout above eventually kills the substream for
/// a frame nothing was wrong with.
///
/// Deliberately NOT imported from `k9rs_proto`: this crate has no protocol
/// dependency, which is the whole reason it is a crate rather than part of
/// the daemon. So the relationship between the two numbers is asserted from
/// `k9rs_daemon`, which sees both.
pub const MAX_STREAM_WINDOW: u32 = 64 * 1024 * 1024;

/// How many substreams a PEER may have open on one connection. Outbound
/// opens are not affected — yamux checks this only when an inbound `SYN`
/// arrives — so this is a bound on what the other side can make us hold.
///
/// It is a *derived* number, because the count and the window are one
/// decision and not two. yamux's default count of 65,535 was chosen
/// against yamux's default 256 KiB window, which puts the product — how
/// much a peer may make us buffer — at about 16 GiB.
/// [`MAX_STREAM_WINDOW`] raises the window 256×; leaving the count at its
/// default raises that product 256× with it, to roughly 4 TiB, which is
/// not a decision anyone made. Dividing the count by the same factor
/// keeps the product the library picked.
///
/// The value today is 255, and ~an order of magnitude above legitimate
/// use: a session holds one control substream plus one per live
/// subscription and one per log stream, so a deep nav stack with logs
/// open is tens.
pub const MAX_INBOUND_SUBSTREAMS: usize = (tokio_yamux::config::DEFAULT_MAX_STREAM_COUNT
    * tokio_yamux::config::INITIAL_STREAM_WINDOW as usize)
    / MAX_STREAM_WINDOW as usize;

// The derivation above divides by the window, so a future window bump
// shrinks the substream budget — silently, and quadratically if someone
// raises the window to keep a big snapshot moving. A 1 GiB window would
// leave 15 substreams, which a single session exceeds. Fail the build
// instead: at that point the product is the thing to re-decide, in the
// open, rather than to discover as "the TUI stops opening views".
const _: () = assert!(
    MAX_INBOUND_SUBSTREAMS >= 64,
    "the derived substream budget has fallen below what one session uses — re-decide the window/count product",
);

/// Yamux configuration tuned for the k9rs workload. Large clusters (9k+
/// pods) produce multi-MB snapshots per subscription. The default 256 KB
/// stream window forces ~24 window-update round-trips per snapshot; if the
/// receiver is slow to drain (CPU busy sorting/rendering), the sender's
/// write stalls and the 10-second connection_write_timeout can fire,
/// silently killing the substream.
///
/// We bump the window to 64 MB so a single large snapshot fits without
/// flow-control pauses, and extend the write timeout so transient stalls
/// on very large clusters don't kill the connection. Raising the window
/// is half a decision: see [`MAX_INBOUND_SUBSTREAMS`] for the other half.
fn mux_config() -> tokio_yamux::Config {
    tokio_yamux::Config {
        max_stream_window_size: MAX_STREAM_WINDOW,
        max_stream_count: MAX_INBOUND_SUBSTREAMS,
        connection_write_timeout: std::time::Duration::from_secs(60),
        ..tokio_yamux::Config::default()
    }
}

impl MuxedConnection {
    /// Construct the **client** side of the muxer over any async
    /// bidirectional transport (Unix socket, in-memory duplex for
    /// `--no-daemon`, etc.). Spawns the driver task immediately; the
    /// connection is usable as soon as this returns.
    pub fn client<T>(socket: T) -> Self
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_session(tokio_yamux::Session::new_client(socket, mux_config()))
    }

    /// Construct the **server** side. The daemon uses this on each
    /// accepted connection from a TUI session.
    pub fn server<T>(socket: T) -> Self
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        Self::from_session(tokio_yamux::Session::new_server(socket, mux_config()))
    }

    /// Internal constructor — wires the session into the driver task and
    /// returns a usable `MuxedConnection`. The session itself is moved
    /// into the driver and never re-exposed.
    fn from_session<T>(mut session: tokio_yamux::Session<T>) -> Self
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let control = session.control();
        let (inbound_tx, inbound_rx) = mpsc::channel::<MuxedStream>(16);
        let driver = tokio::spawn(async move {
            // Drive the session forward. `next()` makes wire-protocol
            // progress *and* yields inbound substreams. We forward each
            // accepted substream into `inbound_tx`. When the underlying
            // socket closes (or the session shuts down), `next()` returns
            // `None` and we exit. Errors are logged and we keep going as
            // long as the session is still alive.
            while let Some(item) = session.next().await {
                match item {
                    Ok(handle) => {
                        if inbound_tx.send(MuxedStream { inner: handle }).await.is_err() {
                            // Receiver gone — connection has been dropped.
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("muxed connection: session error: {}", e);
                        break;
                    }
                }
            }
            tracing::debug!("muxed connection: driver task exiting");
        });
        Self {
            control,
            inbound_rx,
            _driver: k9rs_util::AbortOnDrop::new(driver.abort_handle()),
        }
    }

    /// Get a clone-able handle for opening outbound substreams from
    /// arbitrary tasks. Each clone shares the same underlying muxer; they
    /// don't fight each other.
    pub fn handle(&self) -> MuxHandle {
        MuxHandle { control: self.control.clone() }
    }

    /// Open a fresh outbound substream. Asynchronous: sends a SYN through
    /// the muxer driver and waits for the ACK. Returns once the substream
    /// is established and ready for I/O.
    pub async fn open(&self) -> io::Result<MuxedStream> {
        let mut control = self.control.clone();
        let handle = control.open_stream().await
            .map_err(|e| io::Error::other(format!("yamux open: {}", e)))?;
        Ok(MuxedStream { inner: handle })
    }

    /// Receive the next inbound substream. Returns `None` when the
    /// underlying connection has closed (peer hung up, driver task died,
    /// etc.) and there will be no more substreams.
    pub async fn accept(&mut self) -> Option<MuxedStream> {
        self.inbound_rx.recv().await
    }
}

impl MuxHandle {
    /// Open a fresh outbound substream from this handle.
    pub async fn open(&self) -> io::Result<MuxedStream> {
        let mut control = self.control.clone();
        let handle = control.open_stream().await
            .map_err(|e| io::Error::other(format!("yamux open: {}", e)))?;
        Ok(MuxedStream { inner: handle })
    }
}

// AsyncRead / AsyncWrite delegate straight through to the inner yamux
// StreamHandle, which already implements tokio's traits. No compat
// shim, no copying — the muxer is transparent at the I/O level.

impl AsyncRead for MuxedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for MuxedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
#[path = "tests/lib.rs"]
mod tests;
