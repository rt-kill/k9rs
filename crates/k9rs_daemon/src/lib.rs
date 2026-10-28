//! The daemon: one process per machine, holding watchers and live streams so
//! the TUI can come and go.
//!
//! It owns the Unix socket, the per-context `ServerSession`, the fan-out of a
//! watcher's deltas into per-subscription substreams, and the long-lived
//! things a client would otherwise lose on every restart — port-forwards,
//! exec sessions, log streams.
//!
//! That last sentence is why it does not decide to stop on its own. A
//! self-exit on "my binary was replaced" lived here briefly; it meant a
//! rebuild tore down live work that had nothing to do with the rebuild.
//! Restarting is the user's call, and the handshake's build-id check tells
//! them when it is needed.

pub mod daemon;
pub mod server_session;

// ---------------------------------------------------------------------------
// A cross-crate invariant, checked at BUILD time
// ---------------------------------------------------------------------------

/// The muxer's stream window must admit a message the protocol calls
/// legitimate.
///
/// Two 64 MiB constants in two crates that must not drift apart:
/// [`k9rs_proto::protocol::MAX_MESSAGE_SIZE`] is the largest frame the
/// protocol will DECODE, and [`k9rs_mux::MAX_STREAM_WINDOW`] is how much one
/// substream may have in flight before yamux's flow control makes the sender
/// wait. If the window were the smaller, a frame nothing is wrong with would
/// stall behind flow control until the connection write timeout killed its
/// substream.
///
/// Asserted HERE because this crate depends on both, and neither depends on
/// the other — `k9rs_mux` carries no protocol dependency, which is the whole
/// reason it is a crate rather than part of this one. A `const` assertion
/// rather than a test: both sides are compile-time constants, so drift can
/// be a build failure instead of something a test run has to notice.
const _: () = assert!(
    k9rs_mux::MAX_STREAM_WINDOW >= k9rs_proto::protocol::MAX_MESSAGE_SIZE,
    "the yamux stream window must admit the protocol's largest frame",
);
