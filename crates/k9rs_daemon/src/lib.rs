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
