//! The muxer's contract, over an in-memory transport.
//!
//! This crate had NO tests. It is the thing both ends of the socket depend
//! on, and the one place a transport bug shows up as "the TUI went quiet" —
//! so the properties the rest of the codebase assumes are worth stating
//! here, where a `tokio::io::duplex` pair is the whole fixture and no socket
//! or daemon is involved.

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A client/server pair over an in-memory duplex — the same transport
/// `--no-daemon` uses.
fn pair() -> (MuxedConnection, MuxedConnection) {
    let (a, b) = tokio::io::duplex(64 * 1024);
    (MuxedConnection::client(a), MuxedConnection::server(b))
}

/// A substream opened on one side arrives on the other, and bytes written
/// into it come out in order.
#[tokio::test]
async fn a_substream_carries_bytes_end_to_end() {
    let (client, mut server) = pair();

    let mut out = client.open().await.expect("open");
    out.write_all(b"hello mux").await.expect("write");
    out.flush().await.expect("flush");

    let mut inbound = server.accept().await.expect("the substream arrived");
    let mut buf = [0u8; 9];
    inbound.read_exact(&mut buf).await.expect("read");
    assert_eq!(&buf, b"hello mux");
}

/// Substreams are INDEPENDENT: that is the entire reason this crate exists
/// (one subscription per substream), and interleaved writes must not mix.
#[tokio::test]
async fn two_substreams_do_not_mix_their_bytes() {
    let (client, mut server) = pair();

    let mut a = client.open().await.expect("open a");
    let mut b = client.open().await.expect("open b");
    // Interleaved on purpose.
    a.write_all(b"aaaa").await.expect("write a");
    b.write_all(b"bbbb").await.expect("write b");
    a.write_all(b"AAAA").await.expect("write a again");
    b.write_all(b"BBBB").await.expect("write b again");
    a.flush().await.expect("flush a");
    b.flush().await.expect("flush b");

    let mut first = server.accept().await.expect("first substream");
    let mut second = server.accept().await.expect("second substream");
    let mut fb = [0u8; 8];
    let mut sb = [0u8; 8];
    first.read_exact(&mut fb).await.expect("read first");
    second.read_exact(&mut sb).await.expect("read second");

    // Accept order follows open order, so `first` is `a`.
    assert_eq!(&fb, b"aaaaAAAA", "one substream's bytes, in order and only its own");
    assert_eq!(&sb, b"bbbbBBBB");
}

/// Dropping a substream ends it for the peer — and what the peer SEES is a
/// reset, not EOF.
///
/// The module doc says "Drop sends an RST frame on the substream
/// automatically … The peer's read returns EOF." Measured, it returns
/// `ConnectionReset`, and bytes written-and-flushed before the drop are NOT
/// guaranteed to arrive: an RST discards what is still in flight. Both halves
/// matter, in opposite directions:
///
/// - A reader coded against EOF sees an `Err` instead, so "the peer hung up"
///   has to be read off an error kind rather than off a clean end.
/// - A writer must not treat `write_all` + `flush` + drop as "delivered".
///   The subscription teardown path is fine with that — it drops BECAUSE it
///   is done caring — but it is not a thing to lean on.
#[tokio::test]
async fn dropping_a_substream_resets_it_rather_than_closing_it() {
    let (client, mut server) = pair();

    let mut out = client.open().await.expect("open");
    out.write_all(b"delivered").await.expect("write");
    out.flush().await.expect("flush");

    let mut inbound = server.accept().await.expect("arrived");
    // Read BEFORE the drop: these bytes are genuinely across.
    let mut buf = [0u8; 9];
    inbound.read_exact(&mut buf).await.expect("read");
    assert_eq!(&buf, b"delivered");

    drop(out);

    // Now the peer learns the substream is gone. Not EOF (`Ok(0)`) — a reset.
    let mut after = [0u8; 1];
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        inbound.read(&mut after),
    )
    .await
    .expect("the peer is told, rather than hanging")
    .expect_err("a dropped substream is a RESET, not a clean end");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::ConnectionReset,
        "the module doc claimed EOF here; it is this",
    );
}

/// A handle opens substreams from any task, and they are the same muxer's.
#[tokio::test]
async fn a_cloned_handle_opens_on_the_same_connection() {
    let (client, mut server) = pair();
    let handle = client.handle();

    let joined = tokio::spawn(async move {
        let mut s = handle.open().await.expect("open from another task");
        s.write_all(b"from a task").await.expect("write");
        s.flush().await.expect("flush");
        // Hold it open until the test has read.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    });

    let mut inbound = server.accept().await.expect("arrived");
    let mut buf = [0u8; 11];
    inbound.read_exact(&mut buf).await.expect("read");
    assert_eq!(&buf, b"from a task");
    joined.await.expect("task");
}

/// When the transport goes away, `accept` returns `None` rather than
/// blocking — the signal the daemon's accept loop needs to tear a session
/// down instead of parking on it.
#[tokio::test]
async fn a_closed_transport_ends_the_accept_loop() {
    let (a, b) = tokio::io::duplex(1024);
    let client = MuxedConnection::client(a);
    let mut server = MuxedConnection::server(b);

    drop(client);

    let ended = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        server.accept(),
    )
    .await
    .expect("accept resolves rather than hanging");
    assert!(ended.is_none(), "a dead transform yields no more substreams");
}

/// The window is large enough to move a message the protocol considers
/// legitimate without a flow-control stall. The `>=` relationship to
/// `k9rs_proto`'s frame cap is asserted in `k9rs_daemon` (this crate has no
/// protocol dependency); what is checkable HERE is that a payload of that
/// size actually goes through.
#[tokio::test]
async fn a_large_payload_crosses_one_substream() {
    // 1 MiB: many yamux frames, and far more than the 64 KiB in-memory
    // transport underneath — so this really exercises the flow control the
    // window exists to stay ahead of. Quick enough to keep in the suite.
    let payload: Vec<u8> = (0..1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();

    // `client` stays in the test body on purpose: dropping a
    // `MuxedConnection` stops its driver task, and every substream it opened
    // dies with it. Moving it into the writer task made this fail instantly
    // rather than after the timeout, which is how I found that out.
    let (client, mut server) = pair();
    let mut out = client.open().await.expect("open");
    let writer = tokio::spawn(async move {
        out.write_all(&payload).await.expect("write");
        out.flush().await.expect("flush");
        out
    });

    let mut inbound = server.accept().await.expect("arrived");
    let mut got = vec![0u8; expected.len()];
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        inbound.read_exact(&mut got),
    )
    .await
    .expect("a 1 MiB payload moves without stalling")
    .expect("read");
    assert_eq!(got, expected, "byte-identical across the muxer");
    let _held = writer.await.expect("writer");
}

/// A peer gets [`MAX_INBOUND_SUBSTREAMS`] substreams and not one more.
///
/// yamux checks the cap only when an inbound `SYN` arrives, so this is a
/// property of what the OTHER side can make us hold — and the number it
/// used to be was 65,535, the library default, left alone while
/// `MAX_STREAM_WINDOW` was raised 256×. Over the cap, yamux answers
/// `ACK|RST` and never surfaces the substream: `accept` simply never
/// yields it, which is why the assertion is a timeout.
#[tokio::test]
async fn a_peer_cannot_open_more_substreams_than_the_cap() {
    let (client, mut server) = pair();

    // Hold every accepted substream. The cap counts LIVE substreams, so
    // dropping them as they arrive would never reach it.
    let mut opened = Vec::with_capacity(MAX_INBOUND_SUBSTREAMS);
    let mut accepted = Vec::with_capacity(MAX_INBOUND_SUBSTREAMS);
    for i in 0..MAX_INBOUND_SUBSTREAMS {
        opened.push(client.open().await.expect("open under the cap"));
        accepted.push(
            tokio::time::timeout(std::time::Duration::from_secs(5), server.accept())
                .await
                .unwrap_or_else(|_| panic!("substream {i} should arrive — it is under the cap"))
                .expect("the connection is alive"),
        );
    }
    assert_eq!(accepted.len(), MAX_INBOUND_SUBSTREAMS, "every substream up to the cap arrives");

    // One more. The open itself is local — the client mints the id and
    // sends the SYN without waiting — so the refusal shows up as the
    // substream never reaching the server.
    let mut over_cap = client.open().await.expect("the open is local, so it succeeds");
    let refused = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        server.accept(),
    )
    .await;
    assert!(
        refused.is_err(),
        "the substream over the cap must never be accepted, got {:?}",
        refused.map(|s| s.is_some()),
    );

    // And the peer is told, rather than left writing into a void.
    let mut buf = [0u8; 1];
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        over_cap.read(&mut buf),
    )
    .await
    .expect("the reset arrives rather than hanging")
    .expect_err("a refused substream is reset");
    assert_eq!(err.kind(), std::io::ErrorKind::ConnectionReset);
}
