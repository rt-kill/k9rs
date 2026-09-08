use super::*;
use crate::kube::protocol::ContextName;

fn ctx(n: &str) -> ContextId {
    ContextId::new(ContextName::new(n.to_string()).unwrap(), format!("https://{n}"), 1)
}

fn locals(grace: Option<Duration>) -> ContextKeepalive {
    ContextKeepalive(ContextLocals::new(ctx("t"), Arc::new(Vec::new()), grace))
}

#[tokio::test]
async fn no_grace_mode_drops_immediately() {
    let k = locals(None);
    let weak = Arc::downgrade(&k.0);
    drop(k);
    assert!(weak.upgrade().is_none(), "--no-daemon: last detach = teardown");
}

#[tokio::test]
async fn grace_window_allows_recovery_then_expires() {
    let k = locals(Some(Duration::from_millis(60)));
    let weak = Arc::downgrade(&k.0);
    drop(k);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let recovered = weak.upgrade();
    assert!(recovered.is_some(), "within grace the slice is recoverable");
    drop(recovered); // re-drop WITHOUT a keepalive: no new bump
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(weak.upgrade().is_none(), "after grace the slice is gone");
}

#[tokio::test]
async fn deadline_measures_from_last_dropper() {
    // k1 drops at t=0 (deadline 100ms); k2 drops at t=60 (deadline
    // 160ms). At t=120 — past k1's window — the slice must still be
    // alive; at t=240 it must be gone.
    let k1 = locals(Some(Duration::from_millis(100)));
    let k2 = k1.clone();
    let weak = Arc::downgrade(&k1.0);
    drop(k1);
    tokio::time::sleep(Duration::from_millis(60)).await;
    drop(k2);
    tokio::time::sleep(Duration::from_millis(60)).await; // t≈120
    assert!(weak.upgrade().is_some(), "second dropper extended the window");
    tokio::time::sleep(Duration::from_millis(120)).await; // t≈240
    assert!(weak.upgrade().is_none(), "extended window expired");
}

#[tokio::test]
async fn reattach_within_grace_keeps_slice_alive_past_expiry() {
    let k = locals(Some(Duration::from_millis(40)));
    let weak = Arc::downgrade(&k.0);
    drop(k);
    // "Reconnect": upgrade within the window and hold a new keepalive.
    let re = ContextKeepalive(weak.upgrade().expect("still in grace"));
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(weak.upgrade().is_some(), "held keepalive outlives the old grace window");
    drop(re);
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(weak.upgrade().is_none(), "fresh window from the re-drop then expires");
}
