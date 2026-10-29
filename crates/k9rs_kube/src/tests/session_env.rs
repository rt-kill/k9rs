//! The session's `kubectl` budget.
//!
//! No subprocess runs here, and that is the point: a slot is taken when the
//! invocation is BUILT, not when it is spawned, so the budget is observable
//! without a `kubectl` on the runner or a cluster behind it.

use super::*;

fn env() -> SessionEnv {
    SessionEnv::new(HashMap::new())
}

/// A slot is taken as the invocation is BUILT, one per invocation.
#[tokio::test]
async fn the_budget_is_taken_as_invocations_are_built() {
    let env = env();
    assert_eq!(env.free_kubectl_slots(), MAX_CONCURRENT_KUBECTL);

    let mut held = Vec::new();
    for i in 0..MAX_CONCURRENT_KUBECTL {
        held.push(env.kubectl_once().await);
        assert_eq!(
            env.free_kubectl_slots(),
            MAX_CONCURRENT_KUBECTL - (i + 1),
            "each invocation takes a slot as it is built",
        );
    }
}

/// Over the budget, building an invocation WAITS — it is not refused.
///
/// That distinction is the whole design: a confirmed batch sends one command
/// per marked row and the client's tracker waits for one result per target,
/// so refusing the overflow would strand it. Queueing lets three hundred
/// deletes finish, eight at a time.
#[tokio::test]
async fn over_the_budget_a_kubectl_waits_rather_than_being_refused() {
    let env = env();
    let mut held = Vec::new();
    for _ in 0..MAX_CONCURRENT_KUBECTL {
        held.push(env.kubectl_once().await);
    }

    // One more than the budget: it must not resolve while the others live.
    let waiting = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        env.kubectl_once(),
    )
    .await;
    assert!(
        waiting.is_err(),
        "over the budget, building a kubectl invocation must wait",
    );

    // Releasing one releases the queue — the slot rides the value's lifetime,
    // so dropping it IS the release; nothing has to remember to hand it back.
    held.pop();
    let admitted = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        env.kubectl_once(),
    )
    .await;
    assert!(admitted.is_ok(), "a freed slot admits the waiter");
}

/// `output` holds the slot for exactly the subprocess's life: it takes
/// `self`, so the slot comes back when the call returns and not before.
///
/// This runs the one command a test may assume exists — it does not run
/// `kubectl`; it checks that the slot accounting is tied to the value, by
/// dropping it the way `output` does.
#[tokio::test]
async fn a_finished_invocation_hands_its_slot_back() {
    let env = env();
    {
        let _running = env.kubectl_once().await;
        assert_eq!(env.free_kubectl_slots(), MAX_CONCURRENT_KUBECTL - 1);
    }
    assert_eq!(
        env.free_kubectl_slots(),
        MAX_CONCURRENT_KUBECTL,
        "the slot returns with the value, including on an early return or a cancelled task",
    );
}

/// Clones share one budget. They are one session's several holders — the
/// client builder, the `ServerSession`, its `SessionContext`, each
/// `PortForwardRequest` — not several sessions, and a budget that cloned
/// with them would bound nothing.
#[tokio::test]
async fn clones_of_one_session_share_its_budget() {
    let a = env();
    let b = a.clone();
    let _held = a.kubectl_once().await;
    assert_eq!(
        b.free_kubectl_slots(),
        MAX_CONCURRENT_KUBECTL - 1,
        "the clone sees the same budget, not a fresh one",
    );
}

/// The streaming door is deliberately outside the budget: a `logs -f` holds
/// its subprocess for as long as the user watches it, and eight log views
/// would otherwise mean no describe ever runs again.
#[tokio::test]
async fn the_streaming_door_takes_no_slot() {
    let env = env();
    let _streaming = env.kubectl_streaming();
    assert_eq!(
        env.free_kubectl_slots(),
        MAX_CONCURRENT_KUBECTL,
        "a streaming kubectl is bounded by the substream it rides, not by this",
    );
}

/// The `Debug` impl redacts VALUES, and nothing else in the type may leak
/// them either.
///
/// This type exists for cross-tenant credential isolation: the vars it
/// carries are a session's own `AWS_SECRET_ACCESS_KEY`,
/// `AWS_SESSION_TOKEN`, `GOOGLE_APPLICATION_CREDENTIALS`. The redaction is
/// a hand-written `fmt` with a comment saying why, and nothing pinned it —
/// so a later `#[derive(Debug)]`, or one new `.field()`, would put a
/// secret into the daemon log, and every `tracing` line that formats a
/// session is a candidate.
#[test]
fn the_debug_impl_does_not_print_credential_values() {
    let secret = "AKIA-not-a-real-key-0123456789";
    let token = "FwoGZXIvYXdzE-not-a-real-token";
    let env = SessionEnv::new(HashMap::from([
        ("AWS_SECRET_ACCESS_KEY".to_string(), secret.to_string()),
        ("AWS_SESSION_TOKEN".to_string(), token.to_string()),
        ("KUBECONFIG".to_string(), "/home/someone/.kube/config".to_string()),
    ]));

    let printed = format!("{env:?}");
    for leaked in [secret, token, "/home/someone/.kube/config"] {
        assert!(
            !printed.contains(leaked),
            "a value reached the log: {printed}",
        );
    }
    // The count is the whole point of printing it at all.
    assert!(
        printed.contains("var_count") && printed.contains('3'),
        "it should still say HOW MANY, which is the diagnostic: {printed}",
    );
    // Names are not secrets, but they are not printed either — a name list
    // is a fingerprint of the session's auth method, and nothing needs it.
    assert!(
        !printed.contains("AWS_SECRET_ACCESS_KEY"),
        "not even the names: {printed}",
    );
}
