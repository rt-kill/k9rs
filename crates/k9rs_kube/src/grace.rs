//! Keeping a resource alive briefly after its last user lets go.
//!
//! One gate for every kept-alive thing — watchers, local-resource registries,
//! exec sources, port-forwards — so "how long until this is really dropped"
//! is one answer rather than four. It lived in `kube/mod.rs`, which made the
//! module root a home for behaviour as well as declarations.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

use crate::daemon_config;

/// How long a *watcher* stays warm after its last subscriber drops.
///
/// Three consumers, not the two this used to name: `WatcherCache`,
/// `local::exec_source`, and `shared_poller` — where a shared metrics or
/// discovery poller takes the watcher's window despite costing one
/// metrics-server list per interval rather than a row store plus an
/// apiserver watch. `spawn_grace`'s own doc below enumerates all four
/// kept-alive kinds correctly, so the two lists in this file disagreed.
///
/// 60s: long enough that quick nav-away/back (Esc then re-enter, tab cycling)
/// reuses the warm watcher; short enough that a linear sweep through many
/// resource types doesn't pin a *union* of watchers — each holding its full
/// store and an open apiserver watch — for minutes. Most reuse value decays
/// under a minute.
///
/// Deliberately NOT the window for a context's local slice: see
/// `DEFAULT_LOCALS_GRACE_SECS`.
pub(crate) const DEFAULT_WATCHER_GRACE_SECS: u64 = 60;

/// How long a context's LOCAL slice — its running port-forwards — survives
/// after the last session for that context detaches.
///
/// A different number from `DEFAULT_WATCHER_GRACE_SECS` because it bounds a
/// completely different cost. A warm watcher costs a full row store plus an
/// open apiserver watch, and buys back only a re-LIST; 60s is a fair trade. A
/// port-forward costs one `kubectl` child, was created by an explicit user
/// action, and buying it back means the user noticing it died and recreating
/// it. Sharing one constant meant `:ctx` away for two minutes and back silently
/// killed forwards the user was still using.
///
/// 30 minutes: survives context-hopping, a meeting, lunch — while still
/// bounding a long-lived daemon so forwards for abandoned contexts don't
/// accumulate forever. Override with `daemon.localsGraceSecs`.
pub(crate) const DEFAULT_LOCALS_GRACE_SECS: u64 = 1800;

/// The configured watcher grace window.
pub(crate) fn watcher_grace() -> Duration {
    Duration::from_secs(daemon_config::daemon_config().watcher_grace_secs)
}

/// The configured window for a context's local slice.
pub fn locals_grace() -> Duration {
    Duration::from_secs(daemon_config::daemon_config().locals_grace_secs)
}

/// The "last holder just left" gate: a movable deadline and a single-owner
/// claim, as ONE value.
///
/// Both halves used to be spelled out per call site — an `AtomicBool` here, a
/// pair of dyn-dispatched trait hooks there, a hand-rolled deadline loop in
/// `ContextLocals` — and the two halves could drift apart because nothing tied
/// them together. The trait even carried a doc warning that its `claim`/`reset`
/// pair "MUST target the exact same slot — the type system can't verify it".
/// Now they are one field with one method each, so it can.
///
/// # Why a deadline rather than a claim flag alone
///
/// The claim flag exists to coalesce: at most one grace task per object, so
/// context-switch churn can't stack detached timers. But claim-only measured
/// the window from the FIRST dropper, so with two sessions the second one out
/// could get ~0s of grace — it lost the claim, and the winner's timer was
/// already most of the way through. Every drop now bumps `deadline = now +
/// window` and the single task sleeps until the deadline stops moving, so the
/// window is always measured from the LAST holder out.
pub struct GraceGate {
    window: Duration,
    /// Anchors the deadline arithmetic so a monotonic `Instant` fits in an
    /// `AtomicU64` of elapsed millis.
    created: Instant,
    deadline_ms: AtomicU64,
    claimed: AtomicBool,
}

impl GraceGate {
    pub(crate) fn new(window: Duration) -> Self {
        Self {
            window,
            created: Instant::now(),
            deadline_ms: AtomicU64::new(0),
            claimed: AtomicBool::new(false),
        }
    }

    fn now_ms(&self) -> u64 {
        self.created.elapsed().as_millis() as u64
    }

    /// Push the deadline out to `now + window` and try to become the one
    /// waiter. `true` = you own the grace task.
    fn bump_and_claim(&self) -> bool {
        let deadline = self.now_ms().saturating_add(self.window.as_millis() as u64);
        // `fetch_max`, not `store`: a racing earlier drop must not pull an
        // already-later deadline back in.
        self.deadline_ms.fetch_max(deadline, Ordering::AcqRel);
        self.claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn release(&self) {
        self.claimed.store(false, Ordering::Release);
    }

    /// Sleep until the deadline stops moving.
    async fn wait_out(&self) {
        loop {
            loop {
                let now = self.now_ms();
                let deadline = self.deadline_ms.load(Ordering::Acquire);
                if now >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(deadline - now)).await;
            }
            self.release();
            // Release-recheck: a holder may have bumped the deadline between
            // our last load and the release above — its claim lost against our
            // still-set flag, so that bump is ours to serve. Re-claim and keep
            // waiting; if someone else claimed in the gap, the bump is theirs.
            if self.now_ms() >= self.deadline_ms.load(Ordering::Acquire) {
                return;
            }
            if self
                .claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return;
            }
        }
    }
}

/// The shared "last subscriber dropped" grace dance, driving every kept-alive
/// object in the daemon: K8s watchers (`live_query::Subscription`), shared
/// pollers (`PollerSub`), local sources (`LocalSubscription`) and a context's
/// local slice (`ContextKeepalive`).
///
/// Behaviors, all preserved from the four hand-rolled `Drop`s this replaced:
/// 1. `finished(&arc)` → already dead (a self-terminated watcher); drop `arc`
///    now with no grace, so its cache slot is reclaimed and the next subscribe
///    builds fresh. Objects that can't self-terminate pass `|_| false`.
/// 2. `gate(&arc)` is `None` → this object is not graced at THIS level at all
///    (a port-forward source is owned by its `ContextLocals`, so subscription
///    grace would be meaningless). Drop now.
/// 3. Claim lost → another drop already owns the in-flight grace task, and our
///    `bump_and_claim` has already pushed the deadline out for it to serve.
/// 4. No tokio runtime (dropping during process shutdown) → release the claim
///    and drop immediately; dying now is correct.
/// 5. Otherwise spawn the one task, which waits out the (movable) deadline,
///    releases the claim, and drops.
pub fn spawn_grace<A>(
    arc: A,
    finished: impl FnOnce(&A) -> bool,
    gate: impl Fn(&A) -> Option<&GraceGate> + Send + 'static,
) where
    A: Send + 'static,
{
    if finished(&arc) {
        return;
    }
    let Some(g) = gate(&arc) else {
        return;
    };
    if !g.bump_and_claim() {
        return;
    }
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        g.release();
        return;
    };
    handle.spawn(async move {
        if let Some(g) = gate(&arc) {
            g.wait_out().await;
        }
        drop(arc);
    });
}

#[cfg(test)]
#[path = "tests/grace.rs"]
mod grace_tests;
