//! Operator supervision — the restart loop behind self-healing local
//! resources (port-forwards today; any daemon-side supervised operation
//! tomorrow).
//!
//! A [`LocalOperator`] is ONE supervised operation: `run_once` performs a
//! single attempt to completion and reports how it ended via
//! [`OperatorExit`]. [`supervise`] owns everything around that attempt —
//! the retry loop, the delay policy, and every lifecycle state — and hands
//! back an [`OperatorGuard`]: drop the guard and the loop (plus whatever
//! resources the in-flight attempt owns, e.g. a `kill_on_drop` subprocess)
//! is torn down. RAII owns *teardown*; the loop is deliberate control-flow
//! policy — supervision cannot be a pure ownership construct.
//!
//! # Single state writer
//!
//! The supervisor is the ONLY writer of lifecycle state: it emits every
//! [`OperatorEvent`] through the one `on_event` sink given to [`supervise`].
//! The operator cannot publish state on its own — the sole thing it may
//! signal is "this attempt reached healthy steady-state", and only through
//! the narrow [`AttemptHandle`] the supervisor lends it. An operator that
//! forgets to signal merely stays in its previous (still truthful) state;
//! it cannot invent one. `on_event` is a required parameter, not a
//! defaulted hook — an operator with no state display passes an explicit
//! no-op, visibly.
//!
//! # Delay policy: a schedule is not a failure
//!
//! [`RunDelay::Backoff`] models crash-restart (port-forward): failures back
//! off exponentially, and reaching Active resets the backoff. In this mode
//! the supervisor narrates: `Starting` → `Active` → `Retrying` → …
//! [`RunDelay::Schedule`] models a poller (exec resources): re-running
//! after a fixed interval is the *normal* path, so the supervisor emits no
//! `Starting`/`Retrying` noise — only a `Fatal` would ever surface.
//!
//! # Ownership contract for operators
//!
//! `run_once` must NOT hold a strong reference to its owning source across
//! an `.await`. The supervised task must never keep its source alive: the
//! source owns the guard, the guard aborts the task — a strong ref held
//! across an await inverts that into a cycle (source can't drop while its
//! own task pins it). State access is synchronous upgrade-use-drop via a
//! `Weak`; per-attempt resources (a spawned child, a listener) are owned by
//! the `run_once` future itself so an abort drops and reaps them. This is
//! module discipline, enforced by review — the type system can't check it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::util::AbortOnDrop;

/// How one attempt of a supervised operation ended.
pub enum OperatorExit {
    /// The attempt ran its course; keep supervising. Under
    /// [`RunDelay::Backoff`] the payload is the failure detail shown while
    /// reconnecting; under [`RunDelay::Schedule`] a completed run is normal
    /// and the payload is ignored (pass `""`).
    Continue(String),
    /// Permanent failure — retrying cannot help (binary missing, config
    /// invalid). The supervisor emits [`OperatorEvent::Fatal`] and parks:
    /// no further attempts until the entry is recreated.
    Fatal(String),
    /// The operator's subject is gone (its source `Weak` no longer
    /// upgrades). Exit silently — defense-in-depth for a guard that was
    /// somehow not dropped with its source.
    Gone,
}

/// Lifecycle events, emitted only by the supervisor (see module docs).
pub enum OperatorEvent {
    /// The first attempt is beginning (Backoff mode only).
    Starting,
    /// The current attempt reached healthy steady-state (routed through
    /// [`AttemptHandle::active`]).
    Active,
    /// The previous attempt ended; attempt `attempt` runs after the backoff
    /// delay (Backoff mode only).
    Retrying { attempt: u32, error: String },
    /// Permanent failure; the supervisor has parked.
    Fatal { error: String },
}

/// The one channel through which an operator may speak mid-attempt: it can
/// signal `active()` — nothing else. Cloned per attempt by the supervisor.
#[derive(Clone)]
pub struct AttemptHandle {
    sink: Arc<dyn Fn(OperatorEvent) + Send + Sync>,
    active_seen: Arc<AtomicBool>,
}

impl AttemptHandle {
    /// Signal that this attempt reached healthy steady-state. Publishes
    /// [`OperatorEvent::Active`] and marks the attempt healthy so the
    /// supervisor resets its backoff.
    pub fn active(&self) {
        self.active_seen.store(true, Ordering::Release);
        (self.sink)(OperatorEvent::Active);
    }
}

/// One supervised operation. See the module docs for the ownership
/// contract `run_once` must uphold.
pub trait LocalOperator: Send + Sync + 'static {
    /// Perform one attempt to completion. Everything the attempt owns
    /// (children, sockets) must live inside the returned future so an
    /// abort tears it down.
    fn run_once(
        &self,
        attempt: AttemptHandle,
    ) -> impl std::future::Future<Output = OperatorExit> + Send;
}

/// Exponential backoff between failed attempts. `next()` yields the current
/// delay and doubles it (clamped to `max`); `reset()` returns to `min`.
pub struct Backoff {
    min: Duration,
    max: Duration,
    next: Duration,
}

impl Backoff {
    pub fn new(min: Duration, max: Duration) -> Self {
        Self { min, max, next: min }
    }

    fn next(&mut self) -> Duration {
        let d = self.next;
        self.next = (d.saturating_mul(2)).min(self.max);
        d
    }

    fn reset(&mut self) {
        self.next = self.min;
    }
}

/// When to run the operator again after an attempt ends.
pub enum RunDelay {
    /// A poller: re-run after a fixed interval. Re-running is the normal
    /// path — no failure-flavored events are emitted.
    Schedule(Duration),
    /// Crash-restart: back off exponentially between failures; reaching
    /// Active (via [`AttemptHandle::active`]) resets the backoff.
    Backoff(Backoff),
}

/// RAII handle to a supervised operator. Dropping it aborts the loop and,
/// with it, whatever the in-flight attempt owns (`kill_on_drop` children,
/// listeners). Hold it exactly as long as the operator should live.
pub struct OperatorGuard {
    _abort: AbortOnDrop,
}

/// One-shot trigger releasing a supervised loop that was spawned parked.
/// This preserves the insert-before-run handoff (see
/// `PortForwardSource::create`): spawn parked → insert the entry holding
/// the [`OperatorGuard`] → `arm()`. A guard dropped before `arm()` aborts
/// the parked task and the operator never runs; an `arm()` after the guard
/// died is a no-op.
pub struct StartGate(tokio::sync::oneshot::Sender<()>);

impl StartGate {
    pub fn arm(self) {
        let _ = self.0.send(());
    }
}

/// Spawn the supervision loop for `op`, parked until [`StartGate::arm`].
///
/// The loop: run one attempt → on [`OperatorExit::Continue`] wait out the
/// delay policy and re-run; on `Fatal` emit and park; on `Gone` exit
/// silently. Aborting (dropping the [`OperatorGuard`]) cancels whichever
/// of those it is mid-way through.
pub fn supervise<O: LocalOperator>(
    op: O,
    mut delay: RunDelay,
    on_event: impl Fn(OperatorEvent) + Send + Sync + 'static,
) -> (OperatorGuard, StartGate) {
    let sink: Arc<dyn Fn(OperatorEvent) + Send + Sync> = Arc::new(on_event);
    let active_seen = Arc::new(AtomicBool::new(false));
    let handle_proto = AttemptHandle { sink: Arc::clone(&sink), active_seen: Arc::clone(&active_seen) };

    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        // Parked until the caller finishes inserting the guard wherever it
        // lives. `Err` = the gate was dropped without arming — treated as
        // cancellation, the operator never runs.
        if gate_rx.await.is_err() {
            return;
        }
        if matches!(delay, RunDelay::Backoff(_)) {
            (sink)(OperatorEvent::Starting);
        }
        let mut attempt: u32 = 1;
        loop {
            active_seen.store(false, Ordering::Release);
            let exit = op.run_once(handle_proto.clone()).await;
            let reached_active = active_seen.load(Ordering::Acquire);
            match exit {
                OperatorExit::Gone => return,
                OperatorExit::Fatal(error) => {
                    (sink)(OperatorEvent::Fatal { error });
                    return;
                }
                OperatorExit::Continue(error) => {
                    attempt = attempt.saturating_add(1);
                    let wait = match &mut delay {
                        RunDelay::Schedule(d) => *d,
                        RunDelay::Backoff(b) => {
                            // A run that reached Active earned a fresh
                            // backoff — the next failure starts from `min`,
                            // however long the healthy run lasted.
                            if reached_active {
                                b.reset();
                            }
                            let w = b.next();
                            (sink)(OperatorEvent::Retrying { attempt, error });
                            w
                        }
                    };
                    tokio::time::sleep(wait).await;
                }
            }
        }
    });

    (
        OperatorGuard { _abort: AbortOnDrop::new(task.abort_handle()) },
        StartGate(gate_tx),
    )
}

#[cfg(test)]
#[path = "../../tests/kube/local/supervise.rs"]
mod tests;
