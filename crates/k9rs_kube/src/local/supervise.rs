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
//! The sole thing an operator may signal is "this attempt reached healthy
//! steady-state", and only through the narrow [`AttemptHandle`] the
//! supervisor lends it. An operator that forgets to signal merely stays in
//! its previous (still truthful) state; it cannot invent one. `on_event` is
//! a required parameter, not a defaulted hook — an operator with no state
//! display passes an explicit no-op, visibly.
//!
//! "The operator cannot publish state on its own" is what this said, and
//! for the one operator that had state to publish it was convention:
//! `PortForwardOp` is declared in the same module as the private
//! `set_state`, and held a `Weak<PortForwardSource>` it could upgrade. It
//! holds a [`SourceLiveness`] now — whose field this module keeps — so the
//! source is genuinely out of reach and the claim is the type's. An
//! operator over a source with NO lifecycle state keeps its own `Weak`
//! (`ExecPollOp` reads its command line and folds rows back in); there is
//! nothing there to publish. That pairing is the rule to keep.
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

use k9rs_util::AbortOnDrop;

/// How one attempt of a supervised operation ended.
pub(crate) enum OperatorExit {
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
pub(crate) enum OperatorEvent {
    /// The first attempt is beginning (Backoff mode only).
    Starting,
    /// The current attempt reached healthy steady-state (routed through
    /// [`AttemptHandle::active`]).
    Active,
    /// The previous attempt ended; the next runs after the backoff delay
    /// (Backoff mode only).
    ///
    /// Carried an `attempt: u32` that the supervisor counted and the only
    /// production consumer discarded (`Retrying { error, .. }` in
    /// `port_forward`) — a test was the sole reader. Both the field and its
    /// counter are gone. If the `:pf` row should ever say which attempt it
    /// is on (flapping reads differently from a blip), the loop can count
    /// again in one line; what it must not do is carry the number because
    /// something might want it.
    Retrying { error: String },
    /// Permanent failure; the supervisor has parked.
    Fatal { error: String },
}

/// The one channel through which an operator may speak mid-attempt: it can
/// signal `active()` — nothing else. Cloned per attempt by the supervisor.
#[derive(Clone)]
pub(crate) struct AttemptHandle {
    sink: Arc<dyn Fn(OperatorEvent) + Send + Sync>,
    active_seen: Arc<AtomicBool>,
}

impl AttemptHandle {
    /// Signal that this attempt reached healthy steady-state. Publishes
    /// [`OperatorEvent::Active`] and marks the attempt healthy so the
    /// supervisor resets its backoff.
    pub(crate) fn active(&self) {
        self.active_seen.store(true, Ordering::Release);
        (self.sink)(OperatorEvent::Active);
    }
}

/// An operator's handle to the source it serves, narrowed to the one
/// question it has any business asking: has everyone let go?
///
/// `PortForwardOp` held a `Weak<PortForwardSource>` for exactly this check
/// — and a `Weak` carries `upgrade()`, which carries every method on the
/// source, the private `set_state` included, because the operator is
/// declared in the same module. The field here is private to THIS module,
/// which no operator is in, so there is no upgrade to make: the "single
/// state writer" rule stops being a thing to remember.
///
/// An operator that legitimately needs its own source keeps a `Weak` (see
/// `ExecPollOp`, which reads its command line and folds rows back in). The
/// rule that matters is the pairing: a source with a lifecycle-state writer
/// hands its operator one of these.
pub(crate) struct SourceLiveness<T> {
    weak: std::sync::Weak<T>,
}

impl<T> SourceLiveness<T> {
    /// Build one from the source's own `Weak`. Called where the source
    /// constructs its operator.
    pub(crate) fn new(weak: std::sync::Weak<T>) -> Self {
        Self { weak }
    }

    /// True once nothing holds the source any more — the attempt has
    /// nothing left to serve and should report [`OperatorExit::Gone`].
    pub(crate) fn gone(&self) -> bool {
        self.weak.strong_count() == 0
    }
}

/// One supervised operation. See the module docs for the ownership
/// contract `run_once` must uphold.
pub(crate) trait LocalOperator: Send + Sync + 'static {
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
pub(crate) struct Backoff {
    min: Duration,
    max: Duration,
    next: Duration,
}

impl Backoff {
    pub(crate) fn new(min: Duration, max: Duration) -> Self {
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
pub(crate) enum RunDelay {
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
pub(crate) struct OperatorGuard {
    _abort: AbortOnDrop,
}

/// One-shot trigger releasing a supervised loop that was spawned parked.
/// This preserves the insert-before-run handoff (see
/// `PortForwardSource::create`): spawn parked → insert the entry holding
/// the [`OperatorGuard`] → `arm()`. A guard dropped before `arm()` aborts
/// the parked task and the operator never runs; an `arm()` after the guard
/// died is a no-op.
pub(crate) struct StartGate(tokio::sync::oneshot::Sender<()>);

impl StartGate {
    pub(crate) fn arm(self) {
        let _ = self.0.send(());
    }
}

/// Spawn the supervision loop for `op`, parked until [`StartGate::arm`].
///
/// The loop: run one attempt → on [`OperatorExit::Continue`] wait out the
/// delay policy and re-run; on `Fatal` emit and park; on `Gone` exit
/// silently. Aborting (dropping the [`OperatorGuard`]) cancels whichever
/// of those it is mid-way through.
pub(crate) fn supervise<O: LocalOperator>(
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
                            (sink)(OperatorEvent::Retrying { error });
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
#[path = "../tests/local/supervise.rs"]
mod tests;
