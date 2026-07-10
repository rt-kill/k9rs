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
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;
    use std::sync::Mutex;

    /// Scripted operator: pops the next exit from a list; counts runs;
    /// optionally signals active() before given exits.
    struct Scripted {
        runs: Arc<AtomicU32>,
        script: Mutex<Vec<(bool, OperatorExit)>>, // (signal_active, exit)
    }

    impl Scripted {
        fn new(script: Vec<(bool, OperatorExit)>) -> (Self, Arc<AtomicU32>) {
            let runs = Arc::new(AtomicU32::new(0));
            (Self { runs: Arc::clone(&runs), script: Mutex::new(script) }, runs)
        }
    }

    impl LocalOperator for Scripted {
        async fn run_once(&self, attempt: AttemptHandle) -> OperatorExit {
            self.runs.fetch_add(1, Ordering::SeqCst);
            let next = self.script.lock().unwrap().pop();
            match next {
                Some((signal, exit)) => {
                    if signal {
                        attempt.active();
                    }
                    exit
                }
                // Script exhausted — keep looping cheaply.
                None => OperatorExit::Continue(String::new()),
            }
        }
    }

    /// Record every event name for sequence assertions.
    fn recording_sink() -> (impl Fn(OperatorEvent) + Send + Sync + 'static, Arc<Mutex<Vec<String>>>) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let log2 = Arc::clone(&log);
        let sink = move |ev: OperatorEvent| {
            let name = match ev {
                OperatorEvent::Starting => "starting".to_string(),
                OperatorEvent::Active => "active".to_string(),
                OperatorEvent::Retrying { attempt, error } => format!("retrying:{attempt}:{error}"),
                OperatorEvent::Fatal { error } => format!("fatal:{error}"),
            };
            log2.lock().unwrap().push(name);
        };
        (sink, log)
    }

    fn fast_backoff() -> RunDelay {
        RunDelay::Backoff(Backoff::new(Duration::from_millis(1), Duration::from_millis(4)))
    }

    #[tokio::test]
    async fn gate_blocks_until_armed() {
        let (op, runs) = Scripted::new(vec![]);
        let (_guard, gate) = supervise(op, fast_backoff(), |_| {});
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 0, "must not run before arm()");
        gate.arm();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(runs.load(Ordering::SeqCst) >= 1, "must run after arm()");
    }

    #[tokio::test]
    async fn dropping_gate_without_arm_cancels() {
        let (op, runs) = Scripted::new(vec![]);
        let (_guard, gate) = supervise(op, fast_backoff(), |_| {});
        drop(gate);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 0, "dropped gate = cancelled, never runs");
    }

    #[tokio::test]
    async fn dropping_guard_aborts_loop() {
        let (op, runs) = Scripted::new(vec![]);
        let (guard, gate) = supervise(op, fast_backoff(), |_| {});
        gate.arm();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(runs.load(Ordering::SeqCst) >= 1);
        drop(guard);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let after_drop = runs.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(runs.load(Ordering::SeqCst), after_drop, "no runs after guard drop");
    }

    #[tokio::test]
    async fn fatal_parks_the_loop() {
        let (op, runs) = Scripted::new(vec![(false, OperatorExit::Fatal("kaput".into()))]);
        let (sink, log) = recording_sink();
        let (_guard, gate) = supervise(op, fast_backoff(), sink);
        gate.arm();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 1, "fatal = exactly one attempt");
        let events = log.lock().unwrap().clone();
        assert_eq!(events, vec!["starting", "fatal:kaput"]);
    }

    #[tokio::test]
    async fn gone_exits_silently() {
        let (op, runs) = Scripted::new(vec![(false, OperatorExit::Gone)]);
        let (sink, log) = recording_sink();
        let (_guard, gate) = supervise(op, fast_backoff(), sink);
        gate.arm();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        let events = log.lock().unwrap().clone();
        assert_eq!(events, vec!["starting"], "gone emits nothing further");
    }

    #[tokio::test]
    async fn backoff_mode_narrates_the_lifecycle() {
        // Script is popped from the END: run1 fails, run2 reaches active
        // then ends, run3 is fatal. Expected single-writer sequence:
        // starting → retrying:2 → active → retrying:3 → fatal.
        let (op, _runs) = Scripted::new(vec![
            (false, OperatorExit::Fatal("end".into())), // run 3
            (true, OperatorExit::Continue("died".into())), // run 2
            (false, OperatorExit::Continue("bind".into())), // run 1
        ]);
        let (sink, log) = recording_sink();
        let (_guard, gate) = supervise(op, fast_backoff(), sink);
        gate.arm();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let events = log.lock().unwrap().clone();
        assert_eq!(
            events,
            vec!["starting", "retrying:2:bind", "active", "retrying:3:died", "fatal:end"],
        );
    }

    #[tokio::test]
    async fn schedule_mode_emits_no_lifecycle_noise() {
        let (op, runs) = Scripted::new(vec![]);
        let (sink, log) = recording_sink();
        let (_guard, gate) =
            supervise(op, RunDelay::Schedule(Duration::from_millis(1)), sink);
        gate.arm();
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(runs.load(Ordering::SeqCst) >= 2, "schedule re-runs on interval");
        assert!(log.lock().unwrap().is_empty(), "a schedule is not a failure — no events");
    }

    #[test]
    fn backoff_doubles_to_cap_and_resets() {
        let mut b = Backoff::new(Duration::from_millis(10), Duration::from_millis(35));
        assert_eq!(b.next(), Duration::from_millis(10));
        assert_eq!(b.next(), Duration::from_millis(20));
        assert_eq!(b.next(), Duration::from_millis(35), "clamped to max");
        assert_eq!(b.next(), Duration::from_millis(35));
        b.reset();
        assert_eq!(b.next(), Duration::from_millis(10));
    }
}
