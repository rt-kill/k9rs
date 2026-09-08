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
