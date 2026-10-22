//! Unit tests for `util::one_task`.

use super::OneTask;

/// The half of the rule `try_lock` cannot see.
///
/// Two touchers that TAKE TURNS never contend, so the old
/// `try_lock().expect(…)` passed them without a murmur — and taking turns is
/// exactly what a write from a spawned task between two frames does. The
/// store's epoch floor sequences itself against the last epoch it accepted,
/// so a second writer with its own idea of the sequence corrupts it quietly.
#[tokio::test]
async fn a_second_task_is_caught_even_though_it_waited_its_turn() {
    let cell = std::sync::Arc::new(OneTask::new("probe", 0usize));

    // Two SPAWNED tasks, taking turns. The first claims the lock and
    // finishes; only then does the second run, so they never contend.
    let first = std::sync::Arc::clone(&cell);
    tokio::spawn(async move { *first.lock() = 1 }).await.expect("the first owner");

    let other = std::sync::Arc::clone(&cell);
    let joined = tokio::spawn(async move {
        // No contention whatsoever: the guard above is long gone.
        *other.lock() = 2;
    })
    .await;

    let err = joined.expect_err("a second task must be refused");
    assert!(err.is_panic(), "refused by panic, not by a silent no-op");
    let panic = err.into_panic();
    let msg = panic
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()).unwrap_or_default());
    assert!(
        msg.contains("probe") && msg.contains("need not overlap"),
        "the message names the store and the rule: {msg:?}",
    );

    // Deliberately NOT read back from here: this test body is a THIRD
    // toucher (a `block_on` body is a `Thread`, not a `Task`), and reading
    // would trip the very rule under test. That the interloper's write never
    // landed is asserted by the test below, where the owner IS the body.
    drop(cell);
}

/// One task, many locks — the ordinary case, which must stay silent.
#[tokio::test]
async fn one_task_may_lock_as_often_as_it_likes() {
    let cell = OneTask::new("probe", 0usize);
    for i in 0..100 {
        *cell.lock() = i;
    }
    assert_eq!(*cell.lock(), 99);
}

/// Outside a runtime there is no task at all, so the toucher is the thread —
/// and a plain `#[test]` is still one owner. Most of this suite is one.
#[test]
fn no_runtime_still_has_exactly_one_owner() {
    assert!(tokio::task::try_id().is_none(), "this test is not in a task");
    let cell = OneTask::new("probe", 0usize);
    *cell.lock() = 1;
    assert_eq!(*cell.lock(), 1);
}

/// The pairing that matters, and the one a task-only identity would have
/// missed: the loop body of `#[tokio::main]` is NOT a task (`try_id` is
/// `None` there), so a store it owns would otherwise have been claimed by
/// the first spawned task to touch it, with the loop's own writes waved
/// through forever after.
#[tokio::test]
async fn the_block_on_body_owns_what_it_touches_first() {
    assert!(
        tokio::task::try_id().is_none(),
        "a `block_on` body is where the TUI loop lives — not a task",
    );
    let cell = std::sync::Arc::new(OneTask::new("loop-owned", 0usize));
    *cell.lock() = 1;

    let other = std::sync::Arc::clone(&cell);
    let err = tokio::spawn(async move { *other.lock() = 2 })
        .await
        .expect_err("a spawned task must not write the loop's store");
    assert!(err.is_panic());
    assert_eq!(*cell.lock(), 1, "and it did not");
}
