//! A `Mutex` that may only ever be touched by ONE task.
//!
//! The client's stores are reached through an `Arc` from several places — the
//! event-apply path (each event carries its destination store), the key
//! handlers, and the renderer — but all of them run on the session task. That
//! is not an optimisation, it is what the stores' own logic assumes: the
//! epoch floor rejects a superseded stream's stragglers by comparing against
//! the last epoch IT accepted, the delta tracker ages row highlights against
//! its own clock, and the mark set is read back by the action that was
//! dispatched from the same loop. A second writer sequencing itself
//! independently breaks all three, quietly.
//!
//! Five call sites used to spell the rule out as `try_lock().expect("…
//! single-writer convention violated")`, which says it five times and catches
//! only the half of it that OVERLAPS in time. Two touchers that take turns —
//! a write from a spawned task landing between two frames — pass `try_lock`
//! without a murmur, and that is the shape the convention actually forbids.
//!
//! So the first toucher is recorded in a `OnceLock` — set once, lock-free to
//! read — and every later lock is checked against it.
//!
//! # What counts as a toucher
//!
//! A [`tokio::task::Id`] where there is one, and the thread otherwise. Not
//! the thread alone: `#[tokio::main]` is multi-threaded and a task migrates
//! between workers freely, so a thread id would fire on a perfectly
//! well-behaved task. And not the task alone either — the body of
//! `#[tokio::main]` (or any `block_on`) runs on the current thread WITHOUT a
//! task context, so `try_id` is `None` exactly where the TUI's own loop
//! lives. Recording only tasks would have meant the first spawned task to
//! touch a store claimed it, and the loop's own touches, being `None`, were
//! waved through — missing the one pairing worth catching.
//!
//! Either identity is stable for its owner's whole life, which is all the
//! comparison needs.
//!
//! # Debug builds only
//!
//! The identity check compiles away in release. A release binary must not
//! gain a panic site for a rule it has always kept — and the contention
//! check, which costs nothing and catches the overlapping half, is in both.

use std::sync::{Mutex, MutexGuard};

/// Who is holding the lock — see the module docs on why it is both.
#[cfg(debug_assertions)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Toucher {
    /// Inside a spawned task. Survives the task moving between workers.
    Task(tokio::task::Id),
    /// Inside a `block_on` body (where `#[tokio::main]` puts the TUI loop),
    /// or no runtime at all — a plain `#[test]`.
    Thread(std::thread::ThreadId),
}

#[cfg(debug_assertions)]
impl Toucher {
    fn here() -> Self {
        match tokio::task::try_id() {
            Some(id) => Self::Task(id),
            None => Self::Thread(std::thread::current().id()),
        }
    }
}

/// A `Mutex` whose lock asserts that one task owns it. See the module docs.
#[derive(Debug)]
pub struct OneTask<T> {
    inner: Mutex<T>,
    /// Whoever first took this lock. Written once; every later lock is
    /// compared against it.
    #[cfg(debug_assertions)]
    first: std::sync::OnceLock<Toucher>,
    /// What to name in the panic — the store, not the mutex.
    what: &'static str,
}

impl<T> OneTask<T> {
    pub fn new(what: &'static str, value: T) -> Self {
        Self {
            inner: Mutex::new(value),
            #[cfg(debug_assertions)]
            first: std::sync::OnceLock::new(),
            what,
        }
    }

    /// Take the lock, or panic naming the convention that was broken.
    pub fn lock(&self) -> MutexGuard<'_, T> {
        // The identity check comes FIRST, while no guard exists. A panic
        // thrown with the guard held poisons the mutex, so the next lock
        // reports poisoning and the real fault — which task — is lost behind
        // it. (That is not hypothetical: it is how this was written the
        // first time, and the test for it reported "contended".)
        #[cfg(debug_assertions)]
        {
            let here = Toucher::here();
            let first = *self.first.get_or_init(|| here);
            assert_eq!(
                first, here,
                "{} was touched by {first:?} and is now touched by {here:?} \
                 — they need not overlap to break it (see util::one_task)",
                self.what,
            );
        }
        match self.inner.try_lock() {
            Ok(guard) => guard,
            // Never waits: a wait would mean a second toucher exists, and
            // hiding that behind a block is how it stays hidden.
            Err(std::sync::TryLockError::WouldBlock) => panic!(
                "{} lock contended — it is touched by more than one task, \
                 which its epoch floor and delta tracker both assume cannot \
                 happen (see util::one_task)",
                self.what,
            ),
            // A DIFFERENT fault, and it used to be reported as the one
            // above: something panicked while holding this lock, so the
            // contents are as far through a mutation as it got.
            Err(std::sync::TryLockError::Poisoned(e)) => panic!(
                "{} lock poisoned — a holder panicked mid-mutation: {e}",
                self.what,
            ),
        }
    }
}

#[cfg(test)]
#[path = "../tests/util/one_task.rs"]
mod tests;
