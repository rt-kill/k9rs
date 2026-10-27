//! Background tasks: ending them, and spacing their retries.
//!
//! [`AbortOnDrop`] is the ownership half — a task whose lifetime is a value's
//! lifetime, so no code path can forget to stop it. [`retry_jitter`] is the
//! scheduling half: deterministic per `(seed, attempt)`, so a thundering herd
//! of reconnects spreads out without any of them needing a random source.

pub fn retry_jitter(seed: &[u8], attempt: u64) -> f64 {
    let hash = seed.iter().fold(attempt, |acc, &b| {
        acc.wrapping_mul(31).wrapping_add(b as u64)
    });
    0.75 + (hash % 50) as f64 / 100.0
}

/// RAII guard that aborts a tokio task when dropped. Same pattern family as
/// `SuspendGuard`, `TempFile`, `ChildGuard`: holding the guard keeps the task
/// running; dropping it (on any exit path, including unwind) aborts it.
/// Holding the abort handle in an `Option` lets the guard be `Drop`'d
/// explicitly without `Drop` running twice.
///
/// CONTRACT: abort-only, never join. Drops can run under locks (e.g. a
/// DashMap shard lock while its slots drop); `abort()` is signal-and-return,
/// so that's safe — a blocking join in `Drop` would deadlock.
pub struct AbortOnDrop(Option<tokio::task::AbortHandle>);

impl AbortOnDrop {
    pub fn new(handle: tokio::task::AbortHandle) -> Self {
        Self(Some(handle))
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}
