//! Background tasks: ending them, and spacing their retries.
//!
//! [`AbortOnDrop`] is the ownership half — a task whose lifetime is a value's
//! lifetime, so no code path can forget to stop it. [`retry_jitter`] is the
//! scheduling half.

/// A multiplier in `[0.75, 1.25)` to space one retry against its siblings.
///
/// Deterministic per `(seed, attempt)` WITHIN a process — so one client's
/// twenty subscriptions retry at twenty different offsets, reproducibly —
/// and different between processes, so a daemon restart does not bring every
/// client back on the same schedule.
///
/// Both halves were broken, and the doc claimed both. Measured with the real
/// call shape (`seed` = resource plural, `attempt` = the backoff in
/// milliseconds):
///
/// ```text
///     pods 1.03 1.03 1.03 1.03 1.03
/// services 1.03 1.03 1.03 1.03 1.03
///    nodes 0.82 0.82 0.82 0.82 0.82
/// ```
///
/// One constant per resource, two resources colliding on it, and no movement
/// across attempts at all. The cause: the old hash folded `attempt` in first
/// and then reduced `mod 50`, and every backoff passed as `attempt` is a
/// multiple of 50 — so `31^len · attempt ≡ 0 (mod 50)` and the attempt term
/// vanished except where the multiply overflowed `u64`. Separately the seed
/// held nothing per-process, so "a thundering herd of reconnects spreads
/// out" was the one thing it could not do: every client computed the same
/// factors.
///
/// The fix is a final avalanche step, which moves the varying high bits down
/// into the ones `%` keeps, plus a process-lifetime salt.
pub fn retry_jitter(seed: &[u8], attempt: u64) -> f64 {
    /// Fixed for the life of the process: two clients reconnecting to the
    /// same daemon pick different offsets, while one client's own retries
    /// stay reproducible. Pid plus boot-time nanos — no dependency, and no
    /// need for cryptographic quality to space a backoff.
    fn process_salt() -> u64 {
        static SALT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        *SALT.get_or_init(|| {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.subsec_nanos() as u64)
                .unwrap_or(0);
            u64::from(std::process::id()).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ nanos
        })
    }

    // FNV-1a over the seed, started from the salt and the attempt.
    let mut hash = process_salt() ^ attempt.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for &b in seed {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    // Avalanche (splitmix64's finalizer). Without this the `% 50` below
    // reads only the low bits, which is exactly how the attempt term
    // disappeared.
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94D0_49BB_1331_11EB);
    hash ^= hash >> 31;

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

#[cfg(test)]
#[path = "tests/task.rs"]
mod tests;
