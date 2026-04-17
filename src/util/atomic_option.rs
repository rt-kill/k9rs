//! Lock-free optional value built on `AtomicPtr<T>`.
//!
//! The value is boxed and ownership transfers happen via raw-pointer swaps.
//! Drops of the boxed value are inline with the swap/take that evicted it —
//! no hazard pointers, no deferred reclamation, so `T` must be owned end-to-
//! end by `AtomicOption` (each slot has a single logical owner at a time).
//!
//! The token variants allow CAS-style conditional updates: take a snapshot
//! of "the slot was in state X," do work, then commit only if the slot is
//! still in state X. A reader that wants a cheap concurrent-safe peek uses
//! [`AtomicOption::load_cloned`] on `T: Clone` — typically
//! `AtomicOption<Arc<U>>` so the clone is just an Arc refcount bump.
//!
//! # Concurrency notes
//!
//! - `take`/`swap` replace the slot atomically; the old value drops inline
//!   after the swap returns, so writers never observe a torn state.
//! - A reader that calls `load_cloned` briefly sets the slot to `None` while
//!   cloning, then CAS-restores the original. Concurrent writers that
//!   intervene win (reader's restore CAS fails, writer's value stays) and
//!   the reader's clone is one cycle stale.
//! - Concurrent readers race: one wins the `take`, the other sees `None`
//!   transiently. `load_cloned` spins a bounded number of times before
//!   treating `None` as genuine.

use std::{
    ptr,
    sync::atomic::{AtomicPtr, Ordering},
};

/// Snapshot of an [`AtomicOption`]'s pointer state at a moment in time. Use
/// with [`AtomicOption::swap_if_token`] / [`AtomicOption::take_if_token`] to
/// conditionally apply an update only if the slot hasn't changed since the
/// token was issued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AtomicToken(usize);

/// Thread-safe optional value. `T` is owned; writes drop the previous value
/// inline.
pub struct AtomicOption<T> {
    ptr: AtomicPtr<T>,
}

impl<T> AtomicOption<T> {
    /// An empty `AtomicOption`.
    pub const fn none() -> Self {
        Self { ptr: AtomicPtr::new(ptr::null_mut()) }
    }

    /// An `AtomicOption` containing `value`.
    pub fn some(value: T) -> Self {
        Self { ptr: AtomicPtr::new(Box::into_raw(Box::new(value))) }
    }

    /// Construct from an `Option<T>`.
    pub fn new(value: Option<T>) -> Self {
        match value {
            Some(v) => Self::some(v),
            None => Self::none(),
        }
    }

    /// Atomically swap the slot, returning the previous value.
    pub fn swap(&self, value: Option<T>, ordering: Ordering) -> Option<T> {
        let new_ptr = match value {
            Some(v) => Box::into_raw(Box::new(v)),
            None => ptr::null_mut(),
        };
        let old_ptr = self.ptr.swap(new_ptr, ordering);
        if old_ptr.is_null() {
            None
        } else {
            // Safety: `old_ptr` was obtained from `Box::into_raw` in a
            // previous `swap`/`some`/`new` call; nothing else held a copy
            // because every swap hands exclusive ownership to the caller.
            unsafe { Some(*Box::from_raw(old_ptr)) }
        }
    }

    /// Store `value`, dropping whatever was there.
    pub fn store(&self, value: Option<T>, ordering: Ordering) {
        let _ = self.swap(value, ordering);
    }

    /// Take the value, leaving `None` behind.
    pub fn take(&self, ordering: Ordering) -> Option<T> {
        self.swap(None, ordering)
    }

    /// Insert `value` only if the slot is currently empty. On success returns
    /// `Ok(())`; on failure returns `Err(value)` so the caller can retry or
    /// recover.
    pub fn try_put(
        &self,
        value: T,
        success_ordering: Ordering,
        failure_ordering: Ordering,
    ) -> Result<(), T> {
        let new_ptr = Box::into_raw(Box::new(value));
        match self.ptr.compare_exchange(
            ptr::null_mut(),
            new_ptr,
            success_ordering,
            failure_ordering,
        ) {
            Ok(_) => Ok(()),
            Err(_) => {
                // Safety: `new_ptr` is still ours — the CAS failed, so the
                // slot wasn't updated and nothing else aliases this pointer.
                Err(unsafe { *Box::from_raw(new_ptr) })
            }
        }
    }

    /// True if the slot currently holds `Some`.
    pub fn is_some(&self, ordering: Ordering) -> bool {
        !self.ptr.load(ordering).is_null()
    }

    /// True if the slot currently holds `None`.
    pub fn is_none(&self, ordering: Ordering) -> bool {
        self.ptr.load(ordering).is_null()
    }

    /// Like [`swap`](Self::swap), but also returns a token naming the new
    /// pointer state.
    pub fn swap_with_token(
        &self,
        value: Option<T>,
        ordering: Ordering,
    ) -> (Option<T>, AtomicToken) {
        let new_ptr = match value {
            Some(v) => Box::into_raw(Box::new(v)),
            None => ptr::null_mut(),
        };
        let old_ptr = self.ptr.swap(new_ptr, ordering);
        let token = AtomicToken(new_ptr as usize);
        let old_value = if old_ptr.is_null() {
            None
        } else {
            // Safety: same as `swap`.
            unsafe { Some(*Box::from_raw(old_ptr)) }
        };
        (old_value, token)
    }

    /// Conditional swap: only replace if the current pointer state matches
    /// `token`. On success returns `Ok(old_value)`; on failure returns
    /// `Err(new_value)` so the caller can keep or retry it.
    pub fn swap_if_token(
        &self,
        token: AtomicToken,
        new_value: Option<T>,
        success_ordering: Ordering,
        failure_ordering: Ordering,
    ) -> Result<Option<T>, Option<T>> {
        let expected_ptr = token.0 as *mut T;
        let new_ptr = match new_value {
            Some(v) => Box::into_raw(Box::new(v)),
            None => ptr::null_mut(),
        };
        match self
            .ptr
            .compare_exchange(expected_ptr, new_ptr, success_ordering, failure_ordering)
        {
            Ok(old_ptr) => {
                if old_ptr.is_null() {
                    Ok(None)
                } else {
                    // Safety: same as `swap`.
                    unsafe { Ok(Some(*Box::from_raw(old_ptr))) }
                }
            }
            Err(_) => {
                if new_ptr.is_null() {
                    Err(None)
                } else {
                    // Safety: CAS failed, the slot wasn't updated, and
                    // nothing else aliases `new_ptr`.
                    Err(unsafe { Some(*Box::from_raw(new_ptr)) })
                }
            }
        }
    }
}

impl<T: Clone> AtomicOption<T> {
    /// Best-effort snapshot: clone the current value without permanently
    /// disturbing the slot. Takes briefly (via `swap_with_token(None)`),
    /// clones, then CAS-restores the original.
    ///
    /// - Concurrent writer wins: if another thread swapped a new value in
    ///   while we were cloning, the CAS restore fails and the writer's value
    ///   stays; we return our clone, which is now one write cycle stale.
    /// - Concurrent reader race: if two readers try at once, one wins the
    ///   `take` and the other sees `None` transiently. [`load_cloned`] spins
    ///   up to [`LOAD_SPIN_RETRIES`] before concluding the slot is genuinely
    ///   empty.
    ///
    /// Typical shape is `AtomicOption<Arc<U>>` so the clone is an Arc bump.
    pub fn load_cloned(&self) -> Option<T> {
        for _ in 0..LOAD_SPIN_RETRIES {
            let (taken, token_after_take) = self.swap_with_token(None, Ordering::AcqRel);
            match taken {
                Some(val) => {
                    let cloned = val.clone();
                    // Try to restore. On CAS failure the writer already put
                    // their new value in; drop our `val`, writer wins.
                    let _ = self.swap_if_token(
                        token_after_take,
                        Some(val),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    return Some(cloned);
                }
                None => std::hint::spin_loop(),
            }
        }
        None
    }
}

/// Bound on the number of spins [`AtomicOption::load_cloned`] will perform
/// before treating `None` as genuine. Each iteration is one concurrent
/// reader's take+CAS cycle — a few atomic ops, tens of nanoseconds — so 64
/// accommodates ~64 concurrent readers without false negatives while
/// capping the spin budget around ~1μs on a truly empty slot.
const LOAD_SPIN_RETRIES: usize = 64;

impl<T> Drop for AtomicOption<T> {
    fn drop(&mut self) {
        let ptr = self.ptr.swap(ptr::null_mut(), Ordering::Relaxed);
        if !ptr.is_null() {
            // Safety: final drop, no aliases can exist.
            unsafe { drop(Box::from_raw(ptr)) };
        }
    }
}

// Safety: writes/drops of the boxed `T` are serialized by the atomic
// operations and happen-before the corresponding reads on the other side.
unsafe impl<T: Send> Send for AtomicOption<T> {}
unsafe impl<T: Send> Sync for AtomicOption<T> {}

impl<T> Default for AtomicOption<T> {
    fn default() -> Self {
        Self::none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn new_some_and_none() {
        let a: AtomicOption<u32> = AtomicOption::none();
        assert!(a.is_none(Ordering::Acquire));
        assert!(!a.is_some(Ordering::Acquire));

        let b = AtomicOption::some(42u32);
        assert!(b.is_some(Ordering::Acquire));
    }

    #[test]
    fn swap_returns_old_value() {
        let a = AtomicOption::some(1u32);
        let old = a.swap(Some(2), Ordering::AcqRel);
        assert_eq!(old, Some(1));
        assert_eq!(a.take(Ordering::AcqRel), Some(2));
        assert!(a.is_none(Ordering::Acquire));
    }

    #[test]
    fn try_put_only_when_empty() {
        let a: AtomicOption<u32> = AtomicOption::none();
        assert!(a.try_put(1, Ordering::AcqRel, Ordering::Acquire).is_ok());
        // Slot now occupied — second put returns the value back.
        assert_eq!(
            a.try_put(2, Ordering::AcqRel, Ordering::Acquire),
            Err(2),
        );
    }

    #[test]
    fn swap_if_token_honors_token() {
        let a = AtomicOption::some(1u32);
        // Take to obtain a fresh token for the empty state.
        let (taken, token) = a.swap_with_token(None, Ordering::AcqRel);
        assert_eq!(taken, Some(1));

        // Restore under the correct token — CAS succeeds.
        let restored = a.swap_if_token(
            token,
            Some(1),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        assert_eq!(restored, Ok(None));
        assert_eq!(a.take(Ordering::AcqRel), Some(1));
    }

    #[test]
    fn swap_if_token_rejects_stale_token() {
        let a = AtomicOption::some(1u32);
        // Grab a token for the current state, then perturb the slot so the
        // token is stale.
        let (taken, stale_token) = a.swap_with_token(Some(2), Ordering::AcqRel);
        assert_eq!(taken, Some(1));
        a.store(Some(3), Ordering::AcqRel);

        // CAS under the stale token fails; we get our new value back.
        let result = a.swap_if_token(
            stale_token,
            Some(99),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        assert_eq!(result, Err(Some(99)));
        // Slot is untouched.
        assert_eq!(a.take(Ordering::AcqRel), Some(3));
    }

    #[test]
    fn load_cloned_on_arc_roundtrips() {
        let a: AtomicOption<Arc<Vec<u32>>> =
            AtomicOption::some(Arc::new(vec![1, 2, 3]));

        let snap = a.load_cloned().expect("slot has value");
        assert_eq!(&*snap, &[1, 2, 3]);

        // Slot was restored — a second load sees the same data.
        let snap2 = a.load_cloned().expect("slot still has value");
        assert_eq!(&*snap2, &[1, 2, 3]);
    }

    #[test]
    fn load_cloned_returns_none_on_empty() {
        let a: AtomicOption<Arc<Vec<u32>>> = AtomicOption::none();
        assert!(a.load_cloned().is_none());
    }

    #[test]
    fn load_cloned_sees_latest_write() {
        let a: AtomicOption<Arc<Vec<u32>>> =
            AtomicOption::some(Arc::new(vec![1]));
        a.store(Some(Arc::new(vec![9, 9, 9])), Ordering::AcqRel);
        let snap = a.load_cloned().unwrap();
        assert_eq!(&*snap, &[9, 9, 9]);
    }

    #[test]
    fn drop_releases_value() {
        // Use Arc refcount as a drop witness — when the slot drops, the
        // inner Arc strong-count must fall to 1 (held only by our handle).
        let shared = Arc::new(());
        {
            let a = AtomicOption::some(shared.clone());
            assert_eq!(Arc::strong_count(&shared), 2);
            drop(a);
        }
        assert_eq!(Arc::strong_count(&shared), 1);
    }
}
