//! Lock-free optional value built on a tagged `AtomicPtr`.
//!
//! The slot holds one of two things, distinguished by the low bit:
//!
//! - an **occupied** box pointer — `Box::into_raw(Box::new(Slot2(value)))`,
//!   where [`Slot2`] forces `align >= 2` so the low bit is always free; or
//! - an **empty token** — `(generation << 1) | 1`, low bit `1`, used purely as
//!   a tagged word and never dereferenced.
//!
//! Ownership of the boxed `T` transfers via atomic swaps/CAS; the value drops
//! inline with the operation that evicts it. There is no deferred reclamation,
//! so each slot has a single logical owner at a time and `T` is owned
//! end-to-end by the `AtomicOption`.
//!
//! # Why the generation
//!
//! [`AtomicOption::load_cloned`] can't clone through a raw pointer a concurrent
//! writer might free, so it *takes* the value (swaps an empty token in), clones
//! under exclusive ownership, then CAS-restores the original. If the empty
//! state were a plain null, that take/restore is ABA-prone: two readers
//! straddling a writer can leave the slot holding a *resurrected* older value —
//! a silent lost update (exactly what
//! `concurrent_readers_never_resurrect_a_stale_write` exercises).
//!
//! Tagging each empty token with a fresh, unique generation closes that hole. A
//! reader restores with `compare_exchange(my_empty, original)`, which succeeds
//! only if the slot is *still its own* token. Any intervening write — or another
//! reader's take, which mints a different token — makes the restore fail, and
//! the reader drops its value rather than resurrecting it. The newest value
//! always wins, so the slot is never left stale.
//!
//! # Progress guarantee
//!
//! Lock-free, not wait-free. A reader that observes an empty token can't
//! distinguish "genuinely empty" from "another reader is mid-take", so
//! [`load_cloned`](AtomicOption::load_cloned) spins a bounded number of times
//! before treating empty as genuine; under pathological contention (more
//! concurrent readers than the spin budget) it may return a spurious `None`.
//! Readers never block one another.

use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// Wrapper that forces `align >= 2` so a boxed value's pointer always has its
/// low bit clear, leaving it free for the empty-token tag regardless of `T`'s
/// own alignment. (For `T` that is already `align >= 2`, this is a no-op.)
#[repr(align(2))]
struct Slot2<T>(T);

/// `true` if `p` is an empty token (low bit set) rather than a box pointer.
#[inline]
fn is_empty_word<T>(p: *mut T) -> bool {
    (p as usize) & 1 == 1
}

/// Thread-safe optional value. `T` is owned; writes drop the previous value
/// inline. See the module docs for the tagged-pointer representation.
pub struct AtomicOption<T> {
    /// Box pointer (low bit 0) or empty token `(gen << 1) | 1` (low bit 1).
    slot: AtomicPtr<Slot2<T>>,
    /// Monotonic source of unique empty-token generations for this cell.
    empty_gen: AtomicUsize,
}

impl<T> AtomicOption<T> {
    /// An empty `AtomicOption`.
    pub const fn none() -> Self {
        Self {
            // Empty token, generation 0. `empty_gen` starts at 1 so no minted
            // token ever collides with this initial word — which is, in any
            // case, never a restore target (only takes mint restorable tokens).
            slot: AtomicPtr::new(ptr::without_provenance_mut(1)),
            empty_gen: AtomicUsize::new(1),
        }
    }

    /// An `AtomicOption` containing `value`.
    pub fn some(value: T) -> Self {
        Self {
            slot: AtomicPtr::new(Box::into_raw(Box::new(Slot2(value)))),
            empty_gen: AtomicUsize::new(1),
        }
    }

    /// Construct from an `Option<T>`.
    pub fn new(value: Option<T>) -> Self {
        match value {
            Some(v) => Self::some(v),
            None => Self::none(),
        }
    }

    /// Mint a fresh, unique empty token for this cell. Each call returns a
    /// distinct tagged word; the generation only needs to be unique, so its
    /// `fetch_add` is `Relaxed` — data ordering rides on the slot's atomics.
    #[inline]
    fn mint_empty(&self) -> *mut Slot2<T> {
        let g = self.empty_gen.fetch_add(1, Ordering::Relaxed);
        ptr::without_provenance_mut((g << 1) | 1)
    }

    /// Reconstruct the owned value from an evicted slot word, or `None` if it
    /// was an empty token.
    ///
    /// # Safety
    /// `word` must have just been removed from the slot by the caller (a swap
    /// or a successful CAS), giving exclusive ownership of any boxed allocation.
    #[inline]
    unsafe fn reclaim(word: *mut Slot2<T>) -> Option<T> {
        if is_empty_word(word) {
            None
        } else {
            Some((*Box::from_raw(word)).0)
        }
    }

    /// Atomically swap the slot, returning the previous value.
    pub fn swap(&self, value: Option<T>, ordering: Ordering) -> Option<T> {
        let new_word = match value {
            Some(v) => Box::into_raw(Box::new(Slot2(v))),
            None => self.mint_empty(),
        };
        let old = self.slot.swap(new_word, ordering);
        // Safety: `old` was just removed from the slot, so we own it.
        unsafe { Self::reclaim(old) }
    }

    /// Store `value`, dropping whatever was there.
    pub fn store(&self, value: Option<T>, ordering: Ordering) {
        let _ = self.swap(value, ordering);
    }

    /// Take the value, leaving an empty slot behind.
    pub fn take(&self, ordering: Ordering) -> Option<T> {
        self.swap(None, ordering)
    }

    /// True if the slot currently holds `Some`.
    pub fn is_some(&self, ordering: Ordering) -> bool {
        !is_empty_word(self.slot.load(ordering))
    }

    /// True if the slot currently holds `None`.
    pub fn is_none(&self, ordering: Ordering) -> bool {
        is_empty_word(self.slot.load(ordering))
    }
}

impl<T: Clone> AtomicOption<T> {
    /// Best-effort snapshot: clone the current value without permanently
    /// disturbing the slot.
    ///
    /// Briefly *claims* the value (swaps in a unique empty token) so no
    /// concurrent op can free it mid-clone, clones under that exclusivity, then
    /// CAS-restores the original. The unique token makes the restore airtight:
    ///
    /// - **Concurrent writer wins.** If a writer swapped a new value in while we
    ///   were cloning, our restore CAS fails; we drop our (now-stale) original
    ///   and the writer's value stays. We return our clone, one cycle stale.
    /// - **No resurrection.** Because the token is unique per claim, the restore
    ///   can never succeed against another reader's or a writer's slot state, so
    ///   a stale value is never resurrected over a newer one.
    /// - **Concurrent reader race.** Two readers can't both claim the same
    ///   value; the loser observes an empty token and spins up to
    ///   [`LOAD_SPIN_RETRIES`] before treating the slot as genuinely empty.
    ///
    /// Typical shape is `AtomicOption<Arc<U>>`, so the clone is an Arc bump.
    pub fn load_cloned(&self) -> Option<T> {
        for _ in 0..LOAD_SPIN_RETRIES {
            let cur = self.slot.load(Ordering::Acquire);
            if is_empty_word(cur) {
                std::hint::spin_loop();
                continue;
            }
            // `cur` is an occupied box pointer. Claim it with a unique empty
            // token; the CAS succeeds only if the slot is still `cur`, which
            // also proves `cur` is live (no concurrent take freed it).
            let my_empty = self.mint_empty();
            if self
                .slot
                .compare_exchange(cur, my_empty, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                // The slot moved between the load and the claim; `my_empty`
                // carries no allocation, so there is nothing to reclaim. Retry.
                std::hint::spin_loop();
                continue;
            }
            // We exclusively own the allocation at `cur` (the slot holds our
            // token, so no other op can reach it). Clone through it.
            // Safety: `cur` came from `Box::into_raw` and we hold it exclusively.
            let cloned = unsafe { (*cur).0.clone() };
            // Restore the original — but only if the slot is *still* our token.
            // Any intervening write makes this fail, and we drop `cur` instead
            // of resurrecting it over the newer value.
            if self
                .slot
                .compare_exchange(my_empty, cur, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                // Safety: the CAS failed, so the slot no longer points at `cur`;
                // we still own it and must free it.
                unsafe { drop(Box::from_raw(cur)) };
            }
            return Some(cloned);
        }
        None
    }
}

/// Bound on the spins [`AtomicOption::load_cloned`] performs before treating an
/// empty token as a genuinely empty slot. Each iteration is one concurrent
/// reader's claim+restore cycle — a few atomic ops — so 64 accommodates dozens
/// of concurrent readers without a false negative while capping the spin budget
/// around ~1μs on a truly empty slot.
const LOAD_SPIN_RETRIES: usize = 64;

impl<T> Drop for AtomicOption<T> {
    fn drop(&mut self) {
        // Exclusive access at drop — read the word directly, no atomics needed.
        let word = *self.slot.get_mut();
        if !is_empty_word(word) {
            // Safety: final drop, exclusive access, `word` is a `Box::into_raw`
            // pointer.
            unsafe { drop(Box::from_raw(word)) };
        }
    }
}

// Safety: writes/drops of the boxed `T` are serialized by the slot's atomic
// operations and happen-before the matching reads on the other side; `T` is
// only ever moved (Send) or cloned under exclusive ownership (never shared as
// `&T` across threads), so `Sync` needs only `T: Send`.
unsafe impl<T: Send> Send for AtomicOption<T> {}
unsafe impl<T: Send> Sync for AtomicOption<T> {}

impl<T> Default for AtomicOption<T> {
    fn default() -> Self {
        Self::none()
    }
}

#[cfg(test)]
#[path = "../tests/util/atomic_option.rs"]
mod tests;
