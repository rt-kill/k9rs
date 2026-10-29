//! One registry for resources shared by identity.
//!
//! A `DashMap<K, Weak<V>>` where the map holds only `Weak`: holders own the
//! `Arc`, so the resource dies with its last holder and no policy has to
//! decide when. That is the ownership model four places in this crate already
//! used — the watcher cache, the metrics/discovery poller cache, the
//! per-context locals registry, and the per-name exec-source map — each with
//! its own hand-written copy of the same ~25-line dance.
//!
//! # Why the dance is not three lines
//!
//! Get-or-create over a `Weak` map is double-checked locking, and each check
//! is load-bearing:
//!
//! 1. **Reap.** Dead slots are a `K` clone plus a pointer and counter each;
//!    a daemon that churns through distinct keys accumulates them forever.
//!    It runs on every get-or-create, and `DashMap::retain` takes a WRITE
//!    lock on every shard — so it is the step that costs, and step 2 below
//!    is not the fast path its own wording suggests. Kept anyway because a
//!    get-or-create happens per subscribe and these maps hold tens of
//!    entries; it is the right trade at this size, not a free one.
//! 2. **Optimistic `get`.** A hit answers from a shard READ lock rather than
//!    the entry's write lock. (Which matters for the lock the entry would
//!    hold across `create`, not for avoiding step 1's.)
//! 3. **`entry()`, then check AGAIN.** Between 2 and 3 a concurrent caller
//!    can have created the same key. Skipping this second check is how you
//!    get two live resources for one identity — two apiserver watches, two
//!    row stores — with one of them unreachable in the map.
//! 4. **Liveness**, where the resource can die without its `Arc` dropping: a
//!    watcher task that self-terminates still upgrades, and handing it out
//!    gives the caller a corpse. Declared per registry by the `alive`
//!    predicate, so a registry whose values cannot die that way says so by
//!    passing `|_| true` rather than by omitting a check.
//!
//! Four copies of that is four chances to drop step 3 or 4. Two of the four
//! already differed: two reaped through a `fn reap_dead` duplicated verbatim
//! across two files, two inlined the identical `retain` predicate.
//!
//! # Reporting does not reap
//!
//! [`WeakRegistry::live`] skips dead slots without removing them. Both status
//! paths said so in prose; here it is one function, so a future third caller
//! cannot get it wrong.

use std::hash::Hash;
use std::sync::{Arc, Weak};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

/// What [`WeakRegistry::get_or_create`] did.
///
/// One production reader — `WatcherCache`'s three `tracing::debug!` lines;
/// the other three call sites discard it, each having nothing to say about
/// the difference. That made it look like a three-variant enum whose only
/// observable effect was a debug string, which is how it was reported. It is
/// also the only thing that makes step 3 of the module docs CHECKABLE from
/// outside: `concurrent_callers_for_one_absent_key_build_exactly_one_resource`
/// asserts exactly one `Created` among N racing callers, and
/// `the_entry_recheck_hands_back_what_a_racing_caller_created` asserts the
/// `Reused` that only the re-check can return. Delete this and the re-check
/// goes back to being unpinnable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// An existing live resource was handed back. `create` never ran.
    Reused,
    /// No entry existed, or the one that did had no holders left.
    Created,
    /// An entry existed and still upgraded, but `alive` rejected it — the
    /// resource outlived its own usefulness (a watcher whose task finished).
    /// The old `Arc` stays alive for anyone still holding it.
    ReplacedDead,
}

/// Resources shared by identity: `Arc` out, `Weak` held. See the module docs.
///
/// **What this asks of callers.** `create` and `alive` run while a DashMap
/// SHARD lock is held — that is what makes get-or-create and
/// [`WeakRegistry::replace`] atomic per key, and it is not free: neither
/// closure may block, `.await`, or touch this registry again, because a
/// second access to the same shard would deadlock against the lock it is
/// already inside. In this codebase both are small and synchronous (spawn a
/// task, check a flag), which is why the trade is worth making; a `create`
/// that wanted to do I/O would have to be restructured rather than
/// smuggled in.
pub(crate) struct WeakRegistry<K, V> {
    entries: DashMap<K, Weak<V>>,
}

impl<K, V> Default for WeakRegistry<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self { entries: DashMap::new() }
    }
}

impl<K, V> WeakRegistry<K, V>
where
    K: Eq + Hash + Clone,
{
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The live resource for `key`, creating one if there is none.
    ///
    /// `alive` is consulted only for an entry that still upgrades; a registry
    /// whose values cannot die while held passes `|_| true`. `create` runs
    /// under the entry lock, so two concurrent callers for one key produce
    /// one resource and the loser gets the winner's.
    pub(crate) fn get_or_create(
        &self,
        key: K,
        alive: impl Fn(&Arc<V>) -> bool,
        create: impl FnOnce(&K) -> Arc<V>,
    ) -> (Arc<V>, Outcome) {
        self.reap();

        // Fast path: a shard read lock, not the entry's write lock.
        if let Some(weak) = self.entries.get(&key) {
            if let Some(arc) = weak.upgrade() {
                if alive(&arc) {
                    return (arc, Outcome::Reused);
                }
            }
        }

        match self.entries.entry(key.clone()) {
            // Re-check: a concurrent caller may have created this key since
            // the read above. Without this, both callers create.
            Entry::Occupied(mut e) => {
                if let Some(arc) = e.get().upgrade() {
                    if alive(&arc) {
                        return (arc, Outcome::Reused);
                    }
                    let fresh = create(&key);
                    e.insert(Arc::downgrade(&fresh));
                    return (fresh, Outcome::ReplacedDead);
                }
                let fresh = create(&key);
                e.insert(Arc::downgrade(&fresh));
                (fresh, Outcome::Created)
            }
            Entry::Vacant(e) => {
                let fresh = create(&key);
                e.insert(Arc::downgrade(&fresh));
                (fresh, Outcome::Created)
            }
        }
    }

    /// Create unconditionally, displacing whatever the slot held.
    ///
    /// `create` runs UNDER the entry lock — the same claim [`get_or_create`]
    /// takes — so a concurrent get-or-create for the same key is serialized
    /// with this rather than the two insertions interleaving. The displaced
    /// resource lives as long as anyone still holds its `Arc`.
    ///
    /// That claim was false when it was first written: `create` ran before
    /// `entry()` was called, so a get-or-create could slip between the two,
    /// build its own resource, insert it, and have this call overwrite the
    /// slot a moment later — leaving the other caller holding a resource
    /// that nothing could reach, which is the exact interleaving the doc
    /// said could not happen. `create` is cheap and synchronous here (it
    /// spawns, it does not block), which is what makes holding the shard
    /// lock across it the right trade; see the type's own doc for the
    /// constraint that puts on callers.
    ///
    /// [`get_or_create`]: Self::get_or_create
    pub(crate) fn replace(&self, key: K, create: impl FnOnce(&K) -> Arc<V>) -> Arc<V> {
        match self.entries.entry(key) {
            Entry::Occupied(mut e) => {
                let fresh = create(e.key());
                e.insert(Arc::downgrade(&fresh));
                fresh
            }
            Entry::Vacant(e) => {
                let fresh = create(e.key());
                e.insert(Arc::downgrade(&fresh));
                fresh
            }
        }
    }

    /// Record an externally-built resource under `key`. The registry keeps
    /// only a `Weak`, so the caller must hold the `Arc`.
    ///
    /// Gated like its only caller, `WatcherCache::seed_driven`: minting a
    /// resource outside `get_or_create` skips the race check, which is a
    /// test seam and must not be reachable from a release build.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn insert(&self, key: K, value: &Arc<V>) {
        self.entries.insert(key, Arc::downgrade(value));
    }

    /// Drop slots whose last holder is gone.
    pub(crate) fn reap(&self) {
        self.entries.retain(|_, weak| weak.strong_count() > 0);
    }

    /// Slots, live or dead. The difference between this and `live().len()` is
    /// exactly the un-reaped backlog, which is what pins that reporting does
    /// not reap. Test-only, like its caller.
    #[cfg(test)]
    pub(crate) fn slot_count(&self) -> usize {
        self.entries.len()
    }

    /// Every live resource with its key and holder count, newest-first order
    /// unspecified.
    ///
    /// Skips dead slots WITHOUT reaping them: reporting must not mutate what
    /// it reports on.
    pub(crate) fn live(&self) -> Vec<(K, Arc<V>, usize)> {
        self.entries
            .iter()
            .filter_map(|e| {
                let holders = e.value().strong_count();
                e.value().upgrade().map(|arc| (e.key().clone(), arc, holders))
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "tests/weak_registry.rs"]
mod tests;
