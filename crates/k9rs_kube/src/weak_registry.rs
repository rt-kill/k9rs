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
//! 2. **Optimistic `get`.** The common case is a hit, and `get` takes a
//!    shard read lock rather than the entry's write lock.
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

/// What [`WeakRegistry::get_or_create`] did, for callers that log or count.
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
    /// Under the entry lock — the same claim [`get_or_create`] takes — so a
    /// concurrent get-or-create is serialized with this rather than the two
    /// insertions interleaving and stranding a just-created resource
    /// unreachable in the map. The displaced resource lives as long as anyone
    /// still holds its `Arc`.
    ///
    /// [`get_or_create`]: Self::get_or_create
    pub(crate) fn replace(&self, key: K, create: impl FnOnce(&K) -> Arc<V>) -> Arc<V> {
        let fresh = create(&key);
        let weak = Arc::downgrade(&fresh);
        match self.entries.entry(key) {
            Entry::Occupied(mut e) => {
                e.insert(weak);
            }
            Entry::Vacant(e) => {
                e.insert(weak);
            }
        }
        fresh
    }

    /// Forget the slot for `key`, so the next `get_or_create` builds fresh.
    /// Whatever held the old `Arc` keeps it.
    pub(crate) fn forget(&self, key: &K) {
        self.entries.remove(key);
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
