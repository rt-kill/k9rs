//! TTL cache for describe/yaml kubectl output.
//!
//! Split out of the former `app::types` grab-bag: this is a cohesive
//! unit with its own vocabulary, and it was only ever in `types.rs`
//! because that file was where types went.

use crate::kube::protocol::ObjectRef;
use std::collections::HashMap;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// KubectlCache — TTL cache for describe/yaml kubectl output
// ---------------------------------------------------------------------------

/// What kind of content is cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContentKind {
    Yaml,
    Describe,
}

/// Key for the kubectl output cache. Typed end-to-end: keyed on the full
/// `ObjectRef` so two CRDs sharing a Kind name across groups never collide.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub target: ObjectRef,
    pub kind: ContentKind,
}

impl CacheKey {
    pub fn new(target: ObjectRef, kind: ContentKind) -> Self {
        Self { target, kind }
    }
}

/// A cached content entry with its timestamp.
pub(crate) struct CacheEntry {
    pub(crate) content: String,
    /// Typed describe lines for `ContentKind::Describe` entries; empty for
    /// YAML. Lets a cached describe re-open render by role tags exactly like a
    /// fresh one, with no inference-on-cache-hit inconsistency.
    pub(crate) describe_lines: Vec<crate::kube::protocol::DescribeLine>,
    pub(crate) cached_at: Instant,
}

pub struct KubectlCache {
    entries: HashMap<CacheKey, CacheEntry>,
    insertion_order: Vec<CacheKey>,
    ttl: Duration,
    max_capacity: usize,
}

impl KubectlCache {
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            insertion_order: Vec::new(),
            ttl,
            max_capacity: capacity,
        }
    }

    pub fn get(&self, target: &ObjectRef, kind: ContentKind) -> Option<&str> {
        let key = CacheKey::new(target.clone(), kind);
        self.entries
            .get(&key)
            .and_then(|entry| {
                if entry.cached_at.elapsed() < self.ttl {
                    Some(entry.content.as_str())
                } else {
                    None
                }
            })
    }

    pub fn insert(&mut self, target: ObjectRef, kind: ContentKind, content: String) {
        self.insert_entry(
            CacheKey::new(target, kind),
            CacheEntry { content, describe_lines: Vec::new(), cached_at: Instant::now() },
        );
    }

    /// Cache a structured describe by its typed lines. The flat `content` is
    /// derived so the text-keyed `get` keeps working for this entry too.
    pub fn insert_describe(
        &mut self,
        target: ObjectRef,
        lines: Vec<crate::kube::protocol::DescribeLine>,
    ) {
        let content = crate::kube::protocol::describe_lines_text(&lines);
        self.insert_entry(
            CacheKey::new(target, ContentKind::Describe),
            CacheEntry { content, describe_lines: lines, cached_at: Instant::now() },
        );
    }

    /// Shared insert: update in place if the key exists (preserving insertion
    /// order), else append and, once at capacity, evict the oldest entry —
    /// FIFO by insertion order, not LRU (an in-place update does not refresh
    /// recency, so a frequently-read entry is still evicted on schedule).
    fn insert_entry(&mut self, key: CacheKey, entry: CacheEntry) {
        if let std::collections::hash_map::Entry::Occupied(mut e) = self.entries.entry(key.clone()) {
            e.insert(entry);
            return;
        }
        if self.entries.len() >= self.max_capacity {
            if let Some(oldest_key) = self.insertion_order.first().cloned() {
                self.entries.remove(&oldest_key);
                self.insertion_order.remove(0);
            }
        }
        self.insertion_order.push(key.clone());
        self.entries.insert(key, entry);
    }

    /// Fresh typed describe lines for `target`, if cached and unexpired. Used by
    /// the describe cache-hit path so a re-open renders identically to a fresh
    /// describe instead of falling back to text inference.
    pub fn get_describe_lines(
        &self,
        target: &ObjectRef,
    ) -> Option<Vec<crate::kube::protocol::DescribeLine>> {
        let key = CacheKey::new(target.clone(), ContentKind::Describe);
        self.entries.get(&key).and_then(|entry| {
            // Empty lines count as a miss, not a hit: a describe view fed empty
            // lines would derive empty `content` and wedge on "Loading…". A real
            // describe always has lines, so this only rejects a degenerate
            // entry and re-fetches.
            let usable = entry.cached_at.elapsed() < self.ttl && !entry.describe_lines.is_empty();
            usable.then(|| entry.describe_lines.clone())
        })
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.insertion_order.clear();
    }
}


