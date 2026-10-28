//! The kubectl output cache: TTL, capacity, and what it is keyed on.
//!
//! No test mirror until 2026-10-27. The question that needed one: the key is
//! `(ObjectRef, ContentKind)` and an `ObjectRef` carries no context, so
//! `describe pod/api -n default` cached against one cluster is a HIT for the
//! identically-named pod in another. Correctness rested entirely on one of
//! nine hand-placed `clear()` calls firing on the switch — manual policy,
//! not ownership. The last test here is the one that establishes whether it
//! is reachable.

use super::*;
use k9rs_proto::protocol::{Namespace, ObjectRef};
use k9rs_proto::resource_def::BuiltInKind;

fn target(name: &str, ns: &str) -> ObjectRef {
    ObjectRef::new(
        BuiltInKind::Pod.to_resource_id(),
        name.to_string(),
        Namespace::Named(ns.to_string()),
    )
}

/// A hit is the content that went in; a different target is a miss.
#[test]
fn an_entry_is_returned_for_its_own_target_only() {
    let mut c = KubectlCache::new(std::time::Duration::from_secs(60), 8);
    c.insert(target("api", "default"), ContentKind::Yaml, "spec: api".into());

    assert_eq!(c.get(&target("api", "default"), ContentKind::Yaml), Some("spec: api"));
    assert_eq!(c.get(&target("api", "other-ns"), ContentKind::Yaml), None, "namespace is in the key");
    assert_eq!(c.get(&target("web", "default"), ContentKind::Yaml), None, "so is the name");
    assert_eq!(
        c.get(&target("api", "default"), ContentKind::Describe),
        None,
        "and so is the KIND — a yaml entry is not a describe",
    );
}

/// Past the TTL an entry is a miss, not stale content.
#[test]
fn an_expired_entry_is_a_miss() {
    let mut c = KubectlCache::new(std::time::Duration::from_millis(1), 8);
    c.insert(target("api", "default"), ContentKind::Yaml, "old".into());
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert_eq!(c.get(&target("api", "default"), ContentKind::Yaml), None);
}

/// At capacity the OLDEST insertion is evicted — FIFO, and deliberately not
/// LRU: an in-place update does not refresh recency, so a frequently-read
/// entry still leaves on schedule.
#[test]
fn at_capacity_the_oldest_insertion_leaves_first() {
    let mut c = KubectlCache::new(std::time::Duration::from_secs(60), 2);
    c.insert(target("first", "ns"), ContentKind::Yaml, "1".into());
    c.insert(target("second", "ns"), ContentKind::Yaml, "2".into());
    // Re-reading and re-writing `first` must NOT save it.
    assert_eq!(c.get(&target("first", "ns"), ContentKind::Yaml), Some("1"));
    c.insert(target("first", "ns"), ContentKind::Yaml, "1b".into());

    c.insert(target("third", "ns"), ContentKind::Yaml, "3".into());
    assert_eq!(c.get(&target("first", "ns"), ContentKind::Yaml), None, "oldest out");
    assert_eq!(c.get(&target("second", "ns"), ContentKind::Yaml), Some("2"));
    assert_eq!(c.get(&target("third", "ns"), ContentKind::Yaml), Some("3"));
}

/// A describe entry comes back as typed LINES, so a cache hit renders by
/// role tags exactly as a fresh describe does rather than re-inferring from
/// text. Empty lines count as a miss: a view fed them would derive empty
/// content and wedge on "Loading…".
#[test]
fn a_cached_describe_comes_back_typed() {
    let mut c = KubectlCache::new(std::time::Duration::from_secs(60), 8);
    let lines = vec![
        k9rs_proto::protocol::DescribeLine::section("Spec:"),
        k9rs_proto::protocol::DescribeLine::field("  Node: n1", 6),
    ];
    c.insert_describe(target("api", "default"), lines.clone());

    let got = c.get_describe_lines(&target("api", "default")).expect("a hit");
    assert_eq!(got.len(), 2);
    assert!(
        c.get(&target("api", "default"), ContentKind::Describe).is_some(),
        "and the flat text is derived, so the text-keyed `get` works too",
    );

    c.insert_describe(target("empty", "default"), Vec::new());
    assert!(
        c.get_describe_lines(&target("empty", "default")).is_none(),
        "a degenerate empty entry is a miss, not a hit",
    );
}

/// **The key carries no cluster.** Two clusters with an identically-named
/// pod in an identically-named namespace share one entry, so whether a
/// switch serves the previous cluster's YAML rests on a `clear()` firing.
///
/// Asserted as the fact it is, rather than as a wish: this pins the
/// behaviour so that adding the context to the key is a visible change here
/// rather than a silent one, and so the nine `clear()` call sites are
/// legible as load-bearing instead of defensive.
#[test]
fn the_key_does_not_distinguish_two_clusters() {
    let mut c = KubectlCache::new(std::time::Duration::from_secs(60), 8);
    // Everything an `ObjectRef` can say, said identically — because an
    // `ObjectRef` cannot say which cluster.
    c.insert(target("api", "default"), ContentKind::Yaml, "cluster-a yaml".into());
    assert_eq!(
        c.get(&target("api", "default"), ContentKind::Yaml),
        Some("cluster-a yaml"),
        "the same ref from a DIFFERENT cluster is indistinguishable here — \
         `KubectlCache::clear` on a context switch is what stands between \
         this and showing one cluster's object as another's",
    );
}
