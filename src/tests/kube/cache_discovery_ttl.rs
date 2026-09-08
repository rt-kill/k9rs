use super::*;
use crate::kube::protocol::{ContextId, ContextName};

fn cid(n: u64) -> ContextId {
    ContextId::new(ContextName::new(format!("ctx{n}")).unwrap(), format!("https://{n}"), n)
}

fn set_access(cache: &DiscoveryCache, id: &ContextId, secs: u64) {
    cache.entries.get(id).unwrap().last_access.store(secs, Ordering::Relaxed);
}

/// `sweep_older_than` removes entries idle past the cutoff and keeps the
/// rest. Driven by an explicit cutoff so it doesn't depend on wall clock.
#[test]
fn sweep_evicts_only_idle_entries() {
    let cache = DiscoveryCache::new();
    cache.set_namespaces(cid(1), vec!["default".into()]);
    cache.set_namespaces(cid(2), vec!["default".into()]);
    set_access(&cache, &cid(1), 100); // idle
    set_access(&cache, &cid(2), 500); // recent

    let removed = cache.sweep_older_than(300);
    assert_eq!(removed, 1);
    assert!(cache.namespaces(&cid(1)).is_none());
    assert!(cache.namespaces(&cid(2)).is_some());
}

/// A read bumps `last_access`, rescuing an otherwise-idle entry from the
/// next sweep. Plants a far-future stamp the read must overwrite with now.
#[test]
fn reads_touch_last_access() {
    let cache = DiscoveryCache::new();
    cache.set_namespaces(cid(1), vec!["default".into()]);
    set_access(&cache, &cid(1), 1_000_000);
    let _ = cache.namespaces(&cid(1));
    let after = cache.entries.get(&cid(1)).unwrap().last_access.load(Ordering::Relaxed);
    assert!(after < 1_000_000, "a read should refresh last_access to ~now");
}
