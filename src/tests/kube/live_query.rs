//! `WatcherCache` as `ctl status` sees it.

use super::*;
use crate::kube::protocol::{ContextId, ContextName, Namespace, ResourceId};
use crate::kube::resource_def::BuiltInKind;

fn key(ctx: &str, resource: BuiltInKind) -> QueryKey {
    QueryKey {
        context: ContextId::new(
            ContextName::new(ctx).expect("non-empty"),
            format!("https://{ctx}"),
            1,
        ),
        namespace: Namespace::All,
        resource: ResourceId::BuiltIn(resource),
        filter: None,
    }
}

#[tokio::test]
async fn status_reports_live_watchers_and_forgets_dead_ones() {
    // `ctl status` used to answer only "running, since when", which says
    // nothing about the one thing that actually goes wrong in a long-lived
    // daemon: a watcher that should have been reclaimed and wasn't. The
    // report has to distinguish a live watcher from a cache slot whose `Weak`
    // no longer upgrades, or the number it prints is the number of slots ever
    // used rather than the number of watches currently open.
    let cache = WatcherCache::new();
    let (pods, _pods_driver) = cache.seed_driven(key("prod", BuiltInKind::Pod));
    let (nodes, _nodes_driver) = cache.seed_driven(key("prod", BuiltInKind::Node));

    let stats = cache.stats();
    assert_eq!(stats.len(), 2, "two live watchers");
    let mut resources: Vec<&str> = stats.iter().map(|s| s.resource.as_str()).collect();
    resources.sort_unstable();
    assert_eq!(resources, vec!["nodes", "pods"]);
    assert!(
        stats.iter().all(|s| s.context.as_str() == "prod"),
        "each row names the context that keyed it",
    );

    drop(pods);
    let stats = cache.stats();
    assert_eq!(stats.len(), 1, "a slot whose Weak is dead is not a watcher");
    assert_eq!(stats[0].resource, "nodes");

    drop(nodes);
    assert!(cache.stats().is_empty());
}

#[tokio::test]
async fn asking_what_the_daemon_holds_does_not_change_it() {
    // Reporting must not reap. The obvious implementation of `stats` reuses
    // `reap_dead`, which would make `ctl status` a mutation of the very thing
    // it claims to observe — and would quietly repair the leak it exists to
    // reveal, so the second run of `ctl status` always looks healthy.
    let cache = WatcherCache::new();
    let (pods, _driver) = cache.seed_driven(key("prod", BuiltInKind::Pod));
    drop(pods);

    assert!(cache.stats().is_empty(), "not reported");
    assert_eq!(cache.entry_count(), 1, "but not swept away either");
}
