//! Central registry of all known built-in resource types.
//!
//! Built once at startup via `LazyLock`. The registry maps plural names
//! and aliases to [`ResourceDef`] trait objects, and stores a type-erased
//! `WatcherSpawner` per resource for dispatching watchers without
//! string-matching if/else chains.

use std::collections::HashMap;

use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

use crate::live_query::{BaselineAsk, WatcherMsg};
use k9rs_proto::protocol::{Namespace, ResourceId, SubscriptionFilter};
use k9rs_proto::resource_def::{BuiltInKind, ResourceDef};
use crate::resources::ConvertToRow;
use k9rs_proto::row::ResourceRow;

// ---------------------------------------------------------------------------
// WatcherSpawner — type-erased watcher factory
// ---------------------------------------------------------------------------

/// Arguments passed to a `WatcherSpawner` at subscribe time.
pub(crate) struct WatcherArgs {
    pub client: kube::Client,
    pub namespace: Namespace,
    pub delta_tx: broadcast::Sender<Arc<WatcherMsg>>,
    pub ask_rx: mpsc::Receiver<BaselineAsk>,
    pub filter: Option<SubscriptionFilter>,
    pub streaming_lists: bool,
}

/// Type-erased watcher spawner. Created at registration time when concrete
/// K8s API types are known, called at subscribe time to spawn a typed
/// watcher task. Returns a `JoinHandle` so the `LiveQuery` can track and
/// abort the task.
type WatcherSpawner = Box<dyn Fn(WatcherArgs) -> JoinHandle<()> + Send + Sync>;

// ---------------------------------------------------------------------------
// RegistryEntry
// ---------------------------------------------------------------------------

/// Builds a `convert(K::default())` row for a registered resource. Captured at
/// registration (where the concrete `K` is known) so the alignment test can
/// verify each converter's `cells` length matches its `default_headers()`
/// without naming every concrete type.
type ConvertDefaultFn = Box<dyn Fn() -> ResourceRow + Send + Sync>;

struct RegistryEntry {
    spawner: WatcherSpawner,
    /// Read only by the alignment test; always populated so the guard can run.
    #[cfg_attr(not(test), allow(dead_code))]
    convert_default: ConvertDefaultFn,
}

// ---------------------------------------------------------------------------
// ResourceRegistry
// ---------------------------------------------------------------------------

/// Central registry of all known built-in resource types.
pub(crate) struct ResourceRegistry {
    /// Typed index of watcher factories, keyed by [`BuiltInKind`].
    ///
    /// It used to own the `ResourceDef` trait objects too, and the plural,
    /// alias and ordering indexes over them. All of that moved to
    /// `resource_def`, where `BuiltInKind::def` is generated from the enum's
    /// own declaration — so the metadata table is now reachable without
    /// naming kube-rs, which is the whole reason this split exists. What is
    /// left here is the half that genuinely needs the k8s types: a typed
    /// watcher factory per kind.
    by_kind: HashMap<BuiltInKind, RegistryEntry>,
}

impl Default for ResourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceRegistry {
    pub(crate) fn new() -> Self {
        Self {
            by_kind: HashMap::new(),
        }
    }

    // -- Registration (called once at startup) --------------------------------

    /// Register a **namespaced** resource type with its typed watcher factory.
    ///
    /// The `K` type parameter is the k8s-openapi struct (e.g. `Pod`,
    /// `Deployment`). It must be `NamespaceResourceScope` so the spawner
    /// can create `Api::namespaced` when a specific namespace is selected.
    pub(crate) fn register_namespaced<D, K>(&mut self, def: D)
    where
        D: ResourceDef + ConvertToRow<K> + 'static,
        K: kube::Resource<DynamicType = (), Scope = k8s_openapi::NamespaceResourceScope>
            + Clone
            + std::fmt::Debug
            + Send
            + Sync
            + serde::de::DeserializeOwned
            + Default
            + 'static,
    {
        let convert_fn: fn(K) -> ResourceRow = D::convert;
        let resource_id = def.resource_id();
        let default_headers = def.default_headers();

        let spawner: WatcherSpawner = Box::new(move |args: WatcherArgs| {
            let api: kube::Api<K> = match &args.namespace {
                Namespace::All => kube::Api::all(args.client.clone()),
                Namespace::Named(name) => kube::Api::namespaced(args.client.clone(), name),
            };
            spawn_typed_watcher(api, args, convert_fn, resource_id.clone(), default_headers.clone())
        });

        self.insert(def, spawner, Box::new(move || convert_fn(K::default())));
    }

    /// Register a **cluster-scoped** resource type with its typed watcher factory.
    ///
    /// Cluster resources always use `Api::all` regardless of namespace selection.
    pub(crate) fn register_cluster<D, K>(&mut self, def: D)
    where
        D: ResourceDef + ConvertToRow<K> + 'static,
        K: kube::Resource<DynamicType = ()>
            + Clone
            + std::fmt::Debug
            + Send
            + Sync
            + serde::de::DeserializeOwned
            + Default
            + 'static,
    {
        let convert_fn: fn(K) -> ResourceRow = D::convert;
        let resource_id = def.resource_id();
        let default_headers = def.default_headers();

        let spawner: WatcherSpawner = Box::new(move |args: WatcherArgs| {
            let api: kube::Api<K> = kube::Api::all(args.client.clone());
            spawn_typed_watcher(api, args, convert_fn, resource_id.clone(), default_headers.clone())
        });

        self.insert(def, spawner, Box::new(move || convert_fn(K::default())));
    }

    /// Record a kind's watcher factory.
    ///
    /// The alias, plural and ordering bookkeeping that used to live here —
    /// including the duplicate-alias panic — moved to `resource_def`'s
    /// `INDEX`, built from `BuiltInKind::ALL`. It had to move WITH the data:
    /// a check beside a table it no longer indexes is a check of nothing.
    fn insert(
        &mut self,
        def: impl ResourceDef + 'static,
        spawner: WatcherSpawner,
        convert_default: ConvertDefaultFn,
    ) {
        let kind = def.kind();
        if self.by_kind.insert(kind, RegistryEntry { spawner, convert_default }).is_some() {
            panic!("ResourceRegistry: duplicate watcher registration for {:?}", kind);
        }
    }

    // -- Lookup ---------------------------------------------------------------





    /// Build the `convert(K::default())` row for `kind`. Test-only — feeds the
    /// `default_cells_align_with_headers` alignment guard.
    #[cfg(test)]
    pub(crate) fn default_row(&self, kind: BuiltInKind) -> ResourceRow {
        let entry = self.by_kind.get(&kind).expect("registered kind");
        (entry.convert_default)()
    }

    /// Whether a watcher factory is registered for `kind`.
    ///
    /// Test-only: it exists so `every_built_in_kind_has_a_watcher` can ask
    /// without tripping the panic it is checking for.
    #[cfg(test)]
    pub(crate) fn has_watcher(&self, kind: BuiltInKind) -> bool {
        self.by_kind.contains_key(&kind)
    }

    /// How many kinds have a watcher. Test-only, same guard.
    #[cfg(test)]
    pub(crate) fn watcher_count(&self) -> usize {
        self.by_kind.len()
    }

    // -- Watcher spawning -----------------------------------------------------

    /// Spawn a typed watcher for the given kind. Infallible — every
    /// `BuiltInKind` variant has a registered spawner.
    pub(crate) fn spawn_watcher_for_kind(&self, kind: BuiltInKind, args: WatcherArgs) -> JoinHandle<()> {
        let entry = self.by_kind.get(&kind)
            .unwrap_or_else(|| panic!("ResourceRegistry: missing entry for {:?}", kind));
        (entry.spawner)(args)
    }
}

// ---------------------------------------------------------------------------
// spawn_typed_watcher — shared helper for namespaced/cluster spawners
// ---------------------------------------------------------------------------

/// Spawn a `run_typed_watcher` task with the given Api, converter, and
/// header configuration. Called from the captured spawner closures.
fn spawn_typed_watcher<K>(
    api: kube::Api<K>,
    args: WatcherArgs,
    convert: fn(K) -> ResourceRow,
    resource_id: ResourceId,
    headers: Vec<String>,
) -> JoinHandle<()>
where
    K: kube::Resource<DynamicType = ()>
        + Clone
        + std::fmt::Debug
        + Send
        + Sync
        + serde::de::DeserializeOwned
        + 'static,
{
    tokio::spawn(async move {
        let stream = crate::live_query::watch_stream(api, &args.filter, args.streaming_lists);
        crate::live_query::run_typed_watcher(
            stream,
            args.delta_tx,
            args.ask_rx,
            convert,
            resource_id,
            headers,
            &args.namespace,
        )
        .await;
    })
}

#[cfg(test)]
#[path = "../tests/resource_defs/registry.rs"]
mod tests;
