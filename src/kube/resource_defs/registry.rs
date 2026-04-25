//! Central registry of all known built-in resource types.
//!
//! Built once at startup via `LazyLock`. The registry maps plural names
//! and aliases to [`ResourceDef`] trait objects, and stores a type-erased
//! [`WatcherSpawner`] per resource for dispatching watchers without
//! string-matching if/else chains.

use std::collections::HashMap;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::event::ResourceUpdate;
use crate::kube::live_query::WatcherSnapshot;
use crate::kube::protocol::{Namespace, ResourceId, SubscriptionFilter};
use crate::kube::resource_def::{BuiltInKind, ConvertToRow, ResourceDef};
use crate::kube::resources::row::ResourceRow;

// ---------------------------------------------------------------------------
// WatcherSpawner — type-erased watcher factory
// ---------------------------------------------------------------------------

/// Arguments passed to a [`WatcherSpawner`] at subscribe time.
pub(crate) struct WatcherArgs {
    pub client: kube::Client,
    pub namespace: Namespace,
    pub snapshot_tx: watch::Sender<WatcherSnapshot>,
    pub filter: Option<SubscriptionFilter>,
}

/// Type-erased watcher spawner. Created at registration time when concrete
/// K8s API types are known, called at subscribe time to spawn a typed
/// watcher task. Returns a `JoinHandle` so the `LiveQuery` can track and
/// abort the task.
type WatcherSpawner = Box<dyn Fn(WatcherArgs) -> JoinHandle<()> + Send + Sync>;

// ---------------------------------------------------------------------------
// RegistryEntry
// ---------------------------------------------------------------------------

struct RegistryEntry {
    def: Box<dyn ResourceDef>,
    spawner: WatcherSpawner,
}

// ---------------------------------------------------------------------------
// ResourceRegistry
// ---------------------------------------------------------------------------

/// Central registry of all known built-in resource types.
pub struct ResourceRegistry {
    /// Typed primary index — owns the entries. Every registered def lives
    /// here keyed by its [`BuiltInKind`]. Closed-enum dispatch (`by_kind`,
    /// `spawn_watcher_for_kind`) reads straight from this map with no
    /// string indirection between a typed key and the entry it points at.
    by_kind: HashMap<BuiltInKind, RegistryEntry>,
    /// Plural-name lookup indirection for the few string-keyed callers
    /// (`by_plural`, the `registry_consistency` test). Points at the
    /// typed kind; the entry itself lives in `by_kind`.
    by_plural: HashMap<&'static str, BuiltInKind>,
    /// Insertion-order list of kinds, for deterministic iteration
    /// (tab cycling, alias listing).
    ordered: Vec<BuiltInKind>,
}

impl Default for ResourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceRegistry {
    pub fn new() -> Self {
        Self {
            by_kind: HashMap::new(),
            by_plural: HashMap::new(),
            ordered: Vec::new(),
        }
    }

    // -- Registration (called once at startup) --------------------------------

    /// Register a **namespaced** resource type with its typed watcher factory.
    ///
    /// The `K` type parameter is the k8s-openapi struct (e.g. `Pod`,
    /// `Deployment`). It must be `NamespaceResourceScope` so the spawner
    /// can create `Api::namespaced` when a specific namespace is selected.
    pub fn register_namespaced<D, K>(&mut self, def: D)
    where
        D: ResourceDef + ConvertToRow<K> + 'static,
        K: kube::Resource<DynamicType = (), Scope = k8s_openapi::NamespaceResourceScope>
            + Clone
            + std::fmt::Debug
            + Send
            + Sync
            + serde::de::DeserializeOwned
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

        self.insert(def, spawner);
    }

    /// Register a **cluster-scoped** resource type with its typed watcher factory.
    ///
    /// Cluster resources always use `Api::all` regardless of namespace selection.
    pub fn register_cluster<D, K>(&mut self, def: D)
    where
        D: ResourceDef + ConvertToRow<K> + 'static,
        K: kube::Resource<DynamicType = ()>
            + Clone
            + std::fmt::Debug
            + Send
            + Sync
            + serde::de::DeserializeOwned
            + 'static,
    {
        let convert_fn: fn(K) -> ResourceRow = D::convert;
        let resource_id = def.resource_id();
        let default_headers = def.default_headers();

        let spawner: WatcherSpawner = Box::new(move |args: WatcherArgs| {
            let api: kube::Api<K> = kube::Api::all(args.client.clone());
            spawn_typed_watcher(api, args, convert_fn, resource_id.clone(), default_headers.clone())
        });

        self.insert(def, spawner);
    }

    fn insert(&mut self, def: impl ResourceDef + 'static, spawner: WatcherSpawner) {
        let kind = def.kind();
        let plural_static: &'static str = def.gvr().plural;
        self.ordered.push(kind);
        if self.by_kind.insert(kind, RegistryEntry { def: Box::new(def), spawner }).is_some() {
            panic!("ResourceRegistry: duplicate registration for {:?}", kind);
        }
        if self.by_plural.insert(plural_static, kind).is_some() {
            panic!("ResourceRegistry: duplicate plural registration: {:?}", plural_static);
        }
    }

    // -- Lookup ---------------------------------------------------------------

    /// Look up by plural name. Test-only — production dispatch goes through
    /// `by_kind(BuiltInKind)` / `by_alias(&str)`. Kept alive for the
    /// `registry_consistency` drift guard, which verifies the `by_plural`
    /// indirection round-trips to the correct `BuiltInKind` entry.
    #[cfg(test)]
    fn by_plural(&self, plural: &str) -> Option<&dyn ResourceDef> {
        let kind = self.by_plural.get(plural)?;
        self.by_kind.get(kind).map(|e| &*e.def)
    }

    /// Typed lookup by [`BuiltInKind`] — infallible in correct code, panics
    /// if called with an unregistered variant (adding an enum variant
    /// without a corresponding `register_*` call in `build_registry` is
    /// the only way this can happen, and any test that exercises the new
    /// variant catches it immediately). Returns `&'static dyn ResourceDef`
    /// because the registry lives in a `LazyLock` for the process lifetime.
    pub fn by_kind(&'static self, kind: BuiltInKind) -> &'static dyn ResourceDef {
        let entry = self.by_kind.get(&kind)
            .unwrap_or_else(|| panic!("ResourceRegistry: missing entry for {:?}", kind));
        &*entry.def
    }

    /// Look up by any alias (case-insensitive).
    pub fn by_alias(&self, alias: &str) -> Option<&dyn ResourceDef> {
        let lower = alias.to_lowercase();
        self.by_kind.values()
            .find(|e| e.def.aliases().iter().any(|a| a.eq_ignore_ascii_case(&lower)))
            .map(|e| &*e.def)
    }

    /// Iterate all registered resource definitions in registration order.
    pub fn all(&self) -> impl Iterator<Item = &dyn ResourceDef> {
        self.ordered.iter().filter_map(|k| self.by_kind.get(k).map(|e| &*e.def))
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
        crate::kube::live_query::run_typed_watcher(
            api,
            args.snapshot_tx,
            convert,
            move |mut rows| {
                // Apply overlay coloring rules for built-in resources.
                if let Some(overlay) = crate::kube::overlay::overlay_for(resource_id.plural()) {
                    if !overlay.coloring.is_empty() {
                        let hdrs = &headers;
                        for row in &mut rows {
                            crate::kube::overlay::evaluate_coloring(row, hdrs, &overlay.coloring);
                        }
                    }
                }
                ResourceUpdate::Rows {
                    resource: resource_id.clone(),
                    headers: headers.clone(),
                    rows,
                }
            },
            &args.namespace,
            args.filter,
        )
        .await;
    })
}

#[cfg(test)]
mod tests {
    use crate::kube::resource_defs::REGISTRY;

    /// Every registered def's `gvr().plural` matches the key it was
    /// registered under in `by_plural`, and `kind()` round-trips through
    /// `by_kind`. Walks the real registry (not a hand-maintained slice),
    /// so there's no drift surface: adding a new resource means writing
    /// a def + calling `register_*`, and both sides of the round-trip
    /// check kick in as soon as the def exists.
    #[test]
    fn registry_consistency() {
        for def in REGISTRY.all() {
            let kind = def.kind();
            let by_kind_def = REGISTRY.by_kind(kind);
            assert_eq!(by_kind_def.gvr().plural, def.gvr().plural);
            let by_plural_def = REGISTRY.by_plural(def.gvr().plural).expect("registered");
            assert_eq!(by_plural_def.kind(), kind);
        }
    }
}
