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
    def: Box<dyn ResourceDef>,
    spawner: WatcherSpawner,
    /// Read only by the alignment test; always populated so the guard can run.
    #[cfg_attr(not(test), allow(dead_code))]
    convert_default: ConvertDefaultFn,
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
    pub fn register_cluster<D, K>(&mut self, def: D)
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

    fn insert(
        &mut self,
        def: impl ResourceDef + 'static,
        spawner: WatcherSpawner,
        convert_default: ConvertDefaultFn,
    ) {
        let kind = def.kind();
        let plural_static: &'static str = def.gvr().plural;
        self.ordered.push(kind);
        if self.by_kind.insert(kind, RegistryEntry { def: Box::new(def), spawner, convert_default }).is_some() {
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

    /// Build the `convert(K::default())` row for `kind`. Test-only — feeds the
    /// `default_cells_align_with_headers` alignment guard.
    #[cfg(test)]
    pub(crate) fn default_row(&self, kind: BuiltInKind) -> ResourceRow {
        let entry = self.by_kind.get(&kind).expect("registered kind");
        (entry.convert_default)()
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
            move |rows| {
                // Overlay coloring evaluation moved to client side
                // (prepare_view) where metrics are already populated.
                ResourceUpdate::Rows {
                    resource: resource_id.clone(),
                    headers: headers.clone(),
                    rows,
                }
            },
            &args.namespace,
            args.filter,
            args.streaming_lists,
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

    /// Every def's `column_defs()` (when it tags any) must align positionally
    /// with its `default_headers()` — same length, same header text in order.
    /// These two lists, plus each converter's `cells` vec, are hand-authored
    /// and coupled by index; a drift silently renders data under the wrong
    /// header (and, for metrics resources, misdirects the by-index metrics
    /// overlay). This walks the real registry so the guard covers every
    /// resource, present and future, not just the ones with a bespoke test.
    /// (The third list — per-row `cells` — needs a typed default object per
    /// resource, so it's checked per-converter; see e.g. `nodes`/`pods`.)
    #[test]
    fn column_defs_align_with_headers() {
        for def in REGISTRY.all() {
            let cols = def.column_defs();
            if cols.is_empty() {
                continue; // no explicit tagging — headers are used directly
            }
            let headers = def.default_headers();
            assert_eq!(
                cols.len(), headers.len(),
                "{:?}: column_defs ({}) vs default_headers ({}) length",
                def.kind(), cols.len(), headers.len(),
            );
            for (col, header) in cols.iter().zip(&headers) {
                assert_eq!(
                    col.header, header.as_str(),
                    "{:?}: column_defs/default_headers header text mismatch", def.kind(),
                );
            }
        }
    }

    /// The third leg of the positional column triple: every converter's per-row
    /// `cells` must be the same length as its `default_headers()`. Walks the
    /// real registry via each converter's `convert(K::default())` row, so the
    /// guard covers every resource present and future — previously only nodes
    /// and pods had a hand-written cells check, leaving the other converters'
    /// `cells`↔`headers` coupling untested (a drift silently shifts every
    /// column past the drift point under the wrong header).
    #[test]
    fn default_cells_align_with_headers() {
        for def in REGISTRY.all() {
            let row = REGISTRY.default_row(def.kind());
            let headers = def.default_headers();
            assert_eq!(
                row.cells.len(), headers.len(),
                "{:?}: convert(default) cells ({}) vs default_headers ({})",
                def.kind(), row.cells.len(), headers.len(),
            );
        }
    }
}
