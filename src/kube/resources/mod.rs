pub mod row;
pub mod pods;
pub mod deployments;
pub mod services;
pub mod statefulsets;
pub mod daemonsets;
pub mod jobs;
pub mod cronjobs;
pub mod replicasets;
pub mod nodes;
pub mod configmaps;
pub mod secrets;
pub mod ingresses;
pub mod networkpolicies;
pub mod serviceaccounts;
pub mod namespaces;
pub mod pvs;
pub mod pvcs;
pub mod storageclasses;
pub mod events;
pub mod roles;
pub mod clusterroles;
pub mod rolebindings;
pub mod clusterrolebindings;
pub mod hpa;
pub mod endpoints;
pub mod limitranges;
pub mod resourcequotas;
pub mod pdb;
pub mod crds;
pub mod converters;

/// The contract for anything that can live inside a `StatefulTable`. The
/// only implementor is `ResourceRow`; this trait is the bound used by
/// generic table machinery (`StatefulTable<T>`, `live_query`'s sort
/// helpers) so they don't have to depend on the concrete row type.
pub trait KubeResource: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// The display columns for this row, in header order.
    fn cells(&self) -> &[String];
    /// Resource name (cached for O(1) access in sorts/filters).
    fn name(&self) -> &str;
    /// Resource namespace, or `""` for cluster-scoped rows.
    fn namespace(&self) -> &str;
}

pub fn access_mode_short(mode: &str) -> &str {
    match mode {
        "ReadWriteOnce" => "RWO",
        "ReadOnlyMany" => "ROX",
        "ReadWriteMany" => "RWX",
        "ReadWriteOncePod" => "RWOP",
        other => other,
    }
}
