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

pub trait KubeResource: Clone + std::fmt::Debug + Send + Sync + 'static {
    fn headers() -> &'static [&'static str] where Self: Sized;
    fn row(&self) -> Vec<std::borrow::Cow<'_, str>>;
    /// Direct access to cell strings without allocation.
    /// Default implementation calls row() — override for zero-alloc access.
    fn cells(&self) -> &[String] { &[] }
    fn name(&self) -> &str;
    fn namespace(&self) -> &str { "" }
    fn kind() -> &'static str where Self: Sized;
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
