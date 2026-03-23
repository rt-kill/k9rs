use std::borrow::Cow;

pub mod configmaps;
pub mod crds;
pub mod cronjobs;
pub mod daemonsets;
pub mod deployments;
pub mod endpoints;
pub mod events;
pub mod hpa;
pub mod ingress;
pub mod jobs;
pub mod limitranges;
pub mod namespaces;
pub mod networkpolicies;
pub mod nodes;
pub mod pdb;
pub mod pods;
pub mod pvcs;
pub mod pvs;
pub mod rbac;
pub mod replicasets;
pub mod resourcequotas;
pub mod secrets;
pub mod serviceaccounts;
pub mod services;
pub mod statefulsets;
pub mod storageclasses;

pub trait KubeResource: Clone + std::fmt::Debug + Send + Sync + 'static {
    fn headers() -> &'static [&'static str] where Self: Sized;
    fn row(&self) -> Vec<Cow<'_, str>>;
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
