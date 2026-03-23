pub mod handler;

use crate::app::FlashMessage;
use crate::kube::resources::{
    configmaps::KubeConfigMap,
    cronjobs::KubeCronJob,
    daemonsets::KubeDaemonSet,
    deployments::KubeDeployment,
    endpoints::KubeEndpoints,
    events::KubeEvent,
    hpa::KubeHpa,
    ingress::KubeIngress,
    jobs::KubeJob,
    limitranges::KubeLimitRange,
    namespaces::KubeNamespace,
    networkpolicies::KubeNetworkPolicy,
    nodes::KubeNode,
    pdb::KubePdb,
    pods::KubePod,
    pvcs::KubePvc,
    pvs::KubePv,
    rbac::{KubeClusterRole, KubeClusterRoleBinding, KubeRole, KubeRoleBinding},
    replicasets::KubeReplicaSet,
    resourcequotas::KubeResourceQuota,
    secrets::KubeSecret,
    serviceaccounts::KubeServiceAccount,
    services::KubeService,
    statefulsets::KubeStatefulSet,
    storageclasses::KubeStorageClass,
};

/// Top-level event type for the application event loop.
///
/// Input events (key, mouse, tick) are handled directly by the main event loop
/// via crossterm's `EventStream`. Only resource updates, errors, and flash
/// messages flow through this channel.
#[derive(Debug)]
pub enum AppEvent {
    /// An update to a Kubernetes resource list or content view.
    ResourceUpdate(ResourceUpdate),
    /// An application-level error.
    Error(String),
    /// A temporary flash message shown in the status bar.
    Flash(FlashMessage),
}

/// An update to a particular Kubernetes resource type.
#[derive(Debug)]
pub enum ResourceUpdate {
    Pods(Vec<KubePod>),
    Deployments(Vec<KubeDeployment>),
    Services(Vec<KubeService>),
    Nodes(Vec<KubeNode>),
    Namespaces(Vec<KubeNamespace>),
    ConfigMaps(Vec<KubeConfigMap>),
    Secrets(Vec<KubeSecret>),
    StatefulSets(Vec<KubeStatefulSet>),
    DaemonSets(Vec<KubeDaemonSet>),
    Jobs(Vec<KubeJob>),
    CronJobs(Vec<KubeCronJob>),
    ReplicaSets(Vec<KubeReplicaSet>),
    Ingresses(Vec<KubeIngress>),
    NetworkPolicies(Vec<KubeNetworkPolicy>),
    ServiceAccounts(Vec<KubeServiceAccount>),
    StorageClasses(Vec<KubeStorageClass>),
    Pvs(Vec<KubePv>),
    Pvcs(Vec<KubePvc>),
    Events(Vec<KubeEvent>),
    Roles(Vec<KubeRole>),
    ClusterRoles(Vec<KubeClusterRole>),
    RoleBindings(Vec<KubeRoleBinding>),
    ClusterRoleBindings(Vec<KubeClusterRoleBinding>),
    Hpa(Vec<KubeHpa>),
    Endpoints(Vec<KubeEndpoints>),
    LimitRanges(Vec<KubeLimitRange>),
    ResourceQuotas(Vec<KubeResourceQuota>),
    Pdb(Vec<KubePdb>),
    Yaml(String),
    Describe(String),
    LogLine(String),
}

