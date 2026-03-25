pub mod handler;

use std::collections::HashMap;

use crate::app::FlashMessage;
use crate::kube::cache::CachedCrd;
use crate::kube::resources::{
    configmaps::KubeConfigMap,
    crds::{KubeCrd, DynamicKubeResource},
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
pub enum AppEvent {
    /// An update to a Kubernetes resource list or content view.
    ResourceUpdate(ResourceUpdate),
    /// An application-level error.
    Error(String),
    /// A temporary flash message shown in the status bar.
    Flash(FlashMessage),
    /// Result of a background context switch. Contains (context_name, new_client)
    /// on success, or an error string on failure.
    ContextSwitchResult {
        context: String,
        result: Result<::kube::Client, String>,
    },
    /// Fresh cache data polled from the daemon (populated by another instance).
    DaemonCacheUpdate {
        namespaces: Vec<String>,
        crds: Vec<CachedCrd>,
    },
    /// Pod metrics from the metrics-server: HashMap<(namespace, pod_name), (cpu, mem)>.
    PodMetrics(HashMap<(String, String), (String, String)>),
    /// Node metrics from the metrics-server: HashMap<node_name, (cpu, mem)>.
    NodeMetrics(HashMap<String, (String, String)>),
    /// Cached resource data loaded from the daemon for instant tab display.
    CachedResources {
        resource_type: String,
        data: String,
    },
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
    Crds(Vec<KubeCrd>),
    DynamicResources(Vec<DynamicKubeResource>),
    Yaml(String),
    Describe(String),
    LogLine(String),
}

