//! Trait-based resource type definitions.
//!
//! Each K8s resource is a zero-sized struct implementing [`ResourceDef`]
//! and relevant capability marker traits. The [`REGISTRY`] static
//! provides lookup by plural name or alias, and stores a type-erased
//! watcher spawner per resource for dispatch without string matching.

pub mod registry;

use std::sync::LazyLock;

use k8s_openapi::api::{
    admissionregistration::v1::{MutatingWebhookConfiguration, ValidatingWebhookConfiguration},
    apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet},
    autoscaling::v2::HorizontalPodAutoscaler,
    batch::v1::{CronJob, Job},
    coordination::v1::Lease,
    core::v1::{
        ConfigMap, Endpoints, Event, LimitRange, Namespace, Node, PersistentVolume,
        PersistentVolumeClaim, Pod, ResourceQuota, Secret, Service, ServiceAccount,
    },
    discovery::v1::EndpointSlice,
    networking::v1::{Ingress, NetworkPolicy},
    policy::v1::PodDisruptionBudget,
    rbac::v1::{ClusterRole, ClusterRoleBinding, Role, RoleBinding},
    scheduling::v1::PriorityClass,
    storage::v1::StorageClass,
};
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;

use registry::ResourceRegistry;

/// Global registry of all built-in resource types. Built once at first
/// access. Stores both `dyn ResourceDef` metadata and type-erased watcher
/// spawners -- the single source of truth for resource dispatch.
pub static REGISTRY: LazyLock<ResourceRegistry> = LazyLock::new(build_registry);

fn build_registry() -> ResourceRegistry {
    // Workload defs (in crate::kube::resources::*)
    use crate::kube::resources::pods::PodDef;
    use crate::kube::resources::deployments::DeploymentDef;
    use crate::kube::resources::statefulsets::StatefulSetDef;
    use crate::kube::resources::daemonsets::DaemonSetDef;
    use crate::kube::resources::replicasets::ReplicaSetDef;
    use crate::kube::resources::jobs::JobDef;
    use crate::kube::resources::cronjobs::CronJobDef;

    // Cluster-scoped defs (moved from resource_defs/cluster.rs)
    use crate::kube::resources::namespaces::NamespaceDef;
    use crate::kube::resources::nodes::NodeDef;
    use crate::kube::resources::pvs::PvDef;
    use crate::kube::resources::storageclasses::StorageClassDef;
    use crate::kube::resources::priorityclasses::PriorityClassDef;
    use crate::kube::resources::clusterroles::ClusterRoleDef;
    use crate::kube::resources::clusterrolebindings::ClusterRoleBindingDef;
    use crate::kube::resources::webhooks::{ValidatingWebhookDef, MutatingWebhookDef};
    use crate::kube::resources::crds::CrdDef;
    use crate::kube::resources::roles::RoleDef;
    use crate::kube::resources::rolebindings::RoleBindingDef;

    // Core/networking defs (moved from resource_defs/core.rs)
    use crate::kube::resources::services::ServiceDef;
    use crate::kube::resources::configmaps::ConfigMapDef;
    use crate::kube::resources::secrets::SecretDef;
    use crate::kube::resources::serviceaccounts::ServiceAccountDef;
    use crate::kube::resources::ingresses::IngressDef;
    use crate::kube::resources::networkpolicies::NetworkPolicyDef;
    use crate::kube::resources::hpa::HpaDef;
    use crate::kube::resources::endpoints::EndpointsDef;
    use crate::kube::resources::endpointslices::EndpointSliceDef;
    use crate::kube::resources::limitranges::LimitRangeDef;
    use crate::kube::resources::resourcequotas::ResourceQuotaDef;
    use crate::kube::resources::pdb::PodDisruptionBudgetDef;
    use crate::kube::resources::events::EventDef;
    use crate::kube::resources::pvcs::PvcDef;
    use crate::kube::resources::leases::LeaseDef;

    let mut r = ResourceRegistry::new();

    // Workloads (namespaced)
    r.register_namespaced::<_, Pod>(PodDef);
    r.register_namespaced::<_, Deployment>(DeploymentDef);
    r.register_namespaced::<_, StatefulSet>(StatefulSetDef);
    r.register_namespaced::<_, DaemonSet>(DaemonSetDef);
    r.register_namespaced::<_, ReplicaSet>(ReplicaSetDef);
    r.register_namespaced::<_, Job>(JobDef);
    r.register_namespaced::<_, CronJob>(CronJobDef);

    // Core namespaced
    r.register_namespaced::<_, Service>(ServiceDef);
    r.register_namespaced::<_, ConfigMap>(ConfigMapDef);
    r.register_namespaced::<_, Secret>(SecretDef);
    r.register_namespaced::<_, ServiceAccount>(ServiceAccountDef);
    r.register_namespaced::<_, Ingress>(IngressDef);
    r.register_namespaced::<_, NetworkPolicy>(NetworkPolicyDef);
    r.register_namespaced::<_, HorizontalPodAutoscaler>(HpaDef);
    r.register_namespaced::<_, Endpoints>(EndpointsDef);
    r.register_namespaced::<_, EndpointSlice>(EndpointSliceDef);
    r.register_namespaced::<_, LimitRange>(LimitRangeDef);
    r.register_namespaced::<_, ResourceQuota>(ResourceQuotaDef);
    r.register_namespaced::<_, PodDisruptionBudget>(PodDisruptionBudgetDef);
    r.register_namespaced::<_, Event>(EventDef);
    r.register_namespaced::<_, PersistentVolumeClaim>(PvcDef);
    r.register_namespaced::<_, Lease>(LeaseDef);

    // Cluster-scoped
    r.register_cluster::<_, Namespace>(NamespaceDef);
    r.register_cluster::<_, Node>(NodeDef);
    r.register_cluster::<_, PersistentVolume>(PvDef);
    r.register_cluster::<_, StorageClass>(StorageClassDef);
    r.register_cluster::<_, PriorityClass>(PriorityClassDef);
    r.register_cluster::<_, ClusterRole>(ClusterRoleDef);
    r.register_cluster::<_, ClusterRoleBinding>(ClusterRoleBindingDef);
    r.register_cluster::<_, ValidatingWebhookConfiguration>(ValidatingWebhookDef);
    r.register_cluster::<_, MutatingWebhookConfiguration>(MutatingWebhookDef);
    r.register_cluster::<_, CustomResourceDefinition>(CrdDef);

    // RBAC (namespaced despite being grouped with cluster resources)
    r.register_namespaced::<_, Role>(RoleDef);
    r.register_namespaced::<_, RoleBinding>(RoleBindingDef);

    r
}
