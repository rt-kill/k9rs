//! Cluster-scoped resource definitions: Namespace, Node, PersistentVolume,
//! StorageClass, PriorityClass, Role, ClusterRole, RoleBinding,
//! ClusterRoleBinding, ValidatingWebhookConfiguration,
//! MutatingWebhookConfiguration, CustomResourceDefinition.
//!
//! Note: Role and RoleBinding are Namespaced scope despite being grouped here
//! with cluster-level resources.

use crate::kube::protocol::{OperationKind, ResourceScope};
use crate::kube::resource_def::*;
use crate::kube::resources::row::ResourceRow;

use k8s_openapi::api::admissionregistration::v1::{
    MutatingWebhookConfiguration, ValidatingWebhookConfiguration,
};
use k8s_openapi::api::core::v1::{Namespace, Node, PersistentVolume};
use k8s_openapi::api::rbac::v1::{ClusterRole, ClusterRoleBinding, Role, RoleBinding};
use k8s_openapi::api::scheduling::v1::PriorityClass;
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;

// ---------------------------------------------------------------------------
// Namespace
// ---------------------------------------------------------------------------

pub struct NamespaceDef;

impl ResourceDef for NamespaceDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Namespace }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Namespace",
            plural: "namespaces", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["ns", "namespace", "namespaces"] }
    fn short_label(&self) -> &str { "NS" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "STATUS", "AGE"]
            .into_iter().map(String::from).collect()
    }
    fn is_core(&self) -> bool { true }
}

impl ConvertToRow<Namespace> for NamespaceDef {
    fn convert(obj: Namespace) -> ResourceRow {
        crate::kube::resources::namespaces::namespace_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

pub struct NodeDef;

impl ResourceDef for NodeDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Node }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Node",
            plural: "nodes", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["no", "node", "nodes"] }
    fn short_label(&self) -> &str { "Nodes" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "STATUS", "ROLES", "TAINTS", "VERSION",
         "OS-IMAGE", "KERNEL",
         "INTERNAL-IP", "EXTERNAL-IP", "PODS",
         "CPU%", "MEM%", "ARCH", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }
    fn is_core(&self) -> bool { true }
    fn metrics_kind(&self) -> Option<MetricsKind> { Some(MetricsKind::Node) }
    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, NodeShell]
    }
    fn column_defs(&self) -> Vec<ColumnDef> {
        use ColumnDef as C;
        use MetricsColumn::*;
        vec![
            C::new("NAME"), C::new("STATUS"), C::new("ROLES"),
            C::new("TAINTS"), C::new("VERSION"),
            C::extra("OS-IMAGE"), C::extra("KERNEL"),
            C::new("INTERNAL-IP"), C::extra("EXTERNAL-IP"),
            C::new("PODS"),
            C::new("CPU%").with_metrics(CpuPercent),
            C::new("MEM%").with_metrics(MemPercent),
            C::extra("ARCH"), C::extra("LABELS"), C::age("AGE"),
        ]
    }
}

impl ConvertToRow<Node> for NodeDef {
    fn convert(obj: Node) -> ResourceRow {
        crate::kube::resources::nodes::node_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// PersistentVolume (PV)
// ---------------------------------------------------------------------------

pub struct PvDef;

impl ResourceDef for PvDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::PersistentVolume }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "PersistentVolume",
            plural: "persistentvolumes", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["pv", "persistentvolume", "persistentvolumes", "pvs"] }
    fn short_label(&self) -> &str { "PV" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "CAPACITY", "ACCESS MODES", "RECLAIM POLICY", "STATUS",
         "CLAIM", "STORAGECLASS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<PersistentVolume> for PvDef {
    fn convert(obj: PersistentVolume) -> ResourceRow {
        crate::kube::resources::pvs::pv_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// StorageClass
// ---------------------------------------------------------------------------

pub struct StorageClassDef;

impl ResourceDef for StorageClassDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::StorageClass }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "storage.k8s.io", version: "v1", kind: "StorageClass",
            plural: "storageclasses", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["sc", "storageclass", "storageclasses"] }
    fn short_label(&self) -> &str { "SC" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "PROVISIONER", "RECLAIM POLICY", "VOLUME BINDING MODE",
         "EXPANSION", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<StorageClass> for StorageClassDef {
    fn convert(obj: StorageClass) -> ResourceRow {
        crate::kube::resources::storageclasses::storage_class_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// Role (Namespaced)
// ---------------------------------------------------------------------------

pub struct RoleDef;

impl ResourceDef for RoleDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Role }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "rbac.authorization.k8s.io", version: "v1", kind: "Role",
            plural: "roles", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["role", "roles"] }
    fn short_label(&self) -> &str { "Roles" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "RULES", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<Role> for RoleDef {
    fn convert(obj: Role) -> ResourceRow {
        crate::kube::resources::roles::role_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// ClusterRole
// ---------------------------------------------------------------------------

pub struct ClusterRoleDef;

impl ResourceDef for ClusterRoleDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ClusterRole }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "rbac.authorization.k8s.io", version: "v1", kind: "ClusterRole",
            plural: "clusterroles", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["cr", "clusterrole", "clusterroles"] }
    fn short_label(&self) -> &str { "CRoles" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "RULES", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<ClusterRole> for ClusterRoleDef {
    fn convert(obj: ClusterRole) -> ResourceRow {
        crate::kube::resources::clusterroles::cluster_role_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// RoleBinding (Namespaced)
// ---------------------------------------------------------------------------

pub struct RoleBindingDef;

impl ResourceDef for RoleBindingDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::RoleBinding }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "rbac.authorization.k8s.io", version: "v1", kind: "RoleBinding",
            plural: "rolebindings", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["rb", "rolebinding", "rolebindings"] }
    fn short_label(&self) -> &str { "RB" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "ROLE", "SUBJECTS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<RoleBinding> for RoleBindingDef {
    fn convert(obj: RoleBinding) -> ResourceRow {
        crate::kube::resources::rolebindings::role_binding_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// ClusterRoleBinding
// ---------------------------------------------------------------------------

pub struct ClusterRoleBindingDef;

impl ResourceDef for ClusterRoleBindingDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ClusterRoleBinding }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "rbac.authorization.k8s.io", version: "v1", kind: "ClusterRoleBinding",
            plural: "clusterrolebindings", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["crb", "clusterrolebinding", "clusterrolebindings"] }
    fn short_label(&self) -> &str { "CRB" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "ROLE", "SUBJECTS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<ClusterRoleBinding> for ClusterRoleBindingDef {
    fn convert(obj: ClusterRoleBinding) -> ResourceRow {
        crate::kube::resources::clusterrolebindings::cluster_role_binding_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// CustomResourceDefinition (CRD)
// ---------------------------------------------------------------------------

pub struct CrdDef;

impl ResourceDef for CrdDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::CustomResourceDefinition }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apiextensions.k8s.io", version: "v1", kind: "CustomResourceDefinition",
            plural: "customresourcedefinitions", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["crd", "crds", "customresourcedefinition", "customresourcedefinitions"] }
    fn short_label(&self) -> &str { "CRDs" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "GROUP", "VERSION", "KIND", "SCOPE", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<CustomResourceDefinition> for CrdDef {
    fn convert(obj: CustomResourceDefinition) -> ResourceRow {
        crate::kube::resources::crds::crd_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// PriorityClass
// ---------------------------------------------------------------------------

pub struct PriorityClassDef;

impl ResourceDef for PriorityClassDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::PriorityClass }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "scheduling.k8s.io", version: "v1", kind: "PriorityClass",
            plural: "priorityclasses", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["pc", "priorityclass", "priorityclasses"] }
    fn short_label(&self) -> &str { "PC" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "VALUE", "GLOBAL-DEFAULT", "PREEMPTION", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<PriorityClass> for PriorityClassDef {
    fn convert(obj: PriorityClass) -> ResourceRow {
        crate::kube::resources::priorityclasses::priority_class_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// ValidatingWebhookConfiguration
// ---------------------------------------------------------------------------

pub struct ValidatingWebhookDef;

impl ResourceDef for ValidatingWebhookDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ValidatingWebhookConfiguration }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "admissionregistration.k8s.io", version: "v1",
            kind: "ValidatingWebhookConfiguration",
            plural: "validatingwebhookconfigurations", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["vwc", "validatingwebhook", "validatingwebhookconfigurations"] }
    fn short_label(&self) -> &str { "VWC" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "WEBHOOKS", "FAILURE-POLICY", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<ValidatingWebhookConfiguration> for ValidatingWebhookDef {
    fn convert(obj: ValidatingWebhookConfiguration) -> ResourceRow {
        crate::kube::resources::webhooks::validating_webhook_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// MutatingWebhookConfiguration
// ---------------------------------------------------------------------------

pub struct MutatingWebhookDef;

impl ResourceDef for MutatingWebhookDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::MutatingWebhookConfiguration }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "admissionregistration.k8s.io", version: "v1",
            kind: "MutatingWebhookConfiguration",
            plural: "mutatingwebhookconfigurations", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["mwc", "mutatingwebhook", "mutatingwebhookconfigurations"] }
    fn short_label(&self) -> &str { "MWC" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "WEBHOOKS", "FAILURE-POLICY", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<MutatingWebhookConfiguration> for MutatingWebhookDef {
    fn convert(obj: MutatingWebhookConfiguration) -> ResourceRow {
        crate::kube::resources::webhooks::mutating_webhook_to_row(obj)
    }
}
