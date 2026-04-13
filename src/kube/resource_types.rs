use crate::kube::protocol::ResourceScope;
use crate::kube::protocol::ResourceScope::{Cluster, Namespaced};

/// Metadata for a built-in Kubernetes resource type.
///
/// This is the single source of truth for mapping user-facing resource strings
/// to API coordinates, scopes, aliases, and daemon cache labels.
pub struct ResourceTypeMeta {
    /// The canonical singular name used as the key (e.g., "pod", "deployment")
    pub name: &'static str,
    /// API group (e.g., "", "apps", "batch")
    pub group: &'static str,
    /// API version (e.g., "v1")
    pub version: &'static str,
    /// Kind (e.g., "Pod", "Deployment")
    pub kind: &'static str,
    /// Plural (e.g., "pods", "deployments")
    pub plural: &'static str,
    /// Scope
    pub scope: ResourceScope,
    /// Short aliases used in command mode (e.g., &["po", "pod", "pods"])
    pub aliases: &'static [&'static str],
    /// Short UI label for tab bar/breadcrumbs (e.g., "Pods", "Deploy", "STS").
    pub short_label: &'static str,
    /// Whether this resource supports viewing logs (resolves to pods).
    pub supports_logs: bool,
    /// Whether this resource supports shell exec (must be a pod).
    pub supports_shell: bool,
    /// Whether this resource has a /scale subresource.
    pub supports_scale: bool,
    /// Whether this resource supports rollout restart.
    pub supports_restart: bool,
    /// Whether this resource supports port-forward (pods, workloads, services).
    pub supports_port_forward: bool,
}

impl ResourceTypeMeta {
    /// Build a ResourceId from this metadata.
    pub fn to_resource_id(&self) -> crate::kube::protocol::ResourceId {
        crate::kube::protocol::ResourceId::new(
            self.group, self.version, self.kind, self.plural, self.scope,
        )
    }
}

pub const RESOURCE_TYPES: &[ResourceTypeMeta] = &[
    ResourceTypeMeta {
        name: "pod",
        group: "",
        version: "v1",
        kind: "Pod",
        plural: "pods",
        scope: Namespaced,
        aliases: &["po", "pod", "pods"],
        short_label: "Pods",
        supports_logs: true,
        supports_shell: true,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: true,
    },
    ResourceTypeMeta {
        name: "deployment",
        group: "apps",
        version: "v1",
        kind: "Deployment",
        plural: "deployments",
        scope: Namespaced,
        aliases: &["dp", "deploy", "deployment", "deployments"],
        short_label: "Deploy",
        supports_logs: true,
        supports_shell: false,
        supports_scale: true,
        supports_restart: true,
        supports_port_forward: true,
    },
    ResourceTypeMeta {
        name: "service",
        group: "",
        version: "v1",
        kind: "Service",
        plural: "services",
        scope: Namespaced,
        aliases: &["svc", "service", "services"],
        short_label: "Svc",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: true,
    },
    ResourceTypeMeta {
        name: "configmap",
        group: "",
        version: "v1",
        kind: "ConfigMap",
        plural: "configmaps",
        scope: Namespaced,
        aliases: &["cm", "configmap", "configmaps"],
        short_label: "CM",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "secret",
        group: "",
        version: "v1",
        kind: "Secret",
        plural: "secrets",
        scope: Namespaced,
        aliases: &["sec", "secret", "secrets"],
        short_label: "Secrets",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "statefulset",
        group: "apps",
        version: "v1",
        kind: "StatefulSet",
        plural: "statefulsets",
        scope: Namespaced,
        aliases: &["sts", "statefulset", "statefulsets"],
        short_label: "STS",
        supports_logs: true,
        supports_shell: false,
        supports_scale: true,
        supports_restart: true,
        supports_port_forward: true,
    },
    ResourceTypeMeta {
        name: "daemonset",
        group: "apps",
        version: "v1",
        kind: "DaemonSet",
        plural: "daemonsets",
        scope: Namespaced,
        aliases: &["ds", "daemonset", "daemonsets"],
        short_label: "DS",
        supports_logs: true,
        supports_shell: false,
        supports_scale: false,
        supports_restart: true,
        supports_port_forward: true,
    },
    ResourceTypeMeta {
        name: "job",
        group: "batch",
        version: "v1",
        kind: "Job",
        plural: "jobs",
        scope: Namespaced,
        aliases: &["job", "jobs"],
        short_label: "Jobs",
        supports_logs: true,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: true,
    },
    ResourceTypeMeta {
        name: "cronjob",
        group: "batch",
        version: "v1",
        kind: "CronJob",
        plural: "cronjobs",
        scope: Namespaced,
        aliases: &["cj", "cronjob", "cronjobs"],
        short_label: "CronJobs",
        supports_logs: true,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "replicaset",
        group: "apps",
        version: "v1",
        kind: "ReplicaSet",
        plural: "replicasets",
        scope: Namespaced,
        aliases: &["rs", "replicaset", "replicasets"],
        short_label: "RS",
        supports_logs: true,
        supports_shell: false,
        supports_scale: true,
        supports_restart: false,
        supports_port_forward: true,
    },
    ResourceTypeMeta {
        name: "ingress",
        group: "networking.k8s.io",
        version: "v1",
        kind: "Ingress",
        plural: "ingresses",
        scope: Namespaced,
        aliases: &["ing", "ingress", "ingresses"],
        short_label: "Ing",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "networkpolicy",
        group: "networking.k8s.io",
        version: "v1",
        kind: "NetworkPolicy",
        plural: "networkpolicies",
        scope: Namespaced,
        aliases: &["np", "networkpolicy", "networkpolicies"],
        short_label: "NetPol",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "serviceaccount",
        group: "",
        version: "v1",
        kind: "ServiceAccount",
        plural: "serviceaccounts",
        scope: Namespaced,
        aliases: &["sa", "serviceaccount", "serviceaccounts"],
        short_label: "SA",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "namespace",
        group: "",
        version: "v1",
        kind: "Namespace",
        plural: "namespaces",
        scope: Cluster,
        aliases: &["ns", "namespace", "namespaces"],
        short_label: "NS",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "node",
        group: "",
        version: "v1",
        kind: "Node",
        plural: "nodes",
        scope: Cluster,
        aliases: &["no", "node", "nodes"],
        short_label: "Nodes",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "pv",
        group: "",
        version: "v1",
        kind: "PersistentVolume",
        plural: "persistentvolumes",
        scope: Cluster,
        aliases: &["pv", "persistentvolume", "pvs"],
        short_label: "PV",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "pvc",
        group: "",
        version: "v1",
        kind: "PersistentVolumeClaim",
        plural: "persistentvolumeclaims",
        scope: Namespaced,
        aliases: &["pvc", "persistentvolumeclaim", "pvcs"],
        short_label: "PVC",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "storageclass",
        group: "storage.k8s.io",
        version: "v1",
        kind: "StorageClass",
        plural: "storageclasses",
        scope: Cluster,
        aliases: &["sc", "storageclass", "storageclasses"],
        short_label: "SC",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "role",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        kind: "Role",
        plural: "roles",
        scope: Namespaced,
        aliases: &["role", "roles"],
        short_label: "Roles",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "clusterrole",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        kind: "ClusterRole",
        plural: "clusterroles",
        scope: Cluster,
        aliases: &["cr", "clusterrole", "clusterroles"],
        short_label: "CRoles",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "rolebinding",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        kind: "RoleBinding",
        plural: "rolebindings",
        scope: Namespaced,
        aliases: &["rb", "rolebinding", "rolebindings"],
        short_label: "RB",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "clusterrolebinding",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        kind: "ClusterRoleBinding",
        plural: "clusterrolebindings",
        scope: Cluster,
        aliases: &["crb", "clusterrolebinding", "clusterrolebindings"],
        short_label: "CRB",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "hpa",
        group: "autoscaling",
        version: "v2",
        kind: "HorizontalPodAutoscaler",
        plural: "horizontalpodautoscalers",
        scope: Namespaced,
        aliases: &["hpa", "horizontalpodautoscaler"],
        short_label: "HPA",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "endpoints",
        group: "",
        version: "v1",
        kind: "Endpoints",
        plural: "endpoints",
        scope: Namespaced,
        aliases: &["ep", "endpoints"],
        short_label: "EP",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "limitrange",
        group: "",
        version: "v1",
        kind: "LimitRange",
        plural: "limitranges",
        scope: Namespaced,
        aliases: &["limits", "limitrange", "limitranges"],
        short_label: "Limits",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "resourcequota",
        group: "",
        version: "v1",
        kind: "ResourceQuota",
        plural: "resourcequotas",
        scope: Namespaced,
        aliases: &["quota", "resourcequota", "resourcequotas"],
        short_label: "Quota",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "poddisruptionbudget",
        group: "policy",
        version: "v1",
        kind: "PodDisruptionBudget",
        plural: "poddisruptionbudgets",
        scope: Namespaced,
        aliases: &["pdb", "poddisruptionbudget", "poddisruptionbudgets"],
        short_label: "PDB",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "event",
        group: "",
        version: "v1",
        kind: "Event",
        plural: "events",
        scope: Namespaced,
        aliases: &["ev", "event", "events"],
        short_label: "Events",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
    ResourceTypeMeta {
        name: "customresourcedefinition",
        group: "apiextensions.k8s.io",
        version: "v1",
        kind: "CustomResourceDefinition",
        plural: "customresourcedefinitions",
        scope: Cluster,
        aliases: &["crd", "crds", "customresourcedefinition", "customresourcedefinitions"],
        short_label: "CRDs",
        supports_logs: false,
        supports_shell: false,
        supports_scale: false,
        supports_restart: false,
        supports_port_forward: false,
    },
];

/// Look up a resource type by its canonical name (e.g., "pod", "deployment").
pub fn find_by_name(name: &str) -> Option<&'static ResourceTypeMeta> {
    RESOURCE_TYPES.iter().find(|r| r.name == name)
}

/// Look up a resource type by its plural API name (e.g., "pods", "deployments").
pub fn find_by_plural(plural: &str) -> Option<&'static ResourceTypeMeta> {
    RESOURCE_TYPES.iter().find(|r| r.plural == plural)
}

/// Look up a resource type by any alias (case-insensitive).
pub fn find_by_alias(alias: &str) -> Option<&'static ResourceTypeMeta> {
    let lower = alias.to_lowercase();
    RESOURCE_TYPES
        .iter()
        .find(|r| r.aliases.iter().any(|a| a.eq_ignore_ascii_case(&lower)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_by_name_works() {
        let meta = find_by_name("pod").unwrap();
        assert_eq!(meta.kind, "Pod");
        assert_eq!(meta.plural, "pods");
        assert_eq!(meta.scope, Namespaced);
    }

    #[test]
    fn find_by_name_returns_none_for_unknown() {
        assert!(find_by_name("foobar").is_none());
    }

    #[test]
    fn find_by_alias_works() {
        let meta = find_by_alias("dp").unwrap();
        assert_eq!(meta.name, "deployment");
        assert_eq!(meta.kind, "Deployment");

        let meta = find_by_alias("Po").unwrap();
        assert_eq!(meta.name, "pod");
    }

    #[test]
    fn find_by_alias_returns_none_for_unknown() {
        assert!(find_by_alias("zzz").is_none());
    }

    #[test]
    fn all_entries_have_at_least_one_alias() {
        for r in RESOURCE_TYPES {
            assert!(
                !r.aliases.is_empty(),
                "resource '{}' has no aliases",
                r.name
            );
        }
    }

    #[test]
    fn canonical_name_is_in_aliases() {
        for r in RESOURCE_TYPES {
            assert!(
                r.aliases.contains(&r.name),
                "resource '{}' canonical name not in its aliases",
                r.name
            );
        }
    }

    #[test]
    fn no_duplicate_aliases() {
        let mut seen = std::collections::HashSet::new();
        for r in RESOURCE_TYPES {
            for alias in r.aliases {
                assert!(
                    seen.insert(alias.to_lowercase()),
                    "duplicate alias '{}' (resource '{}')",
                    alias,
                    r.name
                );
            }
        }
    }
}
