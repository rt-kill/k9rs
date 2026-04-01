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
    /// Cache label used by the daemon resource cache (e.g., "Pods", "Deploy"). None if not cached.
    pub cache_label: Option<&'static str>,
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
        cache_label: Some("Pods"),
    },
    ResourceTypeMeta {
        name: "deployment",
        group: "apps",
        version: "v1",
        kind: "Deployment",
        plural: "deployments",
        scope: Namespaced,
        aliases: &["dp", "deploy", "deployment", "deployments"],
        cache_label: Some("Deploy"),
    },
    ResourceTypeMeta {
        name: "service",
        group: "",
        version: "v1",
        kind: "Service",
        plural: "services",
        scope: Namespaced,
        aliases: &["svc", "service", "services"],
        cache_label: Some("Svc"),
    },
    ResourceTypeMeta {
        name: "configmap",
        group: "",
        version: "v1",
        kind: "ConfigMap",
        plural: "configmaps",
        scope: Namespaced,
        aliases: &["cm", "configmap", "configmaps"],
        cache_label: Some("CM"),
    },
    ResourceTypeMeta {
        name: "secret",
        group: "",
        version: "v1",
        kind: "Secret",
        plural: "secrets",
        scope: Namespaced,
        aliases: &["sec", "secret", "secrets"],
        cache_label: Some("Secrets"),
    },
    ResourceTypeMeta {
        name: "statefulset",
        group: "apps",
        version: "v1",
        kind: "StatefulSet",
        plural: "statefulsets",
        scope: Namespaced,
        aliases: &["sts", "statefulset", "statefulsets"],
        cache_label: Some("STS"),
    },
    ResourceTypeMeta {
        name: "daemonset",
        group: "apps",
        version: "v1",
        kind: "DaemonSet",
        plural: "daemonsets",
        scope: Namespaced,
        aliases: &["ds", "daemonset", "daemonsets"],
        cache_label: Some("DS"),
    },
    ResourceTypeMeta {
        name: "job",
        group: "batch",
        version: "v1",
        kind: "Job",
        plural: "jobs",
        scope: Namespaced,
        aliases: &["job", "jobs"],
        cache_label: Some("Jobs"),
    },
    ResourceTypeMeta {
        name: "cronjob",
        group: "batch",
        version: "v1",
        kind: "CronJob",
        plural: "cronjobs",
        scope: Namespaced,
        aliases: &["cj", "cronjob", "cronjobs"],
        cache_label: Some("CronJobs"),
    },
    ResourceTypeMeta {
        name: "replicaset",
        group: "apps",
        version: "v1",
        kind: "ReplicaSet",
        plural: "replicasets",
        scope: Namespaced,
        aliases: &["rs", "replicaset", "replicasets"],
        cache_label: Some("RS"),
    },
    ResourceTypeMeta {
        name: "ingress",
        group: "networking.k8s.io",
        version: "v1",
        kind: "Ingress",
        plural: "ingresses",
        scope: Namespaced,
        aliases: &["ing", "ingress", "ingresses"],
        cache_label: Some("Ing"),
    },
    ResourceTypeMeta {
        name: "networkpolicy",
        group: "networking.k8s.io",
        version: "v1",
        kind: "NetworkPolicy",
        plural: "networkpolicies",
        scope: Namespaced,
        aliases: &["np", "networkpolicy", "networkpolicies"],
        cache_label: Some("NetPol"),
    },
    ResourceTypeMeta {
        name: "serviceaccount",
        group: "",
        version: "v1",
        kind: "ServiceAccount",
        plural: "serviceaccounts",
        scope: Namespaced,
        aliases: &["sa", "serviceaccount", "serviceaccounts"],
        cache_label: Some("SA"),
    },
    ResourceTypeMeta {
        name: "namespace",
        group: "",
        version: "v1",
        kind: "Namespace",
        plural: "namespaces",
        scope: Cluster,
        aliases: &["ns", "namespace", "namespaces"],
        cache_label: Some("NS"),
    },
    ResourceTypeMeta {
        name: "node",
        group: "",
        version: "v1",
        kind: "Node",
        plural: "nodes",
        scope: Cluster,
        aliases: &["no", "node", "nodes"],
        cache_label: Some("Nodes"),
    },
    ResourceTypeMeta {
        name: "pv",
        group: "",
        version: "v1",
        kind: "PersistentVolume",
        plural: "persistentvolumes",
        scope: Cluster,
        aliases: &["pv", "persistentvolume", "pvs"],
        cache_label: Some("PV"),
    },
    ResourceTypeMeta {
        name: "pvc",
        group: "",
        version: "v1",
        kind: "PersistentVolumeClaim",
        plural: "persistentvolumeclaims",
        scope: Namespaced,
        aliases: &["pvc", "persistentvolumeclaim", "pvcs"],
        cache_label: Some("PVC"),
    },
    ResourceTypeMeta {
        name: "storageclass",
        group: "storage.k8s.io",
        version: "v1",
        kind: "StorageClass",
        plural: "storageclasses",
        scope: Cluster,
        aliases: &["sc", "storageclass", "storageclasses"],
        cache_label: Some("SC"),
    },
    ResourceTypeMeta {
        name: "role",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        kind: "Role",
        plural: "roles",
        scope: Namespaced,
        aliases: &["role", "roles"],
        cache_label: Some("Roles"),
    },
    ResourceTypeMeta {
        name: "clusterrole",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        kind: "ClusterRole",
        plural: "clusterroles",
        scope: Cluster,
        aliases: &["cr", "clusterrole", "clusterroles"],
        cache_label: Some("CRoles"),
    },
    ResourceTypeMeta {
        name: "rolebinding",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        kind: "RoleBinding",
        plural: "rolebindings",
        scope: Namespaced,
        aliases: &["rb", "rolebinding", "rolebindings"],
        cache_label: Some("RB"),
    },
    ResourceTypeMeta {
        name: "clusterrolebinding",
        group: "rbac.authorization.k8s.io",
        version: "v1",
        kind: "ClusterRoleBinding",
        plural: "clusterrolebindings",
        scope: Cluster,
        aliases: &["crb", "clusterrolebinding", "clusterrolebindings"],
        cache_label: Some("CRB"),
    },
    ResourceTypeMeta {
        name: "hpa",
        group: "autoscaling",
        version: "v2",
        kind: "HorizontalPodAutoscaler",
        plural: "horizontalpodautoscalers",
        scope: Namespaced,
        aliases: &["hpa", "horizontalpodautoscaler"],
        cache_label: Some("HPA"),
    },
    ResourceTypeMeta {
        name: "endpoints",
        group: "",
        version: "v1",
        kind: "Endpoints",
        plural: "endpoints",
        scope: Namespaced,
        aliases: &["ep", "endpoints"],
        cache_label: Some("EP"),
    },
    ResourceTypeMeta {
        name: "limitrange",
        group: "",
        version: "v1",
        kind: "LimitRange",
        plural: "limitranges",
        scope: Namespaced,
        aliases: &["limits", "limitrange", "limitranges"],
        cache_label: Some("Limits"),
    },
    ResourceTypeMeta {
        name: "resourcequota",
        group: "",
        version: "v1",
        kind: "ResourceQuota",
        plural: "resourcequotas",
        scope: Namespaced,
        aliases: &["quota", "resourcequota", "resourcequotas"],
        cache_label: Some("Quota"),
    },
    ResourceTypeMeta {
        name: "poddisruptionbudget",
        group: "policy",
        version: "v1",
        kind: "PodDisruptionBudget",
        plural: "poddisruptionbudgets",
        scope: Namespaced,
        aliases: &["pdb", "poddisruptionbudget", "poddisruptionbudgets"],
        cache_label: Some("PDB"),
    },
    ResourceTypeMeta {
        name: "event",
        group: "",
        version: "v1",
        kind: "Event",
        plural: "events",
        scope: Namespaced,
        aliases: &["ev", "event", "events"],
        cache_label: Some("Events"),
    },
    ResourceTypeMeta {
        name: "customresourcedefinition",
        group: "apiextensions.k8s.io",
        version: "v1",
        kind: "CustomResourceDefinition",
        plural: "customresourcedefinitions",
        scope: Cluster,
        aliases: &["crd", "crds", "customresourcedefinition", "customresourcedefinitions"],
        cache_label: Some("CRDs"),
    },
];

/// Look up a resource type by its canonical name (e.g., "pod", "deployment").
pub fn find_by_name(name: &str) -> Option<&'static ResourceTypeMeta> {
    RESOURCE_TYPES.iter().find(|r| r.name == name)
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
