use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::rbac::v1::{
    ClusterRole, ClusterRoleBinding, Role, RoleBinding,
};

use super::KubeResource;

// ── Role ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct KubeRole {
    pub namespace: String,
    pub name: String,
    pub rules_count: usize,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeRole {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "RULES", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Owned(self.rules_count.to_string()),
            Cow::Owned(crate::util::format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn kind() -> &'static str {
        "role"
    }
}

impl From<Role> for KubeRole {
    fn from(role: Role) -> Self {
        let metadata = role.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);
        let rules_count = role.rules.map(|r| r.len()).unwrap_or(0);

        KubeRole {
            namespace,
            name,
            rules_count,
            age,
        }
    }
}

// ── ClusterRole ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct KubeClusterRole {
    pub name: String,
    pub rules_count: usize,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeClusterRole {
    fn headers() -> &'static [&'static str] {
        &["NAME", "RULES", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.name),
            Cow::Owned(self.rules_count.to_string()),
            Cow::Owned(crate::util::format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind() -> &'static str {
        "clusterrole"
    }
}

impl From<ClusterRole> for KubeClusterRole {
    fn from(cr: ClusterRole) -> Self {
        let metadata = cr.metadata;
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);
        let rules_count = cr.rules.map(|r| r.len()).unwrap_or(0);

        KubeClusterRole {
            name,
            rules_count,
            age,
        }
    }
}

// ── RoleBinding ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct KubeRoleBinding {
    pub namespace: String,
    pub name: String,
    pub role_ref: String,
    pub subjects: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeRoleBinding {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "ROLE", "SUBJECTS", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.role_ref),
            Cow::Borrowed(&self.subjects),
            Cow::Owned(crate::util::format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn kind() -> &'static str {
        "rolebinding"
    }
}

impl From<RoleBinding> for KubeRoleBinding {
    fn from(rb: RoleBinding) -> Self {
        let metadata = rb.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let role_ref = format!("{}/{}", rb.role_ref.kind, rb.role_ref.name);

        let subjects = rb
            .subjects
            .map(|subs| {
                subs.iter()
                    .map(|s| {
                        let kind = &s.kind;
                        let subj_name = &s.name;
                        format!("{}:{}", kind, subj_name)
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();

        KubeRoleBinding {
            namespace,
            name,
            role_ref,
            subjects,
            age,
        }
    }
}

// ── ClusterRoleBinding ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct KubeClusterRoleBinding {
    pub name: String,
    pub role_ref: String,
    pub subjects: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeClusterRoleBinding {
    fn headers() -> &'static [&'static str] {
        &["NAME", "ROLE", "SUBJECTS", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.role_ref),
            Cow::Borrowed(&self.subjects),
            Cow::Owned(crate::util::format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind() -> &'static str {
        "clusterrolebinding"
    }
}

impl From<ClusterRoleBinding> for KubeClusterRoleBinding {
    fn from(crb: ClusterRoleBinding) -> Self {
        let metadata = crb.metadata;
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let role_ref = format!("{}/{}", crb.role_ref.kind, crb.role_ref.name);

        let subjects = crb
            .subjects
            .map(|subs| {
                subs.iter()
                    .map(|s| {
                        let kind = &s.kind;
                        let subj_name = &s.name;
                        format!("{}:{}", kind, subj_name)
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();

        KubeClusterRoleBinding {
            name,
            role_ref,
            subjects,
            age,
        }
    }
}
