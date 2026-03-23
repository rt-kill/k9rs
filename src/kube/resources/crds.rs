use std::borrow::Cow;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use super::KubeResource;

/// Represents a Custom Resource Definition (CRD) in the cluster.
#[derive(Debug, Clone)]
pub struct KubeCrd {
    pub name: String,        // e.g. "certificates.cert-manager.io"
    pub group: String,       // e.g. "cert-manager.io"
    pub version: String,     // e.g. "v1"
    pub kind: String,        // e.g. "Certificate"
    pub scope: String,       // "Namespaced" or "Cluster"
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeCrd {
    fn headers() -> &'static [&'static str] {
        &["NAME", "GROUP", "VERSION", "KIND", "SCOPE", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.group),
            Cow::Borrowed(&self.version),
            Cow::Borrowed(&self.kind),
            Cow::Borrowed(&self.scope),
            Cow::Owned(crate::util::format_age(self.age)),
        ]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind() -> &'static str {
        "customresourcedefinition"
    }
}

/// Represents a dynamic/generic Kubernetes resource instance (a CRD instance).
#[derive(Debug, Clone)]
pub struct DynamicKubeResource {
    pub namespace: String,
    pub name: String,
    pub data: BTreeMap<String, String>,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for DynamicKubeResource {
    fn headers() -> &'static [&'static str] {
        &["NAMESPACE", "NAME", "AGE"]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
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
        "dynamic"
    }
}
