use std::borrow::Cow;

use chrono::{DateTime, Utc};
use k8s_openapi::api::networking::v1::Ingress;

use super::KubeResource;

#[derive(Debug, Clone)]
pub struct KubeIngress {
    pub namespace: String,
    pub name: String,
    pub class: String,
    pub hosts: String,
    pub address: String,
    pub ports: String,
    pub age: Option<DateTime<Utc>>,
}

impl KubeResource for KubeIngress {
    fn headers() -> &'static [&'static str] {
        &[
            "NAMESPACE", "NAME", "CLASS", "HOSTS", "ADDRESS", "PORTS", "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.class),
            Cow::Borrowed(&self.hosts),
            Cow::Borrowed(&self.address),
            Cow::Borrowed(&self.ports),
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
        "ingress"
    }
}

impl From<Ingress> for KubeIngress {
    fn from(ing: Ingress) -> Self {
        let metadata = ing.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let spec = ing.spec.unwrap_or_default();
        let class = spec
            .ingress_class_name
            .unwrap_or_else(|| "<none>".to_string());

        let hosts = spec
            .rules
            .as_ref()
            .map(|rules| {
                rules
                    .iter()
                    .filter_map(|r| r.host.clone())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        let hosts = if hosts.is_empty() {
            "*".to_string()
        } else {
            hosts
        };

        let address = ing
            .status
            .and_then(|s| s.load_balancer)
            .and_then(|lb| lb.ingress)
            .map(|ingresses| {
                ingresses
                    .iter()
                    .filter_map(|i| i.ip.clone().or_else(|| i.hostname.clone()))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();

        let has_tls = spec.tls.map(|t| !t.is_empty()).unwrap_or(false);
        let ports = if has_tls {
            "80, 443".to_string()
        } else {
            "80".to_string()
        };

        KubeIngress {
            namespace,
            name,
            class,
            hosts,
            address,
            ports,
            age,
        }
    }
}
