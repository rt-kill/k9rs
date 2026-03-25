use std::borrow::Cow;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use k8s_openapi::api::core::v1::Service;

use super::KubeResource;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KubeService {
    pub namespace: String,
    pub name: String,
    pub service_type: String,
    pub cluster_ip: String,
    pub external_ip: String,
    pub ports: String,
    pub age: Option<DateTime<Utc>>,
    pub labels: BTreeMap<String, String>,
}

impl KubeResource for KubeService {
    fn headers() -> &'static [&'static str] {
        &[
            "NAMESPACE",
            "NAME",
            "TYPE",
            "CLUSTER-IP",
            "EXTERNAL-IP",
            "PORT(S)",
            "AGE",
        ]
    }

    fn row(&self) -> Vec<Cow<'_, str>> {
        vec![
            Cow::Borrowed(&self.namespace),
            Cow::Borrowed(&self.name),
            Cow::Borrowed(&self.service_type),
            Cow::Borrowed(&self.cluster_ip),
            Cow::Borrowed(&self.external_ip),
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
        "service"
    }
}

impl From<Service> for KubeService {
    fn from(svc: Service) -> Self {
        let metadata = svc.metadata;
        let namespace = metadata.namespace.unwrap_or_default();
        let name = metadata.name.unwrap_or_default();
        let labels = metadata.labels.unwrap_or_default();
        let age = metadata.creation_timestamp.map(|t| t.0);

        let spec = svc.spec.unwrap_or_default();
        let service_type = spec.type_.unwrap_or_else(|| "ClusterIP".to_string());
        let cluster_ip = spec.cluster_ip.unwrap_or_else(|| "<none>".to_string());

        let external_ip = {
            let mut ips: Vec<String> = Vec::new();

            if let Some(ext_ips) = &spec.external_ips {
                ips.extend(ext_ips.clone());
            }

            if let Some(ref status) = svc.status {
                if let Some(ref lb) = status.load_balancer {
                    if let Some(ref ingress) = lb.ingress {
                        for ing in ingress {
                            if let Some(ref ip) = ing.ip {
                                ips.push(ip.clone());
                            } else if let Some(ref hostname) = ing.hostname {
                                ips.push(hostname.clone());
                            }
                        }
                    }
                }
            }

            if ips.is_empty() {
                "<none>".to_string()
            } else {
                ips.join(",")
            }
        };

        let ports = spec
            .ports
            .unwrap_or_default()
            .iter()
            .map(|p| {
                let port = p.port;
                let protocol = p.protocol.as_deref().unwrap_or("TCP");
                if let Some(node_port) = p.node_port {
                    format!("{}:{}/{}", port, node_port, protocol)
                } else {
                    format!("{}/{}", port, protocol)
                }
            })
            .collect::<Vec<_>>()
            .join(",");

        KubeService {
            namespace,
            name,
            service_type,
            cluster_ip,
            external_ip,
            ports,
            age,
            labels,
        }
    }
}
