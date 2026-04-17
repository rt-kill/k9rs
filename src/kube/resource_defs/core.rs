//! Core and networking resource definitions: Service, ConfigMap, Secret,
//! ServiceAccount, Ingress, NetworkPolicy, HPA, Endpoints, LimitRange,
//! ResourceQuota, PodDisruptionBudget, Event, PVC.

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::row::ResourceRow;

use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
use k8s_openapi::api::core::v1::{
    ConfigMap, Endpoints, Event, LimitRange, PersistentVolumeClaim, ResourceQuota, Secret,
    Service, ServiceAccount,
};
use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

pub struct ServiceDef;

impl ResourceDef for ServiceDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Service }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Service",
            plural: "services", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["svc", "service", "services"] }
    fn short_label(&self) -> &str { "Svc" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "TYPE", "CLUSTER-IP", "EXTERNAL-IP", "SELECTOR",
         "PORT(S)", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn is_port_forwardable(&self) -> bool { true }
}

impl ConvertToRow<Service> for ServiceDef {
    fn convert(obj: Service) -> ResourceRow {
        crate::kube::resources::services::service_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// ConfigMap
// ---------------------------------------------------------------------------

pub struct ConfigMapDef;

impl ResourceDef for ConfigMapDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ConfigMap }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "ConfigMap",
            plural: "configmaps", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["cm", "configmap", "configmaps"] }
    fn short_label(&self) -> &str { "CM" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "DATA", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<ConfigMap> for ConfigMapDef {
    fn convert(obj: ConfigMap) -> ResourceRow {
        crate::kube::resources::configmaps::configmap_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// Secret
// ---------------------------------------------------------------------------

pub struct SecretDef;

impl ResourceDef for SecretDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Secret }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Secret",
            plural: "secrets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["sec", "secret", "secrets"] }
    fn short_label(&self) -> &str { "Secrets" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "TYPE", "DATA", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn is_secret_like(&self) -> bool { true }
}

impl ConvertToRow<Secret> for SecretDef {
    fn convert(obj: Secret) -> ResourceRow {
        crate::kube::resources::secrets::secret_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// ServiceAccount
// ---------------------------------------------------------------------------

pub struct ServiceAccountDef;

impl ResourceDef for ServiceAccountDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ServiceAccount }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "ServiceAccount",
            plural: "serviceaccounts", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["sa", "serviceaccount", "serviceaccounts"] }
    fn short_label(&self) -> &str { "SA" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "SECRETS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<ServiceAccount> for ServiceAccountDef {
    fn convert(obj: ServiceAccount) -> ResourceRow {
        crate::kube::resources::serviceaccounts::service_account_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// Ingress
// ---------------------------------------------------------------------------

pub struct IngressDef;

impl ResourceDef for IngressDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Ingress }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "networking.k8s.io", version: "v1", kind: "Ingress",
            plural: "ingresses", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["ing", "ingress", "ingresses"] }
    fn short_label(&self) -> &str { "Ing" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "CLASS", "HOSTS", "ADDRESS", "PORTS", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<Ingress> for IngressDef {
    fn convert(obj: Ingress) -> ResourceRow {
        crate::kube::resources::ingresses::ingress_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// NetworkPolicy
// ---------------------------------------------------------------------------

pub struct NetworkPolicyDef;

impl ResourceDef for NetworkPolicyDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::NetworkPolicy }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "networking.k8s.io", version: "v1", kind: "NetworkPolicy",
            plural: "networkpolicies", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["np", "networkpolicy", "networkpolicies"] }
    fn short_label(&self) -> &str { "NetPol" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "POD-SELECTOR", "POLICY TYPES", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<NetworkPolicy> for NetworkPolicyDef {
    fn convert(obj: NetworkPolicy) -> ResourceRow {
        crate::kube::resources::networkpolicies::network_policy_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// HorizontalPodAutoscaler (HPA)
// ---------------------------------------------------------------------------

pub struct HpaDef;

impl ResourceDef for HpaDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Hpa }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "autoscaling", version: "v2", kind: "HorizontalPodAutoscaler",
            plural: "horizontalpodautoscalers", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["hpa", "horizontalpodautoscaler", "horizontalpodautoscalers"] }
    fn short_label(&self) -> &str { "HPA" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "REFERENCE", "MIN", "MAX", "CURRENT", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<HorizontalPodAutoscaler> for HpaDef {
    fn convert(obj: HorizontalPodAutoscaler) -> ResourceRow {
        crate::kube::resources::hpa::hpa_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

pub struct EndpointsDef;

impl ResourceDef for EndpointsDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Endpoints }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Endpoints",
            plural: "endpoints", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["ep", "endpoints"] }
    fn short_label(&self) -> &str { "EP" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "ENDPOINTS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<Endpoints> for EndpointsDef {
    fn convert(obj: Endpoints) -> ResourceRow {
        crate::kube::resources::endpoints::endpoints_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// LimitRange
// ---------------------------------------------------------------------------

pub struct LimitRangeDef;

impl ResourceDef for LimitRangeDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::LimitRange }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "LimitRange",
            plural: "limitranges", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["limits", "limitrange", "limitranges"] }
    fn short_label(&self) -> &str { "Limits" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "TYPE", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<LimitRange> for LimitRangeDef {
    fn convert(obj: LimitRange) -> ResourceRow {
        crate::kube::resources::limitranges::limit_range_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// ResourceQuota
// ---------------------------------------------------------------------------

pub struct ResourceQuotaDef;

impl ResourceDef for ResourceQuotaDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::ResourceQuota }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "ResourceQuota",
            plural: "resourcequotas", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["quota", "resourcequota", "resourcequotas"] }
    fn short_label(&self) -> &str { "Quota" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "HARD", "USED", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<ResourceQuota> for ResourceQuotaDef {
    fn convert(obj: ResourceQuota) -> ResourceRow {
        crate::kube::resources::resourcequotas::resource_quota_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// PodDisruptionBudget
// ---------------------------------------------------------------------------

pub struct PodDisruptionBudgetDef;

impl ResourceDef for PodDisruptionBudgetDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::PodDisruptionBudget }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "policy", version: "v1", kind: "PodDisruptionBudget",
            plural: "poddisruptionbudgets", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["pdb", "poddisruptionbudget", "poddisruptionbudgets"] }
    fn short_label(&self) -> &str { "PDB" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "MIN AVAILABLE", "MAX UNAVAILABLE", "ALLOWED DISRUPTIONS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<PodDisruptionBudget> for PodDisruptionBudgetDef {
    fn convert(obj: PodDisruptionBudget) -> ResourceRow {
        crate::kube::resources::pdb::pdb_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

pub struct EventDef;

impl ResourceDef for EventDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Event }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Event",
            plural: "events", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["ev", "event", "events"] }
    fn short_label(&self) -> &str { "Events" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "TYPE", "REASON", "OBJECT", "MESSAGE", "SOURCE", "COUNT", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<Event> for EventDef {
    fn convert(obj: Event) -> ResourceRow {
        crate::kube::resources::events::event_to_row(obj)
    }
}

// ---------------------------------------------------------------------------
// PersistentVolumeClaim (PVC)
// ---------------------------------------------------------------------------

pub struct PvcDef;

impl ResourceDef for PvcDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::PersistentVolumeClaim }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "PersistentVolumeClaim",
            plural: "persistentvolumeclaims", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["pvc", "persistentvolumeclaim", "persistentvolumeclaims", "pvcs"] }
    fn short_label(&self) -> &str { "PVC" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "STATUS", "VOLUME", "CAPACITY", "ACCESS MODES",
         "STORAGECLASS", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<PersistentVolumeClaim> for PvcDef {
    fn convert(obj: PersistentVolumeClaim) -> ResourceRow {
        crate::kube::resources::pvcs::pvc_to_row(obj)
    }
}
