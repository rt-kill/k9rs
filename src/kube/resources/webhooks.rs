use k8s_openapi::api::admissionregistration::v1::{
    ValidatingWebhookConfiguration, MutatingWebhookConfiguration,
};

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// ValidatingWebhookDef
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
    fn convert(vwc: ValidatingWebhookConfiguration) -> ResourceRow {
        let meta = CommonMeta::from_k8s(vwc.metadata);
        let webhooks = vwc.webhooks.as_ref().map(|w| w.len()).unwrap_or(0);
        let failure_policies = vwc.webhooks.as_ref()
            .map(|ws| ws.iter()
                .map(|w| w.failure_policy.as_deref().unwrap_or("Fail"))
                .collect::<Vec<_>>()
                .join(","))
            .unwrap_or_default();
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.name.clone()),
            CellValue::Count(webhooks as i64),
            CellValue::Text(failure_policies),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: None,
            cells,
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------------
// MutatingWebhookDef
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
    fn convert(mwc: MutatingWebhookConfiguration) -> ResourceRow {
        let meta = CommonMeta::from_k8s(mwc.metadata);
        let webhooks = mwc.webhooks.as_ref().map(|w| w.len()).unwrap_or(0);
        let failure_policies = mwc.webhooks.as_ref()
            .map(|ws| ws.iter()
                .map(|w| w.failure_policy.as_deref().unwrap_or("Fail"))
                .collect::<Vec<_>>()
                .join(","))
            .unwrap_or_default();
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.name.clone()),
            CellValue::Count(webhooks as i64),
            CellValue::Text(failure_policies),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: None,
            cells,
            ..Default::default()
        }
    }
}
