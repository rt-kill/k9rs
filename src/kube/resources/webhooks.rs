use k8s_openapi::api::admissionregistration::v1::{
    ValidatingWebhookConfiguration, MutatingWebhookConfiguration,
};


use super::ConvertToRow;
use crate::kube::resource_def::webhooks::{ValidatingWebhookDef, MutatingWebhookDef};
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// ValidatingWebhookDef
// ---------------------------------------------------------------------------


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
