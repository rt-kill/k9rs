use k8s_openapi::api::admissionregistration::v1::{
    ValidatingWebhookConfiguration, MutatingWebhookConfiguration,
};

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

/// Convert a k8s ValidatingWebhookConfiguration into a generic ResourceRow.
pub(crate) fn validating_webhook_to_row(vwc: ValidatingWebhookConfiguration) -> ResourceRow {
    let meta = CommonMeta::from_k8s(vwc.metadata);
    let webhooks = vwc.webhooks.as_ref().map(|w| w.len()).unwrap_or(0);
    let failure_policies = vwc.webhooks.as_ref()
        .map(|ws| ws.iter()
            .map(|w| w.failure_policy.as_deref().unwrap_or("Fail"))
            .collect::<Vec<_>>()
            .join(","))
        .unwrap_or_default();
    ResourceRow {
        cells: vec![
            meta.name.clone(),
            webhooks.to_string(), failure_policies,
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: None,
        ..Default::default()
    }
}

/// Convert a k8s MutatingWebhookConfiguration into a generic ResourceRow.
pub(crate) fn mutating_webhook_to_row(mwc: MutatingWebhookConfiguration) -> ResourceRow {
    let meta = CommonMeta::from_k8s(mwc.metadata);
    let webhooks = mwc.webhooks.as_ref().map(|w| w.len()).unwrap_or(0);
    let failure_policies = mwc.webhooks.as_ref()
        .map(|ws| ws.iter()
            .map(|w| w.failure_policy.as_deref().unwrap_or("Fail"))
            .collect::<Vec<_>>()
            .join(","))
        .unwrap_or_default();
    ResourceRow {
        cells: vec![
            meta.name.clone(),
            webhooks.to_string(), failure_policies,
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: None,
        ..Default::default()
    }
}
