//! The two admission-webhook configuration tables.
//!
//! Validating and mutating webhooks are unrelated k8s structs with
//! identical-looking rows, which is why the two converters were byte-
//! identical bodies differing only in the type name. The shared half takes
//! the extracted parts rather than the object, since there is no trait over
//! the two.

use k8s_openapi::api::admissionregistration::v1::{
    MutatingWebhookConfiguration, ValidatingWebhookConfiguration,
};

use super::ConvertToRow;
use crate::resources::CommonMeta;
use k9rs_proto::resource_def::webhooks::{MutatingWebhookDef, ValidatingWebhookDef};
use k9rs_proto::row::{CellValue, ResourceRow};

/// One webhook-configuration row: how many hooks it declares, and what each
/// does when it cannot be reached.
///
/// `failure_policy` defaults to `Fail` because that is the apiserver's
/// default, and it is the one that matters: a `Fail` hook that is down
/// blocks admissions, so a row showing it blank would hide the reason the
/// cluster had stopped accepting writes.
fn webhook_row(meta: CommonMeta, policies: Vec<&str>) -> ResourceRow {
    let cells: Vec<CellValue> = vec![
        CellValue::Text(meta.name.clone()),
        CellValue::Count(policies.len() as i64),
        CellValue::Text(policies.join(",")),
        CellValue::Age(meta.age.map(|t| t.timestamp())),
    ];
    ResourceRow {
        name: meta.name,
        namespace: None,
        cells,
        ..Default::default()
    }
}

impl ConvertToRow<ValidatingWebhookConfiguration> for ValidatingWebhookDef {
    fn convert(vwc: ValidatingWebhookConfiguration) -> ResourceRow {
        let policies = vwc
            .webhooks
            .iter()
            .flatten()
            .map(|w| w.failure_policy.as_deref().unwrap_or("Fail"))
            .collect();
        webhook_row(CommonMeta::from_k8s(vwc.metadata), policies)
    }
}

impl ConvertToRow<MutatingWebhookConfiguration> for MutatingWebhookDef {
    fn convert(mwc: MutatingWebhookConfiguration) -> ResourceRow {
        let policies = mwc
            .webhooks
            .iter()
            .flatten()
            .map(|w| w.failure_policy.as_deref().unwrap_or("Fail"))
            .collect();
        webhook_row(CommonMeta::from_k8s(mwc.metadata), policies)
    }
}

#[cfg(test)]
#[path = "../tests/resources/webhooks.rs"]
mod tests;
