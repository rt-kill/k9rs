//! Both webhook tables, which had no tests and two identical bodies.

use crate::resources::ConvertToRow;
use k9rs_proto::resource_def::webhooks::{MutatingWebhookDef, ValidatingWebhookDef};
use k9rs_proto::row::ResourceRow;

use k8s_openapi::api::admissionregistration::v1::{
    MutatingWebhook, MutatingWebhookConfiguration, ValidatingWebhook,
    ValidatingWebhookConfiguration, WebhookClientConfig,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

fn meta(name: &str) -> ObjectMeta {
    ObjectMeta { name: Some(name.to_string()), ..Default::default() }
}

fn text(row: &ResourceRow, i: usize) -> String {
    row.cells[i].to_string()
}

/// A hook with no `failurePolicy` reads as `Fail` — the apiserver's own
/// default, and the one that matters: a `Fail` hook that is down BLOCKS
/// admissions, so a blank cell would hide the reason writes had stopped.
#[test]
fn an_unset_failure_policy_reads_as_fail() {
    let vwc = ValidatingWebhookConfiguration {
        metadata: meta("pod-policy"),
        webhooks: Some(vec![ValidatingWebhook {
            name: "check.example.com".to_string(),
            client_config: WebhookClientConfig::default(),
            admission_review_versions: vec!["v1".to_string()],
            side_effects: "None".to_string(),
            failure_policy: None,
            ..Default::default()
        }]),
    };
    let row = ValidatingWebhookDef::convert(vwc);
    assert_eq!(text(&row, 2), "Fail");
    assert_eq!(text(&row, 1), "1");
}

/// The two tables agree, because they are one row builder now. They were
/// byte-identical bodies differing only in the type name.
#[test]
fn the_two_webhook_tables_build_the_same_row() {
    let policies = [Some("Ignore".to_string()), None];
    let vwc = ValidatingWebhookConfiguration {
        metadata: meta("hooks"),
        webhooks: Some(
            policies
                .iter()
                .map(|p| ValidatingWebhook {
                    name: "a.example.com".to_string(),
                    client_config: WebhookClientConfig::default(),
                    admission_review_versions: vec!["v1".to_string()],
                    side_effects: "None".to_string(),
                    failure_policy: p.clone(),
                    ..Default::default()
                })
                .collect(),
        ),
    };
    let mwc = MutatingWebhookConfiguration {
        metadata: meta("hooks"),
        webhooks: Some(
            policies
                .iter()
                .map(|p| MutatingWebhook {
                    name: "a.example.com".to_string(),
                    client_config: WebhookClientConfig::default(),
                    admission_review_versions: vec!["v1".to_string()],
                    side_effects: "None".to_string(),
                    failure_policy: p.clone(),
                    ..Default::default()
                })
                .collect(),
        ),
    };
    let v = ValidatingWebhookDef::convert(vwc);
    let m = MutatingWebhookDef::convert(mwc);
    assert_eq!(v.cells, m.cells);
    assert_eq!(text(&v, 1), "2", "two hooks");
    assert_eq!(text(&v, 2), "Ignore,Fail", "in declaration order, defaults filled");
}

/// A configuration declaring no hooks is a zero count and an empty policy
/// list, not a missing row.
#[test]
fn a_configuration_with_no_hooks_counts_zero() {
    let row = ValidatingWebhookDef::convert(ValidatingWebhookConfiguration {
        metadata: meta("empty"),
        webhooks: None,
    });
    assert_eq!(text(&row, 1), "0");
    assert_eq!(text(&row, 2), "");
    assert_eq!(row.name, "empty");
    assert_eq!(row.namespace, None, "webhook configurations are cluster-scoped");
}
