//! `ValidatingWebhookDef`, `MutatingWebhookDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::webhooks`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::protocol::ResourceScope;

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
