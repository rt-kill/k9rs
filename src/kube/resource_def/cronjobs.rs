//! `CronJobDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::cronjobs`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, Gvr, ResourceDef};
use crate::kube::protocol::{OperationKind, ResourceScope};

pub struct CronJobDef;

impl ResourceDef for CronJobDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::CronJob }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "batch", version: "v1", kind: "CronJob",
            plural: "cronjobs", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["cj", "cronjob", "cronjobs"] }
    fn short_label(&self) -> &str { "CronJobs" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "SCHEDULE", "SUSPEND", "ACTIVE", "LAST SCHEDULE",
         "CONTAINERS", "IMAGES", "LABELS", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn operations(&self) -> Vec<OperationKind> {
        use OperationKind::*;
        vec![Describe, Yaml, Delete, StreamLogs, PreviousLogs, TriggerCronJob, ToggleSuspendCronJob]
    }
}
