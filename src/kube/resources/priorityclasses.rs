use k8s_openapi::api::scheduling::v1::PriorityClass;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::ResourceRow;

/// Convert a k8s PriorityClass into a generic ResourceRow.
pub(crate) fn priority_class_to_row(pc: PriorityClass) -> ResourceRow {
    let meta = CommonMeta::from_k8s(pc.metadata);
    let value = pc.value.to_string();
    let global_default = if pc.global_default.unwrap_or(false) { "true" } else { "false" };
    let preemption = pc.preemption_policy.unwrap_or_else(|| "PreemptLowerPriority".to_string());
    ResourceRow {
        cells: vec![
            meta.name.clone(),
            value, global_default.to_string(), preemption,
            crate::util::format_age(meta.age),
        ],
        name: meta.name,
        namespace: None,
        ..Default::default()
    }
}
