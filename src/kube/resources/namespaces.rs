use k8s_openapi::api::core::v1::Namespace;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{DrillTarget, ResourceRow};

/// Convert a k8s Namespace into a generic ResourceRow.
pub(crate) fn namespace_to_row(ns_obj: Namespace) -> ResourceRow {
    let meta = CommonMeta::from_k8s(ns_obj.metadata);
    let status = ns_obj.status.and_then(|s| s.phase).unwrap_or_else(|| "Active".to_string());
    // The name came from the K8s API's NamespaceList — it's a real
    // identifier, never the user-typed sentinel "all". Use `Named`
    // directly so a namespace literally named `all` would still drill
    // into itself instead of switching to all-namespaces mode.
    let drill_target = Some(DrillTarget::SwitchNamespace(
        crate::kube::protocol::Namespace::Named(meta.name.clone()),
    ));
    ResourceRow {
        cells: vec![meta.name.clone(), status, crate::util::format_age(meta.age)],
        name: meta.name,
        namespace: None,
        drill_target,
        ..Default::default()
    }
}
