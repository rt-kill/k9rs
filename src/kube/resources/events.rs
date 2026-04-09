use k8s_openapi::api::core::v1::Event;

use crate::kube::resources::row::ResourceRow;

/// Convert a k8s Event into a generic ResourceRow.
pub(crate) fn event_to_row(ev: Event) -> ResourceRow {
    let metadata = ev.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let age = metadata.creation_timestamp.map(|t| t.0);
    let event_type = ev.type_.unwrap_or_default();
    let reason = ev.reason.unwrap_or_default();
    let message = ev.message.unwrap_or_default();
    let count = ev.count.unwrap_or(0);
    let _last_seen = ev.last_timestamp.map(|t| crate::util::format_age(Some(t.0)))
        .or_else(|| ev.event_time.map(|t| crate::util::format_age(Some(t.0))))
        .unwrap_or_else(|| crate::util::format_age(age));
    let obj = ev.involved_object;
    let kind = obj.kind.unwrap_or_default();
    let obj_name = obj.name.unwrap_or_default();
    let involved_object = if kind.is_empty() { obj_name } else { format!("{}/{}", kind.to_lowercase(), obj_name) };
    let source = ev.source.and_then(|s| s.component).unwrap_or_default();
    // name() for events returns the involved_object, matching KubeEvent behavior
    ResourceRow {
        cells: vec![ns.clone(), event_type, reason, involved_object.clone(), message, source, count.to_string(), crate::util::format_age(age)],
        name: involved_object,
        namespace: ns,
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        crd_info: None,
        drill_target: None,
    }
}
