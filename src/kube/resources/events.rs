use k8s_openapi::api::core::v1::Event;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{ResourceRow};

/// Convert a k8s Event into a generic ResourceRow.
///
/// Unlike most rows, an Event's `name` is the *involved object* (`kind/name`
/// of whatever the event is about) rather than the event's own metadata
/// name. The event resource uses random metadata names like
/// `pod-foo.17c8...` that aren't useful to the user, so we surface the
/// subject instead — that's what matches sorting/filtering expectations.
pub(crate) fn event_to_row(ev: Event) -> ResourceRow {
    let meta = CommonMeta::from_k8s(ev.metadata);
    let event_type = ev.type_.unwrap_or_default();
    let reason = ev.reason.unwrap_or_default();
    let message = ev.message.unwrap_or_default();
    let count = ev.count.unwrap_or(0);
    let obj = ev.involved_object;
    let kind = obj.kind.unwrap_or_default();
    let obj_name = obj.name.unwrap_or_default();
    let involved_object = if kind.is_empty() {
        obj_name
    } else {
        format!("{}/{}", kind.to_lowercase(), obj_name)
    };
    let source = ev.source.and_then(|s| s.component).unwrap_or_default();
    ResourceRow {
        cells: vec![
            meta.namespace.clone(),
            event_type, reason, involved_object.clone(), message, source,
            count.to_string(), crate::util::format_age(meta.age),
        ],
        name: involved_object,
        namespace: Some(meta.namespace),
        ..Default::default()
    }
}
