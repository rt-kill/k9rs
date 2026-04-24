use k8s_openapi::api::core::v1::Event;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow};

// ---------------------------------------------------------------------------
// EventDef
// ---------------------------------------------------------------------------

pub struct EventDef;

impl ResourceDef for EventDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Event }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "", version: "v1", kind: "Event",
            plural: "events", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["ev", "event", "events"] }
    fn short_label(&self) -> &str { "Events" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "TYPE", "REASON", "OBJECT", "MESSAGE", "SOURCE", "COUNT", "AGE"]
            .into_iter().map(String::from).collect()
    }

    fn column_defs(&self) -> Vec<ColumnDef> {
        use ColumnDef as C;
        vec![
            C::new("NAMESPACE"), C::new("TYPE"), C::new("REASON"),
            C::new("OBJECT"), C::new("MESSAGE"), C::extra("SOURCE"),
            C::new("COUNT"), C::age("AGE"),
        ]
    }
}

impl ConvertToRow<Event> for EventDef {
    /// Unlike most rows, an Event's `name` is the *involved object* (`kind/name`
    /// of whatever the event is about) rather than the event's own metadata
    /// name. The event resource uses random metadata names like
    /// `pod-foo.17c8...` that aren't useful to the user, so we surface the
    /// subject instead -- that's what matches sorting/filtering expectations.
    fn convert(ev: Event) -> ResourceRow {
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
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(event_type),
            CellValue::Text(reason),
            CellValue::Text(involved_object.clone()),
            CellValue::Text(message),
            CellValue::Text(source),
            CellValue::Count(count as i64),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: involved_object,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
