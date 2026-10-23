use k8s_openapi::api::core::v1::Event;

use super::ConvertToRow;
use k9rs_proto::resource_def::events::EventDef;

use crate::resources::k8s_const::{EVENT_TYPE_NORMAL, EVENT_TYPE_WARNING};
use crate::resources::CommonMeta;
use k9rs_proto::row::{CellValue, ResourceRow, RowHealth};

// ---------------------------------------------------------------------------
// EventDef
// ---------------------------------------------------------------------------


impl ConvertToRow<Event> for EventDef {
    /// An Event's `name` is its own `metadata.name` — the ugly
    /// `pod-foo.17c8ab…` — because `ResourceRow.name` is not a display field.
    /// It is the merge key, the remove key, the mark key and the operand of
    /// every operation.
    ///
    /// It used to be the involved object, to surface something readable. That
    /// made every event about one pod share an identity, which only became
    /// visible when `E` put them in a view together: the first delta collapsed
    /// five `FailedScheduling` rows into one (`locate_upsert` matches by
    /// identity), `Remove`s keyed by the real name could never match so expired
    /// events never left, and `d` on the fourth row described the first.
    ///
    /// The subject is still right there in the `OBJECT` column, which is where
    /// a display string belongs.
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
        // `type` rides as a TYPED status cell like every other converter's —
        // not as text that renders a Warning identically to a Normal, which
        // is the one thing you open this view to tell apart. Pending, not
        // Failed: a warning is what the cluster is complaining about, not a
        // thing that has failed. The row's own health stays Normal
        // deliberately — colouring whole rows in a view that is mostly
        // warnings is a wall, not a signal.
        //
        // Both defined values are NAMED. A wildcard here used to absorb the
        // ABSENT case too — `type` is `Option<String>` and this is its
        // `unwrap_or_default()` — so an event with no type rendered as an
        // empty cell coloured Normal. Anything unexpected is worth a look,
        // so it reads as Pending rather than as fine.
        let health = match event_type.as_str() {
            EVENT_TYPE_NORMAL => RowHealth::Normal,
            EVENT_TYPE_WARNING => RowHealth::Pending,
            _ => RowHealth::Pending,
        };
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Status { text: event_type, health },
            CellValue::Text(reason),
            CellValue::Text(involved_object.clone()),
            CellValue::Text(message),
            CellValue::Text(source),
            CellValue::Count(count as i64),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            ..Default::default()
        }
    }
}
