//! `EventDef` — identity, aliases, columns, operations.
//!
//! The METADATA half of `resources::events`: pure data. `impl ConvertToRow<K>`
//! stays with the k8s-openapi type it converts, because the pure/impure seam
//! runs THROUGH each resource file rather than between directories.

use super::{BuiltInKind, ColumnDef, Gvr, ResourceDef};
use crate::kube::protocol::ResourceScope;

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
            C::new("COUNT"), C::new("AGE"),
        ]
    }
}
