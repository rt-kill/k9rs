use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;

use crate::kube::protocol::ResourceScope;
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

// ---------------------------------------------------------------------------
// HpaDef
// ---------------------------------------------------------------------------

pub struct HpaDef;

impl ResourceDef for HpaDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::Hpa }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "autoscaling", version: "v2", kind: "HorizontalPodAutoscaler",
            plural: "horizontalpodautoscalers", scope: ResourceScope::Namespaced,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["hpa", "horizontalpodautoscaler", "horizontalpodautoscalers"] }
    fn short_label(&self) -> &str { "HPA" }
    fn default_headers(&self) -> Vec<String> {
        ["NAMESPACE", "NAME", "REFERENCE", "MIN", "MAX", "CURRENT", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<HorizontalPodAutoscaler> for HpaDef {
    fn convert(hpa: HorizontalPodAutoscaler) -> ResourceRow {
        let meta = CommonMeta::from_k8s(hpa.metadata);
        let spec = hpa.spec.unwrap_or_default();
        let reference = format!("{}/{}", spec.scale_target_ref.kind, spec.scale_target_ref.name);
        let min_replicas = spec.min_replicas.map(|v| v.to_string()).unwrap_or_else(|| "<unset>".to_string());
        let _max_replicas = spec.max_replicas.to_string();
        let status = hpa.status.unwrap_or_default();
        let current_replicas = status.current_replicas.unwrap_or(0);

        // An autoscaler that cannot scale is the interesting state, and the
        // API says so in its conditions — `ScalingActive=False` means it
        // isn't computing a target at all (no metrics, usually). Pinned at
        // MAX is the milder one: it is working, and it has run out of room.
        let cannot_scale = status.conditions.iter().flatten().any(|c| {
            (c.type_ == "ScalingActive" || c.type_ == "AbleToScale") && c.status == crate::kube::resources::k8s_const::STATUS_FALSE
        });
        let health = if cannot_scale {
            RowHealth::Failed
        } else if current_replicas >= spec.max_replicas && spec.max_replicas > 0 {
            RowHealth::Pending
        } else {
            RowHealth::Normal
        };

        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(reference),
            CellValue::Text(min_replicas),
            CellValue::Count(spec.max_replicas as i64),
            // `Count`, not `Status`: `Status` sorts as text (see `row.rs`),
            // which turned `O` on this column into 9, 8, 7, 3, 20, 12, 10, 1.
            // The row health carries the verdict.
            CellValue::Count(current_replicas as i64),
            CellValue::Age(meta.age.map(|t| t.timestamp())),
        ];        ResourceRow {
            name: meta.name,
            namespace: Some(meta.namespace),
            cells,
            health,
            ..Default::default()
        }
    }
}
