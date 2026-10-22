use k8s_openapi::api::core::v1::Node;

use super::ConvertToRow;
use crate::kube::resource_def::nodes::NodeDef;

use crate::kube::resources::k8s_const::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, DrillTarget, QuantityUnit, ResourceRow, RowHealth};

// ---------------------------------------------------------------------------
// NodeDef
// ---------------------------------------------------------------------------


impl ConvertToRow<Node> for NodeDef {
    fn convert(node: Node) -> ResourceRow {
        let meta = CommonMeta::from_k8s(node.metadata);

        // Determine roles from labels. Checks both `node-role.kubernetes.io/<role>`
        // (standard) and `kubernetes.io/role` (legacy) label conventions.
        let roles = {
            let mut role_list: Vec<String> = Vec::new();
            for (key, value) in &meta.labels {
                if let Some(role) = key.strip_prefix("node-role.kubernetes.io/") {
                    if role.is_empty() {
                        if !value.is_empty() {
                            role_list.push(value.clone());
                        }
                    } else {
                        role_list.push(role.to_string());
                    }
                } else if key == "kubernetes.io/role" && !value.is_empty() {
                    role_list.push(value.clone());
                }
            }
            role_list.sort();
            role_list.dedup();
            if role_list.is_empty() {
                "<none>".to_string()
            } else {
                role_list.join(",")
            }
        };

        // Instance type from the well-known node label (modern, then legacy).
        let instance_type = meta.labels.get("node.kubernetes.io/instance-type")
            .or_else(|| meta.labels.get("beta.kubernetes.io/instance-type"))
            .cloned()
            .unwrap_or_else(|| "<none>".to_string());

        let spec = node.spec;

        // Taint count
        let taints = spec.as_ref()
            .and_then(|s| s.taints.as_ref())
            .map(|t| t.len())
            .unwrap_or(0);

        let status_val = node.status.unwrap_or_default();
        // Classify node state + schedulability as typed booleans, then derive
        // both the display string AND the RowHealth from them.
        let is_ready = status_val.conditions.as_deref().unwrap_or(&[])
            .iter()
            .any(|cond| cond.type_ == COND_READY && cond.status == STATUS_TRUE);
        let is_scheduling_disabled = spec.as_ref()
            .and_then(|s| s.unschedulable)
            .unwrap_or(false);

        let status = match (is_ready, is_scheduling_disabled) {
            (true, true) => format!("{},SchedulingDisabled", COND_READY),
            (true, false) => COND_READY.to_string(),
            (false, true) => format!("{},SchedulingDisabled", REASON_NOT_READY),
            (false, false) => REASON_NOT_READY.to_string(),
        };
        let health = if !is_ready {
            RowHealth::Failed
        } else if is_scheduling_disabled {
            RowHealth::Pending
        } else {
            RowHealth::Normal
        };

        let node_info = status_val.node_info.as_ref();
        let version = node_info
            .map(|info| info.kubelet_version.clone())
            .unwrap_or_default();
        let arch = node_info
            .map(|info| info.architecture.clone())
            .unwrap_or_default();
        let os_image = node_info
            .map(|info| info.os_image.clone())
            .unwrap_or_default();
        let kernel = node_info
            .map(|info| info.kernel_version.clone())
            .unwrap_or_default();

        // Addresses
        let addresses = status_val.addresses.unwrap_or_default();
        let internal_ip = addresses.iter()
            .find(|a| a.type_ == ADDR_INTERNAL_IP)
            .map(|a| a.address.clone())
            .unwrap_or_else(|| "<none>".to_string());
        let external_ip = addresses.iter()
            .find(|a| a.type_ == ADDR_EXTERNAL_IP)
            .map(|a| a.address.clone())
            .unwrap_or_else(|| "<none>".to_string());

        // Allocatable (what k9s uses for the % columns) + capacity (for the GPU
        // capacity columns). `allocatable` = capacity minus system-reserved.
        let allocatable = status_val.allocatable.unwrap_or_default();
        let capacity = status_val.capacity.unwrap_or_default();

        // CPU/MEM allocatable as numeric Quantity cells — the metrics overlay
        // reads these to compute %CPU / %MEM (usage * 100 / allocatable).
        let alloc_cpu_milli = allocatable.get("cpu")
            .map(|q| crate::kube::metrics::parse_cpu_to_nano(&q.0) / 1_000_000)
            .unwrap_or(0);
        let alloc_mem_bytes = allocatable.get("memory")
            .map(|q| crate::kube::metrics::parse_mem_to_bytes(&q.0))
            .unwrap_or(0);
        let pods_alloc = allocatable.get("pods").and_then(|q| q.0.parse::<i64>().ok());

        // GPU counts (integers); absent → 0. The shared/time-sliced keys are
        // best-effort — they vary by device-plugin setup.
        let gpu_alloc = allocatable.get("nvidia.com/gpu").and_then(|q| q.0.parse::<i64>().ok()).unwrap_or(0);
        let gpu_cap = capacity.get("nvidia.com/gpu").and_then(|q| q.0.parse::<i64>().ok()).unwrap_or(0);
        let sh_gpu_alloc = allocatable.get("nvidia.com/gpu.shared").and_then(|q| q.0.parse::<i64>().ok()).unwrap_or(0);
        let sh_gpu_cap = capacity.get("nvidia.com/gpu.shared").and_then(|q| q.0.parse::<i64>().ok()).unwrap_or(0);

        let drill_target = Some(DrillTarget::PodsByField(
            crate::kube::resources::row::K8sFieldSelector::SpecNodeName(meta.name.clone()),
        ));

        // CPU, %CPU, MEM, %MEM are Placeholder here — the metrics overlay
        // (apply_node_metrics) fills them when metrics-server data arrives.
        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.name.clone()),
            CellValue::Status { text: status, health },
            CellValue::Text(roles),
            CellValue::Text(instance_type),
            CellValue::Count(taints as i64),
            CellValue::Text(version),
            pods_alloc.map(CellValue::Count).unwrap_or(CellValue::Placeholder),              // PODS/A (allocatable)
            CellValue::Placeholder,                                                          // CPU
            CellValue::Quantity { value: alloc_cpu_milli, unit: QuantityUnit::Millicores },  // CPU/A
            CellValue::Percentage(None),                                                     // %CPU
            CellValue::Placeholder,                                                          // MEM
            CellValue::Quantity { value: alloc_mem_bytes, unit: QuantityUnit::Bytes },       // MEM/A
            CellValue::Percentage(None),                                                     // %MEM
            CellValue::Count(gpu_alloc),                                                     // GPU/A
            CellValue::Count(gpu_cap),                                                       // GPU/C
            CellValue::Count(sh_gpu_alloc),                                                  // SH-GPU/A
            CellValue::Count(sh_gpu_cap),                                                    // SH-GPU/C
            CellValue::Text(os_image),                                                       // OS-IMAGE
            CellValue::Text(kernel),                                                         // KERNEL
            CellValue::Text(internal_ip),                                                    // INTERNAL-IP
            CellValue::Text(external_ip),                                                    // EXTERNAL-IP
            CellValue::Text(arch),                                                           // ARCH
            CellValue::Text(meta.labels_str),                                                // LABELS
            CellValue::Age(meta.age.map(|t| t.timestamp())),                                 // AGE
        ];
        ResourceRow {
            name: meta.name,
            namespace: None,
            health,
            drill_target,
            cells,
            ..Default::default()
        }
    }
}

#[cfg(test)]
#[path = "../../tests/kube/resources/nodes.rs"]
mod tests;
