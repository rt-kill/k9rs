use k8s_openapi::api::core::v1::ResourceQuota;

use super::ConvertToRow;
use crate::kube::resource_def::resourcequotas::ResourceQuotaDef;

use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

// ---------------------------------------------------------------------------
// ResourceQuotaDef
// ---------------------------------------------------------------------------


impl ConvertToRow<ResourceQuota> for ResourceQuotaDef {
    fn convert(rq: ResourceQuota) -> ResourceRow {
        let meta = CommonMeta::from_k8s(rq.metadata);
        let format_qmap = |map: &Option<std::collections::BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>>| -> String {
            match map {
                Some(m) if !m.is_empty() => m.iter().map(|(k, v)| format!("{}: {}", k, v.0)).collect::<Vec<_>>().join(", "),
                _ => String::new(),
            }
        };
        let hard = rq.spec.as_ref().map(|s| format_qmap(&s.hard)).unwrap_or_default();
        let used = rq.status.as_ref().map(|s| format_qmap(&s.used)).unwrap_or_default();

        // A quota at its limit is why the next deploy won't schedule, so the
        // worst-off key decides the row.
        let health = rq
            .status
            .as_ref()
            .zip(rq.spec.as_ref())
            .map(|(status, spec)| quota_health(&status.used, &spec.hard))
            .unwrap_or_default();

        let cells: Vec<CellValue> = vec![
            CellValue::Text(meta.namespace.clone()),
            CellValue::Text(meta.name.clone()),
            CellValue::Text(hard),
            CellValue::Status { text: used, health },
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

type QuotaMap = std::collections::BTreeMap<
    String,
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
>;

/// Health of the WORST key in a quota: at its hard limit means the next
/// object of that kind is refused.
///
/// Every key is parsed by the unit its NAME declares — the quota key
/// vocabulary is defined by the API (`requests.cpu`, `limits.memory`,
/// `pods`, `count/deployments.apps`), so this reads a schema rather than
/// guessing at user data. A key whose units can't be established is SKIPPED:
/// comparing `100Mi` against `1Gi` as 100-against-1 would paint a quota at
/// ten percent bright red, and a wrong red is worse than no red.
fn quota_health(used: &Option<QuotaMap>, hard: &Option<QuotaMap>) -> RowHealth {
    const WARN_PCT: u64 = 90;
    let (Some(used), Some(hard)) = (used.as_ref(), hard.as_ref()) else {
        return RowHealth::Normal;
    };
    let mut worst = RowHealth::Normal;
    for (key, hard_q) in hard {
        let Some(used_q) = used.get(key) else { continue };
        let Some(limit) = parse_quota_value(key, &hard_q.0) else { continue };
        let Some(current) = parse_quota_value(key, &used_q.0) else { continue };
        if limit == 0 {
            continue;
        }
        let pct = current.saturating_mul(100) / limit;
        if pct >= 100 {
            return RowHealth::Failed;
        }
        if pct >= WARN_PCT {
            worst = RowHealth::Pending;
        }
    }
    worst
}

/// Parse a quota value in the unit its key implies. `None` when the value
/// cannot be READ — which is the whole contract, and which the first version
/// broke: it wrapped the shared metrics parsers unconditionally in `Some`,
/// and those return `0` on failure. An unreadable value therefore scored 0 %
/// and painted a full quota GREEN — the precise inverse of the "a wrong red
/// is worse than no red" rule this function is built around. Silence has to
/// mean silence.
fn parse_quota_value(key: &str, value: &str) -> Option<u64> {
    let last = key.rsplit('.').next().unwrap_or(key);
    match last {
        "cpu" => checked_quantity(value, crate::kube::metrics::parse_cpu_to_nano),
        "memory" | "storage" | "ephemeral-storage" => {
            checked_quantity(value, crate::kube::metrics::parse_mem_to_bytes)
        }
        // Everything else in the vocabulary is an object COUNT (`pods`,
        // `services`, `count/deployments.apps`, `services.loadbalancers`).
        _ => value.trim().parse().ok(),
    }
}

/// Run a lossy `&str -> u64` quantity parser and reject its failure sentinel.
///
/// The shared parsers answer `0` both for "zero" and for "I don't know", a
/// conflation that is harmless for a live usage sample and not harmless for a
/// limit comparison. A literal zero is still accepted; anything else that
/// parses to zero is refused.
fn checked_quantity(value: &str, parse: fn(&str) -> u64) -> Option<u64> {
    let trimmed = value.trim();
    match parse(trimmed) {
        0 if !is_literal_zero(trimmed) => None,
        n => Some(n),
    }
}

/// Is this quantity actually zero, as opposed to unparseable? Covers the
/// spellings a serialized `Quantity` can take (`0`, `0Mi`, `0.0`).
fn is_literal_zero(value: &str) -> bool {
    let digits: &str = value
        .trim_end_matches(|c: char| c.is_ascii_alphabetic())
        .trim();
    !digits.is_empty() && digits.parse::<f64>().is_ok_and(|v| v == 0.0)
}

#[cfg(test)]
#[path = "../../tests/kube/resources/resourcequotas.rs"]
mod tests;
