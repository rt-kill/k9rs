use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::ReplicaSet;

use crate::kube::resources::row::{ExtraValue, ResourceRow};

/// Convert a k8s ReplicaSet into a generic ResourceRow.
pub(crate) fn replicaset_to_row(rs: ReplicaSet) -> ResourceRow {
    let metadata = rs.metadata;
    let ns = metadata.namespace.unwrap_or_default();
    let name = metadata.name.unwrap_or_default();
    let uid = metadata.uid.unwrap_or_default();
    let labels = metadata.labels.unwrap_or_default();
    let labels_str = labels.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join(",");
    let age = metadata.creation_timestamp.map(|t| t.0);

    let selector_labels = rs.spec.as_ref()
        .and_then(|s| s.selector.match_labels.clone())
        .unwrap_or_default();
    let desired = rs.spec.and_then(|s| s.replicas).unwrap_or(0);
    let status = rs.status.unwrap_or_default();
    let current = status.replicas;
    let ready = status.ready_replicas.unwrap_or(0);

    let mut extra = BTreeMap::new();
    extra.insert("selector_labels".into(), ExtraValue::Map(selector_labels));
    extra.insert("uid".into(), ExtraValue::Str(uid));

    ResourceRow {
        cells: vec![
            ns.clone(), name.clone(), desired.to_string(), current.to_string(),
            ready.to_string(), labels_str, crate::util::format_age(age),
        ],
        name,
        namespace: ns,
        extra,
    }
}
