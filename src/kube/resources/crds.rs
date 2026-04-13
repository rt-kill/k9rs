use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;

use crate::kube::resources::row::{CrdRowInfo, DrillTarget, ResourceRow};
use crate::kube::protocol::ResourceScope;

/// Convert a k8s CustomResourceDefinition into a generic ResourceRow.
/// The `crd_info` typed field carries group/version/kind/plural/scope for
/// CRD-instance drill-down and command completion.
pub(crate) fn crd_to_row(crd: CustomResourceDefinition) -> ResourceRow {
    let meta = crd.metadata;
    let spec = crd.spec;
    let name = meta.name.unwrap_or_default();
    let group = spec.group;
    let version = spec
        .versions
        .first()
        .map(|v| v.name.clone())
        .unwrap_or_default();
    let kind = spec.names.kind;
    let plural = spec.names.plural;
    let scope_str = format!("{:?}", spec.scope).trim_matches('"').to_string();
    let scope = ResourceScope::from_scope_str(&scope_str);
    let age = meta.creation_timestamp.map(|ts| ts.0);

    let crd_info = Some(CrdRowInfo {
        group: group.clone(),
        version: version.clone(),
        kind: kind.clone(),
        plural: plural.clone(),
        scope,
    });

    let drill_target = Some(DrillTarget::BrowseCrd {
        group: group.clone(),
        version: version.clone(),
        kind: kind.clone(),
        plural: plural.clone(),
        scope,
    });

    ResourceRow {
        cells: vec![
            name.clone(), group, version, kind, scope_str, crate::util::format_age(age),
        ],
        name,
        namespace: None,
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        crd_info,
        drill_target,
    }
}
