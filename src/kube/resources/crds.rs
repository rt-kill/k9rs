use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;

use crate::kube::resources::row::{ExtraValue, ResourceRow};

/// Convert a k8s CustomResourceDefinition into a generic ResourceRow.
/// Extra fields carry group/version/kind/plural/scope for CRD-instance drill-down.
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
    let scope = format!("{:?}", spec.scope).trim_matches('"').to_string();
    let age = meta.creation_timestamp.map(|ts| ts.0);

    let mut extra = std::collections::BTreeMap::new();
    extra.insert("group".into(), ExtraValue::Str(group.clone()));
    extra.insert("version".into(), ExtraValue::Str(version.clone()));
    extra.insert("kind".into(), ExtraValue::Str(kind.clone()));
    extra.insert("plural".into(), ExtraValue::Str(plural.clone()));
    extra.insert("scope".into(), ExtraValue::Str(scope.clone()));

    ResourceRow {
        cells: vec![
            name.clone(), group, version, kind, scope, crate::util::format_age(age),
        ],
        name,
        namespace: String::new(),
        extra,
    }
}
