use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;

use crate::kube::protocol::{CrdRef, ResourceScope};
use crate::kube::resource_def::*;
use crate::kube::resources::CommonMeta;
use crate::kube::resources::row::{DrillTarget, ResourceRow};

// ---------------------------------------------------------------------------
// CrdDef
// ---------------------------------------------------------------------------

pub struct CrdDef;

impl ResourceDef for CrdDef {
    fn kind(&self) -> BuiltInKind { BuiltInKind::CustomResourceDefinition }
    fn gvr(&self) -> &'static Gvr {
        const G: Gvr = Gvr {
            group: "apiextensions.k8s.io", version: "v1", kind: "CustomResourceDefinition",
            plural: "customresourcedefinitions", scope: ResourceScope::Cluster,
        };
        &G
    }
    fn aliases(&self) -> &[&str] { &["crd", "crds", "customresourcedefinition", "customresourcedefinitions"] }
    fn short_label(&self) -> &str { "CRDs" }
    fn default_headers(&self) -> Vec<String> {
        ["NAME", "GROUP", "VERSION", "KIND", "SCOPE", "AGE"]
            .into_iter().map(String::from).collect()
    }
}

impl ConvertToRow<CustomResourceDefinition> for CrdDef {
    fn convert(crd: CustomResourceDefinition) -> ResourceRow {
        let meta = CommonMeta::from_k8s(crd.metadata);
        let spec = crd.spec;
        let version = spec
            .versions
            .first()
            .map(|v| v.name.clone())
            .unwrap_or_default();
        let scope = ResourceScope::from_k8s_spec(&spec.scope);

        // Single typed CrdRef shared between crd_info and the BrowseCrd drill
        // target -- was three field-by-field copies of the same shape before.
        let gvr = CrdRef::new(spec.group, version, spec.names.kind, spec.names.plural, scope);
        let scope_label = scope.k8s_label();

        ResourceRow {
            cells: vec![
                meta.name.clone(),
                gvr.group.clone(),
                gvr.version.clone(),
                gvr.kind.clone(),
                scope_label.to_string(),
                crate::util::format_age(meta.age),
            ],
            name: meta.name,
            namespace: None,
            crd_info: Some(gvr.clone()),
            drill_target: Some(DrillTarget::BrowseCrd(gvr)),
            ..Default::default()
        }
    }
}
