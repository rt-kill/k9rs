//! The CRD row and the CRD cache entry, which must agree.
//!
//! No mirror until 2026-10-27 — which is how two parsers of the same object
//! came to disagree about which version they report.

use super::*;
use crate::resources::ConvertToRow;

/// Both CRD paths report the SERVED version, for the same object.
///
/// They disagreed: the cache parser took the first `served: true` version,
/// the row converter took `versions.first()` whether served or not. For the
/// ordinary deprecation shape — `v1alpha1` kept first for storage while `v1`
/// is served — that produced two different rows for one CRD, and the
/// watcher's row carried a `DrillTarget::BrowseCrd` pointing at the unserved
/// version. Drilling into it asks the apiserver to watch something it does
/// not serve.
#[test]
fn both_crd_paths_report_the_served_version() {
    use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1 as apiext;

    // The shape that bit: the unserved version listed FIRST.
    let crd = apiext::CustomResourceDefinition {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some("widgets.example.com".to_string()),
            ..Default::default()
        },
        spec: apiext::CustomResourceDefinitionSpec {
            group: "example.com".to_string(),
            names: apiext::CustomResourceDefinitionNames {
                kind: "Widget".to_string(),
                plural: "widgets".to_string(),
                ..Default::default()
            },
            scope: "Namespaced".to_string(),
            versions: vec![
                apiext::CustomResourceDefinitionVersion {
                    name: "v1alpha1".to_string(),
                    served: false,
                    storage: true,
                    ..Default::default()
                },
                apiext::CustomResourceDefinitionVersion {
                    name: "v1".to_string(),
                    served: true,
                    storage: false,
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        status: None,
    };

    let cached = crate::cache::cached_crd_from_k8s(&crd).expect("it has a name");
    assert_eq!(cached.gvr.version, "v1", "the cache path takes the served one");

    let row = CrdDef::convert(crd);
    let drilled = match row.drill_target {
        Some(k9rs_proto::row::DrillTarget::BrowseCrd(ref r)) => r.version.clone(),
        other => panic!("a CRD row drills into the CRD: {other:?}"),
    };
    assert_eq!(
        drilled, "v1",
        "and so does the row — a drill into v1alpha1 is a watch the \
         apiserver refuses",
    );
    assert_eq!(
        row.crd_info.as_ref().map(|r| r.version.as_str()),
        Some("v1"),
        "crd_info and the drill target are the same CrdRef",
    );
}
