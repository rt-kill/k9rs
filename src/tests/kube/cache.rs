use super::*;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceColumnDefinition, CustomResourceDefinition, CustomResourceDefinitionNames,
    CustomResourceDefinitionSpec, CustomResourceDefinitionVersion,
};

fn crd(name: &str, served_cols: Vec<(&str, &str, &str)>) -> CustomResourceDefinition {
    CustomResourceDefinition {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(name.to_string()),
            ..Default::default()
        },
        spec: CustomResourceDefinitionSpec {
            group: "karpenter.sh".into(),
            names: CustomResourceDefinitionNames {
                kind: "NodeClaim".into(),
                plural: "nodeclaims".into(),
                ..Default::default()
            },
            scope: "Cluster".into(),
            versions: vec![
                // An UNSERVED older version with different columns — the
                // parser must pick the SERVED one.
                CustomResourceDefinitionVersion {
                    name: "v1beta1".into(),
                    served: false,
                    storage: false,
                    additional_printer_columns: Some(vec![CustomResourceColumnDefinition {
                        name: "OLD".into(),
                        json_path: ".spec.old".into(),
                        type_: "string".into(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                },
                CustomResourceDefinitionVersion {
                    name: "v1".into(),
                    served: true,
                    storage: true,
                    additional_printer_columns: Some(
                        served_cols
                            .iter()
                            .map(|(n, p, t)| CustomResourceColumnDefinition {
                                name: (*n).to_string(),
                                json_path: (*p).to_string(),
                                type_: (*t).to_string(),
                                ..Default::default()
                            })
                            .collect(),
                    ),
                    ..Default::default()
                },
            ],
            ..Default::default()
        },
        status: None,
    }
}

#[test]
fn from_k8s_reads_the_served_versions_recipe() {
    let parsed = CachedCrd::from_k8s(&crd(
        "nodeclaims.karpenter.sh",
        vec![("TYPE", ".spec.type", "string"), ("READY", ".status.ready", "string")],
    ))
    .expect("named CRD parses");
    assert_eq!(parsed.name, "nodeclaims.karpenter.sh");
    assert_eq!(parsed.gvr.version, "v1", "served version wins");
    assert_eq!(parsed.gvr.plural, "nodeclaims");
    assert_eq!(parsed.gvr.scope, ResourceScope::Cluster);
    let names: Vec<&str> = parsed.printer_columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["TYPE", "READY"], "served columns, not the unserved v1beta1 set");
}

#[test]
fn from_k8s_accepts_a_recipe_free_crd() {
    // CRDs may declare no printer columns — that is knowledge, and it
    // parses to an EMPTY recipe (metadata columns), not a failure.
    let parsed = CachedCrd::from_k8s(&crd("nodeclaims.karpenter.sh", vec![])).unwrap();
    assert!(parsed.printer_columns.is_empty());
}

#[test]
fn merge_crd_replaces_or_appends_atomically() {
    let cache = DiscoveryCache::new();
    let ctx = ContextId::new(ContextName::new("ctx-a").unwrap(), "https://test.example".into(), 1);

    // Merge into an EMPTY cache (the cold-subscribe path).
    let first = CachedCrd::from_k8s(&crd(
        "nodeclaims.karpenter.sh",
        vec![("TYPE", ".spec.type", "string")],
    ))
    .unwrap();
    cache.merge_crd(ctx.clone(), first);
    assert_eq!(
        cache
            .printer_columns_for(&ctx, "karpenter.sh", "nodeclaims")
            .unwrap()
            .len(),
        1
    );

    // A reader holding the old snapshot keeps it alive across a merge.
    let old_snapshot = cache.crds(&ctx).unwrap();

    // Merging the same (group, plural) REPLACES, not duplicates.
    let updated = CachedCrd::from_k8s(&crd(
        "nodeclaims.karpenter.sh",
        vec![("TYPE", ".spec.type", "string"), ("READY", ".status.ready", "string")],
    ))
    .unwrap();
    cache.merge_crd(ctx.clone(), updated);
    let crds = cache.crds(&ctx).unwrap();
    assert_eq!(crds.len(), 1);
    assert_eq!(crds[0].printer_columns.len(), 2);
    // The pre-merge snapshot is untouched (atomic swap semantics).
    assert_eq!(old_snapshot[0].printer_columns.len(), 1);
}
