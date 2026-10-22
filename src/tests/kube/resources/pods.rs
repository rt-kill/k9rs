use super::*;
use crate::kube::resource_def::ResourceDef;
use crate::kube::resources::ConvertToRow;

/// Pods are the highest-risk converter: 24 columns and a metrics overlay
/// that writes cells *by index* derived from `column_defs()`. Guard that
/// the `cells` vec aligns with the headers so a one-off drift can't silently
/// land a CPU value under the wrong column. (The headers↔column_defs half is
/// covered registry-wide in `resource_defs::registry::tests`.)
#[test]
fn pod_cells_align_with_headers() {
    let row = PodDef::convert(k8s_openapi::api::core::v1::Pod::default());
    assert_eq!(
        row.cells.len(),
        PodDef.default_headers().len(),
        "pod cells vec must align with default_headers",
    );
}

// ---------------------------------------------------------------------------
// Outward references — what `U` actually answers with (v15)
// ---------------------------------------------------------------------------

use crate::kube::resource_def::BuiltInKind;
use crate::kube::resources::row::ResourceRefInfo;

fn refs_of(yaml: &str) -> Vec<ResourceRefInfo> {
    let spec: k8s_openapi::api::core::v1::PodSpec =
        serde_yaml::from_str(yaml).expect("a well-formed PodSpec");
    super::pod_references(&spec)
}

/// The cases a name grep got wrong. A PVC is found by its claim name, not by
/// a pod that happens to be called something similar; a secret mounted by
/// `api-7d9f` was previously invisible because nothing about the two names
/// matches.
#[test]
fn a_pods_volumes_name_their_claims_and_secrets() {
    let refs = refs_of(
        r#"
containers:
  - name: app
    image: app
volumes:
  - name: data
    persistentVolumeClaim:
      claimName: data-postgres-0
  - name: creds
    secret:
      secretName: db-creds
  - name: cfg
    configMap:
      name: app-config
"#,
    );
    assert!(refs.contains(&ResourceRefInfo {
        kind: BuiltInKind::PersistentVolumeClaim,
        name: "data-postgres-0".into(),
    }));
    assert!(refs.contains(&ResourceRefInfo {
        kind: BuiltInKind::Secret,
        name: "db-creds".into(),
    }));
    assert!(refs.contains(&ResourceRefInfo {
        kind: BuiltInKind::ConfigMap,
        name: "app-config".into(),
    }));
}

/// Environment is the other half, and the one no naming convention could
/// ever have hinted at.
#[test]
fn environment_references_are_extracted_too() {
    let refs = refs_of(
        r#"
serviceAccountName: api-runner
imagePullSecrets:
  - name: registry-creds
containers:
  - name: app
    image: app
    envFrom:
      - secretRef:
          name: api-keys
      - configMapRef:
          name: feature-flags
    env:
      - name: TOKEN
        valueFrom:
          secretKeyRef:
            name: session-token
            key: t
"#,
    );
    for (kind, name) in [
        (BuiltInKind::ServiceAccount, "api-runner"),
        (BuiltInKind::Secret, "registry-creds"),
        (BuiltInKind::Secret, "api-keys"),
        (BuiltInKind::ConfigMap, "feature-flags"),
        (BuiltInKind::Secret, "session-token"),
    ] {
        assert!(
            refs.contains(&ResourceRefInfo { kind, name: name.into() }),
            "{name} missing from {refs:?}",
        );
    }
}

/// A projected volume is several sources wearing one mount, and init
/// containers reference things too.
#[test]
fn projected_sources_and_init_containers_count() {
    let refs = refs_of(
        r#"
initContainers:
  - name: setup
    image: setup
    envFrom:
      - configMapRef:
          name: bootstrap
containers:
  - name: app
    image: app
volumes:
  - name: bundle
    projected:
      sources:
        - secret:
            name: tls
        - configMap:
            name: ca
"#,
    );
    for (kind, name) in [
        (BuiltInKind::ConfigMap, "bootstrap"),
        (BuiltInKind::Secret, "tls"),
        (BuiltInKind::ConfigMap, "ca"),
    ] {
        assert!(
            refs.contains(&ResourceRefInfo { kind, name: name.into() }),
            "{name} missing from {refs:?}",
        );
    }
}

/// The same object named twice is one edge — a config mounted as a volume
/// AND read through `envFrom` shouldn't list the pod twice.
#[test]
fn a_reference_named_twice_is_recorded_once() {
    let refs = refs_of(
        r#"
containers:
  - name: app
    image: app
    envFrom:
      - configMapRef:
          name: shared
volumes:
  - name: v
    configMap:
      name: shared
"#,
    );
    assert_eq!(
        refs.iter()
            .filter(|r| r.name == "shared" && r.kind == BuiltInKind::ConfigMap)
            .count(),
        1,
    );
}

/// A pod that names nothing has no references — not a guess at one.
#[test]
fn a_pod_that_names_nothing_has_no_references() {
    assert!(refs_of("containers:\n  - name: app\n    image: app\n").is_empty());
}
