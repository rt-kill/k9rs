use super::*;
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, PodCondition, PodSpec, PodStatus, ServicePort,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelectorRequirement;

fn ready_pod(name: &str) -> Pod {
    Pod {
        metadata: kube::api::ObjectMeta {
            name: Some(name.to_string()),
            ..Default::default()
        },
        status: Some(PodStatus {
            phase: Some("Running".into()),
            conditions: Some(vec![PodCondition {
                type_: "Ready".into(),
                status: "True".into(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn parse_target_splits_plural_and_bare_is_pod() {
    assert_eq!(parse_target("services/nginx"), ("services", "nginx"));
    assert_eq!(parse_target("pods/foo"), ("pods", "foo"));
    assert_eq!(parse_target("bare-name"), ("pods", "bare-name"));
}

#[test]
fn selector_string_covers_labels_and_expressions() {
    let sel = LabelSelector {
        match_labels: Some([("app".to_string(), "web".to_string())].into()),
        match_expressions: Some(vec![
            LabelSelectorRequirement {
                key: "tier".into(),
                operator: "In".into(),
                values: Some(vec!["a".into(), "b".into()]),
            },
            LabelSelectorRequirement {
                key: "canary".into(),
                operator: "DoesNotExist".into(),
                values: None,
            },
        ]),
    };
    assert_eq!(
        selector_to_string(&sel).unwrap(),
        "app=web,tier in (a,b),!canary",
    );
    assert!(selector_to_string(&LabelSelector::default()).is_err(), "empty = error");
}

#[test]
fn service_port_maps_int_named_and_default() {
    let mut pod = ready_pod("p");
    pod.spec = Some(PodSpec {
        containers: vec![Container {
            name: "c".into(),
            ports: Some(vec![ContainerPort {
                name: Some("http".into()),
                container_port: 8080,
                ..Default::default()
            }]),
            ..Default::default()
        }],
        ..Default::default()
    });
    let ports = vec![
        ServicePort { port: 80, target_port: Some(IntOrString::String("http".into())), ..Default::default() },
        ServicePort { port: 443, target_port: Some(IntOrString::Int(8443)), ..Default::default() },
        ServicePort { port: 9090, target_port: None, ..Default::default() },
    ];
    // Named targetPort resolves against the pod's container ports.
    assert!(matches!(service_target_port(&ports, 80, &pod), Ok(8080)));
    // Numeric targetPort passes through.
    assert!(matches!(service_target_port(&ports, 443, &pod), Ok(8443)));
    // Absent targetPort defaults to the service port.
    assert!(matches!(service_target_port(&ports, 9090, &pod), Ok(9090)));
    // A port the service doesn't expose is a config error → Fatal.
    assert!(matches!(
        service_target_port(&ports, 5000, &pod),
        Err(ResolveError::Fatal(_)),
    ));
    // Named port missing on the pod is rollout-healable → Retry.
    let bare = ready_pod("q");
    assert!(matches!(
        service_target_port(&ports, 80, &bare),
        Err(ResolveError::Retry(_)),
    ));
}

#[test]
fn pick_ready_pod_skips_terminating_and_unready() {
    let mut terminating = ready_pod("dying");
    terminating.metadata.deletion_timestamp =
        Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::Time(
            chrono::Utc::now(),
        ));
    let mut unready = ready_pod("pending");
    unready.status.as_mut().unwrap().conditions = Some(vec![PodCondition {
        type_: "Ready".into(),
        status: "False".into(),
        ..Default::default()
    }]);
    let good = ready_pod("good");
    let picked = pick_ready_pod(vec![terminating, unready, good]).expect("one qualifies");
    assert_eq!(picked.metadata.name.as_deref(), Some("good"));
    assert!(pick_ready_pod(vec![]).is_none());
}
