//! The Service converter's two questions that used to be asked twice: does
//! this service have an external address, and which of its ports can a
//! forward use.

use k8s_openapi::api::core::v1::{
    LoadBalancerIngress, LoadBalancerStatus, Service, ServicePort, ServiceSpec, ServiceStatus,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

use crate::resources::ConvertToRow;
use k9rs_proto::resource_def::services::ServiceDef;
use k9rs_proto::row::{CellValue, ResourceRow, RowHealth};

// Column indices from `ServiceDef::default_headers`.
const EXTERNAL_IP: usize = 4;
const PORTS: usize = 6;

fn service(spec: ServiceSpec, status: ServiceStatus) -> ResourceRow {
    ServiceDef::convert(Service {
        metadata: ObjectMeta {
            name: Some("web".to_string()),
            namespace: Some("default".to_string()),
            ..Default::default()
        },
        spec: Some(spec),
        status: Some(status),
    })
}

fn load_balancer(ingress: Vec<LoadBalancerIngress>) -> ResourceRow {
    service(
        ServiceSpec { type_: Some("LoadBalancer".to_string()), ..Default::default() },
        ServiceStatus {
            load_balancer: Some(LoadBalancerStatus { ingress: Some(ingress) }),
            ..Default::default()
        },
    )
}

fn external_ip(row: &ResourceRow) -> String {
    match &row.cells[EXTERNAL_IP] {
        CellValue::Text(t) => t.clone(),
        other => panic!("EXTERNAL-IP is a text cell, got {other:?}"),
    }
}

/// A LoadBalancer with nothing assigned yet is pending, and SAYS it has no
/// address rather than showing a blank cell.
#[test]
fn an_unprovisioned_load_balancer_is_pending_and_says_none() {
    let none = load_balancer(vec![]);
    assert_eq!(external_ip(&none), "<none>");
    assert_eq!(none.health, RowHealth::Pending);
}

/// An ingress entry that names nothing is not an address.
///
/// `"<none>"` used to be produced as the display string and then re-tested
/// *as a sentinel* to decide health — with a second `is_empty()` arm for the
/// case the sentinel had supposedly already covered. An ingress carrying
/// `ip: ""` reached that arm: health came out right, but the cell rendered
/// blank, which reads as "a value we failed to display" rather than "none".
#[test]
fn an_ingress_naming_nothing_is_not_an_address() {
    let blank = load_balancer(vec![LoadBalancerIngress {
        ip: Some(String::new()),
        ..Default::default()
    }]);
    assert_eq!(external_ip(&blank), "<none>", "not a blank cell");
    assert_eq!(blank.health, RowHealth::Pending);

    let nothing_at_all = load_balancer(vec![LoadBalancerIngress::default()]);
    assert_eq!(external_ip(&nothing_at_all), "<none>");
    assert_eq!(nothing_at_all.health, RowHealth::Pending);
}

/// A hostname is an address — AWS assigns those instead of an IP — and
/// `spec.externalIPs` contributes alongside the LoadBalancer's own.
#[test]
fn hostnames_and_spec_external_ips_both_count() {
    let by_hostname = load_balancer(vec![LoadBalancerIngress {
        hostname: Some("a.elb.amazonaws.com".to_string()),
        ..Default::default()
    }]);
    assert_eq!(external_ip(&by_hostname), "a.elb.amazonaws.com");
    assert_eq!(by_hostname.health, RowHealth::Normal, "it has an address");

    let both = service(
        ServiceSpec {
            type_: Some("LoadBalancer".to_string()),
            external_ips: Some(vec!["10.0.0.1".to_string()]),
            ..Default::default()
        },
        ServiceStatus {
            load_balancer: Some(LoadBalancerStatus {
                ingress: Some(vec![LoadBalancerIngress {
                    ip: Some("10.0.0.2".to_string()),
                    ..Default::default()
                }]),
            }),
            ..Default::default()
        },
    );
    assert_eq!(external_ip(&both), "10.0.0.1,10.0.0.2");
    assert_eq!(both.health, RowHealth::Normal);
}

/// A ClusterIP service with no external address is not pending — only a
/// LoadBalancer is waiting on one.
#[test]
fn a_clusterip_without_an_external_address_is_normal() {
    let row = service(ServiceSpec::default(), ServiceStatus::default());
    assert_eq!(external_ip(&row), "<none>");
    assert_eq!(row.health, RowHealth::Normal);
}

/// The PORT(S) cell shows every port; `pf_ports` offers only the ones a
/// forward can carry. Both read the same list.
#[test]
fn the_ports_cell_shows_all_and_the_forward_list_only_tcp() {
    let row = service(
        ServiceSpec {
            ports: Some(vec![
                ServicePort { port: 80, node_port: Some(31080), ..Default::default() },
                ServicePort {
                    port: 5353,
                    protocol: Some("UDP".to_string()),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        },
        ServiceStatus::default(),
    );
    assert_eq!(
        row.cells[PORTS],
        CellValue::Text("80:31080/TCP,5353/UDP".to_string()),
        "the cell is the whole truth, protocol and nodePort included",
    );
    assert_eq!(
        row.pf_ports,
        vec![80],
        "a UDP port cannot ride the websocket tunnel — offering it loops \
         `Reconnecting`",
    );
}
