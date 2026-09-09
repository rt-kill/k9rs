use super::*;
use crate::kube::resources::row::{CellValue, ResourceRow, RowHealth};

#[test]
fn test_resource_row_bincode_roundtrip() {
    let row = ResourceRow {
        cells: vec![
            CellValue::Text("default".into()),
            CellValue::Text("test-pod".into()),
            CellValue::Text("1/1".into()),
            CellValue::Text("Running".into()),
        ],
        name: "test-pod".into(),
        namespace: Some("default".into()),
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        health: RowHealth::Normal,
        crd_info: None,
        drill_target: None,
        ..Default::default()
    };
    let bytes = bincode::serialize(&row).unwrap();
    let decoded: ResourceRow = bincode::deserialize(&bytes).unwrap();
    assert_eq!(decoded.name, "test-pod");
    assert_eq!(decoded.cells.len(), 4);
    assert_eq!(decoded.cpu_request, None);
    assert_eq!(decoded.cpu_limit, None);
    assert_eq!(decoded.mem_request, None);
    assert_eq!(decoded.mem_limit, None);
}

/// Verify that resource-request/limit fields survive a bincode
/// roundtrip. Bincode is positional and ignores `#[serde(default)]`
/// / `skip_serializing_if` — these fields are always on the wire.
#[test]
fn test_resource_row_bincode_roundtrip_with_metrics_fields() {
    let row = ResourceRow {
        cells: vec![
            CellValue::Text("default".into()),
            CellValue::Text("busy-pod".into()),
        ],
        name: "busy-pod".into(),
        namespace: Some("default".into()),
        cpu_request: Some(500),
        cpu_limit: Some(1000),
        mem_request: Some(128 * 1024 * 1024),
        mem_limit: Some(256 * 1024 * 1024),
        ..Default::default()
    };
    let bytes = bincode::serialize(&row).unwrap();
    let decoded: ResourceRow = bincode::deserialize(&bytes).unwrap();
    assert_eq!(decoded.cpu_request, Some(500));
    assert_eq!(decoded.cpu_limit, Some(1000));
    assert_eq!(decoded.mem_request, Some(128 * 1024 * 1024));
    assert_eq!(decoded.mem_limit, Some(256 * 1024 * 1024));
}

#[test]
fn test_resource_update_rows_bincode_roundtrip() {
    let rid = ResourceId::BuiltIn(BuiltInKind::Pod);
    let row = ResourceRow {
        cells: vec![
            CellValue::Text("default".into()),
            CellValue::Text("test".into()),
        ],
        name: "test".into(),
        namespace: Some("default".into()),
        containers: Vec::new(),
        owner_refs: Vec::new(),
        pf_ports: Vec::new(),
        node: None,
        health: RowHealth::Normal,
        crd_info: None,
        drill_target: None,
        ..Default::default()
    };
    let baseline = TableBaseline {
        resource: rid.clone(),
        headers: vec!["NAMESPACE".into(), "NAME".into()],
        rows: vec![row],
    };
    let bytes = bincode::serialize(&baseline).unwrap();
    let decoded: TableBaseline = bincode::deserialize(&bytes).unwrap();
    assert_eq!(decoded.resource, ResourceId::BuiltIn(BuiltInKind::Pod));
    assert_eq!(decoded.resource.plural(), "pods");
    assert_eq!(decoded.headers.len(), 2);
    assert_eq!(decoded.rows.len(), 1);
}

#[test]
fn test_stream_event_baseline_delta_roundtrip() {
    let rid = ResourceId::BuiltIn(BuiltInKind::Deployment);
    let row = ResourceRow {
        cells: vec![
            CellValue::Text("prod".into()),
            CellValue::Text("web".into()),
            CellValue::Text("3/3".into()),
        ],
        name: "web".into(),
        namespace: Some("prod".into()),
        ..Default::default()
    };
    let baseline = StreamEvent::Baseline(TableBaseline {
        resource: rid,
        headers: vec!["NAMESPACE".into(), "NAME".into(), "READY".into()],
        rows: vec![row.clone()],
    });
    let bytes = bincode::serialize(&baseline).unwrap();
    match bincode::deserialize::<StreamEvent>(&bytes).unwrap() {
        StreamEvent::Baseline(b) => assert_eq!(b.rows[0].name, "web"),
        _ => panic!("Wrong event type"),
    }
    let delta = StreamEvent::Delta(TableDelta {
        changes: vec![
            RowChange::Upsert(row),
            RowChange::Remove(ObjectKey::new("prod".to_string(), "old".to_string())),
        ],
    });
    let bytes = bincode::serialize(&delta).unwrap();
    match bincode::deserialize::<StreamEvent>(&bytes).unwrap() {
        StreamEvent::Delta(d) => {
            assert_eq!(d.changes.len(), 2);
            assert_eq!(d.changes[1].key(), ObjectKey::new("prod".to_string(), "old".to_string()));
        }
        _ => panic!("Wrong event type"),
    }
}

/// Envelope wire-tag stability (bincode u32 LE declaration-index) for
/// the enums the wire depends on. RULE: any change to these goldens is
/// a protocol break — bump PROTOCOL_VERSION in the same change.
/// Appending variants is safe; reordering or mid-enum inserts are not.
/// COMPLETE pins for both envelopes: partial head-pins would let a
/// swap of two adjacent UNPINNED variants (DescribeResult↔YamlResult)
/// sail through while breaking the wire.
#[test]
fn envelope_wire_tags_are_stable() {
    let obj = ObjectRef {
        resource: ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
        namespace: Namespace::All,
        name: String::new(),
    };

    // SessionCommand: complete, in declaration order. Init MUST stay
    // tag 0 — the daemon dispatches on the first frame.
    let init = SessionCommand::Init {
        context: None,
        namespace: Namespace::All,
        readonly: false,
        kubeconfig_yaml: String::new(),
        env_vars: std::collections::HashMap::new(),
        identity: ClusterIdentity::default(),
    };
    let commands: Vec<SessionCommand> = vec![
        init,
        SessionCommand::Describe(obj.clone()),
        SessionCommand::Yaml(obj.clone()),
        SessionCommand::Delete(obj.clone()),
        SessionCommand::ForceKill(obj.clone()),
        SessionCommand::Scale { target: obj.clone(), replicas: 0 },
        SessionCommand::Restart(obj.clone()),
        SessionCommand::GetDiscovery,
        SessionCommand::Apply { target: obj.clone(), yaml: String::new() },
        SessionCommand::DecodeSecret(obj.clone()),
        SessionCommand::TriggerCronJob(obj.clone()),
        SessionCommand::ToggleSuspendCronJob(obj.clone()),
        SessionCommand::PortForward { target: obj.clone(), local_port: 0, container_port: 0 },
        SessionCommand::Ping,
        SessionCommand::Status,
        SessionCommand::Shutdown,
        SessionCommand::Clear { context: None },
    ];
    for (i, cmd) in commands.iter().enumerate() {
        assert_eq!(
            &bincode::serialize(cmd).unwrap()[..4],
            (i as u32).to_le_bytes(),
            "SessionCommand tag drift at index {i}",
        );
    }

    // SessionEvent: complete, in declaration order.
    let events: Vec<SessionEvent> = vec![
        SessionEvent::Ready {
            context: ContextName::new("ctx").unwrap(),
            identity: ClusterIdentity::default(),
            namespaces: vec![],
        },
        SessionEvent::SessionError(String::new()),
        SessionEvent::DescribeResult { target: obj.clone(), lines: vec![] },
        SessionEvent::YamlResult { target: obj.clone(), content: String::new() },
        SessionEvent::CommandResult(Ok(String::new())),
        SessionEvent::Discovery {
            context: ContextName::new("ctx").unwrap(),
            namespaces: vec![],
            crds: vec![],
        },
        SessionEvent::PodMetrics(HashMap::new()),
        SessionEvent::NodeMetrics(HashMap::new()),
        SessionEvent::DaemonStatus(DaemonStatus {
            pid: 0,
            uptime_secs: 0,
            socket_path: String::new(),
        }),
        // v9 appendee — after DaemonStatus(8); appending keeps prior tags.
        // (v10 added the `op` field — a FIELD change, not a tag change.)
        SessionEvent::OpResult {
            op: OperationKind::Delete,
            target: obj.clone(),
            result: Ok(String::new()),
        },
    ];
    for (i, ev) in events.iter().enumerate() {
        assert_eq!(
            &bincode::serialize(ev).unwrap()[..4],
            (i as u32).to_le_bytes(),
            "SessionEvent tag drift at index {i}",
        );
    }

    // SubstreamInit: complete (3 variants) — a Log↔Exec swap would
    // otherwise route subscriptions to the wrong handler.
    let sub = SubstreamInit::Subscribe(SubscriptionInit {
        resource: ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
        namespace: Namespace::All,
        filter: None,
        force: false,
    });
    assert_eq!(&bincode::serialize(&sub).unwrap()[..4], 0u32.to_le_bytes());
    let log_sub = SubstreamInit::Log(LogInit {
        pod: String::new(),
        namespace: Namespace::All,
        container: LogContainer::Default,
        follow: false,
        tail: None,
        since: None,
        previous: false,
    });
    assert_eq!(&bincode::serialize(&log_sub).unwrap()[..4], 1u32.to_le_bytes());
    let exec_sub = SubstreamInit::Exec(ExecInit {
        kubectl_args: vec![],
        term_width: 0,
        term_height: 0,
    });
    assert_eq!(&bincode::serialize(&exec_sub).unwrap()[..4], 2u32.to_le_bytes());

    // StreamEvent: complete (6 variants) — the highest-traffic enum;
    // a Baseline↔Delta swap would corrupt every stream.
    let obj = ObjectRef {
        resource: ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
        namespace: Namespace::All,
        name: String::new(),
    };
    let stream_events: Vec<StreamEvent> = vec![
        StreamEvent::Baseline(TableBaseline {
            resource: obj.resource.clone(), headers: vec![], rows: vec![],
        }),
        StreamEvent::Delta(TableDelta { changes: vec![] }),
        StreamEvent::Error(String::new()),
        StreamEvent::Resolved { original: obj.resource.clone(), resolved: obj.resource.clone() },
        StreamEvent::Stale(String::new()),
        StreamEvent::Live,
    ];
    for (i, ev) in stream_events.iter().enumerate() {
        assert_eq!(
            &bincode::serialize(ev).unwrap()[..4],
            (i as u32).to_le_bytes(),
            "StreamEvent tag drift at index {i}",
        );
    }

    // OperationKind: complete, in declaration order — capabilities
    // ride the typed enum on both sides.
    use OperationKind::*;
    for (i, op) in [Describe, Yaml, Delete, Restart, Scale, StreamLogs,
                    PreviousLogs, PortForward, Shell, ShowNode, ForceKill,
                    NodeShell, DecodeSecret, TriggerCronJob,
                    ToggleSuspendCronJob, Custom(String::new()), Apply]
        .iter().enumerate()
    {
        assert_eq!(
            &bincode::serialize(op).unwrap()[..4],
            (i as u32).to_le_bytes(),
            "OperationKind tag drift at {op:?}",
        );
    }
}

/// `StreamEventRef` must serialize byte-identically to `StreamEvent`
/// for the variants it mirrors — it's used for zero-copy delta
/// serialization on the wire, so any tag/shape drift would silently
/// corrupt the stream.
#[test]
fn stream_event_ref_tags_match() {
    let baseline = TableBaseline {
        resource: ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod),
        headers: vec!["NAME".into()],
        rows: vec![],
    };
    assert_eq!(
        bincode::serialize(&StreamEvent::Baseline(baseline.clone())).unwrap(),
        bincode::serialize(&StreamEventRef::Baseline(&baseline)).unwrap(),
    );
    let delta = TableDelta {
        changes: vec![RowChange::Remove(ObjectKey::new(
            "ns".to_string(), "a".to_string(),
        ))],
    };
    assert_eq!(
        bincode::serialize(&StreamEvent::Delta(delta.clone())).unwrap(),
        bincode::serialize(&StreamEventRef::Delta(&delta)).unwrap(),
    );
    assert_eq!(
        bincode::serialize(&StreamEvent::Error("x".into())).unwrap(),
        bincode::serialize(&StreamEventRef::Error("x")).unwrap(),
    );
    let rid = ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod);
    assert_eq!(
        bincode::serialize(&StreamEvent::Resolved {
            original: rid.clone(), resolved: rid.clone(),
        }).unwrap(),
        bincode::serialize(&StreamEventRef::Resolved {
            original: &rid, resolved: &rid,
        }).unwrap(),
    );
    assert_eq!(
        bincode::serialize(&StreamEvent::Stale("watch died".into())).unwrap(),
        bincode::serialize(&StreamEventRef::Stale("watch died")).unwrap(),
    );
    assert_eq!(
        bincode::serialize(&StreamEvent::Live).unwrap(),
        bincode::serialize(&StreamEventRef::Live).unwrap(),
    );
}

/// Wire-tag stability for the stream enums (bincode u32 LE
/// declaration-index). Appending is safe; reorder/insert is a break.
#[test]
fn stream_wire_tags_are_stable() {
    assert_eq!(PROTOCOL_VERSION, 11);
    let b = bincode::serialize(&StreamEvent::Error("x".into())).unwrap();
    assert_eq!(&b[..4], 2u32.to_le_bytes()); // Baseline=0, Delta=1, Error=2
    // Stale/Live APPENDED for v11 — Resolved=3 keeps its tag.
    assert_eq!(&bincode::serialize(&StreamEvent::Stale("x".into())).unwrap()[..4], 4u32.to_le_bytes());
    assert_eq!(&bincode::serialize(&StreamEvent::Live).unwrap()[..4], 5u32.to_le_bytes());
    let r = bincode::serialize(&RowChange::Remove(ObjectKey::new(String::new(), String::new()))).unwrap();
    assert_eq!(&r[..4], 1u32.to_le_bytes()); // Upsert=0, Remove=1
    // Handshake preamble bytes are pinned: magic then version, BE.
    assert_eq!(PROTOCOL_MAGIC.to_be_bytes(), [0x4B, 0x39, 0x52, 0x53]);
}

/// The wire enums that carry no pin of their own. Grouped here because the
/// 2026-09-09 audit found nine of them — every one rides a hot path
/// (`ResourceId` is in every baseline; `Namespace` decides whether a delete
/// gets `-n`; an `ExecFrame` swap feeds a resize struct to a PTY as terminal
/// bytes) and a reorder would corrupt silently while every same-build test
/// stayed green.
#[test]
fn remaining_wire_enum_tags_are_stable() {
    use crate::kube::protocol::*;
    use crate::kube::resources::row::{ContainerKind, ContainerState};

    let tag = |bytes: Vec<u8>| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let rid = ResourceId::BuiltIn(crate::kube::resource_def::BuiltInKind::Pod);

    // ResourceId: BuiltIn=0, Crd=1, CrdUnresolved=2, Local=3.
    assert_eq!(tag(bincode::serialize(&rid).unwrap()), 0);
    assert_eq!(
        tag(bincode::serialize(&ResourceId::Local(
            crate::kube::local::LocalResourceKind::PortForward
        )).unwrap()),
        3,
    );

    // Namespace: All=0, Named=1. A swap silently drops `-n` from mutations.
    assert_eq!(tag(bincode::serialize(&Namespace::All).unwrap()), 0);
    assert_eq!(tag(bincode::serialize(&Namespace::Named("x".into())).unwrap()), 1);

    // SubscriptionFilter: Labels=0, Field=1, OwnerUid=2.
    assert_eq!(
        tag(bincode::serialize(&SubscriptionFilter::Labels(Default::default())).unwrap()),
        0,
    );
    assert_eq!(tag(bincode::serialize(&SubscriptionFilter::Field("f".into())).unwrap()), 1);
    assert_eq!(tag(bincode::serialize(&SubscriptionFilter::OwnerUid("u".into())).unwrap()), 2);

    // ExecFrame: Data=0, Resize=1. A swap feeds a resize into the PTY.
    assert_eq!(tag(bincode::serialize(&ExecFrame::Data(vec![])).unwrap()), 0);
    assert_eq!(
        tag(bincode::serialize(&ExecFrame::Resize { width: 1, height: 1 }).unwrap()),
        1,
    );

    // LogContainer: All=0, Named=1, Default=2.
    assert_eq!(tag(bincode::serialize(&LogContainer::All).unwrap()), 0);
    assert_eq!(tag(bincode::serialize(&LogContainer::Named("c".into())).unwrap()), 1);
    assert_eq!(tag(bincode::serialize(&LogContainer::Default).unwrap()), 2);

    // ResourceScope: Cluster=0, Namespaced=1 (declaration order — NOT the
    // alphabetical/intuitive order; I guessed the other way writing this and
    // the pin corrected me, which is the point of having one).
    assert_eq!(tag(bincode::serialize(&ResourceScope::Cluster).unwrap()), 0);
    assert_eq!(tag(bincode::serialize(&ResourceScope::Namespaced).unwrap()), 1);

    // Per-pod-row enums (v10). Both are complete: `Unknown` is `#[default]`
    // but LAST, so a "move the default to the front" tidy-up is a break.
    assert_eq!(tag(bincode::serialize(&ContainerState::Running).unwrap()), 0);
    assert_eq!(tag(bincode::serialize(&ContainerState::Waiting).unwrap()), 1);
    assert_eq!(tag(bincode::serialize(&ContainerState::Terminated).unwrap()), 2);
    assert_eq!(tag(bincode::serialize(&ContainerState::Unknown).unwrap()), 3);
    assert_eq!(tag(bincode::serialize(&ContainerKind::Regular).unwrap()), 0);
    assert_eq!(tag(bincode::serialize(&ContainerKind::Init).unwrap()), 1);

    // LocalResourceKind: Context was APPENDED after Custom (tag 3), which the
    // pre-existing pin stops one short of.
    assert_eq!(
        tag(bincode::serialize(&crate::kube::local::LocalResourceKind::Context).unwrap()),
        3,
    );

    // DrillTarget: SwitchContext appended at 8.
    assert_eq!(
        tag(bincode::serialize(&crate::kube::resources::row::DrillTarget::SwitchContext(
            ContextName::new("c").unwrap()
        )).unwrap()),
        8,
    );
}

/// `BuiltInKind` is a WIRE enum (rides inside `ResourceId::BuiltIn` and
/// `DrillTarget` on every baseline) whose declaration is organized under
/// category comments — which invites inserting a new kind IN-category.
/// That silently re-tags every later kind while all same-build tests stay
/// green. This pin makes the mistake loud: new kinds append at the END
/// (or you bump PROTOCOL_VERSION as a conscious break).
#[test]
fn builtinkind_tags_are_stable() {
    use crate::kube::resource_def::BuiltInKind as K;
    let all = [
        K::Pod, K::Deployment, K::StatefulSet, K::DaemonSet, K::ReplicaSet,
        K::Job, K::CronJob,
        K::Service, K::ConfigMap, K::Secret, K::ServiceAccount, K::Ingress,
        K::NetworkPolicy, K::Hpa, K::Endpoints, K::EndpointSlice,
        K::LimitRange, K::ResourceQuota, K::PodDisruptionBudget, K::Event,
        K::PersistentVolumeClaim, K::Lease,
        K::Namespace, K::Node, K::PersistentVolume, K::StorageClass,
        K::PriorityClass, K::Role, K::ClusterRole, K::RoleBinding,
        K::ClusterRoleBinding, K::ValidatingWebhookConfiguration,
        K::MutatingWebhookConfiguration, K::CustomResourceDefinition,
    ];
    // COMPLETE pin: a partial head-pin would let two adjacent unpinned
    // kinds swap undetected.
    assert_eq!(
        all.len(),
        crate::kube::resource_defs::REGISTRY.all().count(),
        "a BuiltInKind exists that this pin does not cover — add it at the END of `all`",
    );
    for (i, kind) in all.iter().enumerate() {
        assert_eq!(
            bincode::serialize(kind).unwrap(),
            (i as u32).to_le_bytes(),
            "BuiltInKind tag drift at {kind:?} (expected tag {i})",
        );
    }
}

#[test]
fn test_subscription_init_bincode_roundtrip() {
    let rid = ResourceId::BuiltIn(BuiltInKind::Pod);
    let init = SubscriptionInit { resource: rid, namespace: Namespace::Named("default".into()), filter: None, force: false };
    let bytes = bincode::serialize(&init).unwrap();
    let decoded: SubscriptionInit = bincode::deserialize(&bytes).unwrap();
    assert_eq!(decoded.resource, ResourceId::BuiltIn(BuiltInKind::Pod));
    assert_eq!(decoded.resource.plural(), "pods");
}

#[test]
fn test_resource_id_crd_roundtrip() {
    let rid = ResourceId::crd(
        "clickhouse.altinity.com", "v1", "ClickHouseInstallation",
        "clickhouseinstallations", ResourceScope::Namespaced,
    );
    let bytes = bincode::serialize(&rid).unwrap();
    let decoded: ResourceId = bincode::deserialize(&bytes).unwrap();
    assert_eq!(decoded, rid);
    assert_eq!(decoded.plural(), "clickhouseinstallations");
    assert!(decoded.is_crd());
}

#[test]
fn test_resource_id_local_roundtrip() {
    let rid = ResourceId::Local(LocalResourceKind::PortForward);
    let bytes = bincode::serialize(&rid).unwrap();
    let decoded: ResourceId = bincode::deserialize(&bytes).unwrap();
    assert_eq!(decoded, rid);
    assert!(decoded.is_local());
    assert_eq!(decoded.plural(), "portforwards");
}

#[test]
fn test_resource_id_distinct_variants_never_equal() {
    let built_in = ResourceId::BuiltIn(BuiltInKind::Pod);
    let crd = ResourceId::crd("", "v1", "Pod", "pods", ResourceScope::Namespaced);
    // Even though the CRD's strings happen to match Pod's GVR, the
    // tagged variants are distinct types — they must never compare equal.
    assert_ne!(built_in, crd);
}

#[test]
fn all_builtin_ops_have_descriptors() {
    use super::OperationKind::*;
    let ops = [Describe, Yaml, Delete, Restart, Scale, StreamLogs,
               PreviousLogs, PortForward, Shell, ShowNode, ForceKill,
               NodeShell, DecodeSecret, TriggerCronJob, ToggleSuspendCronJob];
    for op in ops {
        assert!(!op.descriptor().label.is_empty(), "{:?} has no label", op);
    }
}

#[test]
fn custom_operation_descriptor() {
    let op = OperationKind::Custom("my-op".into());
    let desc = op.descriptor();
    assert_eq!(desc.default_key, None);
    // The client-side From mapping should produce OverlayCapability
    let action = crate::app::actions::Action::from(&op);
    assert!(matches!(action, crate::app::actions::Action::OverlayCapability(ref s) if s == "my-op"));
}

#[test]
fn custom_operation_has_no_schema_or_template() {
    let op = OperationKind::Custom("test".into());
    assert!(op.form_schema().is_none());
    assert!(op.exec_template().is_none());
}

#[test]
fn custom_operation_bincode_roundtrip() {
    let op = OperationKind::Custom("my-custom-op".into());
    let bytes = bincode::serialize(&op).unwrap();
    let decoded: OperationKind = bincode::deserialize(&bytes).unwrap();
    assert_eq!(decoded, op);
}

#[test]
fn custom_local_kind_bincode_roundtrip() {
    let rid = ResourceId::Local(crate::kube::local::LocalResourceKind::Custom("my-resource".into()));
    let bytes = bincode::serialize(&rid).unwrap();
    let decoded: ResourceId = bincode::deserialize(&bytes).unwrap();
    assert_eq!(decoded, rid);
}

// ---------------------------------------------------------------------------
// ContextName — empty is not a name, at every door
// ---------------------------------------------------------------------------

#[test]
fn an_empty_context_name_is_unconstructible() {
    // The bug this closes: `kubectl config unset current-context` writes
    // `current-context: ""` rather than removing the key, so kube-rs hands
    // back `Some("")` — which sailed past a `no current-context` guard and
    // reached the daemon as a request for a context that cannot exist.
    assert!(ContextName::new("").is_none());
    assert!(ContextName::new(String::new()).is_none());
    assert!(ContextName::try_from("").is_err());
    assert!(ContextName::try_from(String::new()).is_err());
    assert!("".parse::<ContextName>().is_err());

    let ok = ContextName::new("prod").expect("a real name");
    assert_eq!(ok.as_str(), "prod");
}

#[test]
fn the_wire_refuses_an_empty_context_name() {
    // The invariant has to hold for names arriving from OUTSIDE too — a
    // constructor-only check protects in-process construction and nothing
    // else, and the daemon builds its session from a deserialized Init.
    // Serialization stays transparent, so this is a bare String on the wire.
    let bytes = bincode::serialize(&String::new()).unwrap();
    assert!(
        bincode::deserialize::<ContextName>(&bytes).is_err(),
        "an empty name must fail to decode, not decode into an impossible value",
    );
    let bytes = bincode::serialize(&"prod".to_string()).unwrap();
    assert_eq!(
        bincode::deserialize::<ContextName>(&bytes).unwrap(),
        ContextName::new("prod").unwrap(),
    );
    // …and the encoding really is byte-identical to the bare String it
    // replaced, which is what makes `context: String` → `ContextName`
    // wire-compatible in the protocol types.
    assert_eq!(
        bincode::serialize(&ContextName::new("prod").unwrap()).unwrap(),
        bincode::serialize(&"prod".to_string()).unwrap(),
    );
}
