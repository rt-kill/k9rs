use super::*;
use crate::row::{CellValue, ResourceRow, RowHealth};

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
        headers: vec!["NAMESPACE".into(), "NAME".into()],
        rows: vec![row],
    };
    let bytes = bincode::serialize(&baseline).unwrap();
    let decoded: TableBaseline = bincode::deserialize(&bytes).unwrap();
    assert_eq!(decoded.headers.len(), 2);
    assert_eq!(decoded.rows.len(), 1);
}

#[test]
fn test_stream_event_baseline_delta_roundtrip() {
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
/// a protocol break — bump WIRE_FORMAT_REVISION in the same change.
/// Appending variants is safe; reordering or mid-enum inserts are not.
/// COMPLETE pins for both envelopes: partial head-pins would let a
/// swap of two adjacent UNPINNED variants (DescribeResult↔YamlResult)
/// sail through while breaking the wire.
#[test]
fn envelope_wire_tags_are_stable() {
    let obj = ObjectRef {
        resource: ResourceId::BuiltIn(crate::resource_def::BuiltInKind::Pod),
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
            caches: Default::default(),
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
        resource: ResourceId::BuiltIn(crate::resource_def::BuiltInKind::Pod),
        namespace: Namespace::All,
        filter: None,
        force: false,
    });
    assert_eq!(&bincode::serialize(&sub).unwrap()[..4], 0u32.to_le_bytes());
    let log_sub = SubstreamInit::Log(LogInit {
        target: LogTarget::one(""),
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
        resource: ResourceId::BuiltIn(crate::resource_def::BuiltInKind::Pod),
        namespace: Namespace::All,
        name: String::new(),
    };
    let stream_events: Vec<StreamEvent> = vec![
        StreamEvent::Baseline(TableBaseline { headers: vec![], rows: vec![] }),
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
    let rid = ResourceId::BuiltIn(crate::resource_def::BuiltInKind::Pod);
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
///
/// The revision assertion is the gate: no wire shape changes without this
/// test being edited, and editing it is what makes the author say which
/// change it was. v19 removed `TableBaseline.resource` — a `ResourceId`
/// every producer wrote and no consumer read, which could only ever
/// disagree with the subscription that routed the baseline, and did on the
/// legacy exec-source path. v20 appended `DrillTarget::HelpSection`, so `?`
/// can list its sections and Enter can drill into one.
#[test]
fn stream_wire_tags_are_stable() {
    assert_eq!(WIRE_FORMAT_REVISION, 20);
    let b = bincode::serialize(&StreamEvent::Error("x".into())).unwrap();
    assert_eq!(&b[..4], 2u32.to_le_bytes()); // Baseline=0, Delta=1, Error=2
    // Stale/Live APPENDED for v11 — Resolved=3 keeps its tag.
    assert_eq!(&bincode::serialize(&StreamEvent::Stale("x".into())).unwrap()[..4], 4u32.to_le_bytes());
    assert_eq!(&bincode::serialize(&StreamEvent::Live).unwrap()[..4], 5u32.to_le_bytes());
    let r = bincode::serialize(&RowChange::Remove(ObjectKey::new(String::new(), String::new()))).unwrap();
    assert_eq!(&r[..4], 1u32.to_le_bytes()); // Upsert=0, Remove=1
    // LogTarget: `Leaf=0` is what every single-pod call site produces, so a
    // reorder would silently turn "this pod" into "these pods". (v14 replaced
    // `Many` with `Union` in place; v17 moved the shapes down into `LogLeaf`
    // to kill the recursion — both are tag changes, which is exactly what the
    // build gate exists for.)
    assert_eq!(
        &bincode::serialize(&LogTarget::Leaf(LogLeaf::One(String::new()))).unwrap()[..4],
        0u32.to_le_bytes(),
    );
    assert_eq!(
        &bincode::serialize(&LogTarget::Union { targets: vec![], label: String::new() })
            .unwrap()[..4],
        1u32.to_le_bytes(),
    );
    // And the leaf's own tags, which ride inside both of the above.
    assert_eq!(&bincode::serialize(&LogLeaf::One(String::new())).unwrap()[..4], 0u32.to_le_bytes());
    assert_eq!(
        &bincode::serialize(&LogLeaf::Selector {
            labels: Default::default(),
            label: String::new(),
        }).unwrap()[..4],
        1u32.to_le_bytes(),
    );
    assert_eq!(
        &bincode::serialize(&LogLeaf::Owner { uid: String::new(), label: String::new() })
            .unwrap()[..4],
        2u32.to_le_bytes(),
    );
    // Handshake preamble: magic then `build_id`, BE. The magic is pinned
    // because it doubles as a poison frame-length for a peer that speaks no
    // preamble; the id deliberately is NOT pinned, because it is derived.
    assert_eq!(PROTOCOL_MAGIC.to_be_bytes(), [0x4B, 0x39, 0x52, 0x53]);
}

/// A log target decodes in BOUNDED STACK, whatever the payload says.
///
/// `LogTarget::Union` used to hold `Vec<LogTarget>`, so the derived
/// `Deserialize` recursed once per nesting level with nothing to stop it.
/// At ~20 bytes of wire per level against a 64 MiB message cap, **200 KB
/// from any client overflowed the stack and aborted the daemon process** —
/// taking every other session on that daemon with it, since one daemon
/// serves a whole context. Measured before the fix: depth 5,000 decoded,
/// depth 10,000 aborted on a 2 MiB stack.
///
/// The fix is the type, not a counter: `Union` holds [`LogLeaf`], which has
/// no union variant, so there is no deeper call to make. This feeds it the
/// OLD recursive encoding — exactly what a stale peer or a hostile one would
/// send — on a deliberately small stack, and the point is simply that the
/// call RETURNS. A regression here does not fail an assertion; it aborts the
/// test binary, which is the loudest signal available and the honest one.
#[test]
fn a_log_target_decodes_in_bounded_stack() {
    /// The pre-flattening encoding: `Union { targets: [ Union { .. } ] }`
    /// nested `depth` deep, hand-rolled so neither construction nor
    /// serialization (both also recursive) can be what dies.
    fn old_nested_bytes(depth: usize) -> Vec<u8> {
        let mut b = Vec::new();
        for _ in 0..depth {
            b.extend_from_slice(&1u32.to_le_bytes()); // old tag: Union
            b.extend_from_slice(&1u64.to_le_bytes()); // targets: Vec len 1
        }
        b.extend_from_slice(&0u32.to_le_bytes()); // old tag: One
        b.extend_from_slice(&0u64.to_le_bytes()); // its String len 0
        for _ in 0..depth {
            b.extend_from_slice(&0u64.to_le_bytes()); // each Union's label
        }
        b
    }

    // 10,000 levels is the depth that aborted a 2 MiB stack before the fix;
    // 128 KiB here is a stack no recursive decode of it could survive.
    let bytes = old_nested_bytes(10_000);
    assert!(bytes.len() > 100_000, "the payload has to be the big one: {}", bytes.len());
    let decoded = std::thread::Builder::new()
        .stack_size(128 * 1024)
        .spawn(move || bincode::deserialize::<LogTarget>(&bytes).is_ok())
        .expect("spawn")
        .join()
        .expect("the decode must RETURN, not unwind and not abort");
    assert!(!decoded, "nested unions are no longer a shape the wire can express");
}

/// `ClusterIdentity` is total: nothing can construct a field that is present
/// but blank, so no reader has to know that a blank one means absent.
#[test]
fn a_blank_identity_field_is_absence_not_a_value() {
    use crate::protocol::ClusterIdentity;

    // Every field independently unknown is the honest state before the
    // handshake, and it is what `Default` means.
    let unknown = ClusterIdentity::default();
    assert_eq!(unknown.cluster(), None);
    assert_eq!(unknown.user(), None);
    assert_eq!(unknown.k8s_version(), None);

    // Blank collapses AT the constructor — the one boundary these cross.
    // kube-rs types `Context::user` as `Option<String>`, and the caller
    // `unwrap_or_default()`s it, so "" is precisely what a `user:`-less
    // kubeconfig entry looks like on the way in.
    let no_user = ClusterIdentity::new("prod-eks".into(), String::new());
    assert_eq!(no_user.cluster(), Some("prod-eks"));
    assert_eq!(no_user.user(), None, "a blank user is not a user named \"\"");

    // Same rule on the late-arriving field: an apiserver that gave us no
    // version did not give us a version called "".
    let mut id = ClusterIdentity::new("c".into(), "u".into());
    assert_eq!(id.k8s_version(), None, "unknown until the daemon asks");
    id.set_k8s_version(String::new());
    assert_eq!(id.k8s_version(), None);
    id.set_k8s_version("v1.29.4".into());
    assert_eq!(id.k8s_version(), Some("v1.29.4"));
}

/// Why v11→v12 is a genuine break even though no field was added, removed or
/// reordered: bincode writes `Option<T>` as a discriminant byte, and wrote
/// `String` as an 8-byte length. An all-unknown identity went from 24 bytes
/// of zero-length strings to 3 discriminant bytes — a v11 peer reading it
/// mis-frames everything that follows.
#[test]
fn the_identity_encoding_is_why_the_version_moved() {
    use crate::protocol::ClusterIdentity;
    let bytes = bincode::serialize(&ClusterIdentity::default()).unwrap();
    assert_eq!(bytes, vec![0u8, 0, 0], "three Nones, one byte each");
    assert_eq!(
        bincode::serialize(&ClusterIdentity::new("c".into(), String::new())).unwrap().len(),
        1 + 8 + 1 + 1 + 1,
        "Some(\"c\") = tag + u64 len + 1 byte; then None, None",
    );
}

/// The wire enums that carry no pin of their own. Grouped here because the
/// 2026-09-09 audit found nine of them — every one rides a hot path
/// (`ResourceId` is in every baseline; `Namespace` decides whether a delete
/// gets `-n`; an `ExecFrame` swap feeds a resize struct to a PTY as terminal
/// bytes) and a reorder would corrupt silently while every same-build test
/// stayed green.
#[test]
fn remaining_wire_enum_tags_are_stable() {
    use crate::protocol::*;
    use crate::row::{ContainerKind, ContainerState};

    let tag = |bytes: Vec<u8>| u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let rid = ResourceId::BuiltIn(crate::resource_def::BuiltInKind::Pod);

    // ResourceId: BuiltIn=0, Crd=1, CrdUnresolved=2, Local=3.
    assert_eq!(tag(bincode::serialize(&rid).unwrap()), 0);
    assert_eq!(
        tag(bincode::serialize(&ResourceId::Local(
            crate::local_types::LocalResourceKind::PortForward
        )).unwrap()),
        3,
    );

    // Namespace: All=0, Named=1. A swap silently drops `-n` from mutations.
    assert_eq!(tag(bincode::serialize(&Namespace::All).unwrap()), 0);
    assert_eq!(tag(bincode::serialize(&Namespace::Named("x".into())).unwrap()), 1);

    // SubscriptionFilter: Labels=0, Field=1, OwnerUid=2, References=3.
    assert_eq!(
        tag(bincode::serialize(&SubscriptionFilter::Labels(Default::default())).unwrap()),
        0,
    );
    assert_eq!(tag(bincode::serialize(&SubscriptionFilter::Field("f".into())).unwrap()), 1);
    assert_eq!(tag(bincode::serialize(&SubscriptionFilter::OwnerUid("u".into())).unwrap()), 2);
    assert_eq!(
        tag(bincode::serialize(&SubscriptionFilter::References {
            kind: crate::resource_def::BuiltInKind::Secret,
            name: "s".into(),
        })
        .unwrap()),
        3,
    );

    // K8sFieldSelector rides inside DrillTarget::PodsByField, so its tags are
    // wire-frozen too. `InvolvedObject` was APPENDED for the `E` drill.
    {
        use crate::row::K8sFieldSelector as F;
        assert_eq!(tag(bincode::serialize(&F::MetadataName("n".into())).unwrap()), 0);
        assert_eq!(tag(bincode::serialize(&F::SpecNodeName("n".into())).unwrap()), 1);
        assert_eq!(tag(bincode::serialize(&F::StatusPhase("p".into())).unwrap()), 2);
        assert_eq!(
            tag(bincode::serialize(&F::InvolvedObject {
                kind: "Pod".into(),
                name: "n".into(),
                namespace: String::new(),
            })
            .unwrap()),
            3,
        );
    }

    // ExecFrame: Data=0, Resize=1. A swap feeds a resize into the PTY.
    assert_eq!(tag(bincode::serialize(&ExecFrame::Data(vec![])).unwrap()), 0);
    assert_eq!(
        tag(bincode::serialize(&ExecFrame::Resize { width: 1, height: 1 }).unwrap()),
        1,
    );

    // LogContainer: All=0, Named=1, Default=2, Set=3 (APPENDED in v14 —
    // inserting it before `Default` would have moved every existing tag).
    assert_eq!(tag(bincode::serialize(&LogContainer::All).unwrap()), 0);
    assert_eq!(tag(bincode::serialize(&LogContainer::Named("c".into())).unwrap()), 1);
    assert_eq!(tag(bincode::serialize(&LogContainer::Default).unwrap()), 2);
    assert_eq!(tag(bincode::serialize(&LogContainer::Set(vec![])).unwrap()), 3);

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

    // LocalResourceKind is NOT pinned here. It was — one variant of it,
    // `Context`, because the dedicated pin used to stop short of the
    // appended variants. That pin covers every variant and asserts the
    // COUNT now, so a second copy here could only ever be the one that
    // disagreed: removing `ExecResource` for v19 shifted the tags and this
    // line failed while the authoritative test passed. See
    // `local_resource_kind_wire_tags_are_stable` in `tests/local_types.rs`.

    // DrillTarget is NOT pinned here. It was — `SwitchContext` alone — and
    // that is the second-copy shape: `drill_wire_tags_are_stable` in
    // `tests/row.rs` pins every tag of it, so a copy here could only ever be
    // the one left behind. (It was: appending `HelpSection` for v20 touched
    // the dedicated test and this one would have sat unchanged.)
}

/// `BuiltInKind` is a WIRE enum (rides inside `ResourceId::BuiltIn` and
/// `DrillTarget` on every baseline) whose declaration is organized under
/// category comments — which invites inserting a new kind IN-category.
/// That silently re-tags every later kind while all same-build tests stay
/// green. This pin makes the mistake loud: new kinds append at the END
/// (or you bump WIRE_FORMAT_REVISION as a conscious break).
#[test]
fn builtinkind_tags_are_stable() {
    use crate::resource_def::BuiltInKind as K;
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
        crate::resource_def::all_defs().count(),
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
    // The other half of this test — that the client's `From<&OperationKind>`
    // maps a Custom op to `Action::OverlayCapability` — moved to
    // `src/tests/app/actions.rs`. It asserted a CLIENT contract from the wire
    // crate's own mirror, so keeping it here would make `cargo test -p
    // k9rs_proto` build the entire TUI.
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
    let rid = ResourceId::Local(crate::local_types::LocalResourceKind::Custom("my-resource".into()));
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

// ---------------------------------------------------------------------------
// The handshake refuses a different build, and nobody maintains a number
// ---------------------------------------------------------------------------

/// `build_id` is an identity, so it has to be the same every time this
/// process asks. It is cached in a `OnceLock` precisely so a binary replaced
/// mid-run cannot make one connection disagree with the next.
#[test]
fn build_id_is_stable_within_a_process() {
    let a = build_id();
    assert_eq!(a, build_id());
    assert_eq!(a, build_id(), "cached, not re-stat'ed per call");
}

/// Two ends of the SAME build accept each other. This is the case that must
/// not regress: the identity is derived, so if the derivation were unstable
/// (hashing something that varies per process, say) every connection would
/// fail and the TUI would never reach a daemon at all.
#[tokio::test]
async fn a_matching_build_completes_the_handshake() {
    let (mut a, mut b) = tokio::io::duplex(64);
    write_handshake(&mut a).await.expect("write");
    read_handshake(&mut b).await.expect("same build must be accepted");
}

/// A peer that speaks no preamble is distinguished from one that is merely a
/// different build — the two need different advice, and the magic is what
/// tells them apart. (It also doubles as a poison frame-length: a peer that
/// reads these four bytes as a length sees ~1.26 GB and bails.)
#[tokio::test]
async fn a_peer_with_no_preamble_is_named_as_such() {
    use tokio::io::AsyncWriteExt;
    let (mut a, mut b) = tokio::io::duplex(64);
    a.write_all(&[0u8; 12]).await.expect("write");
    let err = read_handshake(&mut b).await.expect_err("must be refused");
    let msg = err.to_string();
    assert!(msg.contains("no protocol handshake"), "got: {msg}");
}

/// THE point of the change: a stale daemon is refused, and the error says
/// what to do rather than naming two numbers.
///
/// This used to be a hand-maintained `PROTOCOL_VERSION` you had to remember
/// to bump — and forgetting was SILENT, because bincode is positional, so an
/// old peer decodes new bytes into the wrong variant rather than erroring.
#[tokio::test]
async fn a_different_build_is_refused() {
    use tokio::io::AsyncWriteExt;
    let (mut a, mut b) = tokio::io::duplex(64);
    a.write_all(&PROTOCOL_MAGIC.to_be_bytes()).await.expect("magic");
    // Any id but ours. `wrapping_add(1)` rather than a constant, so this
    // cannot accidentally equal a real build id.
    a.write_all(&build_id().wrapping_add(1).to_be_bytes()).await.expect("id");
    let err = read_handshake(&mut b).await.expect_err("must be refused");
    let msg = err.to_string();
    assert!(msg.contains("different build"), "got: {msg}");
    assert!(
        !msg.contains("version"),
        "the advice must not talk about versions — there is no ordering \
         between two build ids: {msg}",
    );
}

/// A namespace selection that names nothing is refused at DECODE.
///
/// Both constructors map an empty string to `All`, so `Named("")` was
/// unreachable from inside the process — and reachable from the wire, where
/// the derived `Deserialize` took it. That mattered because the daemon's
/// mutating guards refuse `Namespace::All`: `Named("")` is not `All`, so it
/// passed the guard, and `as_option()` then answered `Some("")`, which goes
/// into the API request path as the namespace.
///
/// The valid encoding of "every namespace" is the `All` variant, and there
/// is exactly one of it.
#[test]
fn an_empty_named_namespace_is_refused_on_the_wire() {
    // Round-trips that must keep working, byte-for-byte: the wire form has
    // the same variants in the same order, so the tags cannot have moved.
    for ns in [Namespace::All, Namespace::Named("kube-system".to_string())] {
        let bytes = bincode::serialize(&ns).expect("encode");
        let back: Namespace = bincode::deserialize(&bytes).expect("decode");
        assert_eq!(back, ns, "round trip");
    }
    assert_eq!(
        &bincode::serialize(&Namespace::All).unwrap()[..4],
        0u32.to_le_bytes(),
        "All stays tag 0",
    );
    assert_eq!(
        &bincode::serialize(&Namespace::Named(String::new())).unwrap()[..4],
        1u32.to_le_bytes(),
        "Named stays tag 1",
    );

    // And the value that used to slip through now does not. Encoded through
    // `Namespace` itself, so this is exactly the byte sequence a peer sends.
    let hostile = bincode::serialize(&Namespace::Named(String::new())).expect("encode");
    let err = bincode::deserialize::<Namespace>(&hostile)
        .expect_err("an empty namespace name is not a namespace");
    let msg = err.to_string();
    assert!(
        msg.contains("names nothing"),
        "the refusal says what is wrong: {msg}",
    );

    // The guard it was defeating: `All` is refused by a mutating handler,
    // and `Named("")` is not `All`.
    assert!(Namespace::All.is_all());
    assert!(!Namespace::Named(String::new()).is_all(), "which is the whole problem");
    assert_eq!(Namespace::from_row(""), Namespace::All, "the constructor's rule");
    assert_eq!(Namespace::from_user_command(""), Namespace::All);
}

// ---------------------------------------------------------------------------
// What a command may say about itself in a log
// ---------------------------------------------------------------------------

fn secret_ref() -> ObjectRef {
    ObjectRef::new(
        ResourceId::BuiltIn(crate::resource_def::BuiltInKind::Secret),
        "db-creds",
        Namespace::Named("prod".into()),
    )
}

/// `Init` carries the session's whole kubeconfig and env. Neither may reach
/// a log line, and `Init` has no subject to name either — so the summary is
/// the bare kind.
#[test]
fn an_init_summary_carries_neither_kubeconfig_nor_env() {
    let cmd = SessionCommand::Init {
        context: None,
        namespace: Namespace::All,
        readonly: false,
        kubeconfig_yaml: "client-key-data: LS0tLS1CRUdJTi1QUklWQVRFLUtFWS0t".to_string(),
        env_vars: HashMap::from([("AWS_SESSION_TOKEN".to_string(), "FwoGZXIvYXdz".to_string())]),
        identity: ClusterIdentity::default(),
    };
    let said = cmd.log_summary();
    assert_eq!(said, "Init");
    assert!(!said.contains("client-key-data"), "the kubeconfig must not be loggable");
    assert!(!said.contains("FwoGZXIvYXdz"), "nor a forwarded credential");
}

/// `Apply` on a Secret carries that Secret's DECODED yaml. The subject is
/// loggable and worth logging; the body is not.
#[test]
fn an_apply_summary_names_its_target_and_not_its_body() {
    let cmd = SessionCommand::Apply {
        target: secret_ref(),
        yaml: "stringData:\n  password: hunter2\n".to_string(),
    };
    let said = cmd.log_summary();
    assert_eq!(said, "Apply secrets/db-creds");
    assert!(!said.contains("hunter2"), "the applied body must not be loggable");
}

/// The harmless scalars ARE worth logging — a log that cannot say what the
/// command did to what is not worth keeping.
#[test]
fn a_summary_names_the_harmless_scalars() {
    let scale = SessionCommand::Scale {
        target: ObjectRef::new(
            ResourceId::BuiltIn(crate::resource_def::BuiltInKind::Deployment),
            "api",
            Namespace::Named("prod".into()),
        ),
        replicas: 3,
    };
    assert_eq!(scale.log_summary(), "Scale deployments/api to 3");

    let pf = SessionCommand::PortForward {
        target: ObjectRef::new(
            ResourceId::BuiltIn(crate::resource_def::BuiltInKind::Pod),
            "api-7d9f",
            Namespace::Named("prod".into()),
        ),
        local_port: 8080,
        container_port: 80,
    };
    assert_eq!(pf.log_summary(), "PortForward pods/api-7d9f 8080:80");
}

/// Every command names its kind, so no log line is anonymous.
#[test]
fn every_summary_starts_with_the_kind() {
    let target = secret_ref();
    let all = [
        SessionCommand::Describe(target.clone()),
        SessionCommand::Yaml(target.clone()),
        SessionCommand::Delete(target.clone()),
        SessionCommand::ForceKill(target.clone()),
        SessionCommand::Restart(target.clone()),
        SessionCommand::DecodeSecret(target.clone()),
        SessionCommand::TriggerCronJob(target.clone()),
        SessionCommand::ToggleSuspendCronJob(target.clone()),
        SessionCommand::GetDiscovery,
        SessionCommand::Ping,
        SessionCommand::Status,
        SessionCommand::Shutdown,
        SessionCommand::Clear { context: None },
    ];
    for cmd in all {
        let said = cmd.log_summary();
        assert!(
            said.starts_with(cmd.kind_name()),
            "{said:?} must begin with {:?}",
            cmd.kind_name(),
        );
    }
}

// ---------------------------------------------------------------------------
// Log windows: one declaration, two projections (v18)
// ---------------------------------------------------------------------------

/// kubectl's own duration grammar, written out HERE as the oracle. It is
/// what `--since` would have computed from a label back when the label was
/// what went on the wire — so this test says the labels the UI shows really
/// are the windows the daemon asks for.
fn kubectl_duration_seconds(s: &str) -> Option<u32> {
    for (suffix, mult) in [("s", 1u32), ("m", 60), ("h", 3600), ("d", 86400)] {
        if let Some(val) = s.strip_suffix(suffix) {
            return val.parse::<u32>().ok().map(|v| v * mult);
        }
    }
    None
}

/// Every window's label and its duration agree. They are written on one line
/// of the declaration precisely so this CAN'T drift, but the pair itself can
/// still be mistyped, and `30m => 300` would be a silent five-minute window
/// behind a label reading thirty.
#[test]
fn every_log_window_label_matches_its_seconds() {
    for &since in LogSince::ALL {
        assert_eq!(
            kubectl_duration_seconds(since.label()),
            Some(since.seconds()),
            "{:?}: the label {:?} is not {} seconds",
            since,
            since.label(),
            since.seconds(),
        );
    }
}

/// The windows are distinct and ordered — the number keys walk this list, so
/// a duplicate would be a key that does nothing new and a mis-ordering would
/// put `24h` on `3`.
#[test]
fn the_log_windows_ascend_without_repeats() {
    let secs: Vec<u32> = LogSince::ALL.iter().map(|s| s.seconds()).collect();
    assert!(
        secs.windows(2).all(|w| w[0] < w[1]),
        "the ladder must ascend: {secs:?}",
    );
    assert_eq!(secs, vec![60, 300, 900, 1800, 3600, 86400]);
}

/// The window survives the wire, and `None` still means "everything" —
/// the state a duration cannot express.
#[test]
fn a_log_window_round_trips_and_absence_still_means_everything() {
    for &since in LogSince::ALL {
        let init = LogInit {
            target: LogTarget::one("api-7d9f"),
            namespace: Namespace::Named("prod".into()),
            container: LogContainer::Default,
            follow: true,
            tail: None,
            since: Some(since),
            previous: false,
        };
        let bytes = bincode::serialize(&init).expect("serialize");
        let back: LogInit = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(back.since, Some(since));
    }

    let everything = LogInit {
        target: LogTarget::one("api-7d9f"),
        namespace: Namespace::All,
        container: LogContainer::Default,
        follow: false,
        tail: None,
        since: None,
        previous: false,
    };
    let back: LogInit =
        bincode::deserialize(&bincode::serialize(&everything).expect("serialize")).expect("de");
    assert_eq!(back.since, None);
    assert_eq!(back.tail, None, "both absent IS the whole history");
}

/// And the real list is what the generated one must be: sixteen operations,
/// `Custom` excluded, `Apply` last.
#[test]
fn the_real_built_in_list_is_the_generated_one() {
    assert_eq!(OperationKind::BUILT_IN.len(), 16);
    assert_eq!(OperationKind::BUILT_IN[0], OperationKind::Describe);
    assert_eq!(
        *OperationKind::BUILT_IN.last().expect("non-empty"),
        OperationKind::Apply,
    );
    assert!(
        !OperationKind::BUILT_IN
            .iter()
            .any(|op| matches!(op, OperationKind::Custom(_))),
        "an unbounded, overlay-declared operation has no place in a fixed list",
    );
}

/// An event names its variant and nothing else.
///
/// The twin of `SessionCommand::log_summary`'s rule: `DescribeResult`
/// carries describe lines, which for a `DecodeSecret` reply ARE the decoded
/// secret, and `YamlResult` carries a whole object body. `ctl status` had to
/// name an unexpected event and would have Debug-dumped it to the terminal.
#[test]
fn an_events_kind_name_carries_no_payload() {
    let target = secret_ref();
    let cases = [
        (
            SessionEvent::DescribeResult {
                target: target.clone(),
                lines: vec![DescribeLine::plain("password:  hunter2")],
            },
            "DescribeResult",
        ),
        (
            SessionEvent::YamlResult {
                target: target.clone(),
                content: "stringData:\n  password: hunter2\n".to_string(),
            },
            "YamlResult",
        ),
        (SessionEvent::SessionError("hunter2".to_string()), "SessionError"),
        (SessionEvent::CommandResult(Ok("hunter2".to_string())), "CommandResult"),
    ];
    for (event, expected) in cases {
        let named = event.kind_name();
        assert_eq!(named, expected);
        assert!(!named.contains("hunter2"), "a name is not a payload: {named}");
    }
}

/// Exactly one stream event does NOT reset the backoff, and it is the
/// terminal one.
///
/// This was five `retry_backoff = Duration::from_secs(2)` assignments in
/// five `match` arms, with `Error` distinguished only by the ABSENCE of a
/// sixth. A new variant therefore joined the minority policy — keep backing
/// off — silently.
#[test]
fn only_the_terminal_stream_event_keeps_the_backoff_growing() {
    let rid = ResourceId::BuiltIn(crate::resource_def::BuiltInKind::Pod);
    let progress = [
        StreamEvent::Baseline(TableBaseline {
            headers: vec![],
            rows: vec![],
        }),
        StreamEvent::Delta(TableDelta { changes: vec![] }),
        StreamEvent::Resolved { original: rid.clone(), resolved: rid.clone() },
        StreamEvent::Stale("watch died".into()),
        StreamEvent::Live,
    ];
    for event in progress {
        assert!(
            event.resets_backoff(),
            "{event:?} is a delivered frame, so the substream made progress",
        );
    }
    assert!(
        !StreamEvent::Error("rbac denied".into()).resets_backoff(),
        "Error is terminal — the next attempt is a real retry",
    );
}

/// Every role has a label and declares whether it shows — so the field
/// indicator is a projection of the roles, not a second list.
///
/// The renderer had a `Role::Body => "message"` arm that `SHED_ORDER` can
/// never reach, plus an `is_empty` fallback and a hardcoded `+message`:
/// three places encoding "the body is always shown", which is a fact about
/// `shown()`. `SHED_ORDER`'s own doc promises that adding a role "extends
/// the cycle — no new binding, no new boolean"; the indicator was the thing
/// that would also have needed changing.
#[test]
fn the_field_indicator_is_a_projection_of_the_roles() {
    use crate::protocol::Role;
    // Shed 0: everything shows, body last in display order.
    let shown = |shed: usize| -> Vec<&'static str> {
        Role::ALL.iter().filter(|r| r.shown(shed)).map(|r| r.label()).collect()
    };
    assert_eq!(shown(0), vec!["time", "source", "message"]);
    assert_eq!(shown(1), vec!["source", "message"], "the timestamp goes first");
    assert_eq!(shown(2), vec!["message"], "then the source tag");
    assert_eq!(
        shown(99),
        vec!["message"],
        "and the body is never shed — it is not in SHED_ORDER at all",
    );

    // `ALL` holds every role, including the ones `SHED_ORDER` does not.
    assert_eq!(Role::ALL.len(), 3);
    for role in Role::SHED_ORDER {
        assert!(Role::ALL.contains(&role), "{role:?} is sheddable but not listed");
    }
    assert!(!Role::SHED_ORDER.contains(&Role::Body));
}
