use crate::kube::resource_defs::REGISTRY;
use crate::kube::protocol::OperationKind;

/// Every resource must include the base operations (Describe, Yaml, Delete).
#[test]
fn all_resources_have_base_operations() {
    for def in REGISTRY.all() {
        let ops = def.operations();
        for base in [OperationKind::Describe, OperationKind::Yaml, OperationKind::Delete] {
            assert!(
                ops.contains(&base),
                "def `{}` missing base operation {:?}",
                def.gvr().kind, base,
            );
        }
    }
}

/// bincode encodes a fieldless enum as its u32 declaration-index (LE), so
/// these tags are the on-wire identity of every `ResourceId::BuiltIn`.
/// Appending a variant is safe; reordering or inserting one silently remaps
/// every existing row on the wire. `ordered` MUST be in declaration order —
/// the test asserts each variant serializes to its position, so a reorder
/// fails here and an append trips the count assert (extend the slice and
/// bump `PROTOCOL_VERSION`).
#[test]
fn builtin_kind_wire_tags_are_stable() {
    use super::BuiltInKind::*;
    let ordered = [
        Pod, Deployment, StatefulSet, DaemonSet, ReplicaSet, Job, CronJob,
        Service, ConfigMap, Secret, ServiceAccount, Ingress, NetworkPolicy,
        Hpa, Endpoints, EndpointSlice, LimitRange, ResourceQuota,
        PodDisruptionBudget, Event, PersistentVolumeClaim, Lease,
        Namespace, Node, PersistentVolume, StorageClass, PriorityClass,
        Role, ClusterRole, RoleBinding, ClusterRoleBinding,
        ValidatingWebhookConfiguration, MutatingWebhookConfiguration,
        CustomResourceDefinition,
    ];
    assert_eq!(ordered.len(), 34, "BuiltInKind variant count changed");
    for (i, kind) in ordered.iter().enumerate() {
        assert_eq!(
            bincode::serialize(kind).expect("serialize"),
            (i as u32).to_le_bytes(),
            "{:?}: wire tag drifted from {}", kind, i,
        );
    }
}
