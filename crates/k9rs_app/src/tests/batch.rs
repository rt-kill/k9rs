//! Unit tests for `batch` (mirrored out of the source file).

// ---------------------------------------------------------------------------
// batch_tracker_tests (mirrored out of batch.rs)
// ---------------------------------------------------------------------------

use super::*;
use k9rs_proto::protocol::{Namespace, ResourceId};
use k9rs_proto::resource_def::BuiltInKind;

fn target(name: &str, ns: &str) -> ObjectRef {
    ObjectRef::new(
        ResourceId::BuiltIn(BuiltInKind::Pod),
        name.to_string(),
        Namespace::from_row(ns),
    )
}

/// key_of is the exact inverse of how batch ObjectRefs are built from
/// marked keys (Namespace::from_row): "" ↔ All round-trips.
#[test]
fn key_of_round_trips_the_marked_key() {
    let key = k9rs_proto::protocol::ObjectKey::new("ns1".to_string(), "a".to_string());
    let t = ObjectRef::new(
        ResourceId::BuiltIn(BuiltInKind::Pod),
        key.name.clone(),
        Namespace::from_row(&key.namespace),
    );
    assert_eq!(BatchTracker::key_of(&t), key);

    let cluster_key = k9rs_proto::protocol::ObjectKey::new(String::new(), "n1".to_string());
    let t = ObjectRef::new(
        ResourceId::BuiltIn(BuiltInKind::Node),
        cluster_key.name.clone(),
        Namespace::from_row(&cluster_key.namespace),
    );
    assert_eq!(BatchTracker::key_of(&t), cluster_key);
}

fn pod_rid() -> ResourceId {
    ResourceId::BuiltIn(BuiltInKind::Pod)
}

#[test]
fn consume_correlates_and_aggregates() {
    let targets = [target("a", "ns"), target("b", "ns"), target("c", "ns")];
    let mut tr = BatchTracker::new(
        "Deleted", "pod".to_string(), pod_rid(), &targets, 1, std::sync::Weak::new(),
        k9rs_proto::protocol::OperationKind::Delete,
    );
    assert!(!tr.is_done());

    // A result for a foreign target is NOT ours.
    assert!(!tr.consume(&k9rs_proto::protocol::OperationKind::Delete, &target("other", "ns"), &Ok("Deleted".into())));

    // A result for the RIGHT target but a DIFFERENT OPERATION is not
    // ours either — the op gate (v10) is what keeps a concurrent
    // edit-apply on a batch member from being consumed as the batch's
    // delete outcome (and vice versa).
    assert!(!tr.consume(&k9rs_proto::protocol::OperationKind::Apply, &targets[0], &Ok("Applied pod/a".into())));

    // A result for a DIFFERENT KIND with the same ns+name is NOT
    // ours either — the rid gate is what keeps a concurrent
    // single-op on a same-named object of another kind from being
    // misattributed to the batch.
    let foreign_kind = ObjectRef::new(
        ResourceId::BuiltIn(BuiltInKind::Deployment),
        "a".to_string(),
        Namespace::from_row("ns"),
    );
    assert!(!tr.consume(&k9rs_proto::protocol::OperationKind::Delete, &foreign_kind, &Ok("Deleted".into())));
    assert!(!tr.is_done());

    assert!(tr.consume(&k9rs_proto::protocol::OperationKind::Delete, &targets[0], &Ok("Deleted pod/a".into())));
    assert!(tr.consume(&k9rs_proto::protocol::OperationKind::Delete, &targets[1], &Err("Forbidden".into())));
    assert!(!tr.is_done());
    // A duplicate result for an already-consumed target is not ours.
    assert!(!tr.consume(&k9rs_proto::protocol::OperationKind::Delete, &targets[0], &Ok("again".into())));

    assert!(tr.consume(&k9rs_proto::protocol::OperationKind::Delete, &targets[2], &Ok("Deleted pod/c".into())));
    assert!(tr.is_done());

    let summary = tr.summary();
    assert!(summary.message.contains("Deleted 2 pods"), "{}", summary.message);
    assert!(summary.message.contains("1 skipped"), "{}", summary.message);
    assert!(summary.message.contains("1 FAILED"), "{}", summary.message);
    assert!(summary.message.contains("Forbidden"), "{}", summary.message);
}

#[test]
fn ok_results_unmark_their_rows_through_the_weak_store() {
    use crate::store::{RowStore, StorePayload};
    use k9rs_proto::protocol::TableBaseline;
    use k9rs_proto::row::{CellValue, ResourceRow};

    let store = RowStore::new("pods");
    let row = ResourceRow {
        cells: vec![CellValue::Text("a".into())],
        name: "a".into(),
        namespace: Some("ns".into()),
        ..Default::default()
    };
    store.apply(1, StorePayload::Baseline(TableBaseline {
        resource: ResourceId::BuiltIn(BuiltInKind::Pod),
        headers: vec!["NAME".into()],
        rows: vec![row],
    }));
    let key = k9rs_proto::protocol::ObjectKey::new("ns".to_string(), "a".to_string());
    assert_eq!(store.toggle_mark(&key), Some(true));

    let t = target("a", "ns");
    let mut tr = BatchTracker::new(
        "Restarted", "pod".to_string(), pod_rid(), std::slice::from_ref(&t), 0,
        std::sync::Arc::downgrade(&store), k9rs_proto::protocol::OperationKind::Restart,
    );
    assert!(tr.consume(&k9rs_proto::protocol::OperationKind::Restart, &t, &Ok("Restarted".into())));
    assert!(!store.has_marks(), "success unmarked the row (restart keeps rows in place)");
    assert!(tr.is_done());
}

#[test]
fn failed_results_keep_their_marks() {
    use crate::store::{RowStore, StorePayload};
    use k9rs_proto::protocol::TableBaseline;
    use k9rs_proto::row::{CellValue, ResourceRow};

    let store = RowStore::new("pods");
    let row = ResourceRow {
        cells: vec![CellValue::Text("a".into())],
        name: "a".into(),
        namespace: Some("ns".into()),
        ..Default::default()
    };
    store.apply(1, StorePayload::Baseline(TableBaseline {
        resource: ResourceId::BuiltIn(BuiltInKind::Pod),
        headers: vec!["NAME".into()],
        rows: vec![row],
    }));
    let key = k9rs_proto::protocol::ObjectKey::new("ns".to_string(), "a".to_string());
    store.toggle_mark(&key);

    let t = target("a", "ns");
    let mut tr = BatchTracker::new(
        "Deleted", "pod".to_string(), pod_rid(), std::slice::from_ref(&t), 0,
        std::sync::Arc::downgrade(&store), k9rs_proto::protocol::OperationKind::Delete,
    );
    assert!(tr.consume(&k9rs_proto::protocol::OperationKind::Delete, &t, &Err("RBAC".into())));
    assert!(store.has_marks(), "failure keeps the mark for retry");
    let summary = tr.summary();
    assert!(summary.message.contains("Deleted 0 pods"), "{}", summary.message);
    assert!(summary.message.contains("RBAC"), "{}", summary.message);
}

#[test]
fn fail_send_counts_as_a_result() {
    let t = target("a", "ns");
    let mut tr = BatchTracker::new(
        "Deleted", "pod".to_string(), pod_rid(), std::slice::from_ref(&t), 0, std::sync::Weak::new(),
        k9rs_proto::protocol::OperationKind::Delete,
    );
    tr.fail_send(&t, "send failed: broken pipe".into());
    assert!(tr.is_done());
    assert!(tr.summary().message.contains("broken pipe"));
}
