//! Batch-operation tracking: correlating per-target results back to a launch.
//!
//! Split out of the former `app::types` grab-bag: this is a cohesive
//! unit with its own vocabulary, and it was only ever in `types.rs`
//! because that file was where types went.

use crate::app::types::FlashMessage;
use crate::kube::protocol::{ObjectKey, ObjectRef};

// ---------------------------------------------------------------------------
// Batch tracker — correlates per-target OpResults back to a batch launch
// ---------------------------------------------------------------------------

/// An in-flight batch operation. Marks are NOT cleared at dispatch: each
/// Ok result unmarks its row (so "what is still marked" always means
/// "not yet succeeded"), failures keep their marks — retrying exactly
/// the failed set is one keypress away. Results aggregate into ONE
/// summary flash instead of N racing per-item flashes (single flash
/// slot, last write wins — failures used to vanish behind a final Ok).
#[derive(Debug)]
pub struct BatchTracker {
    /// Past-tense verb for the summary ("Deleted", "Restarted", …).
    verb: &'static str,
    /// Resource noun for the summary ("pod", "deployment", …).
    noun: String,
    /// The batch's operation. `consume` requires the result to carry the
    /// SAME op (v10 wire discriminant) — without it, an edit-apply or any
    /// other operation on a batch member could claim (or be claimed by)
    /// the batch's result for that target. What remains ambiguous is only
    /// the same-op+same-target race (two concurrent restarts of one pod)
    /// — full disambiguation would need a per-request correlation id.
    op: crate::kube::protocol::OperationKind,
    /// The batch's resource kind. Every item of one batch shares it
    /// (targets are built from one element's rid), and `consume` requires
    /// it — without the check, a result for a DIFFERENT kind with the
    /// same ns+name (delete service `web` while a deployment-`web` batch
    /// is outstanding) would be silently misattributed.
    rid: crate::kube::protocol::ResourceId,
    /// Keys still awaiting a result.
    outstanding: std::collections::HashSet<ObjectKey>,
    ok: usize,
    /// (name, error) per failed item.
    failures: Vec<(String, String)>,
    /// Confirmed targets that were already gone at confirm time.
    skipped: usize,
    /// The store the batch launched from — weak: result bookkeeping must
    /// not keep a popped element's store alive.
    store: std::sync::Weak<crate::app::store::RowStore>,
}

impl BatchTracker {
    pub fn new(
        verb: &'static str,
        noun: String,
        rid: crate::kube::protocol::ResourceId,
        targets: &[ObjectRef],
        skipped: usize,
        store: std::sync::Weak<crate::app::store::RowStore>,
        op: crate::kube::protocol::OperationKind,
    ) -> Self {
        Self {
            verb,
            noun,
            op,
            rid,
            outstanding: targets.iter().map(Self::key_of).collect(),
            ok: 0,
            failures: Vec::new(),
            skipped,
            store,
        }
    }

    /// The mark-identity of a target — inverse of how batch `ObjectRef`s
    /// are built from marked keys (`Namespace::from_row` maps "" ↔ `All`).
    pub fn key_of(target: &ObjectRef) -> ObjectKey {
        ObjectKey::new(
            target.namespace.as_option().unwrap_or("").to_string(),
            target.name.clone(),
        )
    }

    /// Record a send failure at dispatch time (the command never left the
    /// client) — same accounting as a server-side Err.
    pub fn fail_send(&mut self, target: &ObjectRef, error: String) {
        self.outstanding.remove(&Self::key_of(target));
        self.failures.push((target.name.clone(), error));
    }

    /// Consume a result if it belongs to this batch; `false` = not ours
    /// (the caller should handle it as an ordinary single-op result).
    /// Correlation = operation AND resource kind AND identity: the daemon
    /// echoes the request's op + full `ObjectRef`, so all three are
    /// authoritative.
    pub fn consume(
        &mut self,
        op: &crate::kube::protocol::OperationKind,
        target: &ObjectRef,
        result: &Result<String, String>,
    ) -> bool {
        if *op != self.op {
            return false;
        }
        if target.resource != self.rid {
            return false;
        }
        let key = Self::key_of(target);
        if !self.outstanding.remove(&key) {
            return false;
        }
        match result {
            Ok(_) => {
                self.ok += 1;
                // Success unmarks the row. For Delete/ForceKill the row's
                // removal prunes the mark anyway; Restart leaves the row
                // in place, so this is the path that clears it.
                if let Some(store) = self.store.upgrade() {
                    store.unmark_keys(std::iter::once(&key));
                }
            }
            Err(e) => self.failures.push((target.name.clone(), e.clone())),
        }
        true
    }

    pub fn is_done(&self) -> bool {
        self.outstanding.is_empty()
    }

    /// The aggregate flash. Info when everything succeeded; error with
    /// the first failure spelled out otherwise.
    pub fn summary(&self) -> FlashMessage {
        let mut msg = format!("{} {} {}{}", self.verb, self.ok, self.noun,
            if self.ok == 1 { "" } else { "s" });
        if self.skipped > 0 {
            msg.push_str(&format!(", {} skipped (gone)", self.skipped));
        }
        if self.failures.is_empty() {
            FlashMessage::info(msg)
        } else {
            let (name, err) = &self.failures[0];
            msg.push_str(&format!(
                ", {} FAILED — {}: {}",
                self.failures.len(), name, err,
            ));
            FlashMessage::error(msg)
        }
    }

    /// Summary for a batch cut short (daemon disconnected with results
    /// still outstanding).
    pub fn interrupted_summary(&self) -> FlashMessage {
        FlashMessage::warn(format!(
            "Batch interrupted: {} ok, {} failed, {} unanswered",
            self.ok, self.failures.len(), self.outstanding.len(),
        ))
    }
}

#[cfg(test)]
mod batch_tracker_tests {
    use super::*;
    use crate::kube::protocol::{Namespace, ResourceId};
    use crate::kube::resource_def::BuiltInKind;

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
        let key = crate::kube::protocol::ObjectKey::new("ns1".to_string(), "a".to_string());
        let t = ObjectRef::new(
            ResourceId::BuiltIn(BuiltInKind::Pod),
            key.name.clone(),
            Namespace::from_row(&key.namespace),
        );
        assert_eq!(BatchTracker::key_of(&t), key);

        let cluster_key = crate::kube::protocol::ObjectKey::new(String::new(), "n1".to_string());
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
            crate::kube::protocol::OperationKind::Delete,
        );
        assert!(!tr.is_done());

        // A result for a foreign target is NOT ours.
        assert!(!tr.consume(&crate::kube::protocol::OperationKind::Delete, &target("other", "ns"), &Ok("Deleted".into())));

        // A result for the RIGHT target but a DIFFERENT OPERATION is not
        // ours either — the op gate (v10) is what keeps a concurrent
        // edit-apply on a batch member from being consumed as the batch's
        // delete outcome (and vice versa).
        assert!(!tr.consume(&crate::kube::protocol::OperationKind::Apply, &targets[0], &Ok("Applied pod/a".into())));

        // A result for a DIFFERENT KIND with the same ns+name is NOT
        // ours either — the rid gate is what keeps a concurrent
        // single-op on a same-named object of another kind from being
        // misattributed to the batch.
        let foreign_kind = ObjectRef::new(
            ResourceId::BuiltIn(BuiltInKind::Deployment),
            "a".to_string(),
            Namespace::from_row("ns"),
        );
        assert!(!tr.consume(&crate::kube::protocol::OperationKind::Delete, &foreign_kind, &Ok("Deleted".into())));
        assert!(!tr.is_done());

        assert!(tr.consume(&crate::kube::protocol::OperationKind::Delete, &targets[0], &Ok("Deleted pod/a".into())));
        assert!(tr.consume(&crate::kube::protocol::OperationKind::Delete, &targets[1], &Err("Forbidden".into())));
        assert!(!tr.is_done());
        // A duplicate result for an already-consumed target is not ours.
        assert!(!tr.consume(&crate::kube::protocol::OperationKind::Delete, &targets[0], &Ok("again".into())));

        assert!(tr.consume(&crate::kube::protocol::OperationKind::Delete, &targets[2], &Ok("Deleted pod/c".into())));
        assert!(tr.is_done());

        let summary = tr.summary();
        assert!(summary.message.contains("Deleted 2 pods"), "{}", summary.message);
        assert!(summary.message.contains("1 skipped"), "{}", summary.message);
        assert!(summary.message.contains("1 FAILED"), "{}", summary.message);
        assert!(summary.message.contains("Forbidden"), "{}", summary.message);
    }

    #[test]
    fn ok_results_unmark_their_rows_through_the_weak_store() {
        use crate::app::store::{RowStore, StorePayload};
        use crate::kube::protocol::TableBaseline;
        use crate::kube::resources::row::{CellValue, ResourceRow};

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
        let key = crate::kube::protocol::ObjectKey::new("ns".to_string(), "a".to_string());
        assert_eq!(store.toggle_mark(&key), Some(true));

        let t = target("a", "ns");
        let mut tr = BatchTracker::new(
            "Restarted", "pod".to_string(), pod_rid(), std::slice::from_ref(&t), 0,
            std::sync::Arc::downgrade(&store), crate::kube::protocol::OperationKind::Restart,
        );
        assert!(tr.consume(&crate::kube::protocol::OperationKind::Restart, &t, &Ok("Restarted".into())));
        assert!(!store.has_marks(), "success unmarked the row (restart keeps rows in place)");
        assert!(tr.is_done());
    }

    #[test]
    fn failed_results_keep_their_marks() {
        use crate::app::store::{RowStore, StorePayload};
        use crate::kube::protocol::TableBaseline;
        use crate::kube::resources::row::{CellValue, ResourceRow};

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
        let key = crate::kube::protocol::ObjectKey::new("ns".to_string(), "a".to_string());
        store.toggle_mark(&key);

        let t = target("a", "ns");
        let mut tr = BatchTracker::new(
            "Deleted", "pod".to_string(), pod_rid(), std::slice::from_ref(&t), 0,
            std::sync::Arc::downgrade(&store), crate::kube::protocol::OperationKind::Delete,
        );
        assert!(tr.consume(&crate::kube::protocol::OperationKind::Delete, &t, &Err("RBAC".into())));
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
            crate::kube::protocol::OperationKind::Delete,
        );
        tr.fail_send(&t, "send failed: broken pipe".into());
        assert!(tr.is_done());
        assert!(tr.summary().message.contains("broken pipe"));
    }
}

