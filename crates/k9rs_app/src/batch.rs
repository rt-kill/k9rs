//! Batch-operation tracking: correlating per-target results back to a launch.
//!
//! Split out of the former `app::types` grab-bag: this is a cohesive
//! unit with its own vocabulary, and it was only ever in `types.rs`
//! because that file was where types went.

use crate::state::FlashMessage;
use k9rs_proto::protocol::{ObjectKey, ObjectRef};

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
    op: k9rs_proto::protocol::OperationKind,
    /// The batch's resource kind. Every item of one batch shares it
    /// (targets are built from one element's rid), and `consume` requires
    /// it — without the check, a result for a DIFFERENT kind with the
    /// same ns+name (delete service `web` while a deployment-`web` batch
    /// is outstanding) would be silently misattributed.
    rid: k9rs_proto::protocol::ResourceId,
    /// Keys still awaiting a result.
    outstanding: std::collections::HashSet<ObjectKey>,
    ok: usize,
    /// (name, error) per failed item.
    failures: Vec<(String, String)>,
    /// Confirmed targets that were already gone at confirm time.
    skipped: usize,
    /// The store the batch launched from — weak: result bookkeeping must
    /// not keep a popped element's store alive.
    store: std::sync::Weak<crate::store::RowStore>,
}

impl BatchTracker {
    pub fn new(
        verb: &'static str,
        noun: String,
        rid: k9rs_proto::protocol::ResourceId,
        targets: &[ObjectRef],
        skipped: usize,
        store: std::sync::Weak<crate::store::RowStore>,
        op: k9rs_proto::protocol::OperationKind,
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
        op: &k9rs_proto::protocol::OperationKind,
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
#[path = "tests/batch.rs"]
mod batch_tracker_tests_mirror;
