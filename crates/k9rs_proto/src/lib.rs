//! The TUI↔daemon contract, plus the resource definitions it is written in.
//!
//! This crate is the wire: `SessionCommand`/`SessionEvent`, the framing and
//! the build-identity handshake, the typed row and cell values that ride it,
//! and the metadata table (`BuiltInKind` and its 34 definitions) that both
//! ends describe resources with.
//!
//! **It names neither kube-rs nor k8s-openapi, and that is the point.** The
//! client and the daemon can only talk through what is declared here, and a
//! change to the contract is a change to this crate's API rather than
//! something that could happen anywhere in 71 files.
//!
//! Getting here took untangling, not just a `Cargo.toml`: `protocol.rs` used
//! to consult the watcher `REGISTRY` for alias and identity lookups, and that
//! registry held 34 type-erased watcher factories built from k8s-openapi
//! types — so the wire module transitively depended on the Kubernetes client
//! it exists to avoid. `BuiltInKind::def` is generated from the enum's own
//! declaration now, and the definitions live in files that import no k8s
//! types at all. `cargo tree -p k9rs_proto` is what checks it — the manifest
//! below names no Kubernetes crate, so the transitive dependency cannot come
//! back without an edit here that says so out loud.

// FIRST, and `#[macro_use]`: `macro_rules!` is textually scoped, so a macro
// used by `local_types` (declared next) has to be declared before it.
#[macro_use]
mod variant_list;

pub mod crd_cache;
pub mod local_types;
pub mod overlay;
pub mod protocol;
pub mod resource_def;
pub mod row;
