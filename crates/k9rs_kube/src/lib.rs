//! Kubernetes access: the typed client, the watchers, and the converters.
//!
//! This is the only crate that names kube-rs and k8s-openapi, and the only
//! one that parses JSON. Everything above it — the client session, the app
//! state, the renderer — sees `k9rs_proto` types and nothing else, which is
//! what makes "JSON only at external API boundaries" a dependency fact
//! rather than a convention.
//!
//! The split that made this possible was inside each resource file: an
//! `impl ResourceDef` is pure metadata and lives in `k9rs_proto`, while the
//! `impl ConvertToRow<K>` beside it needs the k8s type and lives here. The
//! trait is declared HERE for that reason — a local trait may be implemented
//! for foreign types, and with both the trait and the defs down in `proto`
//! those 34 impls would have no local type and be orphan violations.

pub mod cache;
pub mod daemon_config;
pub mod describe;
pub mod grace;
pub mod live_query;
pub mod live_query_dynamic;
pub mod local;
pub mod metrics;
pub mod ops;
pub mod repaint;
pub mod resource_defs;
pub mod resources;
pub mod session_env;
pub mod shared_poller;
