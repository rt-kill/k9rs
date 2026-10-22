//! Metadata for local resource types — lives directly on the closed
//! [`LocalResourceKind`] enum as exhaustive-match `fn` accessors.
//! No parallel static table: adding a variant forces a compile error
//! in every method arm until you fill it in.

use serde::{Deserialize, Serialize};

use crate::kube::protocol::{ResourceId, ResourceScope};

/// Closed enum of every local resource type the daemon serves. All
/// metadata lives on the enum itself via `fn` accessors — no
/// parallel metadata table to keep in sync. Dispatch is compile-time
/// checked: adding a variant forces an update to every exhaustive
/// match below, and any call site that branches on `LocalResourceKind`
/// will equally fail to compile until the new arm is covered.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LocalResourceKind {
    PortForward,
    ExecResource,
    /// User-defined local resource from daemon config.
    Custom(String),
    /// The kubeconfig's contexts. The one kind the daemon does NOT serve:
    /// the rows are a fact about the CLIENT's kubeconfig, and the whole
    /// point of the view is to be usable when there is no session at all
    /// (no `current-context`, or the one you have is unreachable). Its
    /// store is seeded in-process — see the client's `LiveQuery` element.
    ///
    /// APPENDED, not slotted in next to its siblings: this enum rides
    /// `ResourceId` on the wire, so inserting mid-enum silently re-tags
    /// `Custom`. (`local_resource_kind_wire_tags_are_stable` caught exactly
    /// that when this variant was first written in the obvious place.)
    Context,
    /// The keybindings of the view you pressed `?` in.
    ///
    /// Like [`Self::Context`], the daemon does not serve it — the rows are a
    /// fact about the CLIENT (its keymap and its config), seeded in-process.
    /// Modelling help as a resource rather than as an overlay is what gives
    /// it grep, scroll, sort, column movement and copy for free, and is what
    /// makes Esc the ordinary nav pop instead of a bespoke key path that had
    /// to be remembered.
    ///
    /// APPENDED for the same wire-tag reason as `Context` above.
    Keybinding,
    /// What you can type at the `:` prompt: every resource alias, and the
    /// static command words.
    ///
    /// Client-side like [`Self::Keybinding`], and for the same reason — the
    /// rows are a fact about this binary's registry and its command grammar.
    /// `Ctrl-a` used to format a plain-text DOCUMENT instead, which meant it
    /// could not be grepped, sorted or scrolled as a table, and it carried a
    /// hand-written copy of the keymap that had already drifted (missing
    /// `Ctrl-w`, `Ctrl-z`, `Ctrl-Space`, `Ctrl-\`). Keys belong to `?`.
    ///
    /// APPENDED for the same wire-tag reason as `Context` above.
    Alias,
}

impl LocalResourceKind {
    /// The well-known (non-Custom) variants. Custom kinds are registered
    /// at runtime and are not part of this list — callers that need to
    /// enumerate customs maintain their own registry.
    pub fn all() -> Vec<Self> {
        vec![
            Self::PortForward,
            Self::ExecResource,
            Self::Context,
            Self::Keybinding,
            Self::Alias,
        ]
    }

    /// Canonical singular name (e.g. `"portforward"`).
    pub fn name(&self) -> &str {
        match self {
            Self::PortForward => "portforward",
            Self::ExecResource => "execresource",
            Self::Context => "context",
            Self::Keybinding => "keybinding",
            Self::Alias => "alias",
            Self::Custom(name) => name.as_str(),
        }
    }

    /// API version token — free-form for local resources but
    /// conventionally `"v1"`.
    pub fn version(&self) -> &str {
        match self {
            Self::PortForward => "v1",
            Self::ExecResource => "v1",
            Self::Context => "v1",
            Self::Keybinding => "v1",
            Self::Alias => "v1",
            Self::Custom(_) => "v1",
        }
    }

    /// K8s-style kind string (e.g. `"PortForward"`) — for display.
    /// For custom resources, returns the raw name (caller-provided).
    pub fn kind_str(&self) -> &str {
        match self {
            Self::PortForward => "PortForward",
            Self::ExecResource => "ExecResource",
            Self::Context => "Context",
            Self::Keybinding => "Keybinding",
            Self::Alias => "Alias",
            Self::Custom(name) => name.as_str(),
        }
    }

    /// Plural name used as the identifier in the URL-like form.
    /// For custom resources, returns the raw name (custom resources
    /// are registered by their exact identifier).
    pub fn plural(&self) -> &str {
        match self {
            Self::PortForward => "portforwards",
            Self::ExecResource => "execresources",
            Self::Context => "contexts",
            Self::Keybinding => "keybindings",
            Self::Alias => "aliases",
            Self::Custom(name) => name.as_str(),
        }
    }

    /// Scope — most local resources are cluster-scoped (no namespace).
    pub fn scope(&self) -> ResourceScope {
        match self {
            Self::PortForward => ResourceScope::Cluster,
            Self::ExecResource => ResourceScope::Cluster,
            Self::Context => ResourceScope::Cluster,
            Self::Keybinding => ResourceScope::Cluster,
            Self::Alias => ResourceScope::Cluster,
            Self::Custom(_) => ResourceScope::Cluster,
        }
    }

    /// Short aliases for command mode and tab bar lookup.
    /// Custom resources have no aliases — they are referenced by
    /// their exact name.
    pub fn aliases(&self) -> &'static [&'static str] {
        match self {
            Self::PortForward => &["pf", "portforward", "portforwards", "port-forwards"],
            Self::ExecResource => &["exec", "execresource", "execresources"],
            Self::Context => &["ctx", "context", "contexts"],
            // DELIBERATELY EMPTY. Every other alias here is a noun you
            // browse; `?` is a sheet you consult ABOUT the view you are in,
            // and it floats over that view for exactly that reason. Making
            // it typeable at `:` gave it the resource-command path, which
            // `nav.reset`s the stack — so `:keys` destroyed your drill to
            // land on a table that `Action::Help` had never seeded, at depth
            // 1 where `pop` refuses and there is no way back out.
            Self::Keybinding => &[],
            // UNLIKE `Keybinding`: this one IS a noun you browse, and
            // `:alias` has always been the way in. It goes through the
            // ordinary resource-command path, so it resets the stack like
            // every other `:` resource — which is what you want from a
            // root-level reference view.
            Self::Alias => &["alias", "aliases", "a"],
            Self::Custom(_) => &[],
        }
    }

    /// Short label for tab/breadcrumb display (e.g. `"PF"`).
    /// For custom resources, returns the raw name.
    pub fn short_label(&self) -> &str {
        match self {
            Self::PortForward => "PF",
            Self::ExecResource => "EXEC",
            Self::Context => "CTX",
            Self::Keybinding => "KEYS",
            Self::Alias => "ALIAS",
            Self::Custom(name) => name.as_str(),
        }
    }

    /// Column metadata, same shape built-ins get from their `ResourceDef`.
    /// Consulted by `ColumnPolicy` for display level AND width ceiling, so a
    /// local resource is no more special about its columns than any other.
    /// An empty slice means "infer everything", which is what the built-in
    /// path already does for columns a def doesn't mention.
    pub fn column_defs(&self) -> &'static [crate::kube::resource_def::ColumnDef] {
        use crate::kube::resource_def::ColumnDef;
        match self {
            // Cluster and user are usually long and rarely the thing you are
            // reading — on EKS they are full ARNs, which at the global cap
            // push NAME and ACTIVE off the screen entirely.
            Self::Context => {
                static COLS: &[ColumnDef] = &[
                    ColumnDef::new("NAME").max_width(40),
                    ColumnDef::new("CLUSTER").max_width(28),
                    ColumnDef::new("USER").max_width(24),
                    ColumnDef::new("ACTIVE").max_width(8),
                ];
                COLS
            }
            // KEY is narrow and the thing you scan; DESCRIPTION gets the rest.
            Self::Keybinding => {
                static COLS: &[ColumnDef] = &[
                    // The one column in the program that GROUPS: it is the
                    // category the rows were gathered under, so repeating it
                    // per row is noise. Every resource column leaves this
                    // off — see `ColumnDef::groups`.
                    ColumnDef::new("SECTION").max_width(36).groups(),
                    ColumnDef::new("KEY").max_width(16),
                    ColumnDef::new("DESCRIPTION").max_width(60),
                ];
                COLS
            }
            // ALIAS is what you scan and type; TARGET says where it goes.
            // SECTION first so the default sort groups resources apart from
            // commands (and the repeated value collapses as it draws).
            Self::Alias => {
                static COLS: &[ColumnDef] = &[
                    // Groups, for help's reason: `resource` repeated sixty
                    // times down the left edge is noise, because it is the
                    // category the rows were gathered under.
                    ColumnDef::new("SECTION").max_width(12).groups(),
                    ColumnDef::new("ALIAS").max_width(44),
                    ColumnDef::new("TARGET").max_width(40),
                ];
                COLS
            }
            Self::PortForward | Self::ExecResource | Self::Custom(_) => &[],
        }
    }

    /// Build the `ResourceId` that identifies this local resource type.
    pub fn to_resource_id(self) -> ResourceId {
        ResourceId::Local(self)
    }

    /// The operation set this local resource supports. Single source
    /// of truth — both the client-side `ResourceId::capabilities()`
    /// and the server-side `LocalResourceSource::capabilities()` impls
    /// reach for this so they can't drift.
    pub fn operations(&self) -> Vec<crate::kube::protocol::OperationKind> {
        use crate::kube::protocol::OperationKind;
        match self {
            // Port-forward: describe (show config), yaml (edit config),
            // delete (stop the forward). Nothing else applies.
            Self::PortForward => vec![
                OperationKind::Describe,
                OperationKind::Yaml,
                OperationKind::Delete,
            ],
            // Exec resource: describe (show raw JSON), yaml (show raw JSON
            // as yaml), delete (remove entry from internal store).
            // Exec-backed rows ARE a command's output: there is nothing to
            // delete, and the next poll would put the row straight back. The
            // source's `delete` refuses; not advertising it means the key is
            // never offered in the first place.
            Self::ExecResource => vec![OperationKind::Describe, OperationKind::Yaml],
            // A context is switched to (Enter, via `DrillTarget`), not
            // described or deleted — every other operation would have to ask
            // a daemon about the client's own kubeconfig.
            Self::Context => vec![],
            // Nothing to act ON: a keybinding row is documentation.
            Self::Keybinding => vec![],
            // Same — an alias row is documentation. (Drilling into the
            // resource it names would be a nice `Enter`, but it would need a
            // `DrillTarget` from a cell, and the alias is already one
            // keystroke away at `:`.)
            Self::Alias => vec![],
            // Same as `ExecResource` — a `Custom` kind IS an exec source.
            Self::Custom(_) => vec![OperationKind::Describe, OperationKind::Yaml],
        }
    }
}

/// Look up a local resource kind by any alias (case-insensitive).
pub fn find_by_alias(alias: &str) -> Option<LocalResourceKind> {
    let lower = alias.to_lowercase();
    LocalResourceKind::all().into_iter().find(|k| {
        k.aliases().iter().any(|a| a.eq_ignore_ascii_case(&lower))
    })
}

#[cfg(test)]
#[path = "../../tests/kube/local/types.rs"]
mod tests;
