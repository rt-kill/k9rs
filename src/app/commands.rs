//! The `:` command vocabulary — one declaration, read by everything that
//! documents or completes it.
//!
//! It lived in `session::commands` next to the dispatcher, which made `app`
//! (the completer, the aliases view) reference `session` — upward, and the
//! last thing keeping those two in a cycle. The table is a DECLARATION and
//! the dispatcher is a reader, so the declaration belongs in the lower layer.

use crate::kube::protocol::ResourceId;

/// A classified `:command`. Produced by the session layer's
/// `parse_command_input` and consumed by its `handle_command_submit`
/// (named, not linked: those live ABOVE this module). The grammar priority is
/// expressed inside the parser; the dispatcher is a single exhaustive
/// match on this enum, so reordering two rules is a one-line change in
/// the parser and the dispatcher is drift-free.
///
/// The variants are ordered by priority in `parse_command_input` —
/// listing them in the same order here makes the grammar self-documenting.
#[cfg_attr(test, derive(Debug, PartialEq))]
pub(crate) enum ParsedCommand {
    /// `:q`, `:quit`, `:exit`, `:q!`
    Quit,
    /// `:help`, `:h`, `:?`
    Help,
    /// `:overview`, `:home`
    Overview,
    /// `:ctx <name>`, `:context <name>`
    ContextSwitch(crate::kube::protocol::ContextName),
    /// `:ns <name>`, `:namespace <name>`
    NamespaceSwitch(crate::kube::protocol::Namespace),
    /// `:pods /Nginx` — drill into a resource tab with a grep filter.
    /// Matched before `ResourceInNamespace` so `pods /Nginx` never parses
    /// as namespace `/Nginx`.
    ResourceFilter { rid: ResourceId, filter: String },
    /// `:deploy kube-system` — browse a resource in a specific namespace.
    ResourceInNamespace { rid: ResourceId, namespace: crate::kube::protocol::Namespace },
    /// `:pods` — plain resource alias.
    Resource(ResourceId),
    /// `:clickhouseinstallation prod` — namespaced CRD with namespace arg.
    CrdInNamespace { crd: crate::app::CrdInfo, namespace: crate::kube::protocol::Namespace },
    /// Fallback: anything that isn't a recognized shape. The dispatcher
    /// tries to resolve it as a known CRD via discovery cache, then falls
    /// back to a typed unresolved placeholder the daemon can resolve.
    CrdCandidate(String),
}

/// One static `:` word, for the aliases view to list, the completer to
/// offer, and this module's test to probe.
///
/// The same shape as [`crate::app::keymap::Binding`], for the same reason:
/// the grammar below is a linear match, so the only way the documentation and
/// the parser can be held together is to declare the documentation once and
/// probe it. There were THREE hand-written copies of this list — `Ctrl-a`'s
/// text document, and two slices in the completer — and the words that take
/// an argument were spelled differently in each.
///
/// Resource aliases are NOT here: they come from the registry and from
/// `LocalResourceKind`, which are already their single declaration.
pub(crate) struct CommandDoc {
    /// The words that select it, exactly as typed — what the completer
    /// offers. A word that takes an argument appears here bare; the
    /// placeholder is display only.
    pub words: &'static [&'static str],
    /// What the argument is called, for display. `None` takes none.
    pub arg: Option<&'static str>,
    pub label: &'static str,
    /// One input the parser must classify, and the check that it did.
    /// Read only by `every_documented_command_word_is_one_the_parser_accepts`
    /// — it exists to hold the declaration and the grammar together, which is
    /// a test's job, so it is dead in a release build by design.
    #[cfg_attr(not(test), allow(dead_code))]
    pub probe: &'static str,
    #[cfg_attr(not(test), allow(dead_code))]
    pub accepts: fn(&ParsedCommand) -> bool,
}

impl CommandDoc {
    /// How a reference view prints it: `q / quit / exit` or `ns <name>`.
    pub(crate) fn display(&self) -> String {
        match self.arg {
            None => self.words.join(" / "),
            Some(arg) => self
                .words
                .iter()
                .map(|w| format!("{w} <{arg}>"))
                .collect::<Vec<_>>()
                .join(" / "),
        }
    }
}

/// Every `:` word that is a command rather than a resource, in the order the
/// grammar tries them.
pub(crate) const COMMAND_WORDS: &[CommandDoc] = &[
    CommandDoc {
        words: &["q", "quit", "exit", "q!"],
        arg: None,
        label: "Quit",
        probe: "q",
        accepts: |p| matches!(p, ParsedCommand::Quit),
    },
    CommandDoc {
        words: &["help", "h", "?"],
        arg: None,
        label: "Show help for this view",
        probe: "help",
        accepts: |p| matches!(p, ParsedCommand::Help),
    },
    CommandDoc {
        words: &["overview", "home"],
        arg: None,
        label: "Cluster overview",
        probe: "overview",
        accepts: |p| matches!(p, ParsedCommand::Overview),
    },
    CommandDoc {
        words: &["ctx", "context"],
        arg: Some("name"),
        label: "Switch context",
        probe: "ctx dev",
        accepts: |p| matches!(p, ParsedCommand::ContextSwitch(_)),
    },
    CommandDoc {
        words: &["ns", "namespace"],
        arg: Some("name"),
        label: "Switch namespace",
        probe: "ns kube-system",
        accepts: |p| matches!(p, ParsedCommand::NamespaceSwitch(_)),
    },
];
