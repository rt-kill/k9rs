//! Nav-stack elements — self-contained "scopes" (the user's model).
//!
//! An [`Element`] is one level of the navigation stack: the durable,
//! self-defining description of *where you are* — its query/predicate,
//! its owned data handles, its label, its column policy — plus the
//! **persistent interaction state** (cursor, sort, draft filter text)
//! that must survive being covered and revealed.
//!
//! The *view* — what's actually on the terminal — is ephemeral by rule:
//! [`Element::view`] derives it on demand from (element, stores) via
//! [`crate::store::derive_view`], memoized behind a generation key
//! and droppable at any time with nothing lost.
//!
//! Derivation is a construction-time event: push-sites peek the current
//! TOP element (never the stack), hand the new element live handles
//! ([`RowSource`] Arcs), and forget. An element never knows it was
//! derived; references only ever point backward, so strong Arcs are
//! leak-free by construction. Pop = drop: a [`ResourceList`]'s
//! [`SubscriptionStream`](crate::client::SubscriptionStream) aborts its
//! bridge, RST closes the substream,
//! the daemon's end tears down.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use crate::nav::FilterInputState;
use k9rs_util::CompiledGrep;
use crate::store::{
    derive_view, DeriveSpec, LineStore, PreparedView, RowPredicate,
    RowSource, RowStore, SortSpec,
};
use crate::table::TableDataState;
use crate::state::ItemCounts;
use k9rs_proto::row::DerivedViewKind;
use crate::ColumnLevel;
use k9rs_proto::protocol::{Namespace, ObjectKey, ObjectRef, ResourceId};
use k9rs_proto::row::ResourceRow;
use k9rs_util::SearchPattern;


pub(crate) mod content;
pub(crate) mod log;
pub(crate) mod table;

// Re-exported flat so the move does not rewrite call sites: everything
// that said `element::LogSession` still does. The submodules exist to
// scope the SOURCE — `table` and `log` reference each other nowhere —
// not to lengthen 200 paths.
pub use content::*;
pub use log::*;
pub use table::*;

/// Why a derivation could not be constructed from the given top element.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeriveError {
    /// The top element has no row output to refine/project from.
    NoRows,
    /// Nothing is selected (empty view).
    NoSelection,
}

/// How much of the frame a thing claims when it is on top.
///
/// ONE answer for both axes of "on top": a nav ELEMENT
/// ([`Element::extent`]) and an orthogonal operation
/// ([`crate::TakeoverOp::extent`]). That is what lets a floating view be
/// an ordinary element rather than a second kind of thing with its own key
/// routing — and the operations had their own `OpExtent`, with its own
/// `Dialog` and `FullFrame`, until it collapsed into this one. Its `Dialog`
/// could not say how big it was, so its renderer centred itself: a second
/// geometry authority for one rect.
///
/// A `Dialog` is drawn in a centered box over whatever is beneath it (which
/// is rendered first, exactly as it would be if it were on top), placed by
/// the renderer's `dialog_box`. `Full` claims the frame, and the one path that
/// paints OVER a live view erases it first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViewExtent {
    /// Claims the frame. Every element does this unless it says otherwise.
    Full,
    /// A centered box over the view beneath, EXACTLY this big in cells —
    /// already the smaller of what the content needs and what the element
    /// will allow itself.
    ///
    /// It used to be a `Size` per axis, `Cells` or `Pct`, and the doc here
    /// claimed the box was "at most this big and shrunk to whatever the
    /// content actually needs". Nothing shrank anything: the three
    /// content-sized dialogs passed `Cells(exact)`, so cap and need
    /// coincided and the claim looked true, while help — the only user of
    /// `Pct` — got a flat 80% of the frame whatever it held. Its 105-column
    /// table was therefore CLIPPED by 27 columns at a 100-wide terminal and
    /// floating in 53 columns of DEAD SPACE at 200. Both halves of one
    /// unimplemented sentence.
    ///
    /// So each element resolves its own box against the frame it is given,
    /// which is the rule the three already followed, and `Size` — with its
    /// `Pct` variant existing for exactly one caller — is gone.
    Dialog { width: u16, height: u16 },
}

/// Outcome of a mark keypress. The element stays UI-free — the action
/// layer turns these into flashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkOutcome {
    /// Mark state changed.
    Toggled,
    /// No row under the cursor — empty view, or the row vanished between
    /// paint and keypress (stale coalesced frame).
    RowGone,
    /// This element kind has no markable rows.
    Unsupported,
}

/// Outcome of a span-mark keypress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanOutcome {
    Applied,
    /// No marked row is visible in this view to anchor the span.
    NoAnchor,
    /// This element kind has no markable rows.
    Unsupported,
}

/// The landing page. Its content (cluster health stats) comes from the
/// app-level core stores at render time — chrome data, not navigation.
#[derive(Debug, Default)]
pub struct Overview;

// ---------------------------------------------------------------------------
// Element — the closed sum the stack holds
// ---------------------------------------------------------------------------

/// The behavioural classes an [`Element`] can belong to. Closed, so adding
/// an element kind forces a choice — and the choice is then CHECKED against
/// what the element's accessors actually return.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElementClass {
    /// Rows in a `RowStore`: cursor, sort, grep, column filter, marks.
    Table,
    /// A log stream: line store plus a log view.
    Log,
    /// Static fetched content (yaml, describe, aliases).
    Content,
    /// App chrome with no data source of its own.
    Chrome,
    /// A question with a continuation and no data source: one keymap, no
    /// cursor, no scroll. Distinct from `Chrome` because the keys differ —
    /// mapping it to the Overview keymap would document `Tab`-to-next-view
    /// at a box whose `Tab` switches buttons.
    Dialog,
}

/// One nav-stack element. A closed enum: the renderer, key dispatch, and
/// crumb builder all match exhaustively — adding a kind forces every
/// consumer to decide, which is the point.
///
/// # When an accessor may use `_ =>`, and when it may not
///
/// A 2026-09 audit counted "~28 `_ =>` accessors" as a hazard; it is now 44
/// and most of them are correct. The distinction, triaged 2026-10-14, is
/// whether the wildcard is an ANSWER or a GUESS:
///
/// - **An answer.** `Option` accessors for "the kinds that have one" —
///   `log_view`, `data_store`, `form_dialog`, `table_interaction`. `None`
///   means "not that kind of element", the caller must handle it, and a new
///   kind inheriting `None` is telling the truth.
/// - **An answer.** Delegation to the general case — `title` falls back to
///   `label`, `capabilities` to `rid`'s, `marked_count` to the data store's.
///   The default is the composition, not a shrug.
/// - **A GUESS, and therefore banned.** A bare default that a new kind would
///   silently inherit AND be wrong about. Two such existed and are now
///   exhaustive: [`Self::is_live`] (`_ => true` is what suppresses the stale
///   banner, so a new kind with a dead stream claimed to be tracking the
///   cluster) and [`Self::abort_data_stream`] (`_ => {}` leaks a new kind's
///   stream straight past the choke point that exists to abort it).
///
/// The test is not "is there a wildcard" but "would a new variant be WRONG
/// under it". Adding a kind should force a decision exactly where a wrong
/// answer is silent and harmful, and nowhere else — a hundred exhaustive
/// matches over nine kinds would bury the two that matter.
#[derive(Debug)]
pub enum Element {
    ResourceList(ResourceList),
    RowFilter(RowFilter),
    DerivedRows(DerivedRows),
    LogSession(Box<LogSession>),
    LogFilter(LogFilter),
    ContentView(ContentView),
    Overview(Overview),
    /// A yes/no question about an operation, floating over the view that
    /// raised it.
    ///
    /// An ELEMENT rather than a modal slot: Esc is then the ordinary pop,
    /// its keys arrive through the same per-kind dispatch as every other
    /// view, and there is no second authority deciding who owns input. The
    /// guard that used to arbitrate that (`modal_without_key_handler`) was
    /// wrong twice in one day, and it existed only because dialogs captured
    /// keys from outside the stack.
    Confirm(Box<crate::ConfirmDialog>),
    /// "Which container?" — a pick-one list, floating over the view that
    /// asked. Same reasoning as `Confirm`: a dialog is an element whose
    /// extent happens to be a box.
    Picker(Box<crate::ContainerPicker>),
    /// A form gathering input for one operation (scale, port-forward).
    ///
    /// Storage, extent and Esc are the stack's, like the other dialogs. Its
    /// TYPING still goes through the input sink in `try_handle_input_mode`,
    /// and that is the pattern rather than an exception: every text surface
    /// in the app is handled there, because a keystroke that edits text has
    /// to mutate, and `handle_key_event` only names actions.
    Form(Box<crate::FormDialog>),
}

impl Element {
    // -- Derivation constructors (peek-only: they take the top ELEMENT,
    //    never the stack) ---------------------------------------------------

    /// Push-site for `/`, `~`, and the fault filter: narrow the top's row
    /// output by one predicate.
    pub fn derive_filter(top: &Element, predicate: RowPredicate) -> Result<Element, DeriveError> {
        let source = top.output_source().ok_or(DeriveError::NoRows)?;
        let label = match &predicate {
            RowPredicate::Grep(g) => format!("/{}", g.source()),
            RowPredicate::ColumnGrep { pattern, header, .. } => {
                format!("~{}:{}", header, pattern.source())
            }
            RowPredicate::Fault => "⚠ fault".to_string(),
        };
        Ok(Element::RowFilter(RowFilter {
            source: source.narrowed(Arc::new(predicate)),
            label,
            // The policy already carries the parent's rid — that IS this
            // filter's identity (no separate copy).
            policy: top.column_policy().clone(),
            title: top.title().to_string(),
            scope_label: top.scope_label().to_string(),
            interaction: TableInteraction::seeded_from(
                top.table_interaction().expect("output_source() gated: table kinds only"),
            ),
        }))
    }

    /// Push-site for Enter-on-a-row into a derived projection
    /// (containers): the row's identity + the top's live source ARE the
    /// definition; no namespace parenthetical, no ambient anything.
    pub fn derive_projection(
        top: &Element,
        row: &ResourceRow,
        kind: DerivedViewKind,
    ) -> Result<Element, DeriveError> {
        let source = top.output_source().ok_or(DeriveError::NoRows)?;
        let headers = kind.default_headers();
        let rules = k9rs_proto::overlay::build_column_rules(&headers, kind.plural());
        let origin = ObjectRef {
            resource: top.rid().ok_or(DeriveError::NoRows)?.clone(),
            // `from_row` maps "" → All (the ""↔All convention); building
            // `Named("")` directly would be the invalid state that
            // convention exists to prevent. Safe today (projections are
            // pod-only, always namespaced) but honest for any future kind.
            namespace: Namespace::from_row(row.namespace.as_deref().unwrap_or("")),
            name: row.name.clone(),
        };
        let label = format!("{}({})", kind.plural(), row.name);
        Ok(Element::DerivedRows(DerivedRows {
            source: source.clone(),
            key: crate::store::row_key(row),
            kind,
            origin,
            headers,
            rules,
            title: label.clone(),
            label,
            marked: HashSet::new(),
            interaction: TableInteraction::default(),
        }))
    }

    // -- Identity / metadata -------------------------------------------------

    /// The element's self-owned crumb label.
    pub fn label(&self) -> &str {
        match self {
            Element::ResourceList(e) => &e.label,
            Element::RowFilter(e) => &e.label,
            Element::DerivedRows(e) => &e.label,
            Element::Confirm(_) => "confirm",
            Element::Picker(_) => "container",
            Element::Form(f) => f.name(),
            Element::LogSession(e) => &e.label,
            Element::LogFilter(e) => &e.label,
            Element::ContentView(e) => &e.label,
            Element::Overview(_) => "overview",
        }
    }

    /// The element's self-owned table title (heads the widget border).
    pub fn title(&self) -> &str {
        match self {
            Element::ResourceList(e) => &e.title,
            Element::RowFilter(e) => &e.title,
            Element::DerivedRows(e) => &e.title,
            _ => self.label(),
        }
    }

    /// The scope shown in the table title parentheses (element-owned;
    /// empty for all-namespaces / cluster-scoped / projections).
    pub fn scope_label(&self) -> &str {
        match self {
            Element::ResourceList(e) => &e.scope_label,
            Element::RowFilter(e) => &e.scope_label,
            _ => "",
        }
    }

    /// Compiled grep patterns for match-highlighting: every committed
    /// grep in this element's chain, plus the live draft.
    pub fn grep_patterns(&self) -> Vec<SearchPattern> {
        let mut pats: Vec<SearchPattern> = self
            .output_source()
            .map(|s| {
                s.predicates()
                    .iter()
                    .filter_map(|p| match p.as_ref() {
                        RowPredicate::Grep(g) => Some(g.pattern().clone()),
                        _ => None,
                    })
                    .collect()
            })
            .unwrap_or_default();
        let draft = self.filter_input().text();
        if !draft.is_empty() {
            pats.push(SearchPattern::new(draft));
        }
        pats
    }

    /// Whether this element lost matches to its parent's eviction before it
    /// could collect them — the honest answer to "is this the whole story".
    pub fn log_lost_history(&self) -> bool {
        match self {
            Element::LogFilter(e) => e.lost_history(),
            _ => false,
        }
    }

    /// The first grep in play that is NOT running as a regex, and why —
    /// what the filter bars render.
    ///
    /// A pattern that quietly stopped being a regex reads exactly like one
    /// that simply isn't matching, so `0 visible` was the only thing the
    /// screen ever said about `a\{2,3}`. Committed greps are already
    /// compiled (table predicates and log filters both hold a
    /// [`CompiledGrep`]), so this costs at most one compile: the draft,
    /// which changes once per keystroke regardless.
    pub fn grep_fallback(&self) -> Option<String> {
        let committed = match self {
            Element::LogFilter(e) => e
                .source
                .patterns
                .iter()
                .find_map(Narrowing::fallback_reason),
            _ => self.output_source().and_then(|s| {
                s.predicates().iter().find_map(|p| match p.as_ref() {
                    RowPredicate::Grep(g) => g.pattern().fallback_reason(),
                    _ => None,
                })
            }),
        };
        if let Some(why) = committed {
            return Some(why.to_string());
        }
        // The draft lives in two different places depending on the kind;
        // `filter_input` is total (empty for log kinds) and `log_view` is
        // `None` for tables, so exactly one of them speaks.
        let draft = match self.log_view().and_then(|v| v.draft.as_ref().map(|d| d.value())) {
            Some(d) => d,
            None => self.filter_input().text(),
        };
        if draft.is_empty() {
            return None;
        }
        SearchPattern::new(draft).fallback_reason().map(str::to_string)
    }

    /// Capabilities for the help footer / key gating on non-table kinds.
    pub fn is_table(&self) -> bool {
        matches!(self.class(), ElementClass::Table)
    }

    /// What KIND of surface this element is — the ONE declaration the
    /// framework accessors must agree with.
    ///
    /// This exists because nothing could catch the contexts view opting out
    /// of the framework: it answered `None` from `table_interaction`,
    /// `filter_input`, `data_store`, `rid` and `marked_count`, so `/`, sort,
    /// column filters, column movement and marks all dispatched correctly
    /// and then silently did nothing. It was reported a year of little
    /// papercuts later as "`/` doesn't work on the contexts page".
    ///
    /// A kind declares its class here; `element_accessors_match_their_class`
    /// checks the accessors actually deliver it. A future element that
    /// claims `Table` and forgets to wire a `TableInteraction` fails the
    /// test instead of quietly losing half the keymap.
    pub fn class(&self) -> ElementClass {
        match self {
            Element::ResourceList(_) | Element::RowFilter(_) | Element::DerivedRows(_) => {
                ElementClass::Table
            }
            // No data source of its own — it carries the question and the
            // continuation, nothing else.
            Element::Confirm(_) | Element::Picker(_) | Element::Form(_) => ElementClass::Dialog,
            Element::LogSession(_) | Element::LogFilter(_) => ElementClass::Log,
            Element::ContentView(_) => ElementClass::Content,
            Element::Overview(_) => ElementClass::Chrome,
        }
    }

    /// Whether the TOP element renders its own command/filter prompt inside
    /// its own layout, so the shared bottom overlay in `ui::draw` must NOT
    /// also draw one. EXHAUSTIVE over kinds on purpose: a new element kind
    /// must declare its placement here rather than silently defaulting into
    /// the overlay path — which is exactly how the Overview double-command-bar
    /// bug arose (it drew inline AND fell through to the overlay).
    pub fn renders_command_inline(&self) -> bool {
        match self {
            // Table-like views + the overview clone draw the prompt inside
            // their own layout (resource.rs / overview.rs).
            Element::ResourceList(_)
            | Element::RowFilter(_)
            | Element::DerivedRows(_)
            | Element::Overview(_) => true,
            // Sub-views have no inline prompt row — they rely on the overlay.
            Element::ContentView(_)
            | Element::LogSession(_)
            | Element::LogFilter(_)
            // A dialog answers one question; `:` belongs to the view under it.
            | Element::Confirm(_)
            | Element::Picker(_)
            | Element::Form(_) => false,
        }
    }

    /// The resource this element shows, if it is resource-backed.
    /// Refinements report their parent's rid (a grep over pods still
    /// shows pods — describe/yaml/delete act on it).
    pub fn rid(&self) -> Option<&ResourceId> {
        match self {
            Element::ResourceList(e) => Some(e.rid()),
            Element::RowFilter(e) => e.policy.rid.as_ref(),
            _ => None,
        }
    }

    /// The parent object a derived projection came from (the pod whose
    /// containers are shown).
    pub fn origin(&self) -> Option<&ObjectRef> {
        match self {
            Element::DerivedRows(e) => Some(&e.origin),
            _ => None,
        }
    }

    /// The log-bearing kinds' line output (the handle a log grep narrows).
    pub(crate) fn line_output(&self) -> Option<LineSource> {
        match self {
            Element::LogSession(e) => Some(e.line_source()),
            Element::LogFilter(e) => Some(e.source.clone()),
            _ => None,
        }
    }

    /// Push-site for a committed log grep: narrow the top's line output.
    pub fn derive_log_filter(
        top: &Element,
        pattern: CompiledGrep,
        sense: Sense,
    ) -> Result<Element, DeriveError> {
        let source = top.line_output().ok_or(DeriveError::NoRows)?;
        let label = match sense {
            Sense::Keep => format!("/{}", pattern.source()),
            Sense::Drop => format!("/!{}", pattern.source()),
        };
        let parent_view = match top {
            Element::LogSession(e) => &e.view,
            Element::LogFilter(e) => &e.view,
            _ => unreachable!("line_output() gated"),
        };
        let (header_pod, header_container, header_since) =
            top.log_header().unwrap_or_default();
        // Starts where the parent IS, not at zero: everything already
        // evicted from the parent is unreachable, and claiming otherwise
        // would be the silent-gap this design exists to remove.
        let (cursor, cap) = {
            let store = source.store();
            (store.with_read(|i| i.evicted), store.max_lines())
        };
        Ok(Element::LogFilter(LogFilter {
            source: source.narrowed(Arc::new(pattern), sense),
            retained: LineStore::new(cap),
            cursor,
            lost_history: false,
            label,
            header_pod,
            header_container,
            header_since,
            view: LogInteraction::seeded_from(parent_view),
        }))
    }

    /// Display identity for log views: (target, container label, since label).
    pub fn log_header(&self) -> Option<(String, String, String)> {
        match self {
            Element::LogSession(e) => Some(e.header()),
            Element::LogFilter(e) => Some((
                e.header_pod.clone(),
                e.header_container.clone(),
                e.header_since.clone(),
            )),
            _ => None,
        }
    }

    /// The COMMITTED log greps on this element, oldest first — what the grep
    /// bar labels itself with. [`Self::log_patterns`] is the highlight set and
    /// additionally carries the uncommitted draft, so the two are not
    /// interchangeable.
    /// Test-only: the renderer reads `log_chain()`, which carries the
    /// per-step counts this drops.
    #[cfg(any(test, feature = "testing"))]
    pub fn log_committed_patterns(&self) -> Vec<String> {
        match self {
            Element::LogFilter(e) => e.source.patterns.iter().map(Narrowing::label).collect(),
            _ => Vec::new(),
        }
    }

    /// Log view state, for the kinds that have one.
    pub fn log_view(&self) -> Option<&LogInteraction> {
        match self {
            Element::LogSession(e) => Some(&e.view),
            Element::LogFilter(e) => Some(&e.view),
            _ => None,
        }
    }

    pub fn log_view_mut(&mut self) -> Option<&mut LogInteraction> {
        match self {
            Element::LogSession(e) => Some(&mut e.view),
            Element::LogFilter(e) => Some(&mut e.view),
            _ => None,
        }
    }

    /// The visible line indices together with the store they index into.
    /// ONE accessor because they are only meaningful as a pair — asking for
    /// them separately forced callers to match on a kind, get an `Option`
    /// back anyway, and unwrap it.
    pub fn log_lines(&mut self) -> Option<(Arc<[usize]>, Arc<LineStore>)> {
        let store = match self {
            Element::LogSession(e) => Arc::clone(&e.store),
            Element::LogFilter(e) => Arc::clone(&e.retained),
            _ => return None,
        };
        Some((self.log_visible()?, store))
    }

    /// The log store this element reads (session's own, or the filter's
    /// backing store).
    pub fn log_store(&self) -> Option<&Arc<LineStore>> {
        match self {
            Element::LogSession(e) => Some(&e.store),
            // The filter's OWN store — what it renders, saves and copies.
            Element::LogFilter(e) => Some(&e.retained),
            _ => None,
        }
    }

    /// The SESSION's store at the root of this log chain — the identity the
    /// stream-owning element is found by (`with_log_session_of`,
    /// `ensure_top_live`) and the one `ClearLogs` empties.
    ///
    /// Distinct from [`Self::log_store`] since a filter grew its own: those
    /// routing walks compare `Arc::ptr_eq` against the session's store, and
    /// handing them a filter's store would silently find nothing.
    pub fn log_root_store(&self) -> Option<&Arc<LineStore>> {
        match self {
            Element::LogSession(e) => Some(&e.store),
            Element::LogFilter(e) => Some(e.source.store()),
            _ => None,
        }
    }

    /// Whether this element is the `?` keybindings view — the one thing `?`
    /// needs to know to toggle rather than stack a second copy.
    pub fn is_keybindings_view(&self) -> bool {
        // Asked of the RID, not the element kind: `/` over help derives a
        // `RowFilter`, and that is still help. Testing the kind meant the
        // first keystroke of the grep the redesign exists to provide turned
        // the float full-screen and made the next `?` push a SECOND help
        // instead of closing the one you were reading.
        matches!(
            self.rid(),
            Some(k9rs_proto::protocol::ResourceId::Local(
                k9rs_proto::local_types::LocalResourceKind::Keybinding
            ))
        )
    }

    /// The form this element IS, if it is one.
    pub fn form_dialog(&self) -> Option<&crate::FormDialog> {
        match self {
            Element::Form(f) => Some(f),
            _ => None,
        }
    }

    pub fn form_dialog_mut(&mut self) -> Option<&mut crate::FormDialog> {
        match self {
            Element::Form(f) => Some(f),
            _ => None,
        }
    }

    /// Whether this element owns EVERY key, global bindings included.
    ///
    /// That is what "modal" means, and it is a property of the element
    /// rather than of where it is stored — which is the whole reason a
    /// dialog can be a nav element at all. `Confirm` answers yes because
    /// its safety net is "any other key cancels": a spurious keystroke (a
    /// stale terminal response misparsed as `r`, say) must dismiss the box
    /// rather than open the command prompt behind it.
    pub fn captures_all_input(&self) -> bool {
        matches!(self.class(), ElementClass::Dialog)
    }

    /// Whether this element floats, without asking how big.
    ///
    /// Derived from [`Self::extent`] with an empty frame, because WHICH
    /// variant it returns never depends on the frame — only the size does.
    /// One match, so a view that floats cannot also be told it is full
    /// frame (which is how a float once drew the cluster header, the
    /// breadcrumb and the flash line a second time inside its own box).
    pub fn floats(&self) -> bool {
        matches!(
            self.extent(ratatui::layout::Rect::new(0, 0, 0, 0)),
            ViewExtent::Dialog { .. }
        )
    }

    /// The cells a TABLE element's content wants: its columns' natural
    /// widths plus the border, and a row each plus the header and border.
    ///
    /// Read from the view memo, which the draw path warms before asking —
    /// the same `col_widths` the renderer lays out with, so the box and the
    /// table inside it cannot disagree about how wide the table is. A cold
    /// memo answers `u16::MAX`, i.e. "no opinion", which leaves the caller's
    /// allowance as the only bound and is exactly the old behaviour.
    fn table_content_size(&self) -> (u16, u16) {
        let Some(view) = self.table_interaction().and_then(|i| i.cached_view()) else {
            return (u16::MAX, u16::MAX);
        };
        let width = view.col_widths.iter().copied().fold(0u16, u16::saturating_add);
        let rows = u16::try_from(view.rows.len()).unwrap_or(u16::MAX);
        // `+2` border on each axis, `+1` for the header row.
        (width.saturating_add(2), rows.saturating_add(3))
    }

    /// How much of the frame this element claims — see [`ViewExtent`].
    ///
    /// EXHAUSTIVE on the kind axis, not a `_ => Full` wildcard: whether a new
    /// view floats is a decision its author has to make, and the renderer
    /// reads the answer from here rather than each view deciding for itself
    /// (which is how the overlay slot used to leave stale content inside a
    /// dialog's border).
    pub fn extent(&self, frame: ratatui::layout::Rect) -> ViewExtent {
        // A dialog is the SMALLER of what its content needs and what it will
        // allow itself of the frame. One rule, applied by the element that
        // knows the content — which is why the frame comes in here.
        let fit = |need: (u16, u16), allow: (u16, u16)| ViewExtent::Dialog {
            width: need.0.min(allow.0),
            height: need.1.min(allow.1),
        };
        let pct = |p: u16, of: u16| (u32::from(of) * u32::from(p) / 100) as u16;
        // What a `ModalOverlay`-chromed dialog needs AROUND its content —
        // asked of the widget rather than written as `+ 4` here, so the two
        // halves of that arithmetic cannot drift apart in different files.
        let margin = |w: u16, h: u16| {
            let (mw, mh) = crate::geometry::DIALOG_MARGIN;
            (w + mw, h + mh)
        };
        match self {
            // Help floats over the view it documents — you are consulting it
            // ABOUT something, and that something should stay on screen, so
            // it never takes the whole frame however much it holds.
            _ if self.is_keybindings_view() => fit(
                self.table_content_size(),
                (pct(80, frame.width), pct(85, frame.height)),
            ),
            // Sized from its own content: a yes/no box wants the rows its
            // message needs, not a percentage of somebody's terminal.
            Element::Confirm(d) => {
                // EXACTLY the size `ConfirmDialogWidget` asks for, so the
                // centring it does inside the box is a no-op and the two
                // do not disagree about where the dialog is. A batch confirm
                // lists its targets on extra lines and the box grows with
                // them — seeing what the batch will hit is the whole point.
                let (w, h) = crate::geometry::confirm_dialog_size(d);
                // The float reserves the OUTER box; the widget still owns
                // the inside of it, and spends `MARGIN` of the box on its own
                // outer gap — so a box sized to the dialog exactly would
                // shrink it until the message had no room. Asking the widget
                // rather than writing `+ 4` here is what stops the two
                // numbers drifting apart in different files.
                fit(margin(w, h), (frame.width, frame.height))
            }
            Element::Form(d) => {
                let (w, h) = crate::geometry::form_dialog_size(d);
                fit(margin(w, h), (frame.width, frame.height))
            }
            Element::Picker(p) => {
                let (w, h) = crate::geometry::container_picker_size(p);
                fit(margin(w, h), (frame.width, frame.height))
            }
            Element::ResourceList(_)
            | Element::RowFilter(_)
            | Element::DerivedRows(_)
            | Element::Overview(_)
            | Element::ContentView(_)
            | Element::LogSession(_)
            | Element::LogFilter(_) => ViewExtent::Full,
        }
    }

    /// The committed log-grep patterns active on this element (labels for
    /// highlight + the filter bar).
    pub fn log_patterns(&self) -> Vec<String> {
        // HIGHLIGHTS, not labels: an excluding step contributes none, because
        // every line it matched is gone and painting its matches would
        // highlight the one thing certainly not on screen.
        let mut pats: Vec<String> = match self {
            Element::LogFilter(e) => e
                .source
                .patterns
                .iter()
                .filter_map(|n| n.highlight().map(str::to_string))
                .collect(),
            _ => Vec::new(),
        };
        if let Some(view) = self.log_view() {
            if let Some(d) = &view.draft {
                // Same rule for the draft: an excluding draft shows its
                // effect by what disappears, not by colour.
                let (sense, text) = crate::element::sense_of(d.value());
                if !text.is_empty() && sense == Sense::Keep {
                    pats.push(text.to_string());
                }
            }
        }
        pats
    }

    /// The visible line indices for a log element — the ephemeral log
    /// view, derived on read, memoized on (store generation, draft).
    ///
    /// Scroll is NOT healed here, and deliberately: the position is a line
    /// identity now ([`crate::viewport::LineAnchor`]), so eviction moves
    /// nothing that needs repairing. This used to shift the viewport up by the
    /// eviction COUNT — a logical number applied to a physical row offset, so
    /// under wrap it under-corrected (the view crept toward the tail) and
    /// under a filter it over-corrected by the match rate.
    pub fn log_visible(&mut self) -> Option<Arc<[usize]>> {
        // Store, patterns AND view come out of ONE destructure. Fetching the
        // view separately meant asking a question this match had already
        // answered and unwrapping the reply — the Option-as-deferred-lookup
        // shape, which is only ever one refactor away from a panic.
        // A filter pulls from its parent FIRST, then reads only its own
        // store. The committed chain has already been applied by the drain,
        // so the derive below applies the DRAFT alone — which is why the
        // draft gets no storage: it changes on every keystroke.
        if let Element::LogFilter(e) = self {
            e.drain_from_parent();
        }
        let (store, view) = match self {
            Element::LogSession(e) => (Arc::clone(&e.store), &mut e.view),
            Element::LogFilter(e) => (Arc::clone(&e.retained), &mut e.view),
            _ => return None,
        };
        let key = LogDeriveKey {
            generation: store.generation(),
            draft: view.draft.as_ref().map(|d| d.value().to_string()),
        };
        if let Some((k, v)) = &view.cache {
            if *k == key {
                return Some(Arc::clone(v));
            }
        }
        // The draft's SENSE, through the same `sense_of` the COMMIT uses.
        //
        // This compiled the raw draft: `!error` searched for the literal
        // `!error` — zero lines, every time — and `\!urgent` handed `\!` to
        // the vim translator, which refuses it by name. So typing an
        // excluding grep previewed a blank view and then committed a working
        // one. Two answers to "what does this draft do", and only the commit
        // path asked `sense_of`.
        //
        // Emptiness is tested on the TEXT, not the raw draft: `!` alone is a
        // sense with nothing to say yet, and must preview as the unfiltered
        // view rather than as "drop everything".
        let draft = view.draft.as_ref().map(|d| d.value()).and_then(|raw| {
            let (sense, text) = crate::element::sense_of(raw);
            (!text.is_empty()).then(|| (sense, SearchPattern::new(text)))
        });
        let indices = store.with_read(|inner| {
            // The DRAFT only. Committed patterns were applied by the drain
            // when these lines were retained, so re-testing them here would
            // be doing the same work a second time on every frame — which is
            // exactly what this used to do.
            match &draft {
                None => (0..inner.lines.len()).collect::<Vec<usize>>(),
                Some((sense, d)) => (0..inner.lines.len())
                    .filter(|&i| {
                        let hit = inner.lines[i]
                            .segments()
                            .filter(|(role, ..)| role.searchable())
                            .any(|(_, text, _)| d.is_match(text));
                        // An excluding draft shows its effect by what
                        // DISAPPEARS — the same rule `Narrowing` applies on
                        // commit, so the preview and the commit agree.
                        match sense {
                            Sense::Keep => hit,
                            Sense::Drop => !hit,
                        }
                    })
                    .collect(),
            }
        });
        let indices: Arc<[usize]> = indices.into();
        view.cache = Some((key, Arc::clone(&indices)));
        Some(indices)
    }

    pub fn derived_kind(&self) -> Option<&DerivedViewKind> {
        match self {
            Element::DerivedRows(e) => Some(&e.kind),
            _ => None,
        }
    }

    /// The element's column policy (copied into refinements at derivation).
    pub fn column_policy(&self) -> &ColumnPolicy {
        match self {
            Element::ResourceList(e) => &e.policy,
            Element::RowFilter(e) => &e.policy,
            // Projections (and non-table kinds) use header inference.
            _ => {
                static DERIVED: std::sync::LazyLock<ColumnPolicy> =
                    std::sync::LazyLock::new(ColumnPolicy::derived);
                &DERIVED
            }
        }
    }

    /// Whether this element is a fault-filter refinement (Ctrl-Z pops it
    /// when it is the top).
    pub fn is_fault_filter(&self) -> bool {
        match self {
            Element::RowFilter(e) => {
                matches!(e.source.predicates().last().map(AsRef::as_ref), Some(RowPredicate::Fault))
            }
            _ => false,
        }
    }

    /// Whether this element's view is cluster-scoped (no namespace).
    pub fn is_cluster_scoped(&self) -> bool {
        match self {
            Element::DerivedRows(e) => e.kind.is_cluster_scoped(),
            _ => self.rid().map(|r| r.is_cluster_scoped()).unwrap_or(false),
        }
    }

    /// Capability manifest for key-availability and the help footer.
    pub fn capabilities(&self) -> k9rs_proto::protocol::ResourceCapabilities {
        match self {
            Element::DerivedRows(e) => k9rs_proto::protocol::ResourceCapabilities {
                operations: e.kind.operations(),
            },
            _ => match self.rid() {
                Some(rid) => rid.capabilities(),
                None => k9rs_proto::protocol::ResourceCapabilities { operations: Vec::new() },
            },
        }
    }

    /// The row output this element hands to a derived child (the model's
    /// "handed references"): the source INCLUDING this element's own
    /// predicates.
    pub fn output_source(&self) -> Option<&RowSource> {
        match self {
            Element::ResourceList(e) => Some(&e.source),
            Element::RowFilter(e) => Some(&e.source),
            // A projection's output is not row-store-backed; committing
            // filters on top of it is not supported (matches the old
            // behavior where derived views only ever draft-filtered).
            // Log/content/context/overview kinds have no row output.
            _ => None,
        }
    }

    /// The store this element's DATA ultimately lives in (a projection
    /// reads through to its parent's store). Drives the revive walk.
    pub fn data_store(&self) -> Option<&Arc<RowStore>> {
        match self {
            Element::ResourceList(e) => Some(e.query.store()),
            Element::RowFilter(e) => Some(e.source.store()),
            Element::DerivedRows(e) => Some(e.source.store()),
            _ => None,
        }
    }

    /// This element's liveness — the ONE question the render layer asks
    /// before painting rows. The element answers rather than the view,
    /// because the element is what knows where its rows came from: over a
    /// CLIENT-owned query the connection is irrelevant, and consulting it
    /// would blank the contexts list exactly when the daemon is unreachable
    /// and you need to pick a different context.
    pub fn liveness(&self, conn: &crate::connection::Connection) -> crate::Liveness {
        if self.is_client_owned() {
            crate::Liveness::of_store(self.data_state())
        } else {
            crate::Liveness::of(conn, self.data_state())
        }
    }

    /// Whether this element's rows are seeded in-process rather than fed by
    /// a daemon subscription. Derived kinds inherit the answer from the
    /// source they ride.
    ///
    /// Wildcard-free, because [`Self::liveness`] branches on it: a kind that
    /// answered `false` wrongly would have its view blanked exactly when the
    /// daemon is unreachable, which is the bug the contexts list exists to
    /// survive. `_ => false` conflated "has no client-owned store" with "is
    /// a kind nobody has decided about yet", and the second is what a new
    /// variant is.
    pub fn is_client_owned(&self) -> bool {
        match self {
            Element::ResourceList(e) => e.query.is_client_owned(),
            // A filter/projection over a client-owned store is equally
            // independent of the connection.
            Element::RowFilter(e) => e.source.is_client_owned(),
            Element::DerivedRows(e) => e.source.is_client_owned(),
            // Daemon-fed content and logs; and the dialog kinds, which hold
            // no rows at all and so have no store to be independent of.
            Element::Overview(_)
            | Element::ContentView(_)
            | Element::LogSession(_)
            | Element::LogFilter(_)
            | Element::Confirm(_)
            | Element::Picker(_)
            | Element::Form(_) => false,
        }
    }

    /// Abort the live stream this element OWNS (if any), pre-empting a retry
    /// loop against a dead mux at the reconnect/switch choke point. Derived
    /// (filter / projection) kinds own no stream — they ride an ancestor's —
    /// so this is a no-op for them (the ancestor is aborted on its own turn in
    /// the walk). Overview / context / content kinds own no subscription.
    pub fn abort_data_stream(&self) {
        match self {
            Element::ResourceList(e) => e.query().abort(),
            Element::LogSession(s) => s.abort(),
            // EXHAUSTIVE, unlike the `_ => {}` this replaced. Every arm here
            // is a claim that the kind owns no stream, and a new kind that
            // DID own one would have inherited the no-op silently — leaking
            // it past the choke point this function exists to be. Owning a
            // stream is the kind of thing a new element is likely to do, so
            // the compiler should ask.
            Element::RowFilter(_)
            | Element::DerivedRows(_)
            | Element::LogFilter(_)
            | Element::ContentView(_)
            | Element::Overview(_)
            | Element::Confirm(_)
            | Element::Picker(_)
            | Element::Form(_) => {}
        }
    }

    // -- Interaction state ----------------------------------------------------

    /// Persistent table-interaction state — `Some` for the row-bearing
    /// kinds only. Non-table kinds keep their own state shapes
    /// ([`LogInteraction`], [`crate::ContentViewState`], the context
    /// table).
    pub fn table_interaction(&self) -> Option<&TableInteraction> {
        match self {
            Element::ResourceList(e) => Some(&e.interaction),
            Element::RowFilter(e) => Some(&e.interaction),
            Element::DerivedRows(e) => Some(&e.interaction),
            _ => None,
        }
    }

    pub fn table_interaction_mut(&mut self) -> Option<&mut TableInteraction> {
        match self {
            Element::ResourceList(e) => Some(&mut e.interaction),
            Element::RowFilter(e) => Some(&mut e.interaction),
            Element::DerivedRows(e) => Some(&mut e.interaction),
            _ => None,
        }
    }

    /// Whether covering this element should drop its ephemeral view memo.
    pub(crate) fn drop_view_cache(&mut self) {
        if let Some(it) = self.table_interaction_mut() {
            it.drop_cache();
        } else if let Some(lv) = self.log_view_mut() {
            lv.drop_cache();
        }
    }

    pub fn filter_input(&self) -> &FilterInputState {
        static EMPTY: std::sync::LazyLock<FilterInputState> =
            std::sync::LazyLock::new(FilterInputState::default);
        self.table_interaction().map(|i| &i.filter_input).unwrap_or(&EMPTY)
    }

    pub fn filter_input_mut(&mut self) -> Option<&mut FilterInputState> {
        self.table_interaction_mut().map(|i| &mut i.filter_input)
    }

    // -- The ephemeral view ----------------------------------------------------

    /// Materialize (or reuse) the view: the single pipeline feeding
    /// render, selection reads, and export. Pure over (element, stores);
    /// memoized on the derive key; cleared when covered.
    pub fn view(&mut self, level: ColumnLevel, max_col_width: u16) -> Arc<PreparedView> {
        match self {
            Element::Confirm(_)
            | Element::Picker(_)
            | Element::Form(_)
            | Element::LogSession(_)
            | Element::LogFilter(_)
            | Element::ContentView(_)
            | Element::Overview(_) => {
                unreachable!("view() is table-kind-only; the draw dispatch matches kinds")
            }
            Element::ResourceList(e) => {
                let store = Arc::clone(&e.query.store);
                view_over_store(&store, &e.source, &e.policy, &mut e.interaction, level, max_col_width)
            }
            Element::RowFilter(e) => {
                let store = Arc::clone(e.source.store());
                view_over_store(&store, &e.source, &e.policy, &mut e.interaction, level, max_col_width)
            }
            Element::DerivedRows(e) => {
                let key = DeriveKey {
                    generation: e.source.store().generation(),
                    metrics_version: 0,
                    sort: e.interaction.sort,
                    draft: e.interaction.filter_input.text().to_string(),
                    level,
                    max_col_width,
                };
                if let Some((k, v)) = &e.interaction.cache {
                    if *k == key {
                        return Arc::clone(v);
                    }
                }
                // Live projection: find the parent row by identity, project
                // its current data. Row gone → honest empty view.
                let projected: Vec<ResourceRow> = e.source.store().with_read(|inner| {
                    inner
                        .rows
                        .iter()
                        .find(|r| crate::store::row_matches_key(r, &e.key))
                        .map(|r| crate::derived::project(&e.kind, r))
                        .unwrap_or_default()
                });
                let draft = compile_draft(&e.interaction.filter_input);
                let visible: Vec<usize> = (0..e.headers.len()).collect();
                let headers: Vec<&str> = e.headers.iter().map(String::as_str).collect();
                let view = Arc::new(derive_view(
                    &projected,
                    &e.rules,
                    None,
                    &DeriveSpec {
                        predicates: &[],
                        draft: draft.as_ref(),
                        sort: e.interaction.sort,
                        visible_cols: &visible,
                        headers: &headers,
                        // Projections declare no columns, so every one takes
                        // the global fallback.
                        max_col_widths: &vec![max_col_width; headers.len()],
                    },
                ));
                e.interaction.cache = Some((key, Arc::clone(&view)));
                view
            }
        }
    }

    /// Warm the memoized view cache if this is a table-kind element (no-op
    /// otherwise). Pure OPTIMIZATION: selection reads are total on their own
    /// (cold cache → `derive_selection_keys` fallback), but warming here
    /// memoizes the full view once so the action's resolution and the next
    /// paint share one derive instead of running two. Also gives relative
    /// cursor moves (`view_len`) something to clamp against pre-paint.
    /// Cheap: `view()` returns the memo when the cache is already warm.
    pub fn ensure_view_cached(&mut self, level: ColumnLevel, max_col_width: u16) {
        if self.is_table() {
            let _ = self.view(level, max_col_width);
        }
    }

    /// Lifecycle state for the loading/error chrome.
    pub fn data_state(&self) -> TableDataState {
        match self.data_store() {
            Some(store) => store.with_read(|i| i.state.clone()),
            None => TableDataState::Ready,
        }
    }

    /// `filtered/total` for the count chrome, from the last materialized
    /// view (what the user is looking at).
    pub fn counts(&self) -> ItemCounts {
        match self.table_interaction().and_then(|i| i.cached_view()) {
            Some(v) => ItemCounts { filtered: v.keys.len(), total: v.total_rows },
            None => ItemCounts { filtered: 0, total: 0 },
        }
    }

    /// The last materialized view (what was last painted), if any —
    /// clipboard export and save read THIS, so what you copy is exactly
    /// what you see (the element's own columns, filter, sort, and
    /// metrics-overlaid cell values).
    pub fn last_view(&self) -> Option<Arc<PreparedView>> {
        self.table_interaction().and_then(|i| i.cached_view()).cloned()
    }

    /// The element's current column headers (full data set, not just
    /// visible) — for header-name → data-index lookups (form defaults,
    /// overlay drills).
    pub fn headers_snapshot(&self) -> Vec<String> {
        match self {
            Element::DerivedRows(e) => e.headers.clone(),
            _ => self
                .data_store()
                .map(|s| s.with_read(|i| i.headers.clone()))
                .unwrap_or_default(),
        }
    }

    // -- Selection / cursor (act through the LAST PAINTED view — the user
    //    acts on what they see; with none painted, through what the NEXT
    //    paint will show) -----------------------------------------------------

    /// The screen-order identities a FRESH derive would produce — the
    /// fallback that makes selection reads total when no painted view
    /// exists (the memo is dropped on cover, absent on new elements).
    /// Mirrors `view()`'s per-kind data plumbing minus presentation,
    /// which never affects row order. `None` for non-table kinds.
    pub(crate) fn derive_selection_keys(&self) -> Option<Vec<ObjectKey>> {
        use crate::store::derive_key_order;
        let it = self.table_interaction()?;
        let draft = compile_draft(&it.filter_input);
        let level = it.derived_level;
        let policy = self.column_policy();
        match self {
            Element::ResourceList(e) => {
                let lens = e.source.metrics().map(|m| m.lens());
                Some(e.query.store.with_read(|inner| {
                    let visible = policy.visible_indices(&inner.headers, level);
                    derive_key_order(
                        &inner.rows,
                        lens.as_ref(),
                        e.source.predicates(),
                        draft.as_ref(),
                        it.sort,
                        &visible,
                    )
                }))
            }
            Element::RowFilter(e) => {
                let lens = e.source.metrics().map(|m| m.lens());
                Some(e.source.store().with_read(|inner| {
                    let visible = policy.visible_indices(&inner.headers, level);
                    derive_key_order(
                        &inner.rows,
                        lens.as_ref(),
                        e.source.predicates(),
                        draft.as_ref(),
                        it.sort,
                        &visible,
                    )
                }))
            }
            Element::DerivedRows(e) => {
                let projected: Vec<ResourceRow> = e.source.store().with_read(|inner| {
                    inner
                        .rows
                        .iter()
                        .find(|r| crate::store::row_matches_key(r, &e.key))
                        .map(|r| crate::derived::project(&e.kind, r))
                        .unwrap_or_default()
                });
                {
                    // Projections declare no columns — every one is visible.
                    let visible: Vec<usize> = (0..projected.first().map_or(0, |r| r.cells.len())).collect();
                    Some(derive_key_order(&projected, None, &[], draft.as_ref(), it.sort, &visible))
                }
            }
            _ => None,
        }
    }

    /// Identity of the row under the cursor. TOTAL for table kinds: acts
    /// through the last painted view when one exists; when none does, it
    /// resolves against a fresh derive of the same order — a cold memo
    /// right after a nav reveal is no longer a silent no-op.
    pub fn selected_key(&self) -> Option<ObjectKey> {
        let it = self.table_interaction()?;
        if let Some(view) = it.cached_view() {
            return view.keys.get(it.clamped_selected(view.keys.len())).cloned();
        }
        let keys = self.derive_selection_keys()?;
        keys.get(it.clamped_selected(keys.len())).cloned()
    }

    /// The full row under the cursor, resolved by identity against the
    /// element's data (store rows, or the live projection).
    pub fn selected_row(&self) -> Option<ResourceRow> {
        let key = self.selected_key()?;
        match self {
            Element::ResourceList(_) | Element::RowFilter(_) => {
                self.data_store()?.with_read(|inner| {
                    inner.rows.iter().find(|r| crate::store::row_matches_key(r, &key)).cloned()
                })
            }
            Element::DerivedRows(e) => e.source.store().with_read(|inner| {
                inner
                    .rows
                    .iter()
                    .find(|r| crate::store::row_matches_key(r, &e.key))
                    .map(|parent| crate::derived::project(&e.kind, parent))
                    .and_then(|rows| {
                        rows.into_iter().find(|r| crate::store::row_matches_key(r, &key))
                    })
            }),
            _ => None,
        }
    }

    fn view_len(&self) -> usize {
        self.table_interaction()
            .and_then(|i| i.cached_view())
            .map(|v| v.keys.len())
            .unwrap_or(0)
    }

    // Every move re-anchors the stored cursor to the position the user
    // SEES (`clamped_selected`) before applying the step. A cursor carried
    // from a longer view — a child seeded from a deep parent position, or
    // data that shrank underneath — would otherwise sit out of range and
    // burn invisible keypresses re-entering it. Moves are interaction
    // time, so writing the normalized value back here is exactly the
    // doctrine ("only interaction-time moves mutate these fields"); an
    // empty view leaves the cursor untouched (nothing to move over, and
    // the position should survive a transient refresh window).

    pub fn select_next(&mut self) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        if len == 0 { return; }
        it.selected = (it.clamped_selected(len) + 1).min(len - 1);
        it.adjust_offset();
    }

    pub fn select_prev(&mut self) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        if len == 0 { return; }
        it.selected = it.clamped_selected(len).saturating_sub(1);
        it.adjust_offset();
    }

    pub fn page_up(&mut self) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        if len == 0 { return; }
        it.selected = it.clamped_selected(len).saturating_sub(it.viewport.viewport_rows());
        it.adjust_offset();
    }

    pub fn page_down(&mut self) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        if len == 0 { return; }
        it.selected = (it.clamped_selected(len) + it.viewport.viewport_rows()).min(len - 1);
        it.adjust_offset();
    }

    pub fn go_home(&mut self) {
        let Some(it) = self.table_interaction_mut() else { return };
        it.selected = 0;
        it.viewport.reveal(0);
    }

    pub fn go_end(&mut self) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        if len > 0 {
            it.selected = len - 1;
        }
        it.adjust_offset();
    }

    pub fn select(&mut self, idx: usize) {
        let len = self.view_len();
        let Some(it) = self.table_interaction_mut() else { return };
        it.selected = if len == 0 { 0 } else { idx.min(len - 1) };
        it.adjust_offset();
    }

    pub fn col_left(&mut self) {
        let Some(it) = self.table_interaction_mut() else { return };
        it.selected_col = it.selected_col.saturating_sub(1);
    }

    pub fn col_right(&mut self) {
        let num_cols = self
            .table_interaction()
            .and_then(|i| i.cached_view())
            .map(|v| v.visible_cols.len())
            .unwrap_or(0);
        let Some(it) = self.table_interaction_mut() else { return };
        if num_cols > 0 && it.selected_col + 1 < num_cols {
            it.selected_col += 1;
        }
    }

    /// Jump the column cursor to the first (leftmost) column — vim `0`.
    pub fn col_first(&mut self) {
        let Some(it) = self.table_interaction_mut() else { return };
        it.selected_col = 0;
    }

    /// Jump the column cursor to the last (rightmost) column — vim `$`.
    pub fn col_last(&mut self) {
        let num_cols = self
            .table_interaction()
            .and_then(|i| i.cached_view())
            .map(|v| v.visible_cols.len())
            .unwrap_or(0);
        let Some(it) = self.table_interaction_mut() else { return };
        if num_cols > 0 {
            it.selected_col = num_cols - 1;
        }
    }

    /// The cursor column as (DATA index, header) — the single source for
    /// `~` column-grep and `S` sort-by-column. Reads the same visible set
    /// the renderer used, so a mis-map is unrepresentable.
    pub fn selected_data_col(&self) -> Option<(usize, String)> {
        let it = self.table_interaction()?;
        // `selected_col` is a VISIBLE index; cells are addressed by DATA
        // index. The clamp and the lookup are the bridge's, not this
        // function's — see `PreparedView::visible_cols`.
        it.cached_view()?.data_col(it.selected_col)
    }

    // -- Marks -------------------------------------------------------------------
    //
    // Marks live in one of two places, and WHICH is a property of where the
    // rows come from rather than a choice:
    //
    // - A store-backed list (ResourceList / RowFilter) keys its marks on the
    //   shared `RowStore`. That is what lets a filter hide a row from view
    //   without dropping it from the selection, and what prunes a mark
    //   atomically with the delta that removes its row.
    // - A PROJECTION (DerivedRows) has no such store — its rows are derived
    //   from one parent row, per frame — so it owns its marks, and they die
    //   with the element, which is exactly the lifetime its rows have. The
    //   prune that can't happen is replaced by an intersection at READ time
    //   (`live_keys`), so a container that vanished leaves no mark anyone
    //   can observe.

    /// The keys a PROJECTION currently yields, independent of any filter or
    /// painted frame — the set its marks are meaningful against.
    ///
    /// Identity here is `(namespace, name)`, for the parent and the projected
    /// rows alike, because that is all a `ResourceRow` carries. So if the
    /// parent pod is deleted and recreated under the same name while this
    /// view is open, a mark on `app` re-attaches to the new pod's `app`.
    /// That is deliberate rather than overlooked: the element's own origin is
    /// name-keyed too, so the view is already showing the new pod — a
    /// selection that followed the object instead of the name would have to
    /// empty itself under a table that visibly still has rows.
    fn projected_keys(&self) -> Vec<ObjectKey> {
        let Element::DerivedRows(e) = self else { return Vec::new() };
        e.source.store().with_read(|inner| {
            inner
                .rows
                .iter()
                .find(|r| crate::store::row_matches_key(r, &e.key))
                .map(|parent| {
                    crate::derived::project(&e.kind, parent)
                        .iter()
                        .map(crate::store::row_key)
                        .collect()
                })
                .unwrap_or_default()
        })
    }

    /// The keys this element currently offers, in view order: the last
    /// PAINTED view when there is one, else a fresh derive of the same —
    /// the totality contract `selected_key` uses.
    fn live_keys(&self) -> Vec<ObjectKey> {
        match self.table_interaction().and_then(|i| i.cached_view()) {
            Some(view) => view.keys.clone(),
            None => self.derive_selection_keys().unwrap_or_default(),
        }
    }

    /// Whether marking rows here can lead to anything.
    ///
    /// Row-bearing is necessary but not sufficient: select mode exists to
    /// feed BATCH operations, so a resource that declares none has no
    /// consumer for a mark. The contexts view is exactly that — marking a
    /// context did nothing except silently enter select mode, which then
    /// blocked `Enter`, the picker's only function, on the one screen a
    /// first-run user is forced onto.
    pub fn markable(&self) -> bool {
        // Any BATCH-CAPABLE operation makes marking meaningful, not just the
        // per-item ones: containers declare no Delete and no Restart, and
        // their marks exist entirely to say which containers the ONE merged
        // log stream is over.
        fn feeds_a_batch(ops: &[k9rs_proto::protocol::OperationKind]) -> bool {
            ops.iter().any(|op| {
                !matches!(op.batch_support(), k9rs_proto::protocol::BatchSupport::SingleOnly)
            })
        }
        match self {
            Element::ResourceList(_) | Element::RowFilter(_) => match self.rid() {
                Some(rid) => feeds_a_batch(&rid.capabilities().operations),
                None => false,
            },
            Element::DerivedRows(e) => feeds_a_batch(&e.kind.operations()),
            _ => false,
        }
    }

    /// Toggle the mark on the row under the cursor.
    pub fn toggle_mark(&mut self) -> MarkOutcome {
        if !self.markable() {
            return MarkOutcome::Unsupported;
        }
        let Some(key) = self.selected_key() else { return MarkOutcome::RowGone };
        if let Element::DerivedRows(e) = self {
            // The key came from a row that exists right now, so there is no
            // "row gone" case to distinguish here.
            if !e.marked.remove(&key) {
                e.marked.insert(key);
            }
            return MarkOutcome::Toggled;
        }
        let Some(store) = self.data_store() else { return MarkOutcome::Unsupported };
        match store.toggle_mark(&key) {
            Some(_) => MarkOutcome::Toggled,
            None => MarkOutcome::RowGone,
        }
    }

    /// Span-mark, at block granularity and symmetric with Space's
    /// per-row toggle — the state of the row UNDER THE CURSOR decides
    /// the direction:
    /// - cursor row unmarked → mark the span from the nearest VISIBLE
    ///   mark to the cursor (over the derived/visible order);
    /// - cursor row marked → unmark the contiguous marked block
    ///   containing the cursor.
    pub fn span_mark(&mut self) -> SpanOutcome {
        if !self.markable() {
            return SpanOutcome::Unsupported;
        }
        let keys = self.live_keys();
        if keys.is_empty() {
            return SpanOutcome::NoAnchor;
        }
        let Some(current) = self.table_interaction().map(|i| i.clamped_selected(keys.len()))
        else {
            return SpanOutcome::NoAnchor;
        };
        let marked = self.marked_snapshot();

        // Which rows the span covers is decided identically either way; only
        // the write differs.
        let span = if marked.contains(&keys[current]) {
            // Unmark the contiguous marked block around the cursor.
            let mut start = current;
            while start > 0 && marked.contains(&keys[start - 1]) {
                start -= 1;
            }
            let mut end = current;
            while end + 1 < keys.len() && marked.contains(&keys[end + 1]) {
                end += 1;
            }
            Err(start..=end)
        } else {
            // Span needs a VISIBLE anchor. With none — no marks at all, or
            // marks that this element's filter hides — refuse: falling back
            // to row 0 would silently bulk-mark from the top of the view and
            // grow an invisible marked set as a side effect of a miss.
            let Some(anchor) = keys
                .iter()
                .enumerate()
                .filter(|(_, k)| marked.contains(*k))
                .map(|(pos, _)| pos)
                .min_by_key(|&pos| (pos as isize - current as isize).unsigned_abs())
            else {
                return SpanOutcome::NoAnchor;
            };
            Ok(if anchor <= current { anchor..=current } else { current..=anchor })
        };

        if let Element::DerivedRows(_) = self {
            // Filtered to rows the projection actually yields, mirroring the
            // store side's `mark_keys` (which intersects with present rows
            // and says why). Extending straight from the painted view banked
            // keys that no longer had a row behind them.
            let present: HashSet<ObjectKey> = self.projected_keys().into_iter().collect();
            let Element::DerivedRows(e) = self else { unreachable!("matched above") };
            match span {
                Ok(add) => {
                    e.marked.extend(keys[add].iter().filter(|k| present.contains(*k)).cloned())
                }
                Err(remove) => {
                    for k in &keys[remove] {
                        e.marked.remove(k);
                    }
                }
            }
            return SpanOutcome::Applied;
        }
        let Some(store) = self.data_store() else { return SpanOutcome::Unsupported };
        match span {
            Ok(add) => store.mark_keys(keys[add].iter().cloned()),
            Err(remove) => store.unmark_keys(keys[remove].iter()),
        }
        SpanOutcome::Applied
    }

    /// Clear all marks. `false` = this element kind has no markable rows
    /// (same teaching flash as Toggle/Span — the three marking keys must
    /// not disagree about where marking exists).
    pub fn clear_marks(&mut self) -> bool {
        if !self.markable() {
            return false;
        }
        match self {
            Element::DerivedRows(e) => e.marked.clear(),
            _ => {
                if let Some(store) = self.data_store() {
                    store.clear_marks();
                }
            }
        }
        // A DELIBERATE clear is not a stale frame. Without this, `Ctrl-\`
        // then `Ctrl-D` inside one key-drain (up to 200 keys between paints)
        // hit the select-mode race guard and answered "Selection changed —
        // the marked rows are gone (nothing was done)" to a user who had just
        // cleared them on purpose. The guard exists for marks that vanished
        // ASYNCHRONOUSLY; this is the one path that empties them synchronously.
        if let Some(it) = self.table_interaction_mut() {
            it.rendered_select_mode = false;
        }
        true
    }

    pub fn has_marks(&self) -> bool {
        if !self.markable() {
            return false;
        }
        match self {
            // Cheapest honest answer: a projection's marks are only real
            // where a row still backs them.
            Element::DerivedRows(_) => !self.marked_snapshot().is_empty(),
            _ => self.data_store().map(|s| s.has_marks()).unwrap_or(false),
        }
    }

    pub fn marked_keys(&self) -> Vec<ObjectKey> {
        if !self.markable() {
            return Vec::new();
        }
        match self {
            Element::DerivedRows(_) => {
                let marked = self.marked_snapshot();
                self.live_keys().into_iter().filter(|k| marked.contains(k)).collect()
            }
            _ => self.data_store().map(|s| s.marked_keys()).unwrap_or_default(),
        }
    }

    /// Marked count without cloning the set (for the `[N selected]`
    /// chrome — the empty-table branch used to clone the whole set just
    /// to read `.len()`).
    pub fn marked_count(&self) -> usize {
        if !self.markable() {
            return 0;
        }
        match self {
            Element::DerivedRows(_) => self.marked_snapshot().len(),
            _ => self.data_store().map(|s| s.with_read(|i| i.marked_count())).unwrap_or(0),
        }
    }

    /// Marks for render-time row styling (cloned — small sets, per-frame).
    pub fn marked_snapshot(&self) -> HashSet<ObjectKey> {
        if !self.markable() {
            return HashSet::new();
        }
        match self {
            // A projection's marks are intersected with the rows it
            // currently yields: there is no delta to prune them, so a
            // container that disappeared must simply stop counting.
            //
            // Against the PROJECTION, not the painted view. `live_keys()` is
            // the last painted order, which has the draft filter applied — so
            // typing `/c0` while two containers were marked silently dropped
            // the other one from the selection, and typing a draft that
            // matched neither ended select mode outright. A filter hides rows
            // from view; it does not deselect them. That is the rule the
            // store-backed side has always followed.
            Element::DerivedRows(e) => {
                if e.marked.is_empty() {
                    return HashSet::new();
                }
                let live: HashSet<ObjectKey> = self.projected_keys().into_iter().collect();
                e.marked.intersection(&live).cloned().collect()
            }
            _ => self
                .data_store()
                .map(|s| s.with_read(|i| i.marked().clone()))
                .unwrap_or_default(),
        }
    }

    /// Recently-changed rows for flash styling (identity → change time).
    pub fn changed_snapshot(&self) -> std::collections::HashMap<ObjectKey, Instant> {
        match self {
            Element::ResourceList(_) | Element::RowFilter(_) => self
                .data_store()
                .map(|s| s.with_read(|i| i.flash.changed_rows().clone()))
                .unwrap_or_default(),
            _ => std::collections::HashMap::new(),
        }
    }

    // -- Sort ----------------------------------------------------------------

    /// Toggle sort on a target column (same column → flip direction).
    ///
    /// Returns `false` when the target does not exist in THIS table — a
    /// semantic target names a header, and not every table has every header.
    /// Refusing is the honest answer: the previous positional guess always
    /// found *a* column, which is why `N` silently sorted Events by TYPE.
    pub fn sort_by(&mut self, target: crate::SortTarget) -> bool {
        let col = match target {
            crate::SortTarget::Column(c) => c,
            // Resolved HERE, not at key-mapping time. `selected_data_col`
            // reads the last painted view, which is cold on a fresh element
            // and dropped on cover — and keys are drained in a batch with no
            // paint between them, so `Enter` then `S` used to map to no
            // action at all and the key simply vanished. By dispatch the
            // action layer has warmed the view, exactly as `~` relies on.
            crate::SortTarget::CursorColumn => match self.selected_data_col() {
                Some((c, _)) => c,
                None => return false,
            },
            crate::SortTarget::Named(header) => {
                match self
                    .headers_snapshot()
                    .iter()
                    .position(|h| h.eq_ignore_ascii_case(header))
                {
                    Some(c) => c,
                    None => return false,
                }
            }
        };
        let Some(it) = self.table_interaction_mut() else { return false };
        if it.sort.col == col {
            it.sort.ascending = !it.sort.ascending;
        } else {
            it.sort = SortSpec { col, ascending: true };
        }
        true
    }

    pub fn toggle_sort(&mut self) {
        let Some(col) = self.table_interaction().map(|i| i.sort.col) else { return };
        let _ = self.sort_by(crate::SortTarget::Column(col));
    }

    // -- Liveness ----------------------------------------------------------------

    /// Whether this element OWNS a live subscription right now. Elements
    /// without their own stream (filters, projections) report `true` —
    /// their data rides an ancestor's stream, revived by the stack's
    /// owner walk.
    pub fn is_live(&self) -> bool {
        match self {
            Element::ResourceList(e) => e.query.is_live(),
            // EXHAUSTIVE, and the arms are grouped by WHY they are live, not
            // lumped under a `_ => true`. "Assume live" is the unsafe
            // default: it is what suppresses the stale banner, so a new kind
            // that owned a stream would have claimed to be tracking the
            // cluster while its subscription was dead.
            //
            // Riding an ancestor's stream — revived by the stack's owner
            // walk, so liveness is the ancestor's to report.
            Element::RowFilter(_) | Element::DerivedRows(_) | Element::LogFilter(_) => true,
            // Nothing to be stale ABOUT: a one-shot fetch carries its own
            // phase (`ContentPhase`), a log session's liveness is the link's
            // (`Liveness::of_link`), and the rest are chrome or dialogs.
            Element::ContentView(_)
            | Element::LogSession(_)
            | Element::Overview(_)
            | Element::Confirm(_)
            | Element::Picker(_)
            | Element::Form(_) => true,
        }
    }
}

/// Build the committed predicate for the filter input's current state.
pub(crate) fn predicate_for_commit(text: String, column: Option<(usize, String)>) -> RowPredicate {
    match column {
        Some((col, header)) => RowPredicate::ColumnGrep {
            pattern: CompiledGrep::new(text),
            col,
            header,
        },
        None => RowPredicate::Grep(CompiledGrep::new(text)),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/element.rs"]
mod tests;
