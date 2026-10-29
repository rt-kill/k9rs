//! Log elements: a stream, the greps narrowed over it, and the viewport
//! state that reads them.
//!
//! `LogSession` owns the `LineStore` and the `LogStream` bridge;
//! `LogFilter` accumulates the grep chain, where each step carries every
//! pattern above it. `Sense`/`Narrowing` make "this pattern excludes" a
//! declared property rather than something spelled into the pattern — the
//! `regex` crate has no look-around, so exclusion could not be a pattern.
//!
//! Split out of `mod.rs` when it was 3063 lines holding 24 types. The seam is
//! measured, not guessed: this family and `super::table` reference each other
//! ZERO times in either direction — all 16 type names checked both ways.

use std::sync::Arc;

use k9rs_util::CompiledGrep;
use crate::store::LineStore;
// Only `LogSession::for_test` names this, and that is feature-gated — so the
// import has to be too, or `cargo build` and `cargo doc` call it unused while
// `--all-targets` needs it.
#[cfg(any(test, feature = "testing"))]
use k9rs_proto::protocol::Namespace;
use crate::client::{ClientSession, LogStream};
use k9rs_util::SearchPattern;

// ---------------------------------------------------------------------------
// Log elements — a live line stream and predicate refinements over it
// ---------------------------------------------------------------------------

/// Persistent interaction state for log-bearing elements (scroll, follow,
/// display toggles, the draft filter being typed). The visible-line set is
/// the ephemeral view: derived on read, memoized, droppable.
///
/// Named for what it is, and to pair with `TableInteraction`: it used to be
/// `LogViewState`, which is also the name of the log WIDGET's per-draw
/// parameter block (`ui::widgets::LogViewState`) — two different things one
/// `use` away from each other.
#[derive(Debug)]
pub struct LogInteraction {
    /// Vertical scroll relationship, in PHYSICAL (wrap-expanded) rows — owned
    /// by the render pass, which alone knows the width and wrap layout. Carries
    /// the follow/autoscroll flag too.
    pub viewport: crate::viewport::Viewport,
    /// Horizontal panning, in display columns. Its own type because it is the
    /// other axis, not another viewport — see `viewport::Pan`.
    pub pan: crate::viewport::Pan,
    pub wrap: bool,
    /// How many chrome roles are hidden — see
    /// [`k9rs_proto::protocol::Role::SHED_ORDER`]. ONE number rather than a
    /// boolean per role: two booleans are four states for three meaningful
    /// ones, and the unreachable fourth (time without attribution) was a
    /// state the keys could never produce anyway.
    pub shed: usize,
    /// True during the initial tail fetch — the render path skips
    /// follow-mode auto-scroll so the view doesn't jump as the tail
    /// streams in.
    pub initial_load: bool,
    /// Draft filter text; `Some` = the filter bar is open. Editable with a
    /// caret — see [`crate::text_input::TextInput`].
    pub draft: Option<crate::text_input::TextInput>,
    pub(super) cache: Option<(LogDeriveKey, Arc<[usize]>)>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct LogDeriveKey {
    pub(super) generation: u64,
    pub(super) draft: Option<String>,
}

impl LogInteraction {
    fn from_config(cfg: &crate::LogConfig, follow: bool) -> Self {
        Self {
            viewport: if follow {
                crate::viewport::Viewport::tailing()
            } else {
                crate::viewport::Viewport::default()
            },
            pan: crate::viewport::Pan::default(),
            wrap: cfg.default_wrap,
            shed: cfg.initial_shed(),
            initial_load: true,
            draft: None,
            cache: None,
        }
    }

    /// The view for a freshly pushed grep: the parent's display PREFERENCES,
    /// none of its position.
    ///
    /// A filter owns its own store, so the parent's scroll position names
    /// nothing here — not a row (the filtered content has a different extent)
    /// and not a line (the child's sequence numbers are its own, since it
    /// counts only the lines it retained). `restarted()` is the existing
    /// answer to exactly this question, asked by the `since`-range restart:
    /// same surface, new content, keep what the user chose.
    pub(super) fn seeded_from(parent: &LogInteraction) -> Self {
        Self {
            viewport: parent.viewport.restarted(),
            // The pan IS carried, unlike the vertical position: every line in
            // the child wears the same prefix as its parent, so "I have shifted
            // past the chrome" is still true of the narrowed view.
            pan: parent.pan,
            wrap: parent.wrap,
            shed: parent.shed,
            initial_load: false,
            draft: None,
            cache: None,
        }
    }

    pub(crate) fn drop_cache(&mut self) {
        self.cache = None;
    }

    pub fn is_filtering(&self) -> bool {
        self.draft.is_some()
    }
}

/// What a chain step does with the lines its pattern matches.
///
/// The chain could only ever narrow TOWARD something: `/health` shows you the
/// health checks and nothing hid them. That was not a missing feature so much
/// as a wall — vim's negative look-around `\@!` is refused by name in
/// [`k9rs_util`], and `regex` 1.x has no look-around AT ALL, by design (it
/// is a finite-automaton engine, which is what makes a pathological pattern
/// compile promptly instead of exploding). So exclusion cannot be a property
/// of the pattern. It is a property of the STEP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sense {
    /// Keep the lines this pattern matches — `/pattern`.
    Keep,
    /// Drop them — `/!pattern`. Spelled with `!` because `less` spells it
    /// `&!pattern`, and `less` is what everyone has already read a log in.
    Drop,
}

/// One step of the committed grep chain: a pattern and what to do with it.
#[derive(Debug, Clone)]
pub(crate) struct Narrowing {
    pattern: Arc<CompiledGrep>,
    sense: Sense,
}

impl Narrowing {
    /// Whether this step lets a line through, given a tester that says
    /// whether the line matches a pattern. The tester is the caller's,
    /// because WHICH parts of a line a grep sees is `Role::searchable`'s to
    /// say and not this type's.
    pub(crate) fn admits(&self, matches: impl Fn(&k9rs_util::SearchPattern) -> bool) -> bool {
        let hit = matches(self.pattern.pattern());
        match self.sense {
            Sense::Keep => hit,
            Sense::Drop => !hit,
        }
    }

    /// Why this step's pattern is matching literally rather than as a
    /// regex, if it is — the `[literal: …]` chip's input.
    pub(crate) fn fallback_reason(&self) -> Option<&str> {
        self.pattern.pattern().fallback_reason()
    }

    /// As the user typed it, `!` included — what the grep bar and the
    /// breadcrumb label themselves with.
    pub(crate) fn label(&self) -> String {
        match self.sense {
            Sense::Keep => self.pattern.source().to_string(),
            Sense::Drop => format!("!{}", self.pattern.source()),
        }
    }

    /// The pattern to HIGHLIGHT with, if this step is a reason a visible line
    /// is visible. A `Drop` step has none: every line it had anything to say
    /// about is gone, so highlighting its matches would paint the one thing
    /// that is certainly not on screen.
    pub(crate) fn highlight(&self) -> Option<&str> {
        match self.sense {
            Sense::Keep => Some(self.pattern.source()),
            Sense::Drop => None,
        }
    }
}

/// Split a raw draft into its [`Sense`] and the pattern text.
///
/// `!pat` excludes, `\!pat` is a literal `!pat`, anything else keeps.
///
/// Parsed HERE, before the vim translator ever sees the text, so that
/// grammar is untouched: `\!` is not a vim escape (the translator would
/// refuse it by name) and `regex` would reject `\!` as an unknown escape.
/// One prefix, consumed in one place, and the translator keeps speaking
/// exactly vim magic.
pub fn sense_of(draft: &str) -> (Sense, &str) {
    if let Some(rest) = draft.strip_prefix('!') {
        (Sense::Drop, rest)
    } else if draft.starts_with("\\!") {
        // Strip the BACKSLASH only — the `!` is the thing being protected,
        // so it stays in the pattern. Dropping both would turn a search for
        // `!urgent` into a search for `urgent`.
        (Sense::Keep, &draft[1..])
    } else {
        (Sense::Keep, draft)
    }
}

/// One committed grep step, as the bar shows it.
///
/// A struct rather than a tuple because the third field is the one that was
/// missing: the renderer used to get `(label, kept)` and work the sense out
/// of the label, which cannot be done — see `LogFilter::own_sense`.
#[derive(Debug, Clone)]
pub struct ChainStep {
    /// As the user typed it, `!` included.
    pub label: String,
    /// Lines surviving this step.
    pub kept: usize,
    /// Narrowing or excluding.
    pub sense: Sense,
}

/// A composable line source: the backing [`LineStore`] plus the
/// accumulated committed grep chain — the log analogue of
/// [`RowSource`](crate::store::RowSource).
#[derive(Debug, Clone)]
pub(crate) struct LineSource {
    pub(super) store: Arc<LineStore>,
    pub(super) patterns: Vec<Narrowing>,
}

impl LineSource {
    fn new(store: Arc<LineStore>) -> Self {
        Self { store, patterns: Vec::new() }
    }

    pub(super) fn narrowed(&self, pattern: Arc<CompiledGrep>, sense: Sense) -> Self {
        let mut child = self.clone();
        child.patterns.push(Narrowing { pattern, sense });
        child
    }

    pub(crate) fn store(&self) -> &Arc<LineStore> {
        &self.store
    }
}

/// A live log stream: the element's own query spec (`LogInit`), its
/// stream + line store, and its display state.
pub struct LogSession {
    /// The wire-facing definition — restarts (`since` ranges) re-run THIS,
    /// and the header/label/filename derive from it. There is deliberately
    /// no second copy of "which pod, which container": the display identity
    /// used to be a `ContainerRef` beside this spec, and a fan-out target
    /// ("3 pods") is exactly the case where the two would disagree.
    spec: k9rs_proto::protocol::LogInit,
    stream: LogStream,
    pub(super) store: Arc<LineStore>,
    /// Tail size for rate restarts (from config at construction).
    tail_default: u64,
    pub(super) label: String,
    pub view: LogInteraction,
}

impl std::fmt::Debug for LogSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogSession").field("label", &self.label).finish_non_exhaustive()
    }
}

impl LogSession {
    /// Open a fresh log stream element.
    pub fn open(
        session: &ClientSession,
        spec: k9rs_proto::protocol::LogInit,
        cfg: &crate::LogConfig,
    ) -> Self {
        let store = LineStore::new(cfg.max_lines);
        let stream = session.stream_log_substream(
            // A newly opened log session has no predecessor.
            crate::client::Supersedes::Nothing,
            spec.clone(),
            Arc::clone(&store),
        );
        let label = log_label(&spec);
        Self {
            view: LogInteraction::from_config(cfg, spec.follow),
            tail_default: cfg.tail_lines,
            spec,
            stream,
            store,
            label,
        }
    }

    /// Display identity: (what we're logging, which container(s), range).
    pub fn header(&self) -> (String, String, String) {
        (
            self.spec.target.display(),
            self.spec.container.label().to_string(),
            // Derived from the two wire fields rather than stored twice: the
            // spec IS what we asked the daemon for.
            match (&self.spec.since, &self.spec.tail) {
                (Some(since), _) => since.label().to_string(),
                (None, None) => "all".to_string(),
                (None, Some(_)) => "tail".to_string(),
            },
        )
    }

    /// Restart the stream with a new time range (digits 0-6). Re-runs the
    /// element's OWN spec against the SAME store (children keep their
    /// handles); the raised epoch floor keeps the dead stream's queued
    /// lines out.
    pub fn restart_range(
        &mut self,
        session: &ClientSession,
        range: crate::actions::LogRange,
    ) {
        use crate::actions::LogRange;
        self.write_range(range.clone());
        self.spec.follow = true;
        self.store.clear();
        // A RANGE request keeps its head; a plain tail keeps its tail.
        //
        // The apiserver streams forward from the range's start, so the old
        // lines arrive first — and a tail-anchored ring evicted them with the
        // newer ones behind, leaving the newest N of a range asked for
        // precisely to see the oldest. Head retention also stops the stream
        // once full (see `revive_if_dead`'s sibling check in the session
        // loop), so we no longer pay to receive lines we will discard.
        // Anything that reaches BACKWARD keeps its head; a plain tail keeps
        // its tail.
        self.store.set_retain(match range {
            LogRange::Tail => crate::store::Retain::Tail,
            LogRange::Head | LogRange::Since(_) => crate::store::Retain::Head,
        });
        // The extent is gone with the store, but AUTOSCROLL IS A USER
        // PREFERENCE and survives. Unconditionally re-tailing here meant
        // pressing `s` to stop the scroll and then `2` to change the range
        // silently turned the scroll back on — a toggle undone by an
        // unrelated key is exactly what "finicky" feels like.
        self.view.viewport = self.view.viewport.restarted();
        self.view.initial_load = true;
        self.stream = session.stream_log_substream(
            crate::client::Supersedes::LogStream(&self.stream),
            self.spec.clone(),
            Arc::clone(&self.store),
        );
    }

    /// Stop a range fetch once we have the oldest N it was asked for.
    ///
    /// Dropping the handle aborts the bridge, which RSTs the substream; the
    /// daemon's drain already treats a closed reader as the normal end of a
    /// stream (`out.send(..).is_err()`), so the kubelet read stops with no
    /// wire change. Without this we keep receiving — and discarding — every
    /// line behind the window, which is most of them.
    pub(crate) fn stop_if_head_complete(&mut self) -> bool {
        if self.store.head_is_full() && self.stream.is_alive() {
            self.stream.abort();
            self.store.mark_ended(u64::MAX);
            return true;
        }
        false
    }

    /// Abort the log substream without minting a successor — the reconnect/
    /// switch choke-point counterpart to `revive_if_dead`. Idempotent.
    pub fn abort(&self) {
        self.stream.abort();
    }

    /// Re-establish the stream if it DIED — and only then.
    ///
    /// `is_alive() == false` stands for three different things: the transport
    /// died under us, the log legitimately ended (the container exited), or
    /// we stopped it ourselves (`Self::stop_if_head_complete`). The first
    /// wants recovery; the other two emphatically do not, and `restart_range`
    /// begins by CLEARING the store — so reviving a finished stream threw
    /// away a completed log and refetched a 100-line tail, and reviving a
    /// deliberately-stopped `Retain::Head` range discarded the oldest-N
    /// window the user had asked for and re-downloaded it, every time they
    /// popped back into the view.
    ///
    /// The discriminator already exists and needs no new state: both orderly
    /// endings run `mark_ended`, so the store reports `live = false`. A
    /// transport death is exactly the case where it still says `live = true`,
    /// because the bridge's `Ended` died in the closed channel — which is the
    /// symptom this function was written for ("renders forever as a live,
    /// following view"). Asking the store keeps the two facts from drifting.
    /// Returns whether it actually restarted — the caller uses that to reset
    /// the filters deriving from the store this just emptied.
    pub fn revive_if_dead(&mut self, session: &ClientSession) -> bool {
        if !self.needs_revival() {
            return false;
        }
        // Re-ask for the SAME range, read back off the spec — a revival must
        // not quietly change what you were looking at.
        self.restart_range(session, self.current_range());
        true
    }

    /// The range this stream is currently over, recovered from the two wire
    /// fields it was built from. The spec is the authority for what we asked
    /// the daemon for, so there is no second copy to drift.
    pub(crate) fn current_range(&self) -> crate::actions::LogRange {
        use crate::actions::LogRange;
        match (&self.spec.since, &self.spec.tail) {
            (Some(since), _) => LogRange::Since(*since),
            (None, None) => LogRange::Head,
            (None, Some(_)) => LogRange::Tail,
        }
    }

    /// The decision behind [`Self::revive_if_dead`], named so it can be
    /// tested without a live session: the stream is gone and it did NOT end
    /// in an orderly way. Both orderly endings — EOF, and our own
    /// `Self::stop_if_head_complete` — run `mark_ended`, which sets a
    /// STICKY flag. It has to be sticky: `live` is re-set by every arrival,
    /// and a stopped stream keeps delivering for a moment, so asking `live`
    /// meant the first straggler undid the decision.
    pub(crate) fn needs_revival(&self) -> bool {
        !self.stream.is_alive() && !self.store.has_ended()
    }

    /// Test-only: replace the stream with one whose bridge has already
    /// finished, i.e. the transport died under us.
    #[cfg(test)]
    pub(crate) fn kill_stream_for_test(&mut self) {
        self.stream = crate::test_support::finished_log_stream();
    }

    /// Turn the user's range choice into the two wire fields — THE mapping,
    /// and the only one.
    ///
    /// `restart_range` calls it and so does the test that checks it. It was
    /// briefly written out twice, once here and once in a `set_range_for_test`
    /// helper, which meant the test asserted that the COPY round-tripped: had
    /// `restart_range` changed, the guard would have gone on passing. The
    /// same "one decision, two copies" shape this codebase spent 2026-10-13
    /// deleting everywhere else, introduced in the test for it.
    pub(crate) fn write_range(&mut self, range: crate::actions::LogRange) {
        use crate::actions::LogRange;
        let (since, tail) = match &range {
            LogRange::Tail => (None, Some(self.tail_default)),
            // Both absent IS "everything" to the apiserver — the state
            // `Option<String>` could not express.
            LogRange::Head => (None, None),
            LogRange::Since(since) => (Some(*since), None),
        };
        self.spec.since = since;
        self.spec.tail = tail;
    }

    /// Test-only: a log session over a parked stream.
    #[cfg(any(test, feature = "testing"))]
    pub fn for_test(
        target: k9rs_proto::protocol::LogTarget,
        container: k9rs_proto::protocol::LogContainer,
    ) -> Self {
        let cfg = crate::LogConfig::default();
        let store = LineStore::new(cfg.max_lines);
        let spec = k9rs_proto::protocol::LogInit {
            target,
            namespace: Namespace::Named("default".to_string()),
            container,
            follow: true,
            tail: Some(cfg.tail_lines),
            since: None,
            previous: false,
        };
        let label = log_label(&spec);
        Self {
            view: LogInteraction::from_config(&cfg, true),
            tail_default: cfg.tail_lines,
            spec,
            stream: crate::test_support::parked_log_stream(),
            store,
            label,
        }
    }

    pub fn store(&self) -> &Arc<LineStore> {
        &self.store
    }

    pub(super) fn line_source(&self) -> LineSource {
        LineSource::new(Arc::clone(&self.store))
    }
}

impl LogFilter {
    /// This step's OWN label — the last of the accumulated chain, since a
    /// filter carries every pattern above it as well.
    /// Whether this step narrows or excludes.
    ///
    /// Carried rather than re-derived, because [`Narrowing::label`] CANNOT
    /// answer it: `\!foo` is a `Keep` whose label is `!foo`, which is the
    /// same string a `Drop` of `foo` produces. The grep bar re-read that
    /// leading `!` to pick its colour and so painted an escaped literal in
    /// the excluding colour, with no string it could have been given that
    /// would say otherwise.
    pub(crate) fn own_sense(&self) -> Sense {
        self.source.patterns.last().map(|n| n.sense).unwrap_or(Sense::Keep)
    }

    pub(crate) fn own_label(&self) -> String {
        self.source.patterns.last().map(Narrowing::label).unwrap_or_default()
    }

    /// How many lines this step retained.
    pub(crate) fn retained_len(&self) -> usize {
        self.retained.with_read(|i| i.lines.len())
    }

    /// Pull everything new from the parent, keep what matches, advance the
    /// cursor. Idempotent and cheap: O(lines arrived since last call), not
    /// O(lines resident) — the derive it replaces re-scanned the whole store
    /// with every pattern on every frame, because the memo key carries the
    /// store's global generation and any append bumps it.
    ///
    /// Runs on the session task like every other store access, and never
    /// holds two locks: the parent's read is collected into a batch first.
    pub(super) fn drain_from_parent(&mut self) {
        let parent = self.source.store();
        let (evicted, live, batch) = parent.with_read(|inner| {
            let start = self.cursor.max(inner.evicted);
            let end = inner.evicted + inner.lines.len() as u64;
            let matched: Vec<crate::store::StoredLine> = (start..end)
                .filter_map(|seq| inner.lines.get((seq - inner.evicted) as usize))
                .filter(|line| {
                    // Which PARTS a grep sees is `Role::searchable`'s to say,
                    // not this call site's. It used to be spelled out here and
                    // again in the draft derive — two copies of a rule that a
                    // third place (`Role`) also stated, which is how "the
                    // timestamp is not searchable" becomes true in one of them
                    // and not the others.
                    let hit = |p: &SearchPattern| {
                        line.segments()
                            .filter(|(role, ..)| role.searchable())
                            .any(|(_, text, _)| p.is_match(text))
                    };
                    // The FULL committed chain, against the parent's lines.
                    // Applying only our own pattern would rely on the parent
                    // having retained exactly its own matches; the chain is a
                    // conjunction and evaluating it whole keeps that true by
                    // construction, at a cost of one extra `is_match` per
                    // level on lines that are about to be kept anyway.
                    //
                    // Each step decides for itself whether a match is a
                    // reason to keep or to drop — see [`Sense`].
                    self.source.patterns.iter().all(|n| n.admits(|p| hit(p)))
                })
                .cloned()
                .collect();
            (inner.evicted, inner.live, matched)
        });
        // The parent threw lines away before we looked at them. We cannot
        // know how many of them matched — we never saw them — so this is a
        // flag the bar renders, not a count.
        if self.cursor < evicted {
            self.lost_history = true;
        }
        self.cursor = evicted + parent.with_read(|i| i.lines.len() as u64);
        self.retained.extend_derived(batch);
        self.retained.set_live(live);
    }

    /// Whether the parent evicted matches we never got to see.
    /// Start over against a parent whose store was just cleared.
    ///
    /// `restart_range` replaces the session's lines wholesale, but a filter's
    /// `retained` is its OWN store and survives — and since `clear()` leaves
    /// `cursor == evicted`, the next drain appended the new range's matches
    /// onto the old range's. A reconnect under a grep therefore duplicated
    /// every retained match, and the duplicates evicted real history to make
    /// room for themselves.
    pub(crate) fn restarted_from_parent(&mut self) {
        self.retained.clear();
        self.cursor = self.source.store().with_read(|i| i.evicted);
        self.lost_history = false;
    }

    /// The store this filter DERIVES from (the session's), not the one it
    /// owns — the identity a restart is announced by.
    pub(crate) fn source_store(&self) -> &Arc<LineStore> {
        self.source.store()
    }

    pub(crate) fn lost_history(&self) -> bool {
        self.lost_history
    }

    /// Drain without deriving — for elements that are NOT on top.
    ///
    /// `log_visible` runs on `nav.top_mut()` only, so a buried filter (stack
    /// two greps, or open a sub-view over one) would never advance its
    /// cursor: the parent would evict past it and, on reveal, it would
    /// report `lost_history` for lines it could have had. Draining every
    /// frame costs O(lines arrived) per element and keeps a covered filter
    /// exactly as complete as a visible one.
    pub(crate) fn keep_up(&mut self) {
        self.drain_from_parent();
    }
}

/// The nav-stack label for a log element, derived from its spec alone.
fn log_label(spec: &k9rs_proto::protocol::LogInit) -> String {
    format!("logs({}/{})", spec.target.display(), spec.container.label())
}

/// A committed grep over the parent's line output — one element per
/// committed filter, so Esc pops filters one at a time (the old
/// `LogState.filters` stack, made navigation).
#[derive(Debug)]
pub struct LogFilter {
    pub(super) source: LineSource,
    /// This filter's OWN lines — the matches it has seen, with its own cap.
    ///
    /// A filter used to own nothing: it derived indices into the parent's
    /// store on every read, so its retention WAS the parent's. Eviction there
    /// is driven by unmatched noise, which means the rarer the pattern the
    /// faster its matches scrolled out — a grep for `panic` was evicted by
    /// the very traffic it was written to escape. Holding the matches makes
    /// the cap mean "N matches" instead of "N lines, some of which matched".
    pub(super) retained: Arc<LineStore>,
    /// Next ABSOLUTE line sequence (`evicted + index`) to consider from the
    /// parent. Monotone, and monotone across `clear()` — which is what lets
    /// this be a resumable cursor rather than a position that shifts under
    /// eviction.
    pub(super) cursor: u64,
    /// The parent evicted past `cursor` before we drained it, so some
    /// unknown number of matches are missing before our oldest line. The
    /// count is unknowable by construction — we never saw the lines — so
    /// this is a flag, not a number.
    pub(super) lost_history: bool,
    pub(super) label: String,
    /// Display identity, captured from the parent at derivation (the
    /// pod/container never change; the since label can go stale across a
    /// range restart under the filter — cosmetic, the crumb shows the
    /// filter anyway).
    pub(super) header_pod: String,
    pub(super) header_container: String,
    pub(super) header_since: String,
    pub view: LogInteraction,
}
