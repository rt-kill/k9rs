//! One editable line of text, with a cursor.
//!
//! There were four independent implementations of this before: the `:`
//! command prompt, the table filter, the log grep draft and form fields all
//! kept a bare `String` and did `push`/`pop`, plus a fifth shared helper that
//! two of them ignored. None had a cursor, and adding one to each would have
//! been four chances to let a byte index drift out of bounds — the same shape
//! that has already produced a mid-character slice panic in this codebase.
//!
//! So the cursor is not a feature bolted onto four places; it is the reason
//! there is now one place. Word motion, kill-to-end and paste are each one
//! method here rather than four patches.
//!
//! **The invariant**: `cursor` is a byte offset into `value` that always sits
//! on a GRAPHEME boundary. Slicing a `String` anywhere else panics, and
//! moving by `char` would leave the caret between a letter and its combining
//! accent — visibly wrong and, for anything that then slices, fatal. Every
//! mutator below preserves it, so no caller can construct a bad one.
//!
//! `TextKey`, `SearchInputResult` and `handle_search_key` sat in
//! `types.rs` while this file held `TextInput` alone — and
//! `handle_search_key` reached back across that gap by qualified path to
//! take a `&mut crate::text_input::TextInput`. The gate on what counts as
//! text, the outcome of a keystroke, and the buffer it lands in are one
//! concern.

use unicode_segmentation::UnicodeSegmentation;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TextInput {
    value: String,
    /// Byte offset into `value`, always on a grapheme boundary.
    cursor: usize,
}

impl TextInput {
    /// An empty input.
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed with existing text, caret at the end (what every call site that
    /// pre-fills a field wants).
    pub fn seeded(value: impl Into<String>) -> Self {
        let value = value.into();
        Self { cursor: value.len(), value }
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    /// Byte offset of the caret — on a grapheme boundary, so it is safe to
    /// slice `value` here.
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    /// Caret position in COLUMNS, for drawing. Not the byte offset: a
    /// renderer that placed the caret at the byte index would put it past the
    /// end of any line containing a multi-byte character.
    pub fn cursor_col(&self) -> usize {
        use unicode_width::UnicodeWidthStr;
        UnicodeWidthStr::width(&self.value[..self.cursor])
    }

    /// The slice visible in `width` columns, and the caret's column within
    /// it — a one-line viewport over the value, anchored on the caret.
    ///
    /// The algorithm is [`k9rs_util::width::window_around`], which the filter bar
    /// also uses (it is handed a `&str` and a column, not an input). This is
    /// the typed entry point: the caret it passes is this type's invariant,
    /// so no caller can window around a position that does not exist.
    pub fn window(&self, width: usize) -> (&str, usize) {
        k9rs_util::width::window_around(&self.value, self.cursor_col(), width)
    }

    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    /// Insert at the caret and step over it.
    pub fn insert(&mut self, c: char) {
        self.value.insert(self.cursor, c);
        // Re-derived, not advanced by `c.len_utf8()`. The two agree unless
        // the character that FOLLOWS the caret is a combining mark, in which
        // case the inserted base character and that mark are now one
        // grapheme and a byte-length step lands inside it — breaking the
        // invariant this module's doc says every mutator preserves. (No
        // panic: it is still a char boundary and both `prev_boundary` and
        // `next_boundary` recover. It is still the one mutator that did not
        // uphold the rule.)
        self.cursor = self.next_boundary().unwrap_or(self.value.len());
    }

    /// Delete the grapheme BEFORE the caret (Backspace).
    pub fn backspace(&mut self) {
        let Some(prev) = self.prev_boundary() else { return };
        self.value.replace_range(prev..self.cursor, "");
        self.cursor = prev;
    }

    /// Delete the grapheme AT the caret (Delete).
    pub fn delete(&mut self) {
        let Some(next) = self.next_boundary() else { return };
        self.value.replace_range(self.cursor..next, "");
    }

    pub fn left(&mut self) {
        if let Some(prev) = self.prev_boundary() {
            self.cursor = prev;
        }
    }

    pub fn right(&mut self) {
        if let Some(next) = self.next_boundary() {
            self.cursor = next;
        }
    }

    pub fn home(&mut self) {
        self.cursor = 0;
    }

    pub fn end(&mut self) {
        self.cursor = self.value.len();
    }

    pub fn clear(&mut self) {
        self.value.clear();
        self.cursor = 0;
    }

    /// Take the text, leaving the input empty (commit).
    pub fn take(&mut self) -> String {
        self.cursor = 0;
        std::mem::take(&mut self.value)
    }

    /// Replace the text wholesale, caret to the end.
    pub fn set(&mut self, value: impl Into<String>) {
        *self = Self::seeded(value);
    }

    fn prev_boundary(&self) -> Option<usize> {
        if self.cursor == 0 {
            return None;
        }
        self.value[..self.cursor].grapheme_indices(true).next_back().map(|(i, _)| i)
    }

    fn next_boundary(&self) -> Option<usize> {
        self.value[self.cursor..].graphemes(true).next().map(|g| self.cursor + g.len())
    }
}

impl<T: Into<String>> From<T> for TextInput {
    fn from(value: T) -> Self {
        Self::seeded(value)
    }
}

/// Result of processing a search input keystroke.
pub enum SearchInputResult {
    /// Key was consumed, input updated (Char, Backspace).
    Updated,
    /// User pressed Esc — cancel search.
    Cancelled,
    /// User pressed Enter — commit search with this term (empty = clear).
    Committed(String),
}

/// Process a keystroke against a search input buffer.
/// Returns what happened so the caller can apply view-specific logic.
/// Callers gate on their own "search active" state before dispatching.
/// A keystroke that is TEXT.
///
/// Every text sink — the `:` command bar, form fields, the resource filter,
/// the log grep draft, the yaml/describe search — matched on `key.code` alone,
/// so a Ctrl chord typed its bare character instead of falling through to the
/// global handler. Three of the five were fixed one at a time across two
/// separate bug reports; `handle_search_key` could not be fixed at its call
/// site at all, because it took a `KeyCode` and the modifier was already gone.
///
/// So the guard became a TYPE. A sink takes a `TextKey`, which can only be
/// built by [`TextKey::new`], which refuses Ctrl chords — "forgot the guard"
/// is no longer something you can write.
///
/// Concretely this is not hypothetical: `BEL` terminates an OSC reply and
/// crossterm delivers `0x07` as Ctrl-G, so a terminal colour response leaking
/// into a resume window typed a stray `g` into whichever text box was open.
#[derive(Debug, Clone, Copy)]
pub struct TextKey(crossterm::event::KeyEvent);

impl TextKey {
    /// `None` for a Ctrl chord — that is a command, not text, and belongs to
    /// the global handler. Alt chords are likewise not text (an Alt-modified
    /// key is a different chord, not its bare character — the same rule
    /// `combo_of` follows).
    pub fn new(key: crossterm::event::KeyEvent) -> Option<Self> {
        use crossterm::event::KeyModifiers;
        if key.modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) {
            return None;
        }
        Some(Self(key))
    }

    pub fn code(&self) -> crossterm::event::KeyCode {
        self.0.code
    }

    /// The character this key types, if it types one.
    pub fn char(&self) -> Option<char> {
        match self.0.code {
            crossterm::event::KeyCode::Char(c) => Some(c),
            _ => None,
        }
    }
}

pub fn handle_search_key(
    input: &mut TextInput,
    key: TextKey,
) -> SearchInputResult {
    use crossterm::event::KeyCode;
    match key.code() {
        KeyCode::Esc => {
            input.clear();
            SearchInputResult::Cancelled
        }
        KeyCode::Enter => SearchInputResult::Committed(input.take()),
        // Editing and caret motion are the input's, not this function's —
        // the fifth and last place that used to keep its own `String` and
        // reimplement backspace.
        KeyCode::Backspace => {
            input.backspace();
            SearchInputResult::Updated
        }
        KeyCode::Delete => {
            input.delete();
            SearchInputResult::Updated
        }
        KeyCode::Left => {
            input.left();
            SearchInputResult::Updated
        }
        KeyCode::Right => {
            input.right();
            SearchInputResult::Updated
        }
        KeyCode::Home => {
            input.home();
            SearchInputResult::Updated
        }
        KeyCode::End => {
            input.end();
            SearchInputResult::Updated
        }
        KeyCode::Char(c) => {
            input.insert(c);
            SearchInputResult::Updated
        }
        _ => SearchInputResult::Updated,
    }
}

#[cfg(test)]
#[path = "tests/text_input.rs"]
mod tests;
