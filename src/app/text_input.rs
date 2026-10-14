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

    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    /// Insert at the caret and step over it.
    pub fn insert(&mut self, c: char) {
        self.value.insert(self.cursor, c);
        self.cursor += c.len_utf8();
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

#[cfg(test)]
#[path = "../tests/app/text_input.rs"]
mod tests;
