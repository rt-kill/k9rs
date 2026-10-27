//! Terminal text: what is visible, what is safe to print, and where an
//! escape sequence ends.
//!
//! Deliberately NOT a renderer — nothing here knows about ratatui. The
//! escape tokenizer lives here because every text consumer needs it: the
//! daemon sanitizing cluster-controlled strings, the renderer measuring a
//! line, the log view searching one.

/// THE visible text of a line: what the renderer's spans actually contain.
///
/// One definition, because there were four views of a log line and they
/// disagreed. The filter matched raw `content` (escapes and all) while the
/// screen showed this — so `/workers=` missed a line reading `workers=0`
/// whenever the logger coloured the field name, because a reset sequence sat
/// between the `s` and the `=`. Which patterns worked was decided by where a
/// logger happened to put its colour codes.
///
/// Built on [`skip_ansi_escape`] — the SAME tokenizer the renderer's
/// `parse_ansi_line`
/// uses — and dropping the same control characters `sanitize_terminal` does,
/// so `visible_text(s)` equals the concatenation of `parse_ansi_line(s)` BY
/// CONSTRUCTION rather than by coincidence. `visible_text_matches_the_spans`
/// pins that; the `strip_ansi` this replaced could not make the claim,
/// because its CSI rules were its own.
///
/// Keeps tabs, because the spans do. "What ratatui will DRAW" is a different
/// question one layer down (`log_view::Units` drops them, since a grapheme
/// containing a control char is never painted) — matching and layout are
/// genuinely different spaces, and conflating them is how a tab-indented
/// line measured one row wider than it drew.
pub fn visible_text(s: &str) -> std::borrow::Cow<'_, str> {
    let bytes = s.as_bytes();
    let needs_work = {
        let mut i = 0usize;
        loop {
            if i >= bytes.len() {
                break false;
            }
            if skip_ansi_escape(bytes, i).is_some() {
                break true;
            }
            let c = s[i..].chars().next().unwrap();
            if c != '\t' && c.is_control() {
                break true;
            }
            i += c.len_utf8();
        }
    };
    if !needs_work {
        return std::borrow::Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if let Some(next) = skip_ansi_escape(bytes, i) {
            i = next;
            continue;
        }
        let c = s[i..].chars().next().unwrap();
        i += c.len_utf8();
        if c == '\t' || !c.is_control() {
            out.push(c);
        }
    }
    std::borrow::Cow::Owned(out)
}

/// Strip terminal control characters from cluster-controlled text before
/// it is rendered through a path that does NOT sanitize (ratatui's
/// `Paragraph` writes symbols verbatim, unlike `Buffer::set_string`).
/// Keeps `\t`; drops every other C0 control, DEL, and the C1 range
/// (`char::is_control` covers U+0000–U+001F and U+007F–U+009F). This is
/// what stops a hostile resource name / annotation / API-server version
/// string from smuggling an `ESC]52;…` (OSC 52 clipboard) or `ESC[6n`
/// (cursor-report → stdin injection) to the terminal.
pub fn sanitize_terminal(s: &str) -> String {
    s.chars().filter(|&c| c == '\t' || !c.is_control()).collect()
}

/// If `bytes[i]` begins an ANSI escape, return the index just past the WHOLE
/// escape (which contributes zero display columns); otherwise `None` (`bytes[i]`
/// begins a visible character). THE single source of truth for "what is an
/// escape": the renderer's `parse_ansi_line` and the log wrap-height counter
/// (`log_view::wrap_feed`) both tokenize through this, so their visible-width
/// accounting cannot drift — and a divergence here silently reintroduces the
/// "wrap mode can't scroll to the last line" bug.
///
/// `pub` rather than `pub(crate)` BECAUSE of that sentence: the two
/// consumers it names now live in a different crate (`ui::ansi` and
/// `ui::widgets::log_view`), so crate-private would mean each of them
/// tokenizing escapes its own way — which is precisely the drift this
/// function exists to prevent. The first real widening the split has asked
/// for, and it is an API decision rather than an accident.
pub fn skip_ansi_escape(bytes: &[u8], i: usize) -> Option<usize> {
    let len = bytes.len();
    if bytes.get(i) != Some(&0x1b) {
        return None;
    }
    if bytes.get(i + 1) == Some(&b'[') {
        // CSI: ESC '[' <params> <final byte in 0x40..=0x7E>. The final-byte range
        // is ASCII, so `j` only ever stops on an ASCII (char-boundary) byte.
        let mut j = i + 2;
        while j < len && !(0x40..=0x7E).contains(&bytes[j]) {
            j += 1;
        }
        Some((j + 1).min(len)) // also consume the final byte
    } else {
        // Any other escape (including the OSC opener `ESC ]`): drop ESC + the
        // next CHARACTER. Advance by that char's full UTF-8 length (not a fixed
        // +2) so the returned index is always a char boundary — a fixed +2 lands
        // mid-char for `ESC` + a multibyte glyph and panics the callers' slices.
        match bytes.get(i + 1) {
            None => Some(len),
            Some(&lead) => Some((i + 1 + utf8_char_len(lead)).min(len)),
        }
    }
}

/// Byte length (1–4) of the UTF-8 char beginning with lead byte `b`. Used to
/// step past a whole char after a bare ESC without splitting it.
fn utf8_char_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF7 => 4,
        _ => 1, // continuation/invalid lead — can't follow ESC in valid UTF-8
    }
}

#[cfg(test)]
#[path = "tests/text.rs"]
mod tests;
