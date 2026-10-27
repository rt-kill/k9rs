//! Vim-magic search patterns.
//!
//! Not a "helper": 500 lines translating vim's magic-mode dialect into the
//! `regex` crate's, with position rules, vim char classes, `\{n,m}`
//! quantifiers, and a named reason whenever an atom is inexpressible. It was
//! the largest thing in a 1176-line `lib.rs` and the least like the rest of
//! it.

// ---------------------------------------------------------------------------
// Smart search (vim-style smartcase + regex)
// ---------------------------------------------------------------------------

/// Compiled search pattern with vim-style smartcase:
/// - If the pattern contains an uppercase letter → case-sensitive
/// - Otherwise → case-insensitive (`\c` / `\C` override, as in vim)
/// - Translated from vim magic mode (`translate_vim_magic`); falls back to
///   a literal substring search, WITH A STATED REASON, if it can't be a regex
#[derive(Debug, Clone)]
pub struct SearchPattern {
    regex: Option<regex::Regex>,
    literal: String,
    case_insensitive: bool,
    mode: MatchMode,
}

/// How a [`SearchPattern`] is actually being applied.
///
/// The reason is carried, not discarded, because the bars RENDER it: a
/// pattern that quietly stopped being a regex is indistinguishable from one
/// that simply isn't matching anything, and "no lines match" is the least
/// useful thing you can say about `a\{2,3}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchMode {
    /// Compiled; matching as a regex.
    Regex,
    /// Matching the raw pattern text as a plain substring, because it
    /// couldn't be a regex — and this is why.
    Literal(String),
}

/// What `translate_vim_magic` produces: a Rust regex, plus vim's own
/// case-forcing flags, which override smartcase wherever they appear.
struct Translated {
    rust: String,
    /// `\C` → `Some(true)` (match case), `\c` → `Some(false)` (ignore it).
    force_case_sensitive: Option<bool>,
}

/// Translate a vim **magic**-mode pattern into Rust regex syntax.
///
/// Magic is the mode vim starts in: `. * ^ $ [ ] ~ \` are special, and
/// `+ = ? { } ( ) | < >` need a backslash to BECOME special (`\|` is
/// alternation, a bare `|` is a pipe character).
///
/// This is a TRANSLATION, not an escaping pass. The two engines disagree
/// about far more than which characters want a backslash, and every
/// disagreement left unhandled showed up as a pattern that silently did
/// something else:
///
/// - vim makes `^` an anchor only at the start of a pattern or of a `\|`
///   branch, and `$` only at the end of one; elsewhere they are literal
///   characters. Rust anchors them everywhere, so `foo^bar` asked for the
///   impossible and matched nothing instead of finding `foo^bar`.
/// - `\{n,m}` may close with a bare `}` in vim. Passing that through
///   produced `{2,3\}`, which doesn't parse — and the old code's response to
///   "doesn't parse" was an unannounced substring search for the eight
///   characters the user typed.
/// - `\zs` reached Rust's parser as `\z` (end of text) followed by `s`:
///   a valid regex that can never match, forever, with no indication.
/// - vim's character classes are not Rust's. `\a` is "alphabetic" in vim
///   and the BELL character in Rust; `\d` is ASCII-only in vim and
///   Unicode-aware in Rust; `\s` is space-or-tab in vim and all whitespace
///   in Rust. Each is spelled out below rather than passed through.
///
/// Returns `Err(reason)` for vim atoms this engine cannot express —
/// look-around (`\zs`, `\@=`), backreferences, `\&` — and for unknown
/// escapes. Refusing by name is the point: every one of those used to be
/// forwarded to Rust and reinterpreted.
///
/// Deliberately NOT vim-faithful, with nowhere to be faithful to: `~`
/// (vim's "the previous substitution's replacement text") is a literal
/// tilde here, because there is no substitute command to have run.
fn translate_vim_magic(pattern: &str) -> Result<Translated, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len() + 16);
    let mut force_case_sensitive = None;
    let mut i = 0;
    // Vim decides `^` and `*` by POSITION: `^` is an anchor only at the
    // start of the pattern or of a `\|` branch, and a `*` there has nothing
    // to repeat, so it is a literal asterisk.
    let mut at_branch_start = true;
    // …and a `*` after a quantifier has nothing to repeat either — vim
    // rejects `a**` outright (E871). Rust reads it as `(a*)*`, which matches
    // the empty string, which matches EVERY line.
    let mut after_quantifier = false;
    while i < chars.len() {
        let ch = chars[i];
        let mut opens_branch = false;

        let mut emitted_quantifier = false;
        if ch == '\\' {
            let Some(&next) = chars.get(i + 1) else {
                // A trailing backslash is a literal backslash in vim.
                out.push_str(r"\\");
                i += 1;
                at_branch_start = false;
                continue;
            };
            i += 2;
            match next {
                // --- the "backslash makes it special" set ---
                '(' => { out.push('('); opens_branch = true; }
                ')' => out.push(')'),
                '|' => { out.push('|'); opens_branch = true; }
                '+' => { out.push('+'); emitted_quantifier = true; }
                // `\=` is vim's own synonym for `\?`.
                '?' | '=' => { out.push('?'); emitted_quantifier = true; }
                '{' => {
                    i = push_vim_quantifier(&chars, i, &mut out)?;
                    emitted_quantifier = true;
                }
                '}' => out.push_str(r"\}"),
                // Directional, not the symmetric `\b` both used to become:
                // `\b` made `+\>` match `a+b` and `\>e` match the `e` of
                // "the end", neither of which vim matches.
                '<' => out.push_str(r"\<"),
                '>' => out.push_str(r"\>"),
                '%' => {
                    // `\%(` is vim's non-capturing group. Its other `\%`
                    // forms (`\%[`, `\%V`, `\%23l`, …) address buffer
                    // positions, which a log line doesn't have.
                    if chars.get(i) == Some(&'(') {
                        out.push_str("(?:");
                        i += 1;
                        opens_branch = true;
                    } else {
                        return Err(unsupported(r"\%"));
                    }
                }

                // --- case forcing: vim lets these sit anywhere ---
                'c' => force_case_sensitive = Some(false),
                'C' => force_case_sensitive = Some(true),

                // --- character classes, spelled out (see above) ---
                'd' => out.push_str("[0-9]"),
                'D' => out.push_str("[^0-9]"),
                'w' => out.push_str("[0-9A-Za-z_]"),
                'W' => out.push_str("[^0-9A-Za-z_]"),
                // vim's `\s` is space-or-tab ONLY — not Rust's `\s`.
                's' => out.push_str("[ \\t]"),
                'S' => out.push_str("[^ \\t]"),
                'a' => out.push_str("[[:alpha:]]"),
                'A' => out.push_str("[^[:alpha:]]"),
                // `(?-i:…)` because these classes MEAN case: under the
                // case-insensitive flag smartcase applies, `[A-Z]` matches
                // lowercase too, so `\u\u\u` (find an all-caps token)
                // matched every lowercase word instead.
                'l' => out.push_str("(?-i:[a-z])"),
                'L' => out.push_str("(?-i:[^a-z])"),
                'u' => out.push_str("(?-i:[A-Z])"),
                'U' => out.push_str("(?-i:[^A-Z])"),
                'h' => out.push_str("[A-Za-z_]"),
                'H' => out.push_str("[^A-Za-z_]"),
                'o' => out.push_str("[0-7]"),
                'O' => out.push_str("[^0-7]"),
                'x' => out.push_str("[0-9A-Fa-f]"),
                'X' => out.push_str("[^0-9A-Fa-f]"),
                // Identifier/keyword classes depend on vim's `isk`; the
                // default is the word set.
                'i' | 'k' => out.push_str("[0-9A-Za-z_]"),
                'I' | 'K' => out.push_str("[^0-9A-Za-z_]"),
                'p' => out.push_str("[[:print:]]"),
                'P' => out.push_str("[^[:print:]]"),

                // --- escapes both engines share ---
                // A log line and a table cell are single lines by
                // construction, so `\n` could only ever match nothing —
                // silently, forever. Refused by name instead.
                'n' => return Err("\\n can't match: these are single lines".to_string()),
                't' => out.push_str(r"\t"),
                'r' => out.push_str(r"\r"),
                'e' => out.push_str(r"\x1b"),

                // --- literal punctuation ---
                '.' | '*' | '[' | ']' | '^' | '$' | '\\' | '/' | '~' | '-' => {
                    out.push('\\');
                    out.push(next);
                }

                // --- nothing here can express these ---
                'z' => return Err(unsupported(r"\zs / \ze")),
                '@' => return Err(unsupported("look-around (\\@=, \\@!, \\@<=, \\@<!)")),
                '&' => return Err(unsupported(r"\& (branch intersection)")),
                '_' => return Err(unsupported(r"\_ (match across lines)")),
                '0'..='9' => return Err(unsupported("backreferences (\\1-\\9)")),
                'f' | 'F' | 'v' | 'V' | 'm' | 'M' => {
                    return Err(format!(r"\{next} isn't supported (k9rs speaks vim MAGIC only)"))
                }
                other => return Err(format!(r"unknown escape \{other}")),
            }
            at_branch_start = opens_branch;
            after_quantifier = emitted_quantifier;
            continue;
        }

        match ch {
            // A bracket expression is copied through: inside it, vim's
            // backslash rules don't apply and neither does the `^`
            // positioning above — `[^abc]` is a negation, not a literal
            // caret, which is exactly what a naive escape pass got wrong.
            '[' => match scan_class(&chars, i) {
                Some((class, next_i)) => {
                    out.push_str(&class);
                    i = next_i;
                }
                // An unmatched `[` is a literal `[` in vim (and a parse
                // error in Rust).
                None => {
                    out.push_str(r"\[");
                    i += 1;
                }
            },
            '^' if at_branch_start => {
                // An anchor consumes no atom, so a `*` right after it STILL
                // has nothing to repeat. Falling through to the common
                // `at_branch_start = false` below made `^*a` a quantifier on
                // the anchor — which Rust happily compiles into "any number
                // of line starts", i.e. matches everywhere.
                out.push('^');
                i += 1;
                continue;
            }
            '$' if at_branch_end(&chars, i) => { out.push('$'); i += 1; }
            // Nothing to repeat here, so vim reads it as a literal.
            '*' if at_branch_start || after_quantifier => {
                out.push_str(r"\*");
                i += 1;
                // Still nothing to repeat for whatever follows.
                continue;
            }
            '*' => {
                out.push('*');
                i += 1;
                at_branch_start = false;
                after_quantifier = true;
                continue;
            }
            '^' | '$' | '+' | '?' | '{' | '}' | '(' | ')' | '|' | '~' => {
                out.push('\\');
                out.push(ch);
                i += 1;
            }
            _ => { out.push(ch); i += 1; }
        }
        at_branch_start = false;
        after_quantifier = false;
    }
    Ok(Translated { rust: out, force_case_sensitive })
}

fn unsupported(what: &str) -> String {
    format!("{what} has no equivalent in this regex engine")
}

/// Pull the REASON out of a `regex::Error`, which renders as four lines:
/// a header, the echoed pattern, a caret, then `error: <why>`. The bar has
/// one row, and the row worth showing is the last one.
///
/// This used to take the first non-header line — the echoed pattern. So the
/// bar reported the pattern back at you as its own explanation, complete
/// with the `(?i)` prefix smartcase had just prepended, which is neither
/// something you typed nor something you can act on.
fn error_reason(msg: &str) -> &str {
    msg.lines()
        .map(str::trim)
        .find_map(|l| l.strip_prefix("error: "))
        .unwrap_or_else(|| {
            msg.lines()
                .map(str::trim)
                .find(|l| !l.is_empty() && !l.starts_with("regex parse error"))
                .unwrap_or(msg)
        })
}

/// Is the `$` at `i` at the end of a pattern or of a `\|` branch? Only
/// there is it an anchor in vim.
fn at_branch_end(chars: &[char], i: usize) -> bool {
    matches!(
        (chars.get(i + 1), chars.get(i + 2)),
        (None, _) | (Some('\\'), Some(')' | '|'))
    )
}

/// Emit the Rust form of a vim `\{…}` quantifier, whose body starts at `i`
/// (just past the `\{`). Returns the index after it.
///
/// Vim allows a bare `}` to close, spells lazy as a leading `-`, and reads
/// an empty body as `*`. Rust allows none of those, and rejects `{,m}`.
fn push_vim_quantifier(chars: &[char], mut i: usize, out: &mut String) -> Result<usize, String> {
    let lazy = chars.get(i) == Some(&'-');
    if lazy {
        i += 1;
    }
    let mut body = String::new();
    while let Some(&c) = chars.get(i) {
        if c.is_ascii_digit() || c == ',' {
            body.push(c);
            i += 1;
        } else {
            break;
        }
    }
    // Either closer is accepted, and so is running off the end — a
    // half-typed `a\{2` shouldn't explode into an error while you type.
    match (chars.get(i), chars.get(i + 1)) {
        (Some('\\'), Some('}')) => i += 2,
        (Some('}'), _) => i += 1,
        (Some(other), _) => return Err(format!("`{other}` can't appear in a \\{{…}} count")),
        (None, _) => {}
    }
    match body.as_str() {
        "" => out.push('*'),
        b if b.starts_with(',') => out.push_str(&format!("{{0{b}}}")),
        b => out.push_str(&format!("{{{b}}}")),
    }
    if lazy {
        out.push('?');
    }
    Ok(i)
}

/// Longest bracket expression we will scan for a closing `]`.
///
/// An unmatched `[` is a literal in vim, so the scan has to fail before it
/// can say so — and the caller then advances by ONE character and may scan
/// again from the next `[`. Unbounded, that is quadratic: a pasted line of
/// 64k brackets took 2 seconds per keystroke, and the filter draft compiles
/// about twice a frame. No real bracket expression is anywhere near this.
const MAX_CLASS_SCAN: usize = 512;

/// Copy a `[…]` bracket expression through, returning it and the index
/// after the closing `]`, or `None` if it never closes.
///
/// Contents are copied, with the differences neutralized: a leading `]` is a
/// literal in vim (Rust needs it escaped), `&&` means set intersection in
/// Rust and nothing in vim, and — the one that inverted meanings silently —
/// vim has NO class escapes. `[\d]` is the two characters `\` and `d` in
/// vim; passing it through gave Rust its digit class, so `[\d]` matched `5`
/// and not `d`. Same for `\w \s \S \D \W \b`.
fn scan_class(chars: &[char], start: usize) -> Option<(String, usize)> {
    let mut out = String::from("[");
    let mut i = start + 1;
    let limit = chars.len().min(start + MAX_CLASS_SCAN);
    if chars.get(i) == Some(&'^') {
        out.push('^');
        i += 1;
    }
    if chars.get(i) == Some(&']') {
        out.push_str(r"\]");
        i += 1;
    }
    while let Some(&c) = chars.get(i).filter(|_| i < limit) {
        match c {
            ']' => {
                out.push(']');
                return Some((out, i + 1));
            }
            '\\' => {
                // A backslash inside a vim class is a literal backslash, and
                // the next character stands for ITSELF. Both are escaped so
                // Rust cannot reinterpret either.
                out.push_str(r"\\");
                i += 1;
                if let Some(&n) = chars.get(i) {
                    if !n.is_ascii_alphanumeric() {
                        out.push('\\');
                    }
                    out.push(n);
                    i += 1;
                }
            }
            '&' | '~' => { out.push('\\'); out.push(c); i += 1; }
            _ => { out.push(c); i += 1; }
        }
    }
    None
}

impl SearchPattern {
    /// Compile a search pattern with vim-style smartcase and magic mode.
    ///
    /// Smartcase: pattern is case-insensitive unless it contains an
    /// uppercase letter (same as vim's `set smartcase`).
    ///
    /// Magic mode: `. * ^ $ [ ] ~ \` are regex-special (same as vim),
    /// but `+ ? { } ( ) |` are treated as literals (vim requires `\+`
    /// etc. to make them special). Users get intuitive substring search
    /// with basic regex power (`.` = any, `*` = repeat, `^`/`$` = anchors)
    /// without PCRE surprises. See `translate_vim_magic` for where this
    /// does and doesn't reach vim.
    ///
    /// A pattern that can't be a regex still SEARCHES — as a plain
    /// substring of the raw text — but it records why, so the bar can say
    /// so instead of leaving the user to conclude that nothing matched.
    pub fn new(pattern: &str) -> Self {
        // `\c` / `\C` beat smartcase, as they do in vim.
        // Smartcase skips ESCAPE letters, as vim's own `pat_has_uppercase`
        // does: `\S` is "non-whitespace", not a capital S, and letting it
        // flip the whole search to case-sensitive made `foo\S` miss `FOOx`.
        // (It also made `\u` — which contains no capital — case-insensitive,
        // the other half of the same confusion.)
        let smartcase = !{
            let mut chars = pattern.chars();
            let mut has_upper = false;
            while let Some(c) = chars.next() {
                if c == '\\' {
                    chars.next();
                } else if c.is_uppercase() {
                    has_upper = true;
                    break;
                }
            }
            has_upper
        };

        let (regex_body, mode, case_insensitive) = match translate_vim_magic(pattern) {
            Ok(t) => {
                let ci = t.force_case_sensitive.map_or(smartcase, |forced| !forced);
                (Some(t.rust), MatchMode::Regex, ci)
            }
            Err(why) => (None, MatchMode::Literal(why), smartcase),
        };

        let compiled = regex_body.map(|body| {
            let source = if case_insensitive { format!("(?i){body}") } else { body };
            regex::Regex::new(&source).map_err(|e| error_reason(&e.to_string()).to_string())
        });

        let (regex, mode) = match compiled {
            Some(Ok(re)) => (Some(re), mode),
            Some(Err(why)) => (None, MatchMode::Literal(why)),
            None => (None, mode),
        };

        Self {
            regex,
            literal: if case_insensitive { pattern.to_lowercase() } else { pattern.to_string() },
            case_insensitive,
            mode,
        }
    }

    /// Whether smartcase (or an explicit `\C`) made this pattern match case.
    ///
    /// Surfaced because it is the quietest way a pattern can under-match: a
    /// single capital anywhere flips the whole search, so `/Client` silently
    /// skips every `depth_client` and nothing on screen says why. The rule is
    /// wanted — it is vim's, and it is what makes `/INFO` mean INFO — but a
    /// rule you cannot see firing is indistinguishable from a broken filter.
    pub fn is_case_sensitive(&self) -> bool {
        !self.case_insensitive
    }

    /// Why this pattern is matching literally rather than as a regex, if it
    /// is. `None` means it compiled.
    pub fn fallback_reason(&self) -> Option<&str> {
        match &self.mode {
            MatchMode::Regex => None,
            MatchMode::Literal(why) => Some(why),
        }
    }

    /// Check if a line matches the pattern.
    pub fn is_match(&self, text: &str) -> bool {
        if let Some(ref re) = self.regex {
            re.is_match(text)
        } else if self.case_insensitive {
            text.to_lowercase().contains(&self.literal)
        } else {
            text.contains(&self.literal)
        }
    }

    /// Find all (start, end) byte offsets of matches in the text.
    /// Offsets are always into the original `text`, not a lowered copy.
    pub fn find_all(&self, text: &str) -> Vec<(usize, usize)> {
        if let Some(ref re) = self.regex {
            // Zero-width matches are dropped: a pattern like `[0-9]*` matches
            // the empty string at every position, and each one became its own
            // rendered `Span` — 400 spans on a 400-byte line to highlight
            // nothing at all.
            re.find_iter(text)
                .filter(|m| m.end() > m.start())
                .map(|m| (m.start(), m.end()))
                .collect()
        } else if self.case_insensitive {
            // Case-insensitive literal search: scan char-by-char to find
            // matches in the original text (avoids byte offset mismatch
            // from to_lowercase() changing byte lengths for some Unicode).
            let needle: Vec<char> = self.literal.chars().collect();
            if needle.is_empty() { return vec![]; }
            let chars: Vec<(usize, char)> = text.char_indices().collect();
            let mut results = Vec::new();
            'outer: for i in 0..chars.len() {
                if i + needle.len() > chars.len() { break; }
                for (j, &nc) in needle.iter().enumerate() {
                    let tc = chars[i + j].1;
                    if !tc.to_lowercase().eq(nc.to_lowercase()) {
                        continue 'outer;
                    }
                }
                let start = chars[i].0;
                let end = if i + needle.len() < chars.len() {
                    chars[i + needle.len()].0
                } else {
                    text.len()
                };
                results.push((start, end));
            }
            results
        } else {
            text.match_indices(&self.literal)
                .map(|(start, s)| (start, start + s.len()))
                .collect()
        }
    }

    /// The raw pattern string.
    pub fn pattern(&self) -> &str {
        &self.literal
    }

    pub fn is_empty(&self) -> bool {
        self.literal.is_empty()
    }
}

/// A grep pattern compiled exactly once, at predicate construction.
/// Carries both the source text (for crumbs and completion round-trips)
/// and the compiled [`SearchPattern`] so per-frame derives never
/// recompile.
#[derive(Debug, Clone)]
pub struct CompiledGrep {
    source: String,
    pattern: SearchPattern,
}

impl CompiledGrep {
    /// Compile from raw user text. Smartcase rules live inside
    /// [`SearchPattern::new`].
    pub fn new(source: impl Into<String>) -> Self {
        let source = source.into();
        let pattern = SearchPattern::new(&source);
        Self { source, pattern }
    }

    /// The original pattern text (crumb rendering, debug output).
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The compiled pattern — no allocation, no recompile in hot paths.
    pub fn pattern(&self) -> &SearchPattern {
        &self.pattern
    }
}

#[cfg(test)]
#[path = "tests/search.rs"]
mod tests;
