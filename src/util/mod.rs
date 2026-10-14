pub mod atomic_option;
pub mod one_task;

pub use atomic_option::AtomicOption;

use chrono::{DateTime, Utc};
use unicode_width::UnicodeWidthChar;

// ---------------------------------------------------------------------------
// Smart search (vim-style smartcase + regex)
// ---------------------------------------------------------------------------

/// Compiled search pattern with vim-style smartcase:
/// - If the pattern contains an uppercase letter → case-sensitive
/// - Otherwise → case-insensitive (`\c` / `\C` override, as in vim)
/// - Translated from vim magic mode ([`translate_vim_magic`]); falls back to
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

/// What [`translate_vim_magic`] produces: a Rust regex, plus vim's own
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
    /// without PCRE surprises. See [`translate_vim_magic`] for where this
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

// ---------------------------------------------------------------------------
// Per-process safe temp directory
// ---------------------------------------------------------------------------

/// Path to the per-process temp directory, created on first use with mode
/// `0700`. Used by every "save this to disk" path (edit YAML, save logs,
/// save table) so we never dump predictable filenames into world-writable
/// `/tmp`. Files written into this dir use `O_CREAT | O_EXCL` so a symlink
/// planted in advance can't divert the write — see [`safe_create_temp`].
pub fn process_temp_dir() -> std::io::Result<std::path::PathBuf> {
    use std::os::unix::fs::DirBuilderExt;
    let dir = std::env::temp_dir().join(format!("k9rs-{}", std::process::id()));
    match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            // The dir already exists — but `env::temp_dir()` is typically
            // world-writable `/tmp`, so another local user who won the PID
            // race could have pre-created it and now controls files we
            // write there (swapping the YAML we're about to apply, or
            // capturing saved output). Refuse unless it is a REAL
            // directory (not a symlink), owned by us, mode exactly 0700 —
            // the same check the daemon socket path already makes.
            verify_owned_private_dir(&dir)?;
        }
        Err(e) => return Err(e),
    }
    Ok(dir)
}

/// RAII owner of the per-process temp directory.
///
/// `TempFile` removes a FILE; nothing owned the DIRECTORY, so every run that
/// edited a resource or saved a table left a `k9rs-<pid>/` behind forever
/// (there was not one `remove_dir` in the tree). Worse than accumulation: a
/// run killed while an edit file exists leaves that file, and when the OS
/// later recycles the pid the per-process filename counter restarts at 0 —
/// so `create_new` hits EEXIST and `:e` on that resource fails with "File
/// exists" until the user cleans `/tmp` by hand.
///
/// Deliberately `remove_dir`, NOT a recursive delete: `SaveTable` and
/// `SaveLogs` write here on purpose and tell the user the path. Removing the
/// directory only when it is EMPTY reclaims exactly the files we own (edit
/// temp files, which `TempFile::drop` has already taken) and never touches
/// something the user asked us to write.
pub struct TempDirGuard;

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let Ok(dir) = process_temp_dir() else { return };
        reclaim_if_empty(&dir);
    }
}

/// Remove `dir` only if it holds nothing. Split out so the policy is
/// testable without touching the real process directory.
fn reclaim_if_empty(dir: &std::path::Path) {
    // `remove_dir` fails with ENOTEMPTY rather than deleting, which is the
    // whole point — a recursive delete here would take files the user asked
    // us to save.
    let _ = std::fs::remove_dir(dir);
}

/// Assert `dir` is a directory (not a symlink), owned by the current uid,
/// with mode `0700` — rejecting a hostile pre-created temp dir.
fn verify_owned_private_dir(dir: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    // `symlink_metadata` does NOT follow a final symlink, so a planted
    // symlink-to-attacker-dir is caught (it is not itself a dir).
    let md = std::fs::symlink_metadata(dir)?;
    let deny = |msg: &str| Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, msg));
    if !md.file_type().is_dir() {
        return deny("temp dir is not a directory (possible symlink attack)");
    }
    // SAFETY: getuid is always successful and thread-safe.
    let our_uid = unsafe { libc::getuid() };
    if md.uid() != our_uid {
        return deny("temp dir is owned by another user");
    }
    if md.mode() & 0o777 != 0o700 {
        return deny("temp dir has unexpected permissions");
    }
    Ok(())
}

/// Atomically create a fresh file inside [`process_temp_dir`] and return
/// its path + an open `File` handle. Refuses to follow symlinks or
/// overwrite an existing entry. Caller is responsible for writing content
/// and dropping the handle.
pub fn safe_create_temp(filename: &str) -> std::io::Result<(std::path::PathBuf, std::fs::File)> {
    use std::os::unix::fs::OpenOptionsExt;
    let dir = process_temp_dir()?;
    let path = dir.join(filename);
    let f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    Ok((path, f))
}

/// Convenience wrapper: create + write the entire payload. Returns the
/// final path.
pub fn safe_write_temp(filename: &str, content: &[u8]) -> std::io::Result<std::path::PathBuf> {
    use std::io::Write as _;
    let (path, mut f) = safe_create_temp(filename)?;
    // `safe_create_temp` used `O_CREAT|O_EXCL`, so the file EXISTS on disk
    // from here on. A bare `?` on the write left it behind with no owner —
    // and because the per-process name counter restarts at 0 in a later run,
    // a zero-byte orphan makes the next `create_new` for that resource fail
    // with EEXIST. Clean up what we created before reporting the failure.
    if let Err(e) = f.write_all(content) {
        drop(f);
        let _ = std::fs::remove_file(&path);
        return Err(e);
    }
    Ok(path)
}

/// Try to copy text to the system clipboard using available tools.
/// Returns `true` on success.
pub fn try_copy_to_clipboard(text: &str) -> bool {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let tools: &[(&str, &[&str])] = &[
        ("wl-copy", &[]),       // Wayland-native (preferred on Wayland)
        ("pbcopy", &[]),        // macOS
        ("xclip", &["-selection", "clipboard"]),  // X11
        ("xsel", &["--clipboard", "--input"]),    // X11 fallback
    ];

    for (tool, args) in tools {
        if let Ok(mut child) = Command::new(tool)
            .args(*args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            if let Some(ref mut stdin) = child.stdin {
                let _ = stdin.write_all(text.as_bytes());
            }
            if let Ok(status) = child.wait() {
                if status.success() {
                    return true;
                }
            }
        }
    }
    false
}

/// THE visible text of a line: what the renderer's spans actually contain.
///
/// One definition, because there were four views of a log line and they
/// disagreed. The filter matched raw `content` (escapes and all) while the
/// screen showed this — so `/workers=` missed a line reading `workers=0`
/// whenever the logger coloured the field name, because a reset sequence sat
/// between the `s` and the `=`. Which patterns worked was decided by where a
/// logger happened to put its colour codes.
///
/// Built on [`skip_ansi_escape`] — the SAME tokenizer [`parse_ansi_line`]
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

/// Push a text run as a Span, dropping non-tab control characters. Zero-
/// copy (borrowed slice) in the common no-control case; allocates a
/// cleaned `String` only when a control char is present.
fn push_ansi_text<'a>(
    spans: &mut Vec<ratatui::text::Span<'a>>,
    text: &'a str,
    style: ratatui::style::Style,
) {
    use ratatui::text::Span;
    if text.is_empty() {
        return;
    }
    if text.chars().any(|c| c.is_control() && c != '\t') {
        spans.push(Span::styled(sanitize_terminal(text), style));
    } else {
        spans.push(Span::styled(text, style));
    }
}

/// If `bytes[i]` begins an ANSI escape, return the index just past the WHOLE
/// escape (which contributes zero display columns); otherwise `None` (`bytes[i]`
/// begins a visible character). THE single source of truth for "what is an
/// escape": [`parse_ansi_line`] (the renderer) and the log wrap-height counter
/// (`log_view::wrap_feed`) both tokenize through this, so their visible-width
/// accounting cannot drift — and a divergence here silently reintroduces the
/// "wrap mode can't scroll to the last line" bug.
pub(crate) fn skip_ansi_escape(bytes: &[u8], i: usize) -> Option<usize> {
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

/// Parse ANSI SGR escape sequences in a log line into ratatui Spans.
/// Non-SGR escape sequences are stripped (not displayed). Text between
/// escapes becomes a Span styled with the accumulated SGR state. Non-tab
/// control characters in the text runs are dropped (a hostile log line
/// can't leak a raw `\r`/BEL or a stray control into the wrap-mode
/// Paragraph render path).
///
/// The application is the authority on its own output coloring — we
/// preserve it faithfully instead of stripping and re-guessing.
pub fn parse_ansi_line<'a>(line: &'a str, base: ratatui::style::Style) -> Vec<ratatui::text::Span<'a>> {
    let mut spans = Vec::new();
    let mut style = base;
    let bytes = line.as_bytes();
    let len = bytes.len();
    let mut i = 0;
    let mut text_start = 0;

    while i < len {
        if let Some(next) = skip_ansi_escape(bytes, i) {
            // Flush the visible run before this escape.
            if i > text_start {
                push_ansi_text(&mut spans, &line[text_start..i], style);
            }
            // An SGR (`CSI … m`) updates the style; every other escape just
            // vanishes. `next - 1` is the final byte (guaranteed ≥ i+2 for a CSI).
            if bytes.get(i + 1) == Some(&b'[') && next > i + 2 && bytes[next - 1] == b'm' {
                style = apply_sgr(&line[i + 2..next - 1], base, style);
            }
            i = next;
            text_start = i;
        } else {
            i += 1;
        }
    }
    // Flush remaining text.
    if text_start < len {
        push_ansi_text(&mut spans, &line[text_start..], style);
    }
    // A line that is ENTIRELY escapes has no visible content — return no spans
    // (renders blank). Do NOT fall back to pushing the raw line here: that
    // would render the escape bytes as text AND make the wrap-height count
    // (which skips escapes via `skip_ansi_escape`) disagree with the render,
    // reintroducing the "wrap mode can't reach the last line" bug.
    spans
}

/// Apply SGR (Select Graphic Rendition) parameters to the current style.
/// Parameters are semicolon-separated numbers like "1;31" (bold red).
fn apply_sgr(
    params: &str,
    base: ratatui::style::Style,
    mut style: ratatui::style::Style,
) -> ratatui::style::Style {
    use ratatui::style::{Color, Modifier};

    let mut nums = params.split(';').filter_map(|s| s.parse::<u8>().ok()).peekable();
    // \x1b[m (empty params) is equivalent to \x1b[0m (reset).
    if nums.peek().is_none() {
        return base;
    }
    while let Some(n) = nums.next() {
        match n {
            0 => style = base,
            1 => style = style.add_modifier(Modifier::BOLD),
            2 => style = style.add_modifier(Modifier::DIM),
            3 => style = style.add_modifier(Modifier::ITALIC),
            4 => style = style.add_modifier(Modifier::UNDERLINED),
            7 => style = style.add_modifier(Modifier::REVERSED),
            22 => style = style.remove_modifier(Modifier::BOLD | Modifier::DIM),
            23 => style = style.remove_modifier(Modifier::ITALIC),
            24 => style = style.remove_modifier(Modifier::UNDERLINED),
            27 => style = style.remove_modifier(Modifier::REVERSED),
            // Standard foreground colors.
            30 => style = style.fg(Color::Black),
            31 => style = style.fg(Color::Red),
            32 => style = style.fg(Color::Green),
            33 => style = style.fg(Color::Yellow),
            34 => style = style.fg(Color::Blue),
            35 => style = style.fg(Color::Magenta),
            36 => style = style.fg(Color::Cyan),
            37 => style = style.fg(Color::Gray),
            38 => {
                // Extended foreground: 38;5;N (256-color) or 38;2;R;G;B (RGB).
                match nums.next() {
                    Some(5) => {
                        if let Some(idx) = nums.next() {
                            style = style.fg(Color::Indexed(idx));
                        }
                    }
                    Some(2) => {
                        let r = nums.next().unwrap_or(0);
                        let g = nums.next().unwrap_or(0);
                        let b = nums.next().unwrap_or(0);
                        style = style.fg(Color::Rgb(r, g, b));
                    }
                    _ => {}
                }
            }
            39 => style = style.fg(Color::Reset),
            // Standard background colors.
            40..=47 => {
                let colors = [Color::Black, Color::Red, Color::Green, Color::Yellow,
                              Color::Blue, Color::Magenta, Color::Cyan, Color::Gray];
                style = style.bg(colors[(n - 40) as usize]);
            }
            48 => {
                // Extended background: 48;5;N or 48;2;R;G;B.
                match nums.next() {
                    Some(5) => {
                        if let Some(idx) = nums.next() {
                            style = style.bg(Color::Indexed(idx));
                        }
                    }
                    Some(2) => {
                        let r = nums.next().unwrap_or(0);
                        let g = nums.next().unwrap_or(0);
                        let b = nums.next().unwrap_or(0);
                        style = style.bg(Color::Rgb(r, g, b));
                    }
                    _ => {}
                }
            }
            49 => style = style.bg(Color::Reset),
            // Bright foreground colors.
            90 => style = style.fg(Color::DarkGray),
            91 => style = style.fg(Color::LightRed),
            92 => style = style.fg(Color::LightGreen),
            93 => style = style.fg(Color::LightYellow),
            94 => style = style.fg(Color::LightBlue),
            95 => style = style.fg(Color::LightMagenta),
            96 => style = style.fg(Color::LightCyan),
            97 => style = style.fg(Color::White),
            // Bright background colors.
            100..=107 => {
                let colors = [Color::DarkGray, Color::LightRed, Color::LightGreen, Color::LightYellow,
                              Color::LightBlue, Color::LightMagenta, Color::LightCyan, Color::White];
                style = style.bg(colors[(n - 100) as usize]);
            }
            _ => {} // Unknown SGR code — ignore.
        }
    }
    style
}

pub fn retry_jitter(seed: &[u8], attempt: u64) -> f64 {
    let hash = seed.iter().fold(attempt, |acc, &b| {
        acc.wrapping_mul(31).wrapping_add(b as u64)
    });
    0.75 + (hash % 50) as f64 / 100.0
}

/// Truncate a string to fit within `max_width` display columns.
///
/// Uses `UnicodeWidthChar` so that multi-byte / wide characters (emoji, CJK)
/// are measured correctly and we never slice in the middle of a UTF-8 sequence.
pub fn truncate_to_width(s: &str, max_width: usize) -> &str {
    let mut width = 0;
    for (i, c) in s.char_indices() {
        let w = UnicodeWidthChar::width(c).unwrap_or(0);
        if width + w > max_width {
            return &s[..i];
        }
        width += w;
    }
    s
}

/// Formats a total-seconds count into the `2d3h`/`5m10s`/`30s` age string.
/// Shared backend for [`format_age`] (timestamp-based) and
/// [`format_age_duration`] (`Duration`-based).
pub fn format_age_secs(total_secs: i64) -> String {
    if total_secs < 0 {
        return "0s".to_string();
    }
    let days = total_secs / 86400;
    let hours = (total_secs % 86400) / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if days > 0 {
        if hours > 0 {
            format!("{}d{}h", days, hours)
        } else {
            format!("{}d", days)
        }
    } else if hours > 0 {
        if minutes > 0 {
            format!("{}h{}m", hours, minutes)
        } else {
            format!("{}h", hours)
        }
    } else if minutes > 0 {
        if seconds > 0 {
            format!("{}m{}s", minutes, seconds)
        } else {
            format!("{}m", minutes)
        }
    } else {
        format!("{}s", seconds)
    }
}

/// Formats a Kubernetes timestamp into a human-readable age string like "2d3h", "5m", "10s".
/// Returns "<unknown>" if the timestamp is None.
pub fn format_age(timestamp: Option<DateTime<Utc>>) -> String {
    let ts = match timestamp {
        Some(t) => t,
        None => return "<unknown>".to_string(),
    };
    let now = Utc::now();
    format_age_secs(now.signed_duration_since(ts).num_seconds())
}

/// Like [`format_age`] but takes a `Duration` directly — for locally-timed
/// work (port-forward uptime, etc.) that doesn't have a kube timestamp.
pub fn format_age_duration(d: std::time::Duration) -> String {
    format_age_secs(d.as_secs() as i64)
}

/// Formats CPU quantities from Kubernetes resource strings.
///
/// Handles:
/// - Nanocores: "250000000n" -> "250m"
/// - Millicores: "500m" -> "500m"
/// - Whole cores: "2" -> "2000m"
/// - Empty or unparseable: returns the original string or "0".
pub fn format_cpu(cpu_str: &str) -> String {
    let s = cpu_str.trim();
    if s.is_empty() {
        return "0".to_string();
    }

    // Nanocores: e.g. "250000000n"
    if let Some(nano_str) = s.strip_suffix('n') {
        if let Ok(nano) = nano_str.parse::<u64>() {
            let milli = nano / 1_000_000;
            if milli >= 1000 && milli.is_multiple_of(1000) {
                return format!("{}", milli / 1000);
            }
            return format!("{}m", milli);
        }
        return s.to_string();
    }

    // Millicores: e.g. "500m"
    if let Some(milli_str) = s.strip_suffix('m') {
        if let Ok(milli) = milli_str.parse::<u64>() {
            if milli >= 1000 && milli.is_multiple_of(1000) {
                return format!("{}", milli / 1000);
            }
            return format!("{}m", milli);
        }
        return s.to_string();
    }

    // Whole cores: e.g. "2" or "1.5"
    if let Ok(cores) = s.parse::<f64>() {
        let milli = (cores * 1000.0) as u64;
        if milli >= 1000 && milli.is_multiple_of(1000) {
            return format!("{}", milli / 1000);
        }
        return format!("{}m", milli);
    }

    s.to_string()
}

/// Formats memory quantities from Kubernetes resource strings.
///
/// Handles:
/// - Ki (kibibytes): "131072Ki" -> "128Mi"
/// - Mi (mebibytes): "256Mi" -> "256Mi"
/// - Gi (gibibytes): "2Gi" -> "2Gi"
/// - Ti (tebibytes): "1Ti" -> "1Ti"
/// - Bare bytes: "1073741824" -> "1Gi"
/// - "e" notation: "128974848" or "129e6" -> "123Mi"
pub fn format_mem(mem_str: &str) -> String {
    let s = mem_str.trim();
    if s.is_empty() {
        return "0".to_string();
    }

    let bytes: f64 = if let Some(val) = s.strip_suffix("Ti") {
        match val.parse::<f64>() {
            Ok(v) => v * 1024.0 * 1024.0 * 1024.0 * 1024.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix("Gi") {
        match val.parse::<f64>() {
            Ok(v) => v * 1024.0 * 1024.0 * 1024.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix("Mi") {
        match val.parse::<f64>() {
            Ok(v) => v * 1024.0 * 1024.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix("Ki") {
        match val.parse::<f64>() {
            Ok(v) => v * 1024.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix('T') {
        match val.parse::<f64>() {
            Ok(v) => v * 1_000_000_000_000.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix('G') {
        match val.parse::<f64>() {
            Ok(v) => v * 1_000_000_000.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix('M') {
        match val.parse::<f64>() {
            Ok(v) => v * 1_000_000.0,
            Err(_) => return s.to_string(),
        }
    } else if let Some(val) = s.strip_suffix('k').or_else(|| s.strip_suffix('K')) {
        match val.parse::<f64>() {
            Ok(v) => v * 1_000.0,
            Err(_) => return s.to_string(),
        }
    } else {
        // Suffix-less: bare bytes (or scientific notation, which `f64::parse`
        // handles natively — no special branch needed).
        match s.parse::<f64>() {
            Ok(v) => v,
            Err(_) => return s.to_string(),
        }
    };

    format_bytes(bytes)
}

/// Converts a byte count into the most appropriate human-readable unit.
fn format_bytes(bytes: f64) -> String {
    const TI: f64 = 1024.0 * 1024.0 * 1024.0 * 1024.0;
    const GI: f64 = 1024.0 * 1024.0 * 1024.0;
    const MI: f64 = 1024.0 * 1024.0;
    const KI: f64 = 1024.0;

    if bytes >= TI {
        let val = bytes / TI;
        if val == val.floor() {
            format!("{}Ti", val as u64)
        } else {
            format!("{:.1}Ti", val)
        }
    } else if bytes >= GI {
        let val = bytes / GI;
        if val == val.floor() {
            format!("{}Gi", val as u64)
        } else {
            format!("{:.1}Gi", val)
        }
    } else if bytes >= MI {
        let val = bytes / MI;
        if val == val.floor() {
            format!("{}Mi", val as u64)
        } else {
            format!("{:.1}Mi", val)
        }
    } else if bytes >= KI {
        let val = bytes / KI;
        if val == val.floor() {
            format!("{}Ki", val as u64)
        } else {
            format!("{:.1}Ki", val)
        }
    } else {
        format!("{}", bytes as u64)
    }
}

/// RAII guard that aborts a tokio task when dropped. Same pattern family as
/// `SuspendGuard`, `TempFile`, `ChildGuard`: holding the guard keeps the task
/// running; dropping it (on any exit path, including unwind) aborts it.
/// Holding the abort handle in an `Option` lets the guard be `Drop`'d
/// explicitly without `Drop` running twice.
///
/// CONTRACT: abort-only, never join. Drops can run under locks (e.g. a
/// DashMap shard lock while its slots drop); `abort()` is signal-and-return,
/// so that's safe — a blocking join in `Drop` would deadlock.
pub struct AbortOnDrop(Option<tokio::task::AbortHandle>);

impl AbortOnDrop {
    pub fn new(handle: tokio::task::AbortHandle) -> Self {
        Self(Some(handle))
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

/// The slice of `s` visible in `width` columns with the caret kept on screen,
/// and the caret's column within that slice.
///
/// A one-line viewport. The caret is the ANCHOR, because a caret you cannot
/// see is a caret you cannot trust: five render sites each wrote `if cursor_x
/// < right_edge { set_cursor(..) }`, so the moment what you typed outgrew the
/// bar the text clipped AND the caret stopped being drawn at all. The guard
/// looked defensive and WAS the bug.
///
/// Grapheme-granular, for the reason `TextInput`'s cursor is: a byte slice
/// between a letter and its combining accent panics. Scrolls only as far as
/// it must, so short input does not jitter — and a value that exactly fills
/// the width gives up its first column, because the caret sitting after its
/// last character needs a cell of its own.
///
/// Lives here rather than only on [`crate::app::text_input::TextInput`]
/// because the filter bar is handed a `&str` and a column, not the input.
pub fn window_around(s: &str, caret_col: usize, width: usize) -> (&str, usize) {
    use unicode_segmentation::UnicodeSegmentation;
    use unicode_width::UnicodeWidthStr;
    if width == 0 {
        return ("", 0);
    }
    let fits = |run: &str| -> usize {
        let mut w = 0usize;
        for (i, g) in run.grapheme_indices(true) {
            let gw = UnicodeWidthStr::width(g);
            if w + gw > width {
                return i;
            }
            w += gw;
        }
        run.len()
    };
    // The caret may sit one past the last column, so the last usable column
    // is `width - 1`.
    let first_col = caret_col.saturating_sub(width - 1);
    if first_col == 0 {
        let end = fits(s);
        return (&s[..end], caret_col);
    }
    let mut start = s.len();
    let mut skipped = 0usize;
    for (i, g) in s.grapheme_indices(true) {
        if skipped >= first_col {
            start = i;
            break;
        }
        skipped += UnicodeWidthStr::width(g);
    }
    let tail = &s[start..];
    let end = fits(tail);
    (&tail[..end], caret_col.saturating_sub(skipped))
}

/// Keep the END of `s`, marking what was dropped with a leading ellipsis.
///
/// The mirror of [`truncate`], for the surfaces where the TAIL is the answer.
/// A nav breadcrumb is one: `pods > /api > describe(web-7f9)` cut on the
/// right loses `describe(web-7f9)`, which is the only part of it you did not
/// already know — you are looking at the thing it names.
pub fn truncate_left(s: &str, max_width: usize) -> std::borrow::Cow<'_, str> {
    use unicode_segmentation::UnicodeSegmentation;
    use unicode_width::UnicodeWidthStr;
    if UnicodeWidthStr::width(s) <= max_width {
        return std::borrow::Cow::Borrowed(s);
    }
    if max_width <= 1 {
        return std::borrow::Cow::Borrowed(if max_width == 0 { "" } else { "\u{2026}" });
    }
    // One column for the ellipsis; fill the rest from the right, in whole
    // grapheme clusters (a byte slice would split one and panic the caller).
    let budget = max_width - 1;
    let mut width = 0usize;
    let mut start = s.len();
    for (i, g) in s.grapheme_indices(true).rev() {
        let w = UnicodeWidthStr::width(g);
        if width + w > budget {
            break;
        }
        width += w;
        start = i;
    }
    std::borrow::Cow::Owned(format!("\u{2026}{}", &s[start..]))
}

/// Truncates a string to the given maximum number of characters.
/// If truncated, appends an ellipsis character.
pub fn truncate(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        s.to_string()
    } else if max <= 1 {
        "\u{2026}".to_string()
    } else {
        let mut result: String = chars[..max - 1].iter().collect();
        result.push('\u{2026}');
        result
    }
}

#[cfg(test)]
#[path = "../tests/util.rs"]
mod tests;
