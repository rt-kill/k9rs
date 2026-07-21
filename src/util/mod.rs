pub mod atomic_option;

pub use atomic_option::AtomicOption;

use chrono::{DateTime, Utc};
use unicode_width::UnicodeWidthChar;

// ---------------------------------------------------------------------------
// Smart search (vim-style smartcase + regex)
// ---------------------------------------------------------------------------

/// Compiled search pattern with vim-style smartcase:
/// - If the pattern contains an uppercase letter → case-sensitive
/// - Otherwise → case-insensitive
/// - Treated as regex; falls back to literal match if regex is invalid
#[derive(Debug, Clone)]
pub struct SearchPattern {
    regex: Option<regex::Regex>,
    literal: String,
    case_insensitive: bool,
}

/// Translate vim magic mode metacharacters to Rust regex equivalents.
///
/// Vim magic: `. * ^ $ [ ] \` are special (same as Rust regex), but
/// `+ ? { } ( ) |` are LITERAL unless backslash-escaped (`\+`, `\|`, etc.).
///
/// Bare `|` → literal in vim → escape to `\|` for Rust regex.
/// `\|`     → alternation in vim → emit bare `|` for Rust regex.
/// Same for `+ ? { } ( )`.
fn vim_magic_escape(pattern: &str) -> String {
    const VIM_LITERAL: &[char] = &['+', '?', '{', '}', '(', ')', '|'];
    let mut result = String::with_capacity(pattern.len() + 8);
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.peek() {
                Some(&next) if VIM_LITERAL.contains(&next) => {
                    result.push(next);
                    chars.next();
                }
                _ => result.push(ch),
            }
        } else if VIM_LITERAL.contains(&ch) {
            result.push('\\');
            result.push(ch);
        } else {
            result.push(ch);
        }
    }
    result
}

impl SearchPattern {
    /// Compile a search pattern with vim-style smartcase and magic mode.
    ///
    /// Smartcase: pattern is case-insensitive unless it contains an
    /// uppercase letter (same as vim's `set smartcase`).
    ///
    /// Magic mode: `. * ^ $ [ ] \` are regex-special (same as vim),
    /// but `+ ? { } ( ) |` are treated as literals (vim requires `\+`
    /// etc. to make them special). Users get intuitive substring search
    /// with basic regex power (`.` = any, `*` = repeat, `^`/`$` = anchors)
    /// without PCRE surprises.
    pub fn new(pattern: &str) -> Self {
        let has_upper = pattern.chars().any(|c| c.is_uppercase());
        let case_insensitive = !has_upper;

        let escaped = vim_magic_escape(pattern);
        let regex_pattern = if case_insensitive {
            format!("(?i){}", escaped)
        } else {
            escaped
        };

        let regex = regex::Regex::new(&regex_pattern).ok();

        Self {
            regex,
            literal: if case_insensitive { pattern.to_lowercase() } else { pattern.to_string() },
            case_insensitive,
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
            re.find_iter(text).map(|m| (m.start(), m.end())).collect()
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
    f.write_all(content)?;
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

/// Strip ANSI escape sequences from a string.
/// Handles CSI sequences (ESC[...m), OSC sequences (ESC]...BEL/ST), and simple ESC sequences.
pub fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            // ESC character — consume the escape sequence
            match chars.peek() {
                Some('[') => {
                    chars.next(); // consume '['
                    // CSI sequence: consume until a letter (0x40-0x7E)
                    while let Some(&nc) = chars.peek() {
                        chars.next();
                        if nc.is_ascii_alphabetic() || nc == '~' || nc == '@' {
                            break;
                        }
                    }
                }
                Some(']') => {
                    chars.next(); // consume ']'
                    // OSC sequence: consume until BEL (\x07) or ST (ESC \)
                    while let Some(&nc) = chars.peek() {
                        chars.next();
                        if nc == '\x07' { break; }
                        if nc == '\x1b' {
                            if chars.peek() == Some(&'\\') { chars.next(); }
                            break;
                        }
                    }
                }
                _ => {
                    // Simple ESC sequence — skip next char
                    chars.next();
                }
            }
        } else {
            out.push(c);
        }
    }
    out
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
        if bytes[i] == 0x1b && i + 1 < len && bytes[i + 1] == b'[' {
            // Flush text before this escape.
            if i > text_start {
                push_ansi_text(&mut spans, &line[text_start..i], style);
            }
            // Parse CSI sequence: ESC [ <params> <final byte>
            i += 2; // skip ESC [
            let param_start = i;
            while i < len && !(bytes[i] >= 0x40 && bytes[i] <= 0x7E) {
                i += 1;
            }
            if i < len && bytes[i] == b'm' {
                // SGR sequence — parse parameters and update style.
                let params_str = &line[param_start..i];
                style = apply_sgr(params_str, base, style);
            }
            // Skip the final byte (m, or whatever it is for non-SGR).
            if i < len { i += 1; }
            text_start = i;
        } else if bytes[i] == 0x1b {
            // Non-CSI escape — flush text, skip ESC + next byte (if any).
            if i > text_start {
                push_ansi_text(&mut spans, &line[text_start..i], style);
            }
            i += 1; // skip ESC
            if i < len { i += 1; } // skip next byte if present
            text_start = i;
        } else {
            i += 1;
        }
    }
    // Flush remaining text.
    if text_start < len {
        push_ansi_text(&mut spans, &line[text_start..], style);
    }
    if spans.is_empty() {
        push_ansi_text(&mut spans, line, base);
    }
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

pub fn content_max_scroll(total_lines: usize, visible: usize) -> usize {
    let base = total_lines.saturating_sub(visible);
    if base > 0 { base + visible / 4 } else { 0 }
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
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn sanitize_terminal_drops_escapes_keeps_tab() {
        // The OSC 52 clipboard payload and a cursor-report both vanish.
        assert_eq!(sanitize_terminal("a\x1b]52;c;Zm9v\x07b"), "a]52;c;Zm9vb");
        assert_eq!(sanitize_terminal("x\x1b[6ny"), "x[6ny");
        // Tab survives; CR/BEL/DEL/C1 do not.
        assert_eq!(sanitize_terminal("a\tb\r\x07\x7f\u{0090}c"), "a\tbc");
        // Plain text is unchanged.
        assert_eq!(sanitize_terminal("pod-1 Running"), "pod-1 Running");
    }

    #[test]
    fn parse_ansi_line_drops_non_tab_controls_from_text() {
        // SGR coloring is preserved, but a raw CR/BEL in the text run is
        // dropped so it can't reach the wrap-mode Paragraph render path.
        let base = ratatui::style::Style::default();
        let spans = parse_ansi_line("hi\rthere\x07", base);
        let joined: String = spans.iter().map(|s| s.content.as_ref()).collect();
        assert_eq!(joined, "hithere");
        // A real SGR sequence still produces multiple styled spans.
        let colored = parse_ansi_line("\x1b[31mred\x1b[0m", base);
        let text: String = colored.iter().map(|s| s.content.as_ref()).collect();
        assert_eq!(text, "red");
    }

    #[test]
    fn test_format_age_none() {
        assert_eq!(format_age(None), "<unknown>");
    }

    #[test]
    fn test_format_age_seconds() {
        let ts = Utc::now() - Duration::seconds(30);
        assert_eq!(format_age(Some(ts)), "30s");
    }

    #[test]
    fn test_format_age_minutes() {
        let ts = Utc::now() - Duration::minutes(5) - Duration::seconds(10);
        assert_eq!(format_age(Some(ts)), "5m10s");
    }

    #[test]
    fn test_format_age_hours() {
        let ts = Utc::now() - Duration::hours(3) - Duration::minutes(15);
        assert_eq!(format_age(Some(ts)), "3h15m");
    }

    #[test]
    fn test_format_age_days() {
        let ts = Utc::now() - Duration::days(2) - Duration::hours(5);
        assert_eq!(format_age(Some(ts)), "2d5h");
    }

    #[test]
    fn test_format_cpu_nanocores() {
        assert_eq!(format_cpu("250000000n"), "250m");
    }

    #[test]
    fn test_format_cpu_millicores() {
        assert_eq!(format_cpu("500m"), "500m");
    }

    #[test]
    fn test_format_cpu_whole_cores() {
        assert_eq!(format_cpu("2"), "2");
    }

    #[test]
    fn test_format_mem_ki() {
        assert_eq!(format_mem("131072Ki"), "128Mi");
    }

    #[test]
    fn test_format_mem_mi() {
        assert_eq!(format_mem("256Mi"), "256Mi");
    }

    #[test]
    fn test_format_mem_gi() {
        assert_eq!(format_mem("2Gi"), "2Gi");
    }

    #[test]
    fn test_truncate_short() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_exact() {
        assert_eq!(truncate("hello", 5), "hello");
    }

    #[test]
    fn test_truncate_long() {
        let result = truncate("hello world", 8);
        assert_eq!(result, "hello w\u{2026}");
    }

    #[test]
    fn test_truncate_zero() {
        assert_eq!(truncate("hello", 0), "");
    }

    // -- vim magic mode tests -------------------------------------------------

    #[test]
    fn vim_magic_literal_dash() {
        let pat = SearchPattern::new("-wal");
        assert!(pat.is_match("kube-wallet"));
        assert!(!pat.is_match("firewall-proxy")); // wall-, not -wal
    }

    #[test]
    fn vim_magic_dot_is_special() {
        let pat = SearchPattern::new("foo.bar");
        assert!(pat.is_match("foo-bar")); // . matches any char
        assert!(pat.is_match("foo.bar"));
    }

    #[test]
    fn vim_magic_star_is_special() {
        let pat = SearchPattern::new("ng.*proxy");
        assert!(pat.is_match("nginx-proxy"));
        assert!(pat.is_match("ngproxy")); // .* matches zero chars
    }

    #[test]
    fn vim_magic_parens_are_literal() {
        let pat = SearchPattern::new("foo(bar)");
        assert!(pat.is_match("foo(bar)"));
        assert!(!pat.is_match("foobar")); // parens NOT a capture group
    }

    #[test]
    fn vim_magic_plus_is_literal() {
        let pat = SearchPattern::new("a+b");
        assert!(pat.is_match("a+b"));
        assert!(!pat.is_match("aab")); // + NOT a quantifier
    }

    #[test]
    fn vim_magic_pipe_is_literal() {
        let pat = SearchPattern::new("a|b");
        assert!(pat.is_match("a|b"));
        assert!(!pat.is_match("a")); // | NOT alternation
        assert!(!pat.is_match("b"));
    }

    #[test]
    fn vim_magic_question_is_literal() {
        let pat = SearchPattern::new("a?b");
        assert!(pat.is_match("a?b"));
        assert!(!pat.is_match("ab")); // ? NOT optional
        assert!(!pat.is_match("b"));
    }

    #[test]
    fn vim_magic_escaped_pipe_is_alternation() {
        let pat = SearchPattern::new(r"foo\|bar");
        assert!(pat.is_match("foo"));
        assert!(pat.is_match("bar"));
        assert!(!pat.is_match("baz"));
    }

    #[test]
    fn vim_magic_escaped_plus_is_quantifier() {
        let pat = SearchPattern::new(r"ab\+c");
        assert!(pat.is_match("abc"));
        assert!(pat.is_match("abbc"));
        assert!(!pat.is_match("ac")); // + requires at least one b
    }

    #[test]
    fn vim_magic_escaped_parens_are_group() {
        let pat = SearchPattern::new(r"\(foo\)\|bar");
        assert!(pat.is_match("foo"));
        assert!(pat.is_match("bar"));
        assert!(!pat.is_match("baz"));
    }

    #[test]
    fn vim_magic_escaped_question_is_optional() {
        let pat = SearchPattern::new(r"colou\?r");
        assert!(pat.is_match("color"));
        assert!(pat.is_match("colour"));
    }

    #[test]
    fn vim_magic_backslash_non_special_passes_through() {
        let pat = SearchPattern::new(r"foo\dbar");
        assert!(pat.is_match("foo7bar")); // \d is regex digit
    }

    #[test]
    fn vim_magic_trailing_backslash() {
        let pat = SearchPattern::new(r"foo\");
        assert!(pat.is_match(r"foo\"));
    }

    #[test]
    fn vim_magic_smartcase() {
        let lower = SearchPattern::new("nginx");
        assert!(lower.is_match("NGINX")); // case-insensitive
        let upper = SearchPattern::new("Nginx");
        assert!(!upper.is_match("nginx")); // case-sensitive
        assert!(upper.is_match("Nginx"));
    }
}
