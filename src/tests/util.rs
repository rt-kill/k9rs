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

/// Escapes are TRANSLATED, never forwarded. Nothing passes through to the
/// Rust parser unexamined any more — that is what let `\zs` become `\z`,
/// and `\a` the BELL character.
#[test]
fn vim_magic_known_escapes_are_translated() {
    let pat = SearchPattern::new(r"foo\dbar");
    assert!(pat.is_match("foo7bar")); // \d is a digit in both dialects
    assert!(pat.fallback_reason().is_none());

    let unknown = SearchPattern::new(r"foo\qbar");
    assert!(
        unknown.fallback_reason().is_some(),
        "an escape neither dialect defines is refused, not forwarded",
    );
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

// ---------------------------------------------------------------------------
// Foreign resources Drop has to reach for (2026-09 audit)
// ---------------------------------------------------------------------------

/// The process temp directory is reclaimed — but only when it holds nothing.
///
/// `TempFile::drop` removed a FILE; nothing owned the DIRECTORY, so every run
/// that edited a resource left a `k9rs-<pid>/` behind forever. The cleanup
/// must not be recursive: `SaveTable` and `SaveLogs` write there on purpose
/// and tell the user the path.
#[test]
fn the_temp_dir_is_reclaimed_only_when_empty() {
    let base = std::env::temp_dir().join(format!("k9rs-reclaim-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir(&base).expect("scratch dir");

    // Something the user asked us to save → left alone.
    let saved = base.join("pods-20260914.txt");
    std::fs::write(&saved, b"rows").expect("write");
    super::reclaim_if_empty(&base);
    assert!(base.exists(), "a directory with saved output survives");
    assert!(saved.exists(), "…and so does the file in it");

    // Once nothing of ours is left, the directory goes too.
    std::fs::remove_file(&saved).expect("remove");
    super::reclaim_if_empty(&base);
    assert!(!base.exists(), "an empty temp directory is reclaimed");
}

// ---------------------------------------------------------------------------
// Where vim magic and the Rust regex engine disagree
//
// Each of these used to be a pattern that silently did something else. The
// old translator was a character sieve — it knew which characters wanted a
// backslash and nothing more — so every rule that depends on POSITION or on
// an atom's MEANING came out wrong, and "wrong" always looked like "no
// matches".
// ---------------------------------------------------------------------------

/// `^` anchors only at the start of a pattern or of a `\|` branch. Anywhere
/// else vim reads a literal caret; the old pass anchored it everywhere, so
/// `foo^bar` asked the engine for the impossible.
#[test]
fn vim_magic_caret_is_literal_mid_pattern() {
    let pat = SearchPattern::new("foo^bar");
    assert!(pat.is_match("xx foo^bar yy"));
    assert!(pat.fallback_reason().is_none(), "and it is still a regex");
}

/// Same rule, same reason, other end.
#[test]
fn vim_magic_dollar_is_literal_mid_pattern() {
    let pat = SearchPattern::new("foo$bar");
    assert!(pat.is_match("cost: foo$bar"));

    // At the end of a pattern it still anchors.
    let anchored = SearchPattern::new("foo$");
    assert!(anchored.is_match("xfoo"));
    assert!(!anchored.is_match("fooy"));
}

/// Anchors bind per `\|` branch, which is what makes them worth positioning
/// rather than just "first and last character".
#[test]
fn vim_magic_anchors_are_per_branch() {
    let pat = SearchPattern::new(r"^a\|^b");
    assert!(pat.is_match("abc"));
    assert!(pat.is_match("bcd"));
    assert!(!pat.is_match("xa"));

    let end = SearchPattern::new(r"a$\|b$");
    assert!(end.is_match("xa"));
    assert!(end.is_match("xb"));
    assert!(!end.is_match("ax"));
}

/// Vim lets a count close with a bare `}`. Forwarding that produced
/// `{2,3\}`, which doesn't parse — and an unparseable pattern used to become
/// an unannounced substring search for the eight characters you typed.
#[test]
fn vim_magic_count_closes_with_either_brace() {
    for src in [r"a\{2,3}", r"a\{2,3\}"] {
        let pat = SearchPattern::new(src);
        assert!(pat.fallback_reason().is_none(), "{src} must compile");
        assert!(pat.is_match("aab"), "{src}");
        assert!(pat.is_match("aaab"), "{src}");
        assert!(!pat.is_match("xbx"), "{src}");
    }
}

/// Vim's other count spellings: `-` is lazy, an empty body is `*`, and a
/// leading comma means "from zero" (which Rust rejects outright).
#[test]
fn vim_magic_count_variants() {
    let lazy = SearchPattern::new(r"a\{-}b");
    assert!(lazy.fallback_reason().is_none());
    assert!(lazy.is_match("b"));
    assert!(lazy.is_match("aaab"));

    let empty = SearchPattern::new(r"a\{}b");
    assert!(empty.fallback_reason().is_none());
    assert!(empty.is_match("b"));

    let upto = SearchPattern::new(r"a\{,2}b");
    assert!(upto.fallback_reason().is_none(), "`{{,2}}` is not valid Rust syntax");
    assert!(upto.is_match("aab"));
}

/// A half-typed count must not explode: these compile on every keystroke.
#[test]
fn vim_magic_unterminated_count_still_compiles() {
    let pat = SearchPattern::new(r"a\{2");
    assert!(pat.fallback_reason().is_none(), "mid-typing is not an error");
    assert!(pat.is_match("aa"));
}

/// vim's classes are not Rust's. `\a` is "alphabetic" in vim and the BELL
/// character in Rust — it used to ride through and match nothing.
#[test]
fn vim_magic_character_classes_are_vims() {
    let alpha = SearchPattern::new(r"\a\a\a");
    assert!(alpha.is_match("abc"));
    assert!(!alpha.is_match("123"));

    // `\s` is space-or-tab in vim, not Rust's whole whitespace class.
    let space = SearchPattern::new(r"a\sb");
    assert!(space.is_match("a b"));
    assert!(space.is_match("a\tb"));
    assert!(!space.is_match("ab"));

    // `\d` is ASCII in vim; Rust's is Unicode-aware.
    let digit = SearchPattern::new(r"\d\d");
    assert!(digit.is_match("42"));
    assert!(!digit.is_match("４２"), "fullwidth digits are not vim's \\d");

    assert!(SearchPattern::new(r"\u\l").is_match("Ab"));
    assert!(SearchPattern::new(r"\x\x").is_match("beef"));
}

/// `\=` is vim's own synonym for `\?`; it used to reach Rust as an escaped
/// equals sign.
#[test]
fn vim_magic_equals_is_optional() {
    let pat = SearchPattern::new(r"colou\=r");
    assert!(pat.is_match("color"));
    assert!(pat.is_match("colour"));
}

/// `\%(…\)` groups without capturing.
#[test]
fn vim_magic_percent_paren_is_a_group() {
    let pat = SearchPattern::new(r"\%(pimp\|broker\)-svc");
    assert!(pat.fallback_reason().is_none());
    assert!(pat.is_match("pimp-svc"));
    assert!(pat.is_match("broker-svc"));
    assert!(!pat.is_match("redis-svc"));
}

/// `\c` / `\C` override smartcase wherever they appear, as in vim.
#[test]
fn vim_magic_case_flags_beat_smartcase() {
    // Uppercase would make this case-SENSITIVE under smartcase.
    let forced_off = SearchPattern::new(r"FOO\c");
    assert!(forced_off.is_match("foo"));

    // All-lowercase would make this case-INSENSITIVE under smartcase.
    let forced_on = SearchPattern::new(r"foo\C");
    assert!(forced_on.is_match("foo"));
    assert!(!forced_on.is_match("FOO"));
}

/// Inside a bracket expression vim's backslash rules don't apply and the
/// caret means negation. A translator that escaped by character alone would
/// turn `[^abc]` into the SET `{^,a,b,c}` — the opposite of the ask.
#[test]
fn vim_magic_bracket_expressions_are_copied_through() {
    let neg = SearchPattern::new("[^abc]");
    assert!(neg.is_match("x"));
    assert!(!neg.is_match("a"));

    // And the thing that started all this: a class is a set of CHARACTERS,
    // never alternation. `\|` is how you say "or".
    let class = SearchPattern::new("[pimp|broker]");
    assert!(class.is_match("o"), "a class matches any one of its characters");
    let alternation = SearchPattern::new(r"pimp\|broker");
    assert!(alternation.is_match("pimp"));
    assert!(alternation.is_match("broker"));
    assert!(!alternation.is_match("redis"));
}

/// An unmatched `[` is a literal `[` in vim; Rust calls it a parse error.
#[test]
fn vim_magic_unmatched_bracket_is_literal() {
    let pat = SearchPattern::new("[abc");
    assert!(pat.fallback_reason().is_none(), "vim does not consider this an error");
    assert!(pat.is_match("x[abc]"));
}

/// A leading `*` has nothing to repeat, so vim reads it literally; Rust
/// rejects the pattern outright.
#[test]
fn vim_magic_leading_star_is_literal() {
    let pat = SearchPattern::new("*foo");
    assert!(pat.fallback_reason().is_none());
    assert!(pat.is_match("*foo"));
}

/// `~` means "the previous substitution's replacement" in vim. There is no
/// substitute command here, so it is a literal tilde — a documented
/// divergence rather than a silent one.
#[test]
fn vim_magic_tilde_is_a_literal_tilde() {
    let pat = SearchPattern::new("~/logs");
    assert!(pat.is_match("~/logs/app.log"));
}

/// The atoms this engine genuinely cannot express are REFUSED BY NAME.
/// Every one of them used to be forwarded to Rust and reinterpreted:
/// `\zs` parsed as `\z` (end of text) followed by `s`, a valid regex that
/// can never match — forever, with no indication.
#[test]
fn vim_magic_inexpressible_atoms_say_so() {
    for (src, want) in [
        (r"foo\zsbar", "zs"),
        (r"foo\@=bar", "look-around"),
        (r"\(a\)\1", "backreference"),
        (r"foo\&bar", "intersection"),
        (r"foo\_sbar", "across lines"),
        (r"foo\qbar", "unknown escape"),
    ] {
        let pat = SearchPattern::new(src);
        let why = pat
            .fallback_reason()
            .unwrap_or_else(|| panic!("{src} must not pass for a working regex"));
        assert!(
            why.to_lowercase().contains(want),
            "{src}: reason {why:?} should mention {want:?}",
        );
        // It still SEARCHES — as the literal text, which is the honest
        // reading of a pattern we can't compile.
        assert!(pat.is_match(src), "{src} still matches itself literally");
    }
}

/// A pattern that can't compile still searches, but the reason is carried
/// so the bar can show it. "No lines match" was the least useful thing we
/// could say about a pattern that had stopped being a regex.
#[test]
fn a_broken_pattern_reports_why_instead_of_matching_nothing() {
    let good = SearchPattern::new(r"pimp\|broker");
    assert_eq!(good.fallback_reason(), None);

    let bad = SearchPattern::new(r"\zsfoo");
    assert!(bad.fallback_reason().is_some());
}

// ---------------------------------------------------------------------------
// Patterns that matched EVERYTHING, and messages that explained nothing
// ---------------------------------------------------------------------------

/// An anchor consumes no atom, so a `*` after it still has nothing to
/// repeat. `^*a` used to become a quantifier ON the anchor — "any number of
/// line starts" — which Rust compiles happily and which matches every line
/// containing the atom.
#[test]
fn a_star_after_an_anchor_is_still_a_literal() {
    let pat = SearchPattern::new("^*a");
    assert!(pat.is_match("*abc"), "vim's reading: a literal asterisk at the start");
    assert!(!pat.is_match("xa"), "and NOT every line containing an `a`");
    assert!(!pat.is_match("abc"));
}

/// `a**` is an error in vim. It must not quietly become "match everything".
#[test]
fn a_double_star_does_not_match_everything() {
    let pat = SearchPattern::new("a**");
    assert!(!pat.is_match("zzz"), "a pattern mentioning `a` cannot match a line with none");
}

/// The bar has one row, and the row worth showing is the REASON. It used to
/// show the echoed pattern — including the `(?i)` smartcase had just
/// prepended, which is neither something you typed nor something you can act
/// on.
#[test]
fn a_broken_pattern_reports_the_reason_not_itself() {
    // A count whose bounds are inverted: vim spells it the same way, and
    // the engine genuinely cannot build it.
    let why = SearchPattern::new(r"a\{2,1}")
        .fallback_reason()
        .expect("an inverted count cannot compile")
        .to_string();
    assert!(!why.contains("(?i)"), "the internal case flag must not leak: {why}");
    assert!(
        why.contains("start must be") || why.contains("repetition"),
        "the message should say what is wrong: {why}",
    );
}

/// vim has no class escapes: `[\d]` is the two characters `\` and `d`.
/// Passing it through gave Rust its digit class, so the set was inverted —
/// silently, in Regex mode, with no fallback reason to notice.
#[test]
fn a_backslash_inside_a_class_is_a_literal() {
    let pat = SearchPattern::new(r"[\d]");
    assert!(pat.is_match("d"), "vim matches the letter d");
    assert!(pat.is_match(r"\"), "and the backslash itself");
    assert!(!pat.is_match("5"), "but NOT a digit");
}

/// `\u` and `\l` are ABOUT case, so they must survive smartcase. Under the
/// case-insensitive flag `[A-Z]` matches lowercase too, which made `\u\u\u`
/// — find an all-caps token — match every lowercase word.
#[test]
fn the_case_classes_are_case_sensitive() {
    let upper = SearchPattern::new(r"\u\u\u");
    assert!(upper.is_match("ERROR here"));
    assert!(!upper.is_match("error here"));

    let lower = SearchPattern::new(r"\l\l\l");
    assert!(lower.is_match("abc"));
    assert!(!lower.is_match("ABC"));
}

/// Smartcase skips escape letters, as vim's own `pat_has_uppercase` does:
/// `\S` is "non-whitespace", not a capital S.
#[test]
fn smartcase_ignores_the_letter_inside_an_escape() {
    let pat = SearchPattern::new(r"foo\S");
    assert!(pat.is_match("FOOx"), "the `S` in `\\S` must not force case-sensitivity");
    // A real capital still does.
    assert!(!SearchPattern::new("Foo").is_match("foo"));
}

/// `\<` and `\>` are directional. Both becoming `\b` made `+\>` match `a+b`
/// and `\>e` match the `e` of "the end" — neither of which vim matches.
#[test]
fn word_boundaries_are_directional() {
    assert!(SearchPattern::new(r"\<word\>").is_match("a word here"));
    assert!(!SearchPattern::new(r"\<word\>").is_match("wordy"));
    assert!(!SearchPattern::new(r"+\>").is_match("a+b"), "`+` ends no word");
}

/// These haystacks are single lines by construction, so `\n` could only ever
/// match nothing — silently, forever. Refused by name instead.
#[test]
fn a_newline_escape_is_refused_rather_than_never_matching() {
    let pat = SearchPattern::new(r"error\n");
    let why = pat.fallback_reason().expect(r"\n must not pass as a working regex");
    assert!(why.contains("single lines"), "{why}");
}

/// An unclosed `[` is a literal in vim, so the scan must FAIL before saying
/// so — and the caller then advances one character and may scan again. That
/// is quadratic unless bounded, and the filter draft recompiles about twice
/// a frame.
#[test]
fn a_pathological_bracket_run_compiles_promptly() {
    // Budget is generous because this runs in a DEBUG build; what it
    // separates is the shapes, not the constant. Capped, the work is
    // n × MAX_CLASS_SCAN (~4M char steps here); uncapped it is n² (~64M),
    // which took seconds.
    let pattern: String = "[".repeat(8_000);
    let started = std::time::Instant::now();
    let _ = SearchPattern::new(&pattern);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "took {:?} — an unbounded class rescan is O(n²)",
        started.elapsed(),
    );
}

/// A zero-width match is not a highlight. `[0-9]*` matches the empty string
/// at every position; each one used to become its own rendered span.
#[test]
fn zero_width_matches_are_not_highlighted() {
    assert!(SearchPattern::new("[0-9]*").find_all("abcdef").is_empty());
    // A real match still reports.
    assert_eq!(SearchPattern::new("[0-9]*").find_all("ab12ef"), vec![(2, 4)]);
}

/// `=` is LITERAL in vim's magic mode and needs no escape; `\=` is vim's
/// quantifier for "zero or one" (a synonym for `\?`).
///
/// Reported as a bug — `/shard=` "not matching" and `/shard\=` matching
/// "shard" but not the `=`. Both behaviours are vim's, and the second is the
/// surprising one: `shard\=` is `shar` followed by an OPTIONAL `d`, so it
/// matches strictly more than the unescaped form, not less. Pinned because
/// the intuition that a backslash escapes punctuation is near-universal and
/// exactly backwards here.
#[test]
fn vim_magic_bare_equals_is_literal_and_escaped_equals_is_a_quantifier() {
    let bare = SearchPattern::new("shard=");
    assert!(bare.is_match("relay.shard=3"), "`=` needs no escaping");
    assert!(!bare.is_match("shard without eq"));
    assert!(SearchPattern::new("shard=3").is_match("relay.shard=3"), "…and takes a suffix");

    let escaped = SearchPattern::new(r"shard\=");
    assert!(escaped.is_match("shard without eq"), r"`\=` makes the `d` optional");
    assert!(escaped.is_match("shar"), "…so it matches the stem alone");
    assert!(
        !SearchPattern::new(r"shard\=3").is_match("relay.shard=3"),
        r"`shard\=3` is `shar` + optional `d` + `3` — it cannot match an `=`",
    );
}

/// The bug in one test: a pattern spanning a colour boundary.
///
/// `/workers=` against a line that READS `workers=0` on screen. It failed
/// because the filter ran on the raw bytes, where a reset sequence sits
/// between the `s` and the `=`. Nothing about `=`, and nothing about the
/// format — any pattern crossing a style change was affected.
#[test]
fn a_pattern_can_span_a_colour_boundary() {
    let pat = SearchPattern::new("workers=");
    for raw in [
        "workers=0",
        "\u{1b}[2mworkers\u{1b}[0m=0",
        "\u{1b}[32mworkers\u{1b}[0m=\u{1b}[1m0\u{1b}[0m",
        "\u{1b}[2mts\u{1b}[0m \u{1b}[32mINFO\u{1b}[0m depth=1, workers=0",
    ] {
        let seen = crate::util::visible_text(raw);
        assert!(seen.contains("workers="), "sanity: {seen:?} should read workers=");
        assert!(
            pat.is_match(&seen),
            "a pattern the user can SEE must match: {raw:?} reads {seen:?}",
        );
    }
}

/// The mirror of `truncate`, for the surfaces where the TAIL is the answer.
#[test]
fn truncate_left_keeps_the_end_and_marks_the_cut() {
    use crate::util::truncate_left;

    // Fits: untouched, and borrowed.
    assert_eq!(truncate_left("pods", 10), std::borrow::Cow::Borrowed("pods"));

    // Does not fit: the END survives, with a leading ellipsis.
    // 12 columns in ALL: eleven of tail plus the mark, not twelve plus.
    assert_eq!(truncate_left("pods > /api > describe(web)", 12), "\u{2026}scribe(web)");
    assert_eq!(truncate_left("abcdef", 3), "\u{2026}ef");

    // Degenerate budgets are answers, not panics.
    assert_eq!(truncate_left("abcdef", 1), "\u{2026}");
    assert_eq!(truncate_left("abcdef", 0), "");

    // Never wider than asked, for anything — including wide glyphs and
    // clusters a byte slice would split down the middle.
    for s in [
        "pods > /api > describe(web-7f9c)",
        "日本語のリソース > ポッド",
        "a\u{1F469}\u{200D}\u{1F467}b",
        "\u{26a0}\u{FE0F} warning",
    ] {
        for max in 0..24usize {
            let out = truncate_left(s, max);
            assert!(
                unicode_width::UnicodeWidthStr::width(out.as_ref()) <= max,
                "truncate_left({s:?}, {max}) = {out:?} is wider than {max}",
            );
        }
    }
}
