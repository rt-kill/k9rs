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
