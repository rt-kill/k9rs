//! Unit tests for `kube::describe` (mirrored out of the source file).

use super::*;
use k9rs_proto::protocol::{describe_lines_text, DescribeLineKind};

#[test]
fn classify_blank_and_no_colon_are_plain() {
    assert_eq!(classify_describe_line(""), DescribeLineKind::Plain);
    assert_eq!(classify_describe_line("    "), DescribeLineKind::Plain);
    assert_eq!(classify_describe_line("- item"), DescribeLineKind::Plain);
    assert_eq!(classify_describe_line("              app=foo"), DescribeLineKind::Plain);
}

#[test]
fn classify_section_ends_with_colon() {
    assert_eq!(classify_describe_line("Spec:"), DescribeLineKind::Section);
    assert_eq!(classify_describe_line("  Limits:"), DescribeLineKind::Section);
}

#[test]
fn classify_field_colon_is_absolute_index() {
    // Top-level: colon at byte 4.
    assert_eq!(classify_describe_line("Name:         foo"), DescribeLineKind::Field { key_end: 4 });
    // Indented YAML sub-line: key_end is the absolute index (indent 2 + 8).
    assert_eq!(classify_describe_line("  replicas: 3"), DescribeLineKind::Field { key_end: 10 });
}

#[test]
fn field_line_targets_label_colon_not_value_colon() {
    // The fragility the typed wire fixes. `field_line` (used by the
    // structured producer) tags at the LABEL's colon, ignoring colons in
    // the value...
    assert_eq!(
        field_line("Annotations:  ref: a:b".to_string()).kind,
        DescribeLineKind::Field { key_end: 11 }, // "Annotations".len()
    );
    // ...and keeps a value that ENDS in ':' a Field, where the opaque
    // heuristic used to (and still, for genuinely-opaque text) reads it as
    // a Section header. This divergence is exactly the bug being fixed.
    assert_eq!(
        field_line("Annotations:  trailing:".to_string()).kind,
        DescribeLineKind::Field { key_end: 11 },
    );
    assert_eq!(
        classify_describe_line("Annotations:  trailing:"),
        DescribeLineKind::Section,
    );
}

#[test]
fn flat_projection_roundtrips_line_count() {
    // Flatten-then-split must preserve the line count exactly (no trailing
    // newline drift, no embedded newlines) so search/scroll/line_count are
    // unaffected by the typed representation.
    let lines = describe_lines_from_text("a: 1\nSection:\n  b: 2\n\n- x");
    assert_eq!(lines.len(), 5);
    assert_eq!(describe_lines_text(&lines).lines().count(), lines.len());
}

#[test]
fn format_describe_tags_precisely() {
    let obj: kube::api::DynamicObject = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": "foo", "namespace": "default" },
        "spec": { "replicas": 3 },
    }))
    .expect("valid DynamicObject");
    let lines = format_describe(&obj);

    // The Name field is tagged Field by the producer (not inferred).
    assert!(matches!(lines[0].kind, DescribeLineKind::Field { .. }));
    assert!(lines[0].text.starts_with("Name:"));
    // The Spec subsection header is a Section; the Kind footer a Field.
    assert!(lines.iter().any(|l| l.text == "Spec:" && l.kind == DescribeLineKind::Section));
    assert!(lines
        .iter()
        .any(|l| l.text.starts_with("Kind:") && matches!(l.kind, DescribeLineKind::Field { .. })));
    // Flat projection round-trips for search/scroll.
    assert_eq!(describe_lines_text(&lines).lines().count(), lines.len());
}

/// A describe line's `key_end` indexes the text the VIEW will slice, so the
/// cleaning and the classification have to be one step.
///
/// The kubectl path used to clean one layer up (`strip_ansi` over the whole
/// document) and the client still sanitizes typed lines at ingest — both
/// shorten the string under an index that was measured against the longer
/// one. `field_spans` is defensive enough not to panic, so the failure mode
/// was silent: the key colour lands on the wrong bytes, or the whole line
/// quietly renders plain.
#[test]
fn cleaning_a_line_cannot_slide_its_key_off_the_colon() {
    // A secret value, an annotation, or a coloured kubectl field — anything
    // with an escape BEFORE the structural colon.
    let lines = describe_lines_from_text("\u{1b}[1mAnnotations\u{1b}[0m:  app=foo");
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].text, "Annotations:  app=foo", "the escapes are gone");
    let DescribeLineKind::Field { key_end } = lines[0].kind else {
        panic!("expected a Field, got {:?}", lines[0].kind);
    };
    assert_eq!(&lines[0].text[..key_end], "Annotations", "and the key is intact");

    // The vector the ingest sanitizer exists for: an OSC 52 clipboard write
    // smuggled through a decoded Secret, which renders via `Paragraph` and
    // so is never stripped by ratatui.
    //
    // The ESC and the BEL go; the BODY stays, as literal text. That is the
    // renderer's rule (`skip_ansi_escape` consumes ESC + one char for a
    // non-CSI escape, by design, so the index it returns is always a char
    // boundary) and therefore ours — the screen shows exactly this. It is
    // also the better answer for a Secret: the escape can no longer reach
    // the terminal, and we have not silently deleted 18 bytes of somebody's
    // value. The old `strip_ansi` swallowed the body, which made the
    // kubectl path disagree with the typed path about the CONTENT of a
    // line, not merely about offsets.
    let hostile = describe_lines_from_text("password:  \u{1b}]52;c;cm0gLXJmIC8K\u{7}done");
    assert_eq!(hostile[0].text, "password:  52;c;cm0gLXJmIC8Kdone");
    assert!(!hostile[0].text.contains('\u{1b}'), "nothing executable survives");

    // Every produced line is clean, whatever the producer did.
    for l in describe_lines_from_text("a:\x1b[32m 1\nSection:\n\x07  b: 2") {
        assert!(
            !l.text.chars().any(|c| c.is_control() && c != '\t'),
            "{:?} still carries a control char",
            l.text,
        );
    }
}
