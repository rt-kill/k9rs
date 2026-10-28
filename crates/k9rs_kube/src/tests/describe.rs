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

/// A field line's `key_end` indexes the text it ships with, even when the
/// cluster put an escape in the key.
///
/// `classified`'s doc explains why cleaning and indexing must be ONE step —
/// clean afterwards and the index slides off its colon — and then named
/// `field_line` as a path that still did it in the wrong order.
/// `field_line`'s input is label and annotation keys and values, which are
/// arbitrary cluster-supplied UTF-8.
#[test]
fn a_field_lines_key_end_indexes_its_own_cleaned_text() {
    // An escape sequence before the colon. Cleaning shortens the string, so
    // an index taken before it would land past the colon.
    let line = super::field_line("Labels\u{1b}[31m:  app=web".to_string());
    assert!(
        !line.text.contains('\u{1b}'),
        "the shipped text is cleaned: {:?}",
        line.text,
    );
    let DescribeLineKind::Field { key_end } = line.kind else {
        panic!("field_line builds a Field: {:?}", line.kind)
    };
    assert!(
        key_end <= line.text.len() && line.text.is_char_boundary(key_end),
        "key_end {key_end} must index the cleaned text {:?}",
        line.text,
    );
    assert_eq!(
        &line.text[..key_end],
        "Labels",
        "and it lands ON the colon, not past it: {:?}",
        line.text,
    );
}

// ---------------------------------------------------------------------------
// The one BuiltInKind -> ApiResource mapping
// ---------------------------------------------------------------------------

/// `api_version` follows K8s' rule for EVERY built-in kind: the version
/// alone for the core group, `group/version` otherwise.
///
/// Over all kinds, not a sample, because the rule had been re-spelled by
/// hand in the daemon's scale handler — a third copy of a mapping already
/// reduced to one, and the kind of copy that agrees until a kind moves
/// groups. The check is a property rather than the formula again: writing
/// the formula in the test would be a fourth copy that could only fail
/// together with the third.
#[test]
fn every_built_in_kinds_api_version_follows_the_group_rule() {
    for &kind in k9rs_proto::resource_def::BuiltInKind::ALL {
        let (ar, scope) = crate::describe::built_in_api_resource(kind);
        let gvr = kind.def().gvr();
        assert_eq!(ar.plural, gvr.plural, "{kind:?}");
        assert_eq!(ar.kind, gvr.kind, "{kind:?}");
        assert_eq!(scope, gvr.scope, "{kind:?}: the scope rides along");
        if ar.group.is_empty() {
            assert_eq!(
                ar.api_version, ar.version,
                "{kind:?}: a core-group kind's apiVersion is its version alone",
            );
        } else {
            assert_eq!(
                ar.api_version,
                format!("{}/{}", ar.group, ar.version),
                "{kind:?}: a grouped kind's apiVersion carries its group",
            );
            assert!(
                ar.api_version.contains('/'),
                "{kind:?}: …and is therefore not a bare version",
            );
        }
    }
}

/// Pods are the core-group case and Deployments the grouped one — spelled
/// out so the rule above is anchored to something a reader can check.
#[test]
fn the_two_api_version_shapes_read_as_expected() {
    use k9rs_proto::resource_def::BuiltInKind;
    let (pod, _) = crate::describe::built_in_api_resource(BuiltInKind::Pod);
    assert_eq!(pod.api_version, "v1");
    assert_eq!(pod.plural, "pods");
    let (dep, _) = crate::describe::built_in_api_resource(BuiltInKind::Deployment);
    assert_eq!(dep.api_version, "apps/v1");
    assert_eq!(dep.plural, "deployments");
}
