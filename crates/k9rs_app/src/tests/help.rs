//! What `?` actually projects.
//!
//! `help.rs` had no mirror, which left its central claim unchecked: that
//! every key on the screen comes from the keymap declaration or the
//! capability manifest, for the view the user pressed `?` in. A context-free
//! dump is the thing it replaced, and the only way to know it stays replaced
//! is to ask it per class.

use super::*;

fn rows_for(class: crate::keymap::ViewClass) -> (Vec<String>, Vec<k9rs_proto::row::ResourceRow>) {
    HelpContent::rows(None, crate::KeysConfig::default(), class, false)
}

fn cell(row: &k9rs_proto::row::ResourceRow, i: usize) -> String {
    match row.cells.get(i) {
        Some(k9rs_proto::row::CellValue::Text(t)) => t.clone(),
        other => panic!("expected text at {i}, got {other:?}"),
    }
}

/// Every class produces rows, with the three columns the table declares.
#[test]
fn every_view_class_has_help_to_show() {
    for &class in crate::keymap::ViewClass::ALL {
        let (headers, rows) = rows_for(class);
        assert_eq!(headers, vec!["SECTION", "KEY", "DESCRIPTION"]);
        assert!(!rows.is_empty(), "{class:?}: `?` must say something");
        for row in &rows {
            assert_eq!(row.cells.len(), 3, "{class:?}: {row:?}");
            assert!(!cell(row, 2).is_empty(), "{class:?}: a row with no description");
        }
    }
}

/// Help is CONTEXTUAL: the class's own section leads, and its keys are that
/// class's. The screen this replaced described every view at once, so `s`
/// and `Ctrl-d` appeared with no indication of which view they meant.
#[test]
fn help_leads_with_the_class_the_user_pressed_it_in() {
    for &class in crate::keymap::ViewClass::ALL {
        let (_, rows) = rows_for(class);
        let first = cell(&rows[0], 0);
        assert_eq!(
            first,
            class.title(),
            "{class:?}: the first section must be this view's",
        );
    }
}

/// Every KEY in the two PROJECTED sections — this view's and "Everywhere" —
/// is one the keymap declares. Those two are the ones the file projects
/// rather than writes, and they are where a hand-written entry would drift.
///
/// The other sections are hand-written on purpose and say so: the batch
/// section is derived from the row's `batch_support()` manifest, and the
/// "Search & filter syntax" section's KEY column holds PATTERNS
/// (`foo\|bar`), not chords — which is what this test found when it first
/// claimed every row.
#[test]
fn every_projected_help_key_comes_from_the_keymap() {
    let cfg = crate::KeysConfig::default();
    for &class in crate::keymap::ViewClass::ALL {
        let declared: std::collections::HashSet<String> = crate::keymap::for_class(class)
            .iter()
            .chain(crate::keymap::global().iter())
            .map(|b| b.display_keys(&cfg))
            .collect();
        let (_, rows) = rows_for(class);
        let mut checked = 0;
        for row in &rows {
            let section = cell(row, 0);
            if section != class.title() && section != "Everywhere" {
                continue;
            }
            let key = cell(row, 1);
            // A key-less line is prose belonging to its section, and keeps
            // its row so the statement is not lost.
            if key.is_empty() {
                continue;
            }
            assert!(
                declared.contains(&key),
                "{class:?}: `?` shows {key:?}, which `keymap` does not declare for it",
            );
            checked += 1;
        }
        assert!(checked > 0, "{class:?}: nothing was actually checked");
    }
}

/// A rebound key shows the USER's chord, not the default — help shows
/// effective bindings, which is the whole reason it takes a `KeysConfig`.
#[test]
fn help_shows_the_users_own_bindings() {
    let keys = crate::KeysConfig {
        col_left: Some(crate::KeyCombo::plain('h')),
        col_right: Some(crate::KeyCombo::plain('l')),
        ..Default::default()
    };
    let (_, rows) = HelpContent::rows(None, keys, crate::keymap::ViewClass::Table, false);
    let shown: Vec<String> = rows.iter().map(|r| cell(r, 1)).collect();
    assert!(
        shown.iter().any(|k| k.contains("h / l")),
        "the configured column chords must appear: {shown:?}",
    );
}

/// Each row's `name` is its section and key, so the table's own identity —
/// marks, sort, grep — is stable across redraws. Two rows sharing a name
/// would be one row to the store.
#[test]
fn help_rows_have_distinct_identities() {
    for &class in crate::keymap::ViewClass::ALL {
        let (_, rows) = rows_for(class);
        let names: std::collections::HashSet<&String> = rows.iter().map(|r| &r.name).collect();
        assert_eq!(
            names.len(),
            rows.len(),
            "{class:?}: two help rows share a name, so the store sees one",
        );
    }
}
