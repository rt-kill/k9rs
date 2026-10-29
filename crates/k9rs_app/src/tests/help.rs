//! What `?` actually projects.
//!
//! `help.rs` had no mirror, which left its central claim unchecked: that
//! every key on the screen comes from the keymap declaration or the
//! capability manifest, for the view the user pressed `?` in. A context-free
//! dump is the thing it replaced, and the only way to know it stays replaced
//! is to ask it per class.

use super::*;

fn sheet_for(class: crate::keymap::ViewClass) -> crate::help::HelpSheet {
    HelpContent::sheet(None, crate::KeysConfig::default(), class, false)
}

/// The sheet flattened back to `(section, key, description)` — what the
/// single-table version used to be. The content claims below are about the
/// content, so they are asked of every row at both levels; only the SHAPE
/// changed when `?` became an index.
fn flat_for(class: crate::keymap::ViewClass) -> Vec<(String, String, String)> {
    flatten(&sheet_for(class))
}

fn flatten(sheet: &crate::help::HelpSheet) -> Vec<(String, String, String)> {
    sheet
        .sections
        .iter()
        .flat_map(|(title, table)| {
            table.rows.iter().map(move |r| (title.clone(), cell(r, 0), cell(r, 1)))
        })
        .collect()
}

fn cell(row: &k9rs_proto::row::ResourceRow, i: usize) -> String {
    match row.cells.get(i) {
        Some(k9rs_proto::row::CellValue::Text(t)) => t.clone(),
        other => panic!("expected text at {i}, got {other:?}"),
    }
}

/// Every class produces an index and sections behind it, with the columns
/// each level declares.
#[test]
fn every_view_class_has_help_to_show() {
    for &class in crate::keymap::ViewClass::ALL {
        let sheet = sheet_for(class);
        let (headers, index) = (&sheet.index.headers, &sheet.index.rows);
        assert_eq!(headers, &vec!["SECTION", "KEYS", "DESCRIPTION"]);
        assert!(!index.is_empty(), "{class:?}: `?` must say something");
        for row in index {
            assert_eq!(row.cells.len(), 3, "{class:?}: {row:?}");
            assert!(!cell(row, 2).is_empty(), "{class:?}: a section with no summary");
        }

        assert!(!sheet.sections.is_empty());
        for (title, table) in &sheet.sections {
            assert_eq!(table.headers, vec!["KEY", "DESCRIPTION"]);
            assert!(!table.rows.is_empty(), "{class:?}: section {title:?} is empty");
            for row in &table.rows {
                assert_eq!(row.cells.len(), 2, "{class:?}: {row:?}");
                assert!(!cell(row, 1).is_empty(), "{class:?}: a row with no description");
            }
        }
    }
}

/// The index names exactly the sections that exist, each with its real
/// count, and every index row carries the drill that opens it.
///
/// Both halves come from one `sections()` walk precisely so this holds: a
/// second projection could name a section that was never built, and that
/// row's Enter would go nowhere.
#[test]
fn the_index_and_the_sections_are_the_same_list() {
    use k9rs_proto::row::{CellValue, DrillTarget};
    for &class in crate::keymap::ViewClass::ALL {
        let sheet = sheet_for(class);
        let index = &sheet.index.rows;

        let listed: Vec<String> = index.iter().map(|r| cell(r, 0)).collect();
        let built: Vec<String> = sheet.sections.iter().map(|(t, _)| t.clone()).collect();
        assert_eq!(listed, built, "{class:?}: the index and the sections disagree");

        for (row, (title, table)) in index.iter().zip(&sheet.sections) {
            assert_eq!(
                row.drill_target,
                Some(DrillTarget::HelpSection(title.clone())),
                "{class:?}: the row must say what Enter does",
            );
            assert_eq!(row.name, *title, "the row's identity is its section");
            assert_eq!(
                row.cells[1],
                CellValue::Count(table.rows.len() as i64),
                "{class:?}: section {title:?} advertises the wrong count",
            );
        }
    }
}

/// The index is a handful of rows where the flat sheet was dozens — and it
/// does not grow with the number of keys, which is the property that made
/// this fix cross a threshold sectioning and trimming could not.
///
/// A PROJECTION fact, deliberately: no terminal arithmetic here. The first
/// version of this test asserted "fits 80x24" by writing
/// `(24.0 * 0.85) as usize - 3` — restating the float's allowance and the
/// table's chrome, both of which live in `element::Element::extent` and
/// `table_content_size`. Had either moved, this would have gone on passing
/// while its claim stopped being about anything.
/// `the_help_float_is_tall_enough_for_its_index` asks the real geometry.
#[test]
fn the_index_is_far_shorter_than_the_sheet_behind_it() {
    for &class in crate::keymap::ViewClass::ALL {
        let sheet = sheet_for(class);
        let sections = sheet.index.rows.len();
        let flat = flatten(&sheet).len();
        assert!(
            sections <= 8,
            "{class:?}: the index is {sections} rows — it is supposed to be one \
             per section, not one per key",
        );
        assert!(
            flat >= sections * 3,
            "{class:?}: the index is {sections} rows against {flat} behind it — if \
             those are close, the index is not buying anything",
        );
    }
}

/// Help is CONTEXTUAL: the class's own section leads, and its keys are that
/// class's. The screen this replaced described every view at once, so `s`
/// and `Ctrl-d` appeared with no indication of which view they meant.
#[test]
fn help_leads_with_the_class_the_user_pressed_it_in() {
    for &class in crate::keymap::ViewClass::ALL {
        let index = sheet_for(class).index.rows;
        assert_eq!(
            cell(&index[0], 0),
            class.title(),
            "{class:?}: the index must LEAD with this view's section",
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
        let mut checked = 0;
        for (section, key, _) in flat_for(class) {
            if section != class.title() && section != "Everywhere" {
                continue;
            }
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
    let sheet = HelpContent::sheet(None, keys, crate::keymap::ViewClass::Table, false);
    let shown: Vec<String> = flatten(&sheet).into_iter().map(|(_, k, _)| k).collect();
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
        let sheet = sheet_for(class);
        // Per STORE, since each level is its own store: the index, and each
        // section.
        let mut stores: Vec<Vec<String>> =
            vec![sheet.index.rows.iter().map(|r| r.name.clone()).collect()];
        stores.extend(
            sheet.sections.iter().map(|(_, table)| {
                table.rows.iter().map(|r| r.name.clone()).collect::<Vec<_>>()
            }),
        );
        for names in stores {
            let distinct: std::collections::HashSet<&String> = names.iter().collect();
            assert_eq!(
                distinct.len(),
                names.len(),
                "{class:?}: two rows of one help store share a name, so it sees one",
            );
        }
    }
}

/// Pressing `?` captures the whole sheet, so every section the index names
/// can be opened — and the stores hold that section's own rows.
///
/// Captured at the press rather than re-derived on Enter because help is
/// CONTEXTUAL: by the time Enter lands, the top of the stack is help, so
/// `current_capabilities()` and `ViewClass::of` would answer about help
/// instead of the view it documents. That is the bug this shape avoids, and
/// it is invisible unless the capture is checked against the index.
#[test]
fn a_press_captures_every_section_the_index_names() {
    use k9rs_proto::row::{CellValue, DrillTarget};

    let mut app = crate::App::new_for_test();
    let class = crate::keymap::ViewClass::of(app.nav.top());
    let caps = Some(app.current_capabilities());
    let markable = app.nav.top().markable();
    let index = app.core.seed_keybindings(caps.clone(), app.config.keys, class, markable);

    let index_rows = index.with_read(|i| i.rows.clone());
    assert!(!index_rows.is_empty(), "`?` listed nothing");

    for row in &index_rows {
        let Some(DrillTarget::HelpSection(title)) = &row.drill_target else {
            panic!("every index row must say what Enter does: {row:?}");
        };
        let store = app
            .core
            .keybinding_section(title)
            .unwrap_or_else(|| panic!("the index names {title:?} but nothing captured it"));

        let rows = store.with_read(|i| i.rows.clone());
        let advertised = match &row.cells[1] {
            CellValue::Count(n) => *n as usize,
            other => panic!("KEYS is a count, got {other:?}"),
        };
        assert_eq!(rows.len(), advertised, "{title}: the count must be the truth");
        assert!(
            rows.iter().all(|r| r.name.starts_with(title.as_str())),
            "{title}: a section holds its OWN rows",
        );
    }

    // A second press REPLACES the capture — contextual content and shared
    // storage cannot both be true, which is the rule `seed_keybindings`
    // already kept for the index store.
    let before = app.core.keybinding_section(
        &match &index_rows[0].drill_target {
            Some(DrillTarget::HelpSection(t)) => t.clone(),
            other => panic!("{other:?}"),
        },
    );
    let _ = app.core.seed_keybindings(caps, app.config.keys, crate::keymap::ViewClass::Log, false);
    let log_sections: Vec<String> = app
        .core
        .keybinding_section(crate::keymap::ViewClass::Log.title())
        .map(|_| vec![crate::keymap::ViewClass::Log.title().to_string()])
        .unwrap_or_default();
    assert_eq!(
        log_sections,
        vec![crate::keymap::ViewClass::Log.title().to_string()],
        "the second press captured the LOG view's sections",
    );
    assert!(
        before.is_some(),
        "sanity: the first press had captured its own leading section",
    );
    assert!(
        app.core.keybinding_section(class.title()).is_none(),
        "…and the table view's section is gone, not left to be opened under \
         the log view's breadcrumb",
    );
}
