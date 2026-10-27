//! The contract: absent is not the same as broken.
//!
//! None of this was testable before the I/O and the parse were separated —
//! the only way to reach these branches was to change `$HOME`, which is the
//! same reason the daemon section's values leaked into the test suite.

use super::*;

#[derive(Debug, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
struct Probe {
    size: u32,
    name: String,
}

impl Default for Probe {
    fn default() -> Self {
        Self { size: 7, name: "default".into() }
    }
}

fn section<T: serde::de::DeserializeOwned>(yaml: &str, s: &str) -> Result<Option<T>, String> {
    section_of(yaml, s, "fixture.yaml")
}

#[test]
fn an_absent_root_is_absent_not_an_error() {
    assert_eq!(section::<Probe>("other: {}\n", "").unwrap(), None);
    assert_eq!(section::<Probe>("", "").unwrap(), None);
}

#[test]
fn an_absent_section_is_absent_even_when_the_root_is_there() {
    let yaml = "k9rs:\n  somethingElse: 1\n";
    assert_eq!(section::<Probe>(yaml, "sub").unwrap(), None);
}

/// The distinction the whole module exists for. A typo must NOT read as
/// "nothing configured" — that would revert every other setting in the file
/// with no feedback.
#[test]
fn a_present_but_invalid_section_is_an_error_naming_the_file_and_the_section() {
    let err = section::<Probe>("k9rs:\n  sub:\n    sixe: 1\n", "sub")
        .expect_err("an unknown field is a rejection, not an absence");
    assert!(err.contains("fixture.yaml"), "names the file: {err}");
    assert!(err.contains("'sub'"), "names the section: {err}");
}

#[test]
fn broken_yaml_is_an_error_not_an_absence() {
    let err = section::<Probe>("k9rs:\n  sub:\n   - [unclosed\n", "sub")
        .expect_err("a syntax error is a rejection");
    assert!(err.contains("YAML syntax error"), "says what kind: {err}");
}

#[test]
fn the_root_itself_is_the_empty_section_name() {
    let got: Probe = section("k9rs:\n  size: 3\n  name: root\n", "")
        .unwrap()
        .expect("the root deserializes");
    assert_eq!(got, Probe { size: 3, name: "root".into() });
}

#[test]
fn a_partial_section_keeps_the_rest_of_its_defaults() {
    let got: Probe = section("k9rs:\n  sub:\n    size: 9\n", "sub").unwrap().unwrap();
    assert_eq!(got, Probe { size: 9, name: "default".into() });
}

/// `path()` is the only place the location is written, so this is the only
/// place it can be checked.
#[test]
fn the_path_is_under_the_homes_config_dir() {
    let Some(p) = path() else { return };
    let s = p.to_string_lossy();
    assert!(s.ends_with("/.config/k9rs/config.yaml"), "unexpected location: {s}");
}
