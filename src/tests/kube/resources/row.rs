use super::*;
use std::cmp::Ordering;

// ---- Wire-tag goldens -----------------------------------------------------
//
// DrillTarget (and the two enums embedded in it) ride the wire inside
// ResourceRow. Bincode encodes enum tags positionally (u32 LE), so a
// reorder or mid-enum insert silently corrupts proto-8 framing. These
// pins turn that mistake into a red test. Every variant of all three
// enums is pinned — extend (append-only) when adding variants.

#[test]
fn drill_wire_tags_are_stable() {
    use crate::kube::protocol::{CrdRef, Namespace, ResourceScope};
    use crate::kube::resource_def::BuiltInKind;

    let tag = |t: &DrillTarget| bincode::serialize(t).unwrap()[..4].to_vec();

    assert_eq!(tag(&DrillTarget::PodsInNamespace(Namespace::All)), 0u32.to_le_bytes());
    let crd = CrdRef {
        group: "g".into(), version: "v1".into(), kind: "K".into(),
        plural: "ks".into(), scope: ResourceScope::Namespaced,
    };
    assert_eq!(tag(&DrillTarget::BrowseCrd(crd)), 1u32.to_le_bytes());
    assert_eq!(
        tag(&DrillTarget::PodsByLabels { labels: BTreeMap::new(), breadcrumb: String::new() }),
        2u32.to_le_bytes()
    );
    assert_eq!(
        tag(&DrillTarget::PodsByOwner { uid: String::new(), kind: BuiltInKind::Deployment, name: String::new() }),
        3u32.to_le_bytes()
    );
    assert_eq!(
        tag(&DrillTarget::PodsByField(K8sFieldSelector::MetadataName("x".into()))),
        4u32.to_le_bytes()
    );
    assert_eq!(tag(&DrillTarget::PodsByNameGrep(String::new())), 5u32.to_le_bytes());
    assert_eq!(
        tag(&DrillTarget::JobsByOwner { uid: String::new(), kind: BuiltInKind::CronJob, name: String::new() }),
        6u32.to_le_bytes()
    );
    assert_eq!(tag(&DrillTarget::Derived(DerivedViewKind::Containers)), 7u32.to_le_bytes());

    // Embedded enums, same treatment.
    let sel_tag = |s: &K8sFieldSelector| bincode::serialize(s).unwrap()[..4].to_vec();
    assert_eq!(sel_tag(&K8sFieldSelector::MetadataName("x".into())), 0u32.to_le_bytes());
    assert_eq!(sel_tag(&K8sFieldSelector::SpecNodeName("x".into())), 1u32.to_le_bytes());
    assert_eq!(sel_tag(&K8sFieldSelector::StatusPhase("x".into())), 2u32.to_le_bytes());

    assert_eq!(
        bincode::serialize(&DerivedViewKind::Containers).unwrap()[..4],
        0u32.to_le_bytes()
    );
}

// ---- Display ------------------------------------------------------------

#[test]
fn display_text() {
    assert_eq!(CellValue::Text("hello".into()).to_string(), "hello");
}

#[test]
fn display_ratio() {
    assert_eq!(
        CellValue::Ratio { num: 3, denom: 5 }.to_string(),
        "3/5"
    );
}

#[test]
fn display_quantity_millicores() {
    // 500m stays as "500m"
    assert_eq!(
        CellValue::Quantity { value: 500, unit: QuantityUnit::Millicores }.to_string(),
        "500m"
    );
    // 2000m normalizes to "2" (whole cores)
    assert_eq!(
        CellValue::Quantity { value: 2000, unit: QuantityUnit::Millicores }.to_string(),
        "2"
    );
}

#[test]
fn display_quantity_bytes() {
    // 1 GiB in bytes
    let gib = 1024 * 1024 * 1024;
    assert_eq!(
        CellValue::Quantity { value: gib, unit: QuantityUnit::Bytes }.to_string(),
        "1Gi"
    );
}

#[test]
fn display_age_none() {
    assert_eq!(CellValue::Age(None).to_string(), "<unknown>");
}

#[test]
fn display_count() {
    assert_eq!(CellValue::Count(42).to_string(), "42");
    assert_eq!(CellValue::Count(-1).to_string(), "-1");
}

#[test]
fn display_bool() {
    assert_eq!(CellValue::Bool(true).to_string(), "true");
    assert_eq!(CellValue::Bool(false).to_string(), "false");
}

#[test]
fn display_list() {
    assert_eq!(
        CellValue::List(vec!["a".into(), "b".into(), "c".into()]).to_string(),
        "a,b,c"
    );
    assert_eq!(CellValue::List(vec![]).to_string(), "");
}

#[test]
fn display_status() {
    assert_eq!(
        CellValue::Status { text: "Running".into(), health: RowHealth::Normal }.to_string(),
        "Running"
    );
}

#[test]
fn display_percentage() {
    assert_eq!(CellValue::Percentage(Some(85)).to_string(), "85%");
    assert_eq!(CellValue::Percentage(None).to_string(), "n/a");
}

#[test]
fn display_placeholder() {
    assert_eq!(CellValue::Placeholder.to_string(), "n/a");
}

// ---- Ordering -----------------------------------------------------------

#[test]
fn ord_text() {
    let a = CellValue::Text("alpha".into());
    let b = CellValue::Text("beta".into());
    assert_eq!(a.cmp(&b), Ordering::Less);
}

#[test]
fn ord_count_numeric() {
    let c2 = CellValue::Count(2);
    let c10 = CellValue::Count(10);
    // Numeric, not lexicographic: 2 < 10
    assert_eq!(c2.cmp(&c10), Ordering::Less);
}

#[test]
fn ord_ratio_by_fraction() {
    // 1/3 < 1/2
    let r1 = CellValue::Ratio { num: 1, denom: 3 };
    let r2 = CellValue::Ratio { num: 1, denom: 2 };
    assert_eq!(r1.cmp(&r2), Ordering::Less);

    // 3/4 > 2/4
    let r3 = CellValue::Ratio { num: 3, denom: 4 };
    let r4 = CellValue::Ratio { num: 2, denom: 4 };
    assert_eq!(r3.cmp(&r4), Ordering::Greater);
}

#[test]
fn ord_age_some_before_none() {
    let some_age = CellValue::Age(Some(1000));
    let no_age = CellValue::Age(None);
    // Some sorts before None
    assert_eq!(some_age.cmp(&no_age), Ordering::Less);
}

#[test]
fn ord_age_larger_epoch_sorts_later() {
    let older = CellValue::Age(Some(1000));
    let newer = CellValue::Age(Some(2000));
    assert_eq!(older.cmp(&newer), Ordering::Less);
}

#[test]
fn ord_bool_false_lt_true() {
    assert_eq!(CellValue::Bool(false).cmp(&CellValue::Bool(true)), Ordering::Less);
}

#[test]
fn ord_percentage_some_before_none() {
    let some_pct = CellValue::Percentage(Some(50));
    let no_pct = CellValue::Percentage(None);
    assert_eq!(some_pct.cmp(&no_pct), Ordering::Less);
}

#[test]
fn ord_percentage_numeric() {
    let low = CellValue::Percentage(Some(10));
    let high = CellValue::Percentage(Some(90));
    assert_eq!(low.cmp(&high), Ordering::Less);
}

#[test]
fn ord_quantity() {
    let small = CellValue::Quantity { value: 100, unit: QuantityUnit::Millicores };
    let big = CellValue::Quantity { value: 500, unit: QuantityUnit::Millicores };
    assert_eq!(small.cmp(&big), Ordering::Less);
}

#[test]
fn ord_list_by_length_then_content() {
    let short = CellValue::List(vec!["a".into()]);
    let long = CellValue::List(vec!["a".into(), "b".into()]);
    assert_eq!(short.cmp(&long), Ordering::Less);

    // Same length: compare by joined content
    let ab = CellValue::List(vec!["a".into(), "b".into()]);
    let ac = CellValue::List(vec!["a".into(), "c".into()]);
    assert_eq!(ab.cmp(&ac), Ordering::Less);
}

#[test]
fn ord_placeholder_equal() {
    assert_eq!(CellValue::Placeholder.cmp(&CellValue::Placeholder), Ordering::Equal);
}

#[test]
fn ord_cross_variant_deterministic() {
    let text = CellValue::Text("z".into());
    let count = CellValue::Count(0);
    // Text (disc 0) < Count (disc 4)
    assert_eq!(text.cmp(&count), Ordering::Less);
}

// ---- Bincode roundtrip --------------------------------------------------

#[test]
fn bincode_roundtrip() {
    let values = vec![
        CellValue::Text("hello".into()),
        CellValue::Ratio { num: 3, denom: 5 },
        CellValue::Quantity { value: 500, unit: QuantityUnit::Millicores },
        CellValue::Quantity { value: 1024 * 1024, unit: QuantityUnit::Bytes },
        CellValue::Age(Some(1713600000)),
        CellValue::Age(None),
        CellValue::Count(42),
        CellValue::Bool(true),
        CellValue::List(vec!["a".into(), "b".into()]),
        CellValue::Status { text: "Running".into(), health: RowHealth::Normal },
        CellValue::Percentage(Some(85)),
        CellValue::Percentage(None),
        CellValue::Placeholder,
    ];

    for val in &values {
        let encoded = bincode::serialize(val).expect("serialize");
        let decoded: CellValue = bincode::deserialize(&encoded).expect("deserialize");
        assert_eq!(&decoded, val, "roundtrip failed for {:?}", val);
    }
}

// ---- set_cell -------------------------------------------------------------

#[test]
fn set_cell_in_bounds() {
    let mut row = ResourceRow {
        cells: vec![CellValue::Placeholder, CellValue::Placeholder],
        ..Default::default()
    };
    row.set_cell(1, CellValue::Count(7));
    assert_eq!(row.cells[1], CellValue::Count(7));
}

#[test]
fn set_cell_out_of_bounds_noop() {
    let mut row = ResourceRow {
        cells: vec![CellValue::Placeholder],
        ..Default::default()
    };
    // Should not panic
    row.set_cell(5, CellValue::Count(99));
    assert_eq!(row.cells.len(), 1);
}

// ---- Default cells is empty ---------------------------------------

#[test]
fn default_cells_empty() {
    let row = ResourceRow::default();
    assert!(row.cells.is_empty());
}

// ---- from_comma_str --------------------------------------------------

#[test]
fn from_comma_str_empty() {
    assert_eq!(CellValue::from_comma_str(""), CellValue::List(vec![]));
}

#[test]
fn from_comma_str_single() {
    assert_eq!(
        CellValue::from_comma_str("foo"),
        CellValue::List(vec!["foo".into()])
    );
}

#[test]
fn from_comma_str_multiple() {
    assert_eq!(
        CellValue::from_comma_str("a,b,c"),
        CellValue::List(vec!["a".into(), "b".into(), "c".into()])
    );
}

#[test]
fn from_comma_str_trailing_comma() {
    // Trailing comma should not produce an empty entry.
    assert_eq!(
        CellValue::from_comma_str("a,b,"),
        CellValue::List(vec!["a".into(), "b".into()])
    );
}
