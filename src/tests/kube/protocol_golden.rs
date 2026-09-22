use super::{DescribeLine, DescribeLineKind, LogLine};
use crate::kube::resources::row::{CellValue, QuantityUnit, ResourceRow, RowHealth};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn cell_tag(v: &CellValue) -> u32 {
    let b = bincode::serialize(v).unwrap();
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

#[test]
fn cellvalue_variant_tags_are_positional() {
    // bincode encodes the enum discriminant as a u32 LE tag = declaration
    // index. This order IS the wire contract: reordering or inserting a
    // variant silently remaps every cell. (The golden below pins the full
    // byte layout; this documents the tag↔variant map explicitly.)
    assert_eq!(cell_tag(&CellValue::Text(String::new())), 0);
    assert_eq!(cell_tag(&CellValue::Ratio { num: 0, denom: 0 }), 1);
    assert_eq!(cell_tag(&CellValue::Quantity { value: 0, unit: QuantityUnit::Millicores }), 2);
    assert_eq!(cell_tag(&CellValue::Age(None)), 3);
    assert_eq!(cell_tag(&CellValue::Count(0)), 4);
    assert_eq!(cell_tag(&CellValue::Bool(false)), 5);
    assert_eq!(cell_tag(&CellValue::List(Vec::new())), 6);
    assert_eq!(cell_tag(&CellValue::Status { text: String::new(), health: RowHealth::Normal }), 7);
    assert_eq!(cell_tag(&CellValue::Percentage(None)), 8);
    assert_eq!(cell_tag(&CellValue::Placeholder), 9);
}

#[test]
fn cellvalue_wire_layout_golden() {
    // One of every variant, with distinctive payloads — pins both the
    // variant order AND each variant's field layout.
    let sample = vec![
        CellValue::Text("t".into()),
        CellValue::Ratio { num: 1, denom: 2 },
        CellValue::Quantity { value: 3, unit: QuantityUnit::Bytes },
        CellValue::Age(Some(4)),
        CellValue::Count(5),
        CellValue::Bool(true),
        CellValue::List(vec!["a".into()]),
        CellValue::Status { text: "s".into(), health: RowHealth::Failed },
        CellValue::Percentage(Some(6)),
        CellValue::Placeholder,
    ];
    assert_eq!(
        hex(&bincode::serialize(&sample).unwrap()),
        "0a00000000000000000000000100000000000000740100000001000000020000000200000003000000000000000100000003000000010400000000000000040000000500000000000000050000000106000000010000000000000001000000000000006107000000010000000000000073020000000800000001060000000000000009000000",
    );
}

#[test]
fn resourcerow_wire_layout_golden() {
    // Pins ResourceRow field order/presence (incl. the `#[serde(default)]`
    // fields, which are always positionally on the wire). `references` sits
    // between `owner_refs` and `pf_ports` — v15 inserted it there, which is a
    // layout change and therefore a version break, which is the whole reason
    // this test exists.
    let row = ResourceRow {
        cells: vec![CellValue::Text("c".into()), CellValue::Count(9)],
        name: "row".into(),
        namespace: Some("ns".into()),
        drill_target: None,
        containers: Vec::new(),
        owner_refs: Vec::new(),
        references: vec![crate::kube::resources::row::ResourceRefInfo {
            kind: crate::kube::resource_def::BuiltInKind::Secret,
            name: "db".into(),
        }],
        pf_ports: vec![80, 443],
        crd_info: None,
        node: None,
        health: RowHealth::Pending,
        cpu_request: Some(7),
        cpu_limit: None,
        mem_request: None,
        mem_limit: None,
    };
    assert_eq!(
        hex(&bincode::serialize(&row).unwrap()),
        "0200000000000000000000000100000000000000630400000009000000000000000300000000000000726f770102000000000000006e7300000000000000000000000000000000000100000000000000090000000200000000000000646202000000000000005000bb01000001000000010700000000000000000000",
    );
}

#[test]
fn describeline_wire_layout_golden() {
    // The describe wire type added in PROTOCOL_VERSION 6.
    let lines = vec![
        DescribeLine { text: "Name: x".into(), kind: DescribeLineKind::Field { key_end: 4 } },
        DescribeLine { text: "Spec:".into(), kind: DescribeLineKind::Section },
        DescribeLine { text: String::new(), kind: DescribeLineKind::Plain },
    ];
    assert_eq!(
        hex(&bincode::serialize(&lines).unwrap()),
        "030000000000000007000000000000004e616d653a20780100000004000000000000000500000000000000537065633a00000000000000000000000002000000",
    );
}

#[test]
fn logline_wire_layout_golden() {
    // The log-stream wire type added in PROTOCOL_VERSION 7. Field order is
    // `source` (Option — 1-byte bincode tag, then the String if Some), then
    // `timestamp` (same shape, added v16), then `content`. A multi-source
    // stream rides the tagged shape; every single-source line rides the
    // untagged (None) shape.
    let tagged = LogLine { timestamp: None, source: Some("c".into()), content: "hi".into() };
    assert_eq!(
        hex(&bincode::serialize(&tagged).unwrap()),
        "010100000000000000630002000000000000006869",
    );
    // Both Options None: two tag bytes, then the content.
    let untagged = LogLine::untagged("hi");
    assert_eq!(
        hex(&bincode::serialize(&untagged).unwrap()),
        "000002000000000000006869",
    );
}

#[test]
fn logline_flat_text() {
    // Export carries the line's SUBSTANCE, not a transcript of the drawn
    // cells: both pieces of provenance ride along whatever `t` and `p` are
    // set to. All FOUR shapes are pinned, because the one the daemon
    // actually produces — timestamped — is the one the old test left out,
    // and it was the one being silently dropped.
    assert_eq!(LogLine::untagged("hi").flat_text().as_ref(), "hi");
    assert_eq!(
        LogLine { timestamp: None, source: Some("web".into()), content: "msg".into() }.flat_text().as_ref(),
        "[web] msg",
    );
    let ts = "2026-09-18T20:29:40.6Z";
    assert_eq!(
        LogLine { timestamp: Some(ts.into()), source: None, content: "msg".into() }.flat_text().as_ref(),
        format!("{ts} msg"),
    );
    assert_eq!(
        LogLine { timestamp: Some(ts.into()), source: Some("web".into()), content: "msg".into() }
            .flat_text()
            .as_ref(),
        format!("{ts} [web] msg"),
    );
}
