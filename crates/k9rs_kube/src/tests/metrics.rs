use super::*;

#[test]
fn parse_mem_handles_integers_and_units() {
    assert_eq!(parse_mem_to_bytes("0"), 0);
    assert_eq!(parse_mem_to_bytes("512"), 512);
    assert_eq!(parse_mem_to_bytes("1Ki"), 1024);
    assert_eq!(parse_mem_to_bytes("2Mi"), 2 * 1024 * 1024);
    assert_eq!(parse_mem_to_bytes("1Gi"), 1024u64.pow(3));
    assert_eq!(parse_mem_to_bytes("1M"), 1_000_000); // decimal SI
    assert_eq!(parse_mem_to_bytes("nonsense"), 0);
}

#[test]
fn parse_mem_handles_fractional_quantities() {
    // The old u64-only parse rendered these as 0.
    assert_eq!(parse_mem_to_bytes("1.5Gi"), 1024u64.pow(3) * 3 / 2);
    assert_eq!(parse_mem_to_bytes("0.5Mi"), 1024 * 1024 / 2);
}
