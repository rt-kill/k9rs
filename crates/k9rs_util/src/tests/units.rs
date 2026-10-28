//! Ages, CPU and memory as a human reads them.

use super::*;

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
