//! Ages, CPU and memory as a human reads them.

use super::*;
use chrono::Duration;

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
