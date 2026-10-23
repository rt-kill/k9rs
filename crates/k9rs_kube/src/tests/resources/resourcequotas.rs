use super::*;

fn q(pairs: &[(&str, &str)]) -> Option<QuotaMap> {
    Some(
        pairs
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    k8s_openapi::apimachinery::pkg::api::resource::Quantity(v.to_string()),
                )
            })
            .collect(),
    )
}

/// At the limit is the state you came to find: the next pod won't schedule.
#[test]
fn a_quota_at_its_limit_reads_as_failed() {
    assert_eq!(
        quota_health(&q(&[("pods", "50")]), &q(&[("pods", "50")])),
        RowHealth::Failed,
    );
    assert_eq!(
        quota_health(&q(&[("pods", "46")]), &q(&[("pods", "50")])),
        RowHealth::Pending,
        "92% has no slack left",
    );
    assert_eq!(
        quota_health(&q(&[("pods", "5")]), &q(&[("pods", "50")])),
        RowHealth::Normal,
    );
}

/// The WORST key decides — one exhausted resource blocks the namespace even
/// if everything else is idle.
#[test]
fn the_worst_key_decides_the_row() {
    assert_eq!(
        quota_health(
            &q(&[("pods", "1"), ("services", "10")]),
            &q(&[("pods", "50"), ("services", "10")]),
        ),
        RowHealth::Failed,
    );
}

/// Units come from the KEY, which the API defines — `requests.cpu` is
/// millicores and `limits.memory` is bytes, and both have suffixes that a
/// naive numeric compare would drop.
#[test]
fn quota_values_are_parsed_in_the_unit_their_key_declares() {
    // 2 cores used of 4: comfortable. A bare numeric compare would read
    // "2000m" as 2000 against "4" and scream.
    assert_eq!(
        quota_health(&q(&[("requests.cpu", "2000m")]), &q(&[("requests.cpu", "4")])),
        RowHealth::Normal,
    );
    // 100Mi of 1Gi is ten percent — the case that made a wrong red possible.
    assert_eq!(
        quota_health(&q(&[("limits.memory", "100Mi")]), &q(&[("limits.memory", "1Gi")])),
        RowHealth::Normal,
    );
    // And it still fires when the units genuinely say so.
    assert_eq!(
        quota_health(&q(&[("limits.memory", "1Gi")]), &q(&[("limits.memory", "1Gi")])),
        RowHealth::Failed,
    );
}

/// A key with no comparable reading is SKIPPED, not guessed at. Silence is
/// the honest answer; a wrong red is worse than no red.
#[test]
fn an_unparseable_key_is_skipped_rather_than_guessed() {
    assert_eq!(
        quota_health(&q(&[("pods", "not-a-number")]), &q(&[("pods", "50")])),
        RowHealth::Normal,
    );
    // A key present in `hard` but absent from `used` says nothing either.
    assert_eq!(
        quota_health(&q(&[]), &q(&[("pods", "50")])),
        RowHealth::Normal,
    );
    // And a zero limit can't be divided by.
    assert_eq!(
        quota_health(&q(&[("pods", "3")]), &q(&[("pods", "0")])),
        RowHealth::Normal,
    );
}

/// The promise this module is built on: a value it cannot READ is skipped,
/// never scored. The first version wrapped the shared metrics parsers
/// unconditionally in `Some`, and those answer `0` on failure — so an
/// unreadable value scored 0 % and painted a full quota GREEN.
#[test]
fn an_unreadable_quantity_is_skipped_not_scored_as_zero() {
    // Petabyte-scale storage: 1Pi of 1024Ti is exactly 100 % consumed.
    assert_eq!(
        quota_health(
            &q(&[("requests.storage", "1Pi")]),
            &q(&[("requests.storage", "1024Ti")]),
        ),
        RowHealth::Failed,
        "Pi/Ei are real suffixes on large clusters and must not read as 0",
    );
    // A suffix nothing understands is silence, not green-because-zero.
    assert_eq!(parse_quota_value("limits.memory", "12Qi"), None);
    assert_eq!(parse_quota_value("requests.cpu", "lots"), None);
    // But a genuine zero is a value.
    assert_eq!(parse_quota_value("limits.memory", "0"), Some(0));
    assert_eq!(parse_quota_value("limits.memory", "0Mi"), Some(0));
}
