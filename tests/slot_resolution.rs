use ipsa_gateway::query::resolve_requested_slot;

#[test]
fn a_named_slot_is_used_as_given() {
    assert_eq!(resolve_requested_slot(63907880, 64000000), Some(63907880));
    assert_eq!(resolve_requested_slot(1, 64000000), Some(1));
}

#[test]
fn zero_resolves_to_the_ledger_before_the_published_head() {
    assert_eq!(resolve_requested_slot(0, 64000000), Some(63999999));
}

#[test]
fn zero_with_nothing_published_is_refused_rather_than_fetching_ledger_zero() {
    assert_eq!(resolve_requested_slot(0, 0), None);
    assert_eq!(resolve_requested_slot(0, 1), None);
}
