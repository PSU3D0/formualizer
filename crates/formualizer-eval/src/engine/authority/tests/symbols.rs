use super::super::identity::IdentityTable;
use super::super::store::{AuthorityError, Budget};
use super::super::symbols::{SymbolId, SymbolTable};
use super::alloc::measure;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn symbol_churn_preserves_live_ids_and_never_reuses_retired_ids(
        trace in prop::collection::vec(prop::collection::btree_set(0u32..64, 0..24), 1..80)
    ) {
        let mut ids = IdentityTable::new();
        let mut table = SymbolTable::default();
        let mut model = BTreeMap::new();
        let mut ever = BTreeSet::new();
        for (row, keys) in trace.iter().enumerate() {
            let cell_id = ids.next_id();
            ids.place(0, row as u32, 0, 1, 0);
            prop_assert!(ever.insert(cell_id));
            let input: Vec<_> = keys.iter().copied().map(SymbolId::new).collect();
            let before = ids.next_id();
            let (next, work) = SymbolTable::rebuild(&input, &table, &mut ids, Budget::default()).unwrap();
            let mut next_model = BTreeMap::new();
            for &key in keys {
                let actual = next.lookup(SymbolId::new(key)).unwrap();
                if let Some(&prior) = model.get(&key) {
                    prop_assert_eq!(actual, prior);
                } else {
                    prop_assert!(actual >= before);
                    prop_assert!(ever.insert(actual));
                }
                prop_assert_eq!(ids.locate(actual), None);
                next_model.insert(key, actual);
            }
            for &key in model.keys().filter(|key| !keys.contains(key)) {
                prop_assert_eq!(next.lookup(SymbolId::new(key)), None);
            }
            prop_assert_eq!(u64::from(ids.next_id() - before), work.created);
            prop_assert_eq!(ids.id_of((0, row as u32, 0)), Some(cell_id));
            prop_assert!(work.visits <= 6 * input.len() as u64 + 2 * model.len() as u64);
            table = next;
            model = next_model;
            ids.check().unwrap();
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn store_mixed_cell_symbol_rebuilds_preserve_live_ids(
        trace in prop::collection::vec((
            prop::collection::btree_set(0u32..24, 0..12),
            prop::collection::btree_set(0u32..24, 0..12),
        ), 1..40)
    ) {
        use super::super::store::Store;
        let mut prior = Store::new();
        let mut cell_model = BTreeMap::new();
        let mut symbol_model = BTreeMap::new();
        let mut ever = BTreeSet::new();
        let formula = super::support::Formula { refs: Vec::new(), l: 1, literal: 0 };
        for (cells, symbols) in trace {
            let input = cells.iter().map(|&row| ((0, row, 0), formula.facts())).collect();
            let live = symbols.iter().copied().map(SymbolId::new).collect();
            let next = Store::rebuild_with_symbols(input, live, Some(&prior), Budget::default()).unwrap();
            let mut next_cells = BTreeMap::new();
            let mut next_symbols = BTreeMap::new();
            for row in cells {
                let id = next.ids().id_of((0, row, 0)).unwrap();
                if let Some(&kept) = cell_model.get(&row) {
                    prop_assert_eq!(id, kept);
                } else {
                    prop_assert!(ever.insert(id));
                    prop_assert!(id >= prior.ids().next_id());
                }
                next_cells.insert(row, id);
            }
            for key in symbols {
                let id = next.symbol_id(SymbolId::new(key)).unwrap();
                if let Some(&kept) = symbol_model.get(&key) {
                    prop_assert_eq!(id, kept);
                } else {
                    prop_assert!(ever.insert(id));
                    prop_assert!(id >= prior.ids().next_id());
                }
                prop_assert_eq!(next.ids().locate(id), None);
                next_symbols.insert(key, id);
            }
            for &key in symbol_model.keys().filter(|k| !next_symbols.contains_key(k)) {
                prop_assert_eq!(next.symbol_id(SymbolId::new(key)), None);
            }
            prop_assert!(next.ids().next_id() >= prior.ids().next_id());
            next.check().unwrap();
            prior = next;
            cell_model = next_cells;
            symbol_model = next_symbols;
        }
    }
}

#[test]
fn store_rebuild_admits_symbols_and_preserves_shared_history() {
    use super::super::store::Store;
    let initial =
        Store::rebuild_with_symbols(Vec::new(), keys(&[4, 8]), None, Budget::default()).unwrap();
    let bytes = initial.heap_bytes();
    assert_eq!(bytes, initial.census_heap_bytes());
    assert_eq!(initial.symbol_id(SymbolId::new(4)), Some(0));
    assert_eq!(initial.symbol_id(SymbolId::new(8)), Some(1));
    assert_eq!(initial.ids().next_id(), 2);
    initial.check().unwrap();
    let before = initial.digest();
    assert!(
        Store::rebuild_with_symbols(
            Vec::new(),
            keys(&[4, 8, 12]),
            Some(&initial),
            Budget {
                retained: Some(bytes),
                scratch: None
            },
        )
        .is_err()
    );
    assert!(
        Store::rebuild_with_symbols(
            Vec::new(),
            keys(&[4, 8]),
            Some(&initial),
            Budget {
                retained: None,
                scratch: Some(bytes + 7)
            },
        )
        .is_err()
    );
    assert_eq!(initial.digest(), before);
    let failing_input = keys(&[4, 8, 12]);
    let input_bytes = (failing_input.capacity() * size_of::<SymbolId>()) as i64;
    let (failure, measured) = measure(Some(0), || {
        Store::rebuild_with_symbols(Vec::new(), failing_input, Some(&initial), Budget::default())
    });
    assert!(matches!(failure, Err(AuthorityError::Alloc)));
    assert!(measured.failed);
    assert_eq!(
        measured.net, -input_bytes,
        "only consumed input was released"
    );
    assert_eq!(initial.ids().next_id(), 2);
    assert_eq!(initial.symbol_id(SymbolId::new(4)), Some(0));
    let same = Store::rebuild_with_symbols(
        Vec::new(),
        keys(&[4, 8]),
        Some(&initial),
        Budget {
            retained: Some(bytes),
            scratch: Some(bytes + 8),
        },
    )
    .unwrap();
    assert_eq!(same.symbol_id(SymbolId::new(4)), Some(0));
    let empty = Store::rebuild_with_symbols(Vec::new(), Vec::new(), Some(&same), Budget::default())
        .unwrap();
    assert_eq!(empty.ids().next_id(), 2);
    let restored =
        Store::rebuild_with_symbols(Vec::new(), keys(&[4]), Some(&empty), Budget::default())
            .unwrap();
    assert_eq!(restored.symbol_id(SymbolId::new(4)), Some(2));
    assert_eq!(restored.ids().next_id(), 3);
    restored.check().unwrap();
}

fn keys(values: &[u32]) -> Vec<SymbolId> {
    values.iter().copied().map(SymbolId::new).collect()
}

#[test]
fn symbols_share_cell_counter_and_survive_rebuild_without_grid_addresses() {
    let mut ids = IdentityTable::new();
    ids.place(0, 10, 0, 2, 0);
    let (prior, work) = SymbolTable::rebuild(
        &keys(&[4, 8]),
        &SymbolTable::default(),
        &mut ids,
        Budget::default(),
    )
    .unwrap();
    assert_eq!(work.created, 2);
    assert_eq!(prior.lookup(SymbolId::new(4)), Some(2));
    assert_eq!(prior.lookup(SymbolId::new(8)), Some(3));
    assert_eq!(ids.locate(2), None);
    ids.place(0, 1, 0, 1, 1);
    assert_eq!(ids.id_of((0, 1, 0)), Some(4));
    let (next, work) =
        SymbolTable::rebuild(&keys(&[2, 4]), &prior, &mut ids, Budget::default()).unwrap();
    assert_eq!(work.created, 1);
    assert_eq!(next.lookup(SymbolId::new(4)), Some(2));
    assert_eq!(next.lookup(SymbolId::new(2)), Some(5));
    assert_eq!(next.lookup(SymbolId::new(8)), None);
    let mut continued = IdentityTable::continuing(ids.next_id(), ids.limit());
    let (restored, work) =
        SymbolTable::rebuild(&keys(&[2, 4, 8]), &next, &mut continued, Budget::default()).unwrap();
    assert_eq!(work.created, 1);
    assert_eq!(restored.lookup(SymbolId::new(8)), Some(6));
    assert_eq!(restored.lookup(SymbolId::new(4)), Some(2));
    assert_eq!(continued.next_id(), 7);
    assert_eq!(ids.id_of((0, 10, 0)), Some(0));
    ids.check().unwrap();
    continued.check().unwrap();
}

#[test]
fn symbols_reject_allocation_admission_exhaustion_and_bad_input_atomically() {
    let mut ids = IdentityTable::with_limit(3);
    let input = keys(&[1, 3]);
    let (prior, _) =
        SymbolTable::rebuild(&input, &SymbolTable::default(), &mut ids, Budget::default()).unwrap();
    let before = ids.next_id();
    let bytes = prior.heap_bytes();
    for budget in [
        Budget {
            retained: Some(bytes - 1),
            scratch: None,
        },
        Budget {
            retained: None,
            scratch: Some(bytes - 1),
        },
    ] {
        assert!(matches!(
            SymbolTable::rebuild(&input, &prior, &mut ids, budget),
            Err(AuthorityError::Admission { .. })
        ));
        assert_eq!(ids.next_id(), before);
    }
    let ((exact, _), measured) = measure(None, || {
        SymbolTable::rebuild(
            &input,
            &prior,
            &mut ids,
            Budget {
                retained: Some(bytes),
                scratch: Some(bytes),
            },
        )
        .unwrap()
    });
    assert_eq!(measured.allocs, 1);
    assert_eq!(measured.peak as u64, exact.heap_bytes());
    assert_eq!(measured.net as u64, exact.heap_bytes());
    let (failure, measured) = measure(Some(0), || {
        SymbolTable::rebuild(&input, &prior, &mut ids, Budget::default())
    });
    assert!(matches!(failure, Err(AuthorityError::Alloc)));
    assert!(measured.failed);
    assert_eq!(measured.net, 0);
    assert_eq!(ids.next_id(), before);
    for bad in [keys(&[1, 1]), keys(&[3, 1]), keys(&[1, 2, 3, 4])] {
        assert!(matches!(
            SymbolTable::rebuild(&bad, &prior, &mut ids, Budget::default()),
            Err(AuthorityError::Identity(_))
        ));
        assert_eq!(ids.next_id(), before);
    }
    assert_eq!(prior.lookup(SymbolId::new(1)), Some(0));
    assert_eq!(prior.lookup(SymbolId::new(3)), Some(1));
    assert!(ids.check_alloc(u64::MAX).is_err());
    let mut wrong_counter = IdentityTable::new();
    assert!(SymbolTable::rebuild(&input, &prior, &mut wrong_counter, Budget::default()).is_err());
    assert_eq!(wrong_counter.next_id(), 0);
}

#[test]
fn symbol_rebuild_counted_work_and_retention_are_linear() {
    for n in [1024u32, 4096, 16384] {
        let input: Vec<_> = (0..n).map(|i| SymbolId::new(i * 2)).collect();
        let shifted: Vec<_> = (0..n).map(|i| SymbolId::new(i * 2 + 1)).collect();
        let mut ids = IdentityTable::new();
        let (prior, initial) =
            SymbolTable::rebuild(&input, &SymbolTable::default(), &mut ids, Budget::default())
                .unwrap();
        let (same, stable) =
            SymbolTable::rebuild(&input, &prior, &mut ids, Budget::default()).unwrap();
        let (replacement, churn) =
            SymbolTable::rebuild(&shifted, &same, &mut ids, Budget::default()).unwrap();
        assert_eq!(initial.visits, 6 * u64::from(n));
        assert_eq!(stable.visits, 8 * u64::from(n) - 2);
        assert_eq!(churn.visits, 8 * u64::from(n));
        assert_eq!(stable.created, 0);
        assert_eq!(churn.created, u64::from(n));
        assert_eq!(replacement.heap_bytes(), 8 * u64::from(n));
        let (empty, _) =
            SymbolTable::rebuild(&[], &replacement, &mut ids, Budget::default()).unwrap();
        assert_eq!(empty.heap_bytes(), 0);
        assert_eq!(ids.next_id(), 2 * n);
        println!(
            "symbols n={n} initial={} stable={} churn={} retained={}",
            initial.visits,
            stable.visits,
            churn.visits,
            replacement.heap_bytes()
        );
    }
}
