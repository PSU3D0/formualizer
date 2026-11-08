use super::super::{
    geom::Rect,
    level_index::QueryWorkBudget,
    store::{F_DYNAMIC, LkKey, OriginSpec, Store, Tag, TagFilter},
};
use super::support::{Formula, abs};

#[derive(Debug, PartialEq, Eq)]
enum Error {
    Limit,
    Visitor,
}
struct Cap {
    used: u64,
    max: u64,
}
impl QueryWorkBudget for Cap {
    type Error = Error;
    fn charge_work(&mut self, units: u64) -> Result<(), Error> {
        let next = self.used.checked_add(units).ok_or(Error::Limit)?;
        if next > self.max {
            return Err(Error::Limit);
        }
        self.used = next;
        Ok(())
    }
}
fn facts(template: u64) -> super::super::store::FormulaFacts {
    Formula {
        refs: vec![abs(0, 0, 0)],
        l: template,
        literal: 0,
    }
    .facts()
}
#[test]
fn bounded_store_streams_extended_edges_and_stops_on_visitor_failure() {
    let mut extended = facts(1);
    extended.edges[0].tag = Tag::X;
    extended.edges[0].origin = OriginSpec::Symbol(LkKey {
        ctx: 0,
        kind: 0,
        name: "Alias".into(),
    });
    let store = Store::build(vec![((0, 0, 1), extended), ((0, 0, 2), facts(2))]);
    let before = store.digest();
    let mut cap = Cap {
        used: 0,
        max: u64::MAX,
    };
    let mut all = Vec::new();
    store
        .visit_direct_dependents_bounded(
            0,
            &Rect::cell(0, 0),
            TagFilter::All,
            &mut cap,
            &mut |sheet, rect, _| {
                all.push((sheet, rect));
                Ok(())
            },
        )
        .unwrap();
    all.sort_unstable();
    assert_eq!(all, vec![(0, Rect::cell(0, 1)), (0, Rect::cell(0, 2))]);
    let mut r1 = Vec::new();
    store
        .visit_direct_dependents_bounded(
            0,
            &Rect::cell(0, 0),
            TagFilter::R1Only,
            &mut cap,
            &mut |sheet, rect, _| {
                r1.push((sheet, rect));
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(r1, vec![(0, Rect::cell(0, 2))]);
    let mut visited = 0;
    assert_eq!(
        store.visit_direct_dependents_bounded(
            0,
            &Rect::cell(0, 0),
            TagFilter::All,
            &mut cap,
            &mut |_, _, _| {
                visited += 1;
                Err(Error::Visitor)
            }
        ),
        Err(Error::Visitor)
    );
    assert_eq!(visited, 1);
    assert_eq!(store.digest(), before);
}
#[test]
fn bounded_store_zero_work_denies_nonmatching_lookup_before_visiting() {
    let store = Store::build(vec![((0, 0, 1), facts(1))]);
    let before = store.digest();
    let mut cap = Cap { used: 0, max: 0 };
    let mut visited = false;
    assert_eq!(
        store.visit_direct_dependents_bounded(
            0,
            &Rect::cell(900, 0),
            TagFilter::All,
            &mut cap,
            &mut |_, _, _| {
                visited = true;
                Ok(())
            }
        ),
        Err(Error::Limit)
    );
    assert!(!visited);
    assert_eq!(cap.used, 0);
    assert_eq!(store.digest(), before);
}
#[test]
fn bounded_store_inventory_admits_dead_slots_and_includes_singletons() {
    let mut input = Vec::new();
    for row in 0..10 {
        let mut item = facts(row as u64);
        item.ltokens = None;
        item.flags = F_DYNAMIC;
        input.push(((0, row, 1), item));
    }
    let mut store = Store::build(input);
    for row in 0..5 {
        store.clear_cell((0, row, 1)).unwrap();
    }
    let before = store.digest();
    let mut cap = Cap {
        used: 0,
        max: u64::MAX,
    };
    let mut live = Vec::new();
    store
        .visit_owner_slots_bounded(&mut cap, &mut |sheet, rect, flags, _| {
            assert_eq!(flags, F_DYNAMIC);
            assert!(rect.is_cell());
            live.push((sheet, rect));
            Ok(())
        })
        .unwrap();
    assert_eq!(live.len(), 5);
    assert!(
        cap.used > live.len() as u64,
        "dead slots must be admitted too"
    );
    let mut short = Cap {
        used: 0,
        max: cap.used - 1,
    };
    assert_eq!(
        store.visit_owner_slots_bounded(&mut short, &mut |_, _, _, _| Ok(())),
        Err(Error::Limit)
    );
    assert!(short.used <= short.max);
    assert_eq!(store.digest(), before);
}
