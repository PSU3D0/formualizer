use crate::engine::graph::tombstones::{EntityKind, TombstoneRegistry};
use crate::engine::vertex::VertexId;

#[test]
fn test_registry_basic_registration_and_take() {
    let mut registry = TombstoneRegistry::default();
    // Use the tuple constructor directly
    let v1 = VertexId(1);
    let v2 = VertexId(2);

    registry.register(EntityKind::Sheet, "OldSheet", v1);
    registry.register(EntityKind::Sheet, "OldSheet", v2);

    let dependents = registry.take_dependents(EntityKind::Sheet, "OldSheet");
    assert_eq!(dependents.len(), 2);
}

#[test]
fn test_registry_entity_isolation() {
    let mut registry = TombstoneRegistry::default();
    let v_sheet = VertexId(10);
    let v_range = VertexId(20);

    registry.register(EntityKind::Sheet, "Data", v_sheet);
    registry.register(EntityKind::NamedRange, "Data", v_range);

    assert_eq!(
        registry.take_dependents(EntityKind::Sheet, "Data"),
        vec![v_sheet]
    );
    assert_eq!(
        registry.take_dependents(EntityKind::NamedRange, "Data"),
        vec![v_range]
    );
}

#[test]
fn test_registry_multiple_names() {
    let mut registry = TombstoneRegistry::default();

    registry.register(EntityKind::Table, "Table1", VertexId(1));
    registry.register(EntityKind::Table, "Table2", VertexId(2));

    assert_eq!(
        registry.take_dependents(EntityKind::Table, "Table1").len(),
        1
    );
    assert_eq!(
        registry.take_dependents(EntityKind::Table, "Table2").len(),
        1
    );
}
