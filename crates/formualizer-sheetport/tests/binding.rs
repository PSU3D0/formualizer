use formualizer_common::RangeAddress;
use formualizer_sheetport::{
    AreaLocation, BoundPort, ManifestBindings, ScalarLocation, TableLocation,
};
use sheetport_spec::Manifest;

#[test]
fn binds_supply_planning_manifest() {
    let yaml = include_str!("../../sheetport-spec/tests/fixtures/supply_planning.yaml");
    let manifest: Manifest = Manifest::from_yaml_str(yaml).expect("fixture parses");
    let bindings = ManifestBindings::new(manifest).expect("manifest binds");

    assert_eq!(bindings.bindings().len(), 4);

    let warehouse = bindings.get("warehouse_code").expect("warehouse port");
    match &warehouse.kind {
        BoundPort::Scalar(binding) => match &binding.location {
            ScalarLocation::Cell(addr) => {
                let expected = RangeAddress::new("Inputs", 2, 2, 2, 2).unwrap();
                assert_eq!(addr, &expected);
            }
            other => panic!("unexpected warehouse location: {other:?}"),
        },
        other => panic!("warehouse port kind mismatch: {other:?}"),
    }

    let planning = bindings
        .get("planning_window")
        .expect("planning_window port");
    match &planning.kind {
        BoundPort::Record(binding) => {
            match &binding.location {
                AreaLocation::Range(addr) => {
                    let expected = RangeAddress::new("Inputs", 1, 2, 1, 3).unwrap();
                    assert_eq!(addr, &expected);
                }
                other => panic!("unexpected planning_window location: {other:?}"),
            }
            let month = binding.fields.get("month").expect("month field");
            match &month.location {
                formualizer_sheetport::FieldLocation::Cell(addr) => {
                    let expected = RangeAddress::new("Inputs", 1, 2, 1, 2).unwrap();
                    assert_eq!(addr, &expected);
                }
                other => panic!("unexpected month field location: {other:?}"),
            }
            let year = binding.fields.get("year").expect("year field");
            match &year.location {
                formualizer_sheetport::FieldLocation::Cell(addr) => {
                    let expected = RangeAddress::new("Inputs", 1, 3, 1, 3).unwrap();
                    assert_eq!(addr, &expected);
                }
                other => panic!("unexpected year field location: {other:?}"),
            }
        }
        other => panic!("planning_window port kind mismatch: {other:?}"),
    }

    let sku_inventory = bindings.get("sku_inventory").expect("inventory port");
    match &sku_inventory.kind {
        BoundPort::Table(binding) => match &binding.location {
            TableLocation::Layout(layout) => {
                assert_eq!(layout.sheet, "Inventory");
                assert_eq!(layout.header_row, 1);
                assert_eq!(layout.anchor_col, "A");
            }
            other => panic!("unexpected sku_inventory location: {other:?}"),
        },
        other => panic!("sku_inventory port kind mismatch: {other:?}"),
    }

    let summary = bindings
        .get("restock_summary")
        .expect("restock_summary port");
    match &summary.kind {
        BoundPort::Record(binding) => match &binding.location {
            AreaLocation::Range(addr) => {
                let expected = RangeAddress::new("Outputs", 2, 2, 6, 2).unwrap();
                assert_eq!(addr, &expected);
            }
            other => panic!("unexpected summary location: {other:?}"),
        },
        other => panic!("restock_summary port kind mismatch: {other:?}"),
    }
}
