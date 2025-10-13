use formualizer_sheetport::SheetPort;
use formualizer_workbook::Workbook;
use sheetport_spec::Manifest;

fn make_test_workbook() -> Workbook {
    let mut wb = Workbook::new();
    wb.add_sheet("Inputs");
    wb.add_sheet("Inventory");
    wb.add_sheet("Outputs");
    wb
}

#[test]
fn sheetport_initializes_with_matching_workbook() {
    let yaml = include_str!("../../sheetport-spec/tests/fixtures/supply_planning.yaml");
    let manifest: Manifest = Manifest::from_yaml_str(yaml).expect("fixture parses");
    let mut workbook = make_test_workbook();

    SheetPort::new(&mut workbook, manifest).expect("sheetport binds to workbook");
}

#[test]
fn sheetport_fails_for_missing_sheet() {
    let yaml = include_str!("../../sheetport-spec/tests/fixtures/supply_planning.yaml");
    let manifest: Manifest = Manifest::from_yaml_str(yaml).expect("fixture parses");
    let mut workbook = Workbook::new();
    workbook.add_sheet("Inputs");
    workbook.add_sheet("Outputs");

    let err = match SheetPort::new(&mut workbook, manifest) {
        Ok(_) => panic!("expected missing sheet error"),
        Err(err) => err,
    };
    match err {
        formualizer_sheetport::SheetPortError::MissingSheet { port, sheet } => {
            assert_eq!(port, "sku_inventory");
            assert_eq!(sheet, "Inventory");
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
