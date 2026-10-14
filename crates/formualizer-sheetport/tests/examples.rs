#[path = "../examples/loan.rs"]
mod loan;
#[path = "../examples/table.rs"]
mod table;

#[test]
fn table_example() {
    table::main().unwrap();
}

#[test]
fn loan_example() {
    loan::main().unwrap();
}
