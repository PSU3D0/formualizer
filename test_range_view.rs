use formualizer_eval::test_workbook::TestWorkbook;
use formualizer_eval::traits::ArgumentHandle;
use formualizer_common::LiteralValue;
use formualizer_core::parser::{ASTNode, ASTNodeType};

fn main() {
    // Simple test case: array literal
    let wb = TestWorkbook::new();
    let ctx = wb.interpreter();
    
    let arr = ASTNode::new(
        ASTNodeType::Literal(LiteralValue::Array(vec![vec![
            LiteralValue::Int(1),
            LiteralValue::Int(2),
            LiteralValue::Text("x".into()),
        ]])),
        None,
    );
    
    let arg = ArgumentHandle::new(&arr, &ctx);
    
    // Try to get range_view
    match arg.range_view() {
        Ok(view) => {
            println!("Got RangeView successfully!");
            println!("Dimensions: {:?}", view.dims());
            println!("Kind: {:?}", view.kind_probe());
            
            // Try iterating
            let mut count = 0;
            let res = view.for_each_cell(&mut |v| {
                println!("Cell {}: {:?}", count, v);
                count += 1;
                Ok(())
            });
            
            match res {
                Ok(()) => println!("Iteration successful, {} cells", count),
                Err(e) => println!("Iteration failed: {:?}", e),
            }
        }
        Err(e) => {
            println!("Failed to get range_view: {:?}", e);
        }
    }
}