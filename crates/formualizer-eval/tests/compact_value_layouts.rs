use formualizer_common::{ExcelError, LiteralValue};
use formualizer_eval::traits::CalcValue;
use formualizer_parse::parser::{ASTNode, ReferenceType};

#[test]
fn compact_value_layouts() {
    println!(
        "ExcelError={} LiteralValue={} Result={} CalcValue={} ReferenceType={} ASTNode={}",
        size_of::<ExcelError>(),
        size_of::<LiteralValue>(),
        size_of::<Result<LiteralValue, ExcelError>>(),
        size_of::<CalcValue<'_>>(),
        size_of::<ReferenceType>(),
        size_of::<ASTNode>()
    );
    println!(
        "String={} Array={} Duration={} DateTime={}",
        size_of::<String>(),
        size_of::<Vec<Vec<LiteralValue>>>(),
        size_of::<chrono::Duration>(),
        size_of::<chrono::NaiveDateTime>()
    );
    assert!(size_of::<ExcelError>() <= 16);
    assert!(size_of::<LiteralValue>() <= 32);
    assert!(size_of::<Result<LiteralValue, ExcelError>>() <= 40);
}
