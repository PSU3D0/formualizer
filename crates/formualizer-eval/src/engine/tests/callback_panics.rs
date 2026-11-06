use crate::{engine::{Engine,EvalConfig,EvaluationStop,StepPhase}, function::{Function,FnCaps,FamilyKernel}, traits::*, test_workbook::TestWorkbook};
use formualizer_common::{ExcelError,ExcelErrorExtra,LiteralValue};
use formualizer_parse::parser::{parse,TableReference};
use std::sync::{Arc,atomic::{AtomicBool,Ordering}};

struct Metadata { mode:u8, armed:Arc<AtomicBool> }
impl Function for Metadata {
    fn name(&self)->&'static str { "FAULT_METADATA" }
    fn caps(&self)->FnCaps { if self.mode==0 && self.armed.load(Ordering::SeqCst){panic!("caps metadata fault")}; FnCaps::PURE }
    fn reports_known_limitations(&self)->bool { if self.mode==1 && self.armed.load(Ordering::SeqCst){panic!("reporting metadata fault")}; true }
    fn family_kernel(&self)->Option<FamilyKernel> { if self.mode==2 && self.armed.load(Ordering::SeqCst){panic!("family metadata fault")}; None }
    fn eval<'a,'b,'c>(&self,_:&'c [ArgumentHandle<'a,'b>],_:&dyn FunctionContext<'b>)->Result<CalcValue<'b>,ExcelError>{Ok(CalcValue::Scalar(LiteralValue::Number(7.)))}
}
fn run<R:EvaluationContext>(e:&mut Engine<R>,job:bool) {
    if job { e.begin_recalc(Default::default()).unwrap(); assert_eq!(e.step_recalc(EvaluationStop::none()).unwrap().phase,StepPhase::Finished); }
    else { e.evaluate_all().unwrap(); }
}
fn check_error<R:EvaluationContext>(e:&Engine<R>,row:u32,name:&str) {
    assert!(!e.is_poisoned());
    let Some(LiteralValue::Error(error))=e.get_cell_value("Sheet1",row,1) else {panic!("missing typed callback failure at {row}")};
    assert!(matches!(error.extra(),ExcelErrorExtra::FunctionPanicked{function,message:Some(_)} if function==name),"{error:?}");
}
#[test]
fn runtime_metadata_faults_are_request_scoped_cell_failures() {
    for mode in 0..3 { for family in [false,true] { for job in [false,true] {
        let armed=Arc::new(AtomicBool::new(false));
        let mut e=Engine::new(TestWorkbook::new().with_function(Arc::new(Metadata{mode,armed:armed.clone()})),EvalConfig{family_execution:family,enable_parallel:false,..Default::default()});
        e.configure_operation_diagnostics(Default::default());
        for row in 1..=32 {e.set_cell_formula("Sheet1",row,1,parse("=FAULT_METADATA()").unwrap()).unwrap();}
        armed.store(true,Ordering::SeqCst);
        // family_kernel is queried only when family planning is enabled.
        if mode==2 && !family {run(&mut e,job);continue;}
        run(&mut e,job);
        for row in 1..=32 {check_error(&e,row,"FAULT_METADATA");}
        assert_eq!(e.operation_diagnostics().unwrap().unwrap().logical_root_cell_count,32);
        armed.store(false,Ordering::SeqCst);
        for row in 1..=32 {e.set_cell_formula("Sheet1",row,1,parse("=FAULT_METADATA()").unwrap()).unwrap();}
        run(&mut e,job);
        for row in 1..=32 {assert_eq!(e.get_cell_value("Sheet1",row,1),Some(LiteralValue::Number(7.)));}
    }}}
}

struct Sources { base:TestWorkbook, armed:Arc<AtomicBool> }
impl ReferenceResolver for Sources {fn resolve_cell_reference(&self,s:Option<&str>,r:u32,c:u32)->Result<LiteralValue,ExcelError>{self.base.resolve_cell_reference(s,r,c)}}
impl RangeResolver for Sources {fn resolve_range_reference(&self,s:Option<&str>,sr:Option<u32>,sc:Option<u32>,er:Option<u32>,ec:Option<u32>)->Result<Box<dyn Range>,ExcelError>{self.base.resolve_range_reference(s,sr,sc,er,ec)}}
impl NamedRangeResolver for Sources {fn resolve_named_range_reference(&self,n:&str)->Result<Vec<Vec<LiteralValue>>,ExcelError>{self.base.resolve_named_range_reference(n)}}
impl TableResolver for Sources {fn resolve_table_reference(&self,t:&TableReference)->Result<Box<dyn Table>,ExcelError>{self.base.resolve_table_reference(t)}}
impl FunctionProvider for Sources {fn get_function(&self,ns:&str,n:&str)->Option<Arc<dyn Function>>{self.base.get_function(ns,n)}}
impl SourceResolver for Sources {
    fn resolve_source_scalar(&self,_:&str)->Result<LiteralValue,ExcelError>{
        assert!(!self.armed.load(Ordering::SeqCst),"source scalar callback fault");
        Ok(LiteralValue::Number(7.))
    }
}
impl Resolver for Sources {}
impl EvaluationContext for Sources {}
#[test]
fn direct_and_wrapped_source_faults_keep_source_attribution_and_recover() {
    for family in [false,true] {for job in [false,true] {for expression in ["=ReviewSource","=ABS(ReviewSource)"] {
        let armed=Arc::new(AtomicBool::new(true));
        let mut e=Engine::new(Sources{base:TestWorkbook::new(),armed:armed.clone()},EvalConfig{family_execution:family,enable_parallel:false,..Default::default()});
        e.configure_operation_diagnostics(Default::default());
        e.define_source_scalar("ReviewSource",Some(1)).unwrap();
        e.set_cell_formula("Sheet1",1,1,parse(expression).unwrap()).unwrap();
        run(&mut e,job);
        check_error(&e,1,"resolver:resolve_source_scalar:ReviewSource");
        assert_eq!(e.operation_diagnostics().unwrap().unwrap().logical_root_cell_count,1);
        armed.store(false,Ordering::SeqCst);
        e.set_cell_formula("Sheet1",1,1,parse(expression).unwrap()).unwrap();
        run(&mut e,job);
        assert_eq!(e.get_cell_value("Sheet1",1,1),Some(LiteralValue::Number(7.)));
    }}}
}
