//! Byte-oriented text functions for non-DBCS locales.
//!
//! Excel's `*B` functions count bytes in double-byte character set locales. Formualizer's locale
//! layer is currently invariant/non-DBCS, so these functions delegate to their character-counting
//! counterparts.

use super::{FindFn, LeftFn, LenFn, MidFn, ReplaceFn, RightFn, SearchFn};
use crate::args::ArgSchema;
use crate::builtins::utils::ARG_ANY_ONE;
use crate::function::Function;
use crate::traits::{ArgumentHandle, FunctionContext};
use formualizer_common::ExcelError;
use formualizer_macros::func_caps;

/// Finds text within text using non-DBCS byte-compatible semantics.
///
/// In Formualizer's invariant locale, FINDB delegates to FIND and counts Unicode
/// scalar characters rather than double-byte locale bytes.
///
/// ```yaml,sandbox
/// title: "Find byte-compatible text"
/// formula: '=FINDB("CD","abcDEFCD")'
/// expected: 7
/// ```
///
/// ```yaml,docs
/// related:
///   - FIND
///   - SEARCHB
///   - LENB
/// faq:
///   - q: "Does FINDB use DBCS byte counts?"
///     a: "Not currently. In the invariant locale it delegates to FIND."
/// ```
#[derive(Debug)]
pub struct FindBFn;
/// [formualizer-docgen:schema:start]
/// Name: FINDB
/// Type: FindBFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: FINDB(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for FindBFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "FINDB"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        FindFn.eval(args, ctx)
    }
}

/// Returns the leftmost characters using non-DBCS byte-compatible semantics.
///
/// In Formualizer's invariant locale, LEFTB delegates to LEFT.
///
/// ```yaml,sandbox
/// title: "Left byte-compatible characters"
/// formula: '=LEFTB("hello",2)'
/// expected: "he"
/// ```
///
/// ```yaml,docs
/// related:
///   - LEFT
///   - RIGHTB
///   - MIDB
/// faq:
///   - q: "Does LEFTB split UTF-8 bytes?"
///     a: "No. It follows the non-DBCS behavior and delegates to LEFT."
/// ```
#[derive(Debug)]
pub struct LeftBFn;
/// [formualizer-docgen:schema:start]
/// Name: LEFTB
/// Type: LeftBFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: LEFTB(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for LeftBFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LEFTB"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        LeftFn.eval(args, ctx)
    }
}

/// Returns text length using non-DBCS byte-compatible semantics.
///
/// In Formualizer's invariant locale, LENB delegates to LEN.
///
/// ```yaml,sandbox
/// title: "Length of text"
/// formula: '=LENB("abc")'
/// expected: 3
/// ```
///
/// ```yaml,docs
/// related:
///   - LEN
///   - LEFTB
///   - RIGHTB
/// faq:
///   - q: "Does LENB count UTF-8 bytes?"
///     a: "No. In the invariant locale it matches LEN."
/// ```
#[derive(Debug)]
pub struct LenBFn;
/// [formualizer-docgen:schema:start]
/// Name: LENB
/// Type: LenBFn
/// Min args: 1
/// Max args: 1
/// Variadic: false
/// Signature: LENB(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for LenBFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "LENB"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        LenFn.eval(args, ctx)
    }
}

/// Extracts text from the middle using non-DBCS byte-compatible semantics.
///
/// In Formualizer's invariant locale, MIDB delegates to MID.
///
/// ```yaml,sandbox
/// title: "Middle byte-compatible characters"
/// formula: '=MIDB("abcdef",2,3)'
/// expected: "bcd"
/// ```
///
/// ```yaml,docs
/// related:
///   - MID
///   - LEFTB
///   - RIGHTB
/// faq:
///   - q: "How are byte positions interpreted?"
///     a: "In the invariant locale, positions are character positions matching MID."
/// ```
#[derive(Debug)]
pub struct MidBFn;
/// [formualizer-docgen:schema:start]
/// Name: MIDB
/// Type: MidBFn
/// Min args: 3
/// Max args: 1
/// Variadic: false
/// Signature: MIDB(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for MidBFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "MIDB"
    }
    fn min_args(&self) -> usize {
        3
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        MidFn.eval(args, ctx)
    }
}

/// Replaces text using non-DBCS byte-compatible semantics.
///
/// In Formualizer's invariant locale, REPLACEB delegates to REPLACE.
///
/// ```yaml,sandbox
/// title: "Replace byte-compatible text"
/// formula: '=REPLACEB("abcdef",3,2,"ZZ")'
/// expected: "abZZef"
/// ```
///
/// ```yaml,docs
/// related:
///   - REPLACE
///   - MIDB
///   - FINDB
/// faq:
///   - q: "Does REPLACEB operate on raw UTF-8 bytes?"
///     a: "No. In the invariant locale it delegates to REPLACE."
/// ```
#[derive(Debug)]
pub struct ReplaceBFn;
/// [formualizer-docgen:schema:start]
/// Name: REPLACEB
/// Type: ReplaceBFn
/// Min args: 4
/// Max args: 1
/// Variadic: false
/// Signature: REPLACEB(arg1: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for ReplaceBFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "REPLACEB"
    }
    fn min_args(&self) -> usize {
        4
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        ReplaceFn.eval(args, ctx)
    }
}

/// Returns the rightmost characters using non-DBCS byte-compatible semantics.
///
/// In Formualizer's invariant locale, RIGHTB delegates to RIGHT.
///
/// ```yaml,sandbox
/// title: "Right byte-compatible characters"
/// formula: '=RIGHTB("hello",2)'
/// expected: "lo"
/// ```
///
/// ```yaml,docs
/// related:
///   - RIGHT
///   - LEFTB
///   - MIDB
/// faq:
///   - q: "Does RIGHTB count DBCS bytes?"
///     a: "Not currently. In the invariant locale it matches RIGHT."
/// ```
#[derive(Debug)]
pub struct RightBFn;
/// [formualizer-docgen:schema:start]
/// Name: RIGHTB
/// Type: RightBFn
/// Min args: 1
/// Max args: variadic
/// Variadic: true
/// Signature: RIGHTB(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for RightBFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "RIGHTB"
    }
    fn min_args(&self) -> usize {
        1
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        RightFn.eval(args, ctx)
    }
}

/// Searches text using non-DBCS byte-compatible semantics.
///
/// In Formualizer's invariant locale, SEARCHB delegates to SEARCH and supports
/// the same wildcard behavior.
///
/// ```yaml,sandbox
/// title: "Wildcard byte-compatible search"
/// formula: '=SEARCHB("d?f","abcDEF")'
/// expected: 4
/// ```
///
/// ```yaml,docs
/// related:
///   - SEARCH
///   - FINDB
///   - LENB
/// faq:
///   - q: "Is SEARCHB case-sensitive?"
///     a: "No. It follows SEARCH behavior in the invariant locale."
/// ```
#[derive(Debug)]
pub struct SearchBFn;
/// [formualizer-docgen:schema:start]
/// Name: SEARCHB
/// Type: SearchBFn
/// Min args: 2
/// Max args: variadic
/// Variadic: true
/// Signature: SEARCHB(arg1...: any@scalar)
/// Arg schema: arg1{kinds=any,required=true,shape=scalar,by_ref=false,coercion=None,max=None,repeating=None,default=false}
/// Caps: PURE
/// [formualizer-docgen:schema:end]
impl Function for SearchBFn {
    func_caps!(PURE);
    fn name(&self) -> &'static str {
        "SEARCHB"
    }
    fn min_args(&self) -> usize {
        2
    }
    fn variadic(&self) -> bool {
        true
    }
    fn arg_schema(&self) -> &'static [ArgSchema] {
        &ARG_ANY_ONE[..]
    }
    fn eval<'a, 'b, 'c>(
        &self,
        args: &'c [ArgumentHandle<'a, 'b>],
        ctx: &dyn FunctionContext<'b>,
    ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
        SearchFn.eval(args, ctx)
    }
}

pub fn register_builtins() {
    use crate::function_registry::register_function;
    use std::sync::Arc;

    register_function(Arc::new(FindBFn));
    register_function(Arc::new(LeftBFn));
    register_function(Arc::new(LenBFn));
    register_function(Arc::new(MidBFn));
    register_function(Arc::new(ReplaceBFn));
    register_function(Arc::new(RightBFn));
    register_function(Arc::new(SearchBFn));
}

#[cfg(test)]
mod tests {
    use crate::test_workbook::TestWorkbook;
    use formualizer_common::LiteralValue;
    use formualizer_parse::parser::parse;

    fn eval(formula: &str) -> LiteralValue {
        let wb = TestWorkbook::new()
            .with_function(std::sync::Arc::new(super::FindBFn))
            .with_function(std::sync::Arc::new(super::LeftBFn))
            .with_function(std::sync::Arc::new(super::LenBFn))
            .with_function(std::sync::Arc::new(super::MidBFn))
            .with_function(std::sync::Arc::new(super::ReplaceBFn))
            .with_function(std::sync::Arc::new(super::RightBFn))
            .with_function(std::sync::Arc::new(super::SearchBFn));
        let interp = wb.interpreter();
        let ast = parse(formula).expect("parse");
        interp.evaluate_ast(&ast).expect("eval").into_literal()
    }

    #[test]
    fn byte_functions_delegate_in_non_dbcs_locale() {
        assert_eq!(eval("=LENB(\"éx\")"), LiteralValue::Int(2));
        assert_eq!(eval("=LEFTB(\"hello\",2)"), LiteralValue::Text("he".into()));
        assert_eq!(
            eval("=RIGHTB(\"hello\",2)"),
            LiteralValue::Text("lo".into())
        );
        assert_eq!(
            eval("=MIDB(\"abcdef\",2,3)"),
            LiteralValue::Text("bcd".into())
        );
        assert_eq!(
            eval("=REPLACEB(\"abcdef\",3,2,\"ZZ\")"),
            LiteralValue::Text("abZZef".into())
        );
        assert_eq!(eval("=FINDB(\"CD\",\"abcDEFCD\")"), LiteralValue::Int(7));
        assert_eq!(eval("=SEARCHB(\"d?f\",\"abcDEF\")"), LiteralValue::Int(4));
    }
}
