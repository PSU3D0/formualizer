use proc_macro::TokenStream;
use quote::quote;
use syn::{Path, Token, parse::Parser, punctuated::Punctuated};

/// Macro to generate the caps() method and validate that required methods are implemented
/// Usage:
/// ```ignore
/// use formualizer_eval::function::Function;
/// pub struct SumFn;
/// impl Function for SumFn {
///     func_caps!(PURE, REDUCTION, NUMERIC_ONLY, STREAM_OK);
///     
///     fn name(&self) -> &'static str { "SUM" }
///     // ... rest of implementation
/// }
/// ```
#[proc_macro]
pub fn func_caps(input: TokenStream) -> TokenStream {
    let parser = Punctuated::<Path, Token![,]>::parse_terminated;
    let paths = parser
        .parse(input)
        .expect("Failed to parse capability flags");

    let cap_tokens: Vec<_> = paths
        .iter()
        .map(|path| {
            quote! { crate::function::FnCaps::#path }
        })
        .collect();

    let caps_expr = if cap_tokens.is_empty() {
        quote! { crate::function::FnCaps::empty() }
    } else {
        let first = &cap_tokens[0];
        let rest = &cap_tokens[1..];

        rest.iter().fold(quote! { #first }, |acc, token| {
            quote! { #acc | #token }
        })
    };

    let expanded = quote! {
        fn caps(&self) -> crate::function::FnCaps {
            #caps_expr
        }
    };

    TokenStream::from(expanded)
}
