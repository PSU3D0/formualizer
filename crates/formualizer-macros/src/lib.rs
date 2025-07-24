use formualizer_common::ArgKind;
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{AttributeArgs, ItemFn, Lit, Meta, NestedMeta, parse_macro_input};

/* ────────────────────────── the attribute macro ───────────────────── */
#[proc_macro_attribute]
pub fn excel_fn(attr: TokenStream, item: TokenStream) -> TokenStream {
    /* -------- 1 · parse inputs -------- */
    let args = parse_macro_input!(attr as AttributeArgs);
    let func_item = parse_macro_input!(item as ItemFn);

    /* -------- 2 · defaults -------- */
    let mut attr_name = func_item.sig.ident.to_string();
    let mut namespace = String::new();
    let mut min_args = 0usize;
    let mut variadic = false;
    let mut volatile = false;
    let mut arg_kinds = Vec::<ArgKind>::new();

    /* -------- 3 · examine #[excel_fn(...)] -------- */
    for meta in args {
        match meta {
            NestedMeta::Meta(Meta::NameValue(nv)) if nv.path.is_ident("name") => {
                if let Lit::Str(s) = &nv.lit {
                    attr_name = s.value();
                }
            }
            NestedMeta::Meta(Meta::NameValue(nv)) if nv.path.is_ident("namespace") => {
                if let Lit::Str(s) = &nv.lit {
                    namespace = s.value();
                }
            }
            NestedMeta::Meta(Meta::NameValue(nv)) if nv.path.is_ident("min") => {
                if let Lit::Int(i) = &nv.lit {
                    min_args = i.base10_parse::<usize>().unwrap();
                }
            }
            NestedMeta::Meta(Meta::Path(p)) if p.is_ident("variadic") => variadic = true,
            NestedMeta::Meta(Meta::Path(p)) if p.is_ident("volatile") => volatile = true,
            NestedMeta::Meta(Meta::NameValue(nv)) if nv.path.is_ident("arg_types") => {
                if let Lit::Str(list) = nv.lit {
                    arg_kinds = list.value().split(',').map(ArgKind::parse).collect();
                }
            }
            _ => {}
        }
    }

    /* -------- 4 · build ArgSpec slice -------- */
    let arg_spec_tokens = arg_kinds.iter().map(|k| {
        let variant = match k {
            ArgKind::Number => quote! { Number  },
            ArgKind::Text => quote! { Text    },
            ArgKind::Logical => quote! { Logical },
            ArgKind::Range => quote! { Range   },
            ArgKind::Any => quote! { Any     },
        };
        quote! { formualizer_common::ArgSpec::new(formualizer_common::ArgKind::#variant) }
    });

    /* -------- 5 · names for generated items -------- */
    let vis = &func_item.vis;
    let user_fn_ident = &func_item.sig.ident;
    let wrapper_ident = format_ident!("__Fn{}", attr_name.to_uppercase());

    /* -------- 6 · assemble expanded code -------- */
    let expanded = quote! {
        #func_item                            /* original user fn */

        pub struct #wrapper_ident;

        impl crate::traits::Function for #wrapper_ident {
            fn name(&self)      -> &'static str { #attr_name }
            fn namespace(&self) -> &'static str { #namespace }
            fn volatile(&self)  -> bool         { #volatile }
            fn min_args(&self)  -> usize        { #min_args }
            fn variadic(&self)  -> bool         { #variadic }
            fn arg_schema(&self)-> &'static [formualizer_common::ArgSpec] {
                const SCHEMA: &[formualizer_common::ArgSpec] = &[ #(#arg_spec_tokens),* ];
                SCHEMA
            }

            fn eval(
                &self,
                args: &[crate::traits::ArgumentHandle],
                ctx : &dyn crate::traits::EvaluationContext
            ) -> Result<formualizer_common::LiteralValue, formualizer_common::error::ExcelError>
            {
                // zero eager evaluation – user code decides what to touch
                #user_fn_ident(args, ctx)
            }
        }
    };

    TokenStream::from(expanded)
}
