// Modified from https://users.rust-lang.org/t/syn-how-do-i-iterate-on-the-fields-of-a-struct/42600/5
#![allow(unused_imports)]
extern crate proc_macro;

use ::proc_macro::TokenStream;
use ::proc_macro2::{Span, TokenStream as TokenStream2};
use ::quote::{quote, quote_spanned, ToTokens};
use ::syn::{*, parse::{Parse, Parser, ParseStream}, punctuated::Punctuated, spanned::Spanned, Result};

#[proc_macro_derive(PartialInformationCompare)]
pub fn partial_information_compare_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as _);
    TokenStream::from(match impl_partial_information_compare(ast) {
        | Ok(it) => it,
        | Err(err) => err.to_compile_error(),
    })
}

fn impl_partial_information_compare(ast: DeriveInput) -> Result<TokenStream2> {
    Ok({
        let name = ast.ident;
        let fields = match ast.data {
            | Data::Enum(DataEnum { enum_token: token::Enum { span }, .. })
            | Data::Union(DataUnion { union_token: token::Union { span }, .. })
            => {
                return Err(Error::new(
                    span,
                    "Expected a `struct`",
                ));
            }

            | Data::Struct(DataStruct { fields: Fields::Named(it), .. })
            => it,

            | Data::Struct(_)
            => {
                return Err(Error::new(
                    Span::call_site(),
                    "Expected a `struct` with named fields",
                ));
            }
        };

        let get_conflicts = fields.named.into_iter().map(|field| {
            let field_name = field.ident.expect("Unreachable");
            let span = field_name.span();
            let field_name_stringified = LitStr::new(&field_name.to_string(), span);
            quote_spanned! { span=>
            ::partial_information::PartialInformationFieldCompare::get_conflicts(
                    #field_name_stringified.to_string(), &self.#field_name, &other.#field_name).into_iter()
        }
        }).fold(quote!{ ::std::iter::empty() }, |lhs, rhs| {
            quote!{ #lhs.chain(#rhs) }
        });

        quote! {
        impl PartialInformationCompare for #name {
            fn get_conflicts(&self, other: &Self) -> Vec<String> {
                #get_conflicts.collect()
            }
        }
    }
    })
}