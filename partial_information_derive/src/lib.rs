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
                {
                    let (conflicts, canonical) = self.#field_name.get_conflicts_internal(&other.#field_name, time,
                            &format!(concat!("{}/", #field_name_stringified), field_path.clone() /* TEMP */));
                    all_canonical &= canonical;
                    if !canonical { println!(concat!("{}/", #field_name_stringified, " not canonical"), field_path); }
                    output = match (output, conflicts) {
                        (None, None) => { None }
                        (None, Some(c)) => { Some(c) }
                        (Some(o), None) => { Some(o) }
                        (Some(o), Some(c)) => { Some(o + "\n" + &c) }
                    };
                }
            }
        });

        quote! {
            impl PartialInformationCompare for #name {
                fn get_conflicts_internal(&self, other: &Self, time: DateTime<Utc>, field_path: &str) -> (Option<String>, bool) {
                    let mut output = None;
                    let mut all_canonical = true;
                    #(#get_conflicts);*
                    (output, all_canonical)
                }
            }
        }
    })
}