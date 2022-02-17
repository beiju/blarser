// Modified from https://users.rust-lang.org/t/syn-how-do-i-iterate-on-the-fields-of-a-struct/42600/5
#![allow(unused_imports)]
extern crate proc_macro;

use ::proc_macro::TokenStream;
use std::iter;
use ::proc_macro2::{Span, TokenStream as TokenStream2};
use ::quote::{quote, ToTokens};
use ::syn::{*, parse::{Parse, Parser, ParseStream}, punctuated::Punctuated, spanned::Spanned, Result};

#[proc_macro_derive(PartialInformationCompare, attributes(derive_raw))]
pub fn partial_information_compare_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as _);
    TokenStream::from(match impl_partial_information_compare(ast) {
        | Ok(it) => it,
        | Err(err) => err.to_compile_error(),
    })
}

fn impl_partial_information_compare(ast: DeriveInput) -> Result<TokenStream2> {
    Ok({
        let item_vis = ast.vis;
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

        let diff_method_items = fields.named.iter()
            .map(|field| {
                let field_name = field.ident.as_ref().expect("Unreachable");
                // let field_name_stringified = LitStr::new(&field_name.to_string(), span);
                quote! {
                    #field_name: self.#field_name.diff(&other.#field_name, time)
                }
            });

        let observe_method_items = fields.named.iter()
            .map(|field| {
                let field_name = field.ident.as_ref().expect("Unreachable");
                let field_name_stringified = LitStr::new(&field_name.to_string(), field_name.span());
                quote! {
                    conflicts.extend(
                        self.#field_name.observe(&observed.#field_name).into_iter()
                            .map(|conflict| conflict.with_prefix(#field_name_stringified))
                    );
                }
            });

        let raw_attrs = ast.attrs.iter()
            .filter(|attr| {
                attr.style == AttrStyle::Outer && attr.path.is_ident("serde")
            });
        let raw_name = Ident::new(&format!("{}Raw", name), name.span());
        let raw_members = fields.named.iter()
            .map(|field| {
                let field_attrs = &field.attrs;
                let field_vis = &field.vis;
                let field_name = field.ident.as_ref().expect("Unreachable");
                let field_type = &field.ty;
                quote! {
                    #(#field_attrs)*
                    #field_vis #field_name: <#field_type as PartialInformationCompare>::Raw
                }
            });

        let diff_name = Ident::new(&format!("{}Diff", name), name.span());
        let diff_members = fields.named.iter()
            .map(|field| {
                let field_vis = &field.vis;
                let field_name = field.ident.as_ref().expect("Unreachable");
                let field_type = &field.ty;
                quote! {
                    #field_vis #field_name: <#field_type as PartialInformationCompare>::Diff<'d>
                }
            });
        let is_empty_members = fields.named.iter()
            .map(|field| {
                let field_name = field.ident.as_ref().expect("Unreachable");
                quote! {
                    self.#field_name.is_empty()
                }
            })
            .chain(iter::once(quote!{ true }));

        let from_raw_members = fields.named.iter()
            .map(|field| {
                let field_name = field.ident.as_ref().expect("Unreachable");
                let field_type = &field.ty;
                quote! {
                    #field_name: <#field_type as PartialInformationCompare>::from_raw(raw.#field_name)
                }
            });

        quote! {
            impl ::partial_information::PartialInformationCompare for #name {
                type Raw = #raw_name;
                type Diff<'d> = #diff_name<'d>;

                fn diff<'d>(&'d self, other: &'d Self::Raw, time: DateTime<Utc>) -> Self::Diff<'d> {
                    #diff_name {
                        _phantom: ::std::default::Default::default(),
                        #(#diff_method_items),*
                    }
                }

                fn observe(&mut self, observed: &Self::Raw) -> Vec<::partial_information::Conflict> {
                    let mut conflicts = Vec::new();
                    #(#observe_method_items)*

                    conflicts
                }

                fn from_raw(raw: Self::Raw) -> Self {
                    Self {
                        #(#from_raw_members),*
                    }
                }
            }

            #[derive(::core::fmt::Debug, ::serde::Deserialize)]
            #(#raw_attrs)*
            #item_vis struct #raw_name {
                #(#raw_members),*
            }

            #[derive(::core::fmt::Debug)]
            #item_vis struct #diff_name<'d> {
                _phantom: ::std::marker::PhantomData<&'d ()>,
                #(#diff_members),*
            }

            impl<'d> ::partial_information::PartialInformationDiff<'d> for #diff_name<'d> {
                fn is_empty(&self) -> bool {
                    #(#is_empty_members)&&*
                }
            }
        }
    })
}