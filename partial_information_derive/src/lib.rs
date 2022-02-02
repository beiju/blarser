extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn;
use syn::Data;


#[proc_macro_derive(PartialInformationCompare)]
pub fn partial_information_compare_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_partial_information_compare(&ast)
}

fn impl_partial_information_compare(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let fields_to_compare = match &ast.data {
        Data::Struct(st) => {
            st.fields.iter().map(|field| {
                match &field.ident {
                    None => panic!("PartialInformationCompare only supports named fields"),
                    Some(ident) => {
                        quote! {
                            match ::partial_information::PartialInformationFieldCompare(self.#ident, other.#ident) {
                                Some(message) => msg_vec.push(message),
                                None => {}
                            }
                        }
                    }
                }
            })
        }
        _ => {
            panic!("PartialInformationCompare only supports Struct items");
        }
    };
    let gen = quote! {
        impl PartialInformationCompare for #name {
            fn get_conflicts(&self, other: &Self) -> Vec<String> {

                println!("Hello, Macro! My name is {}!", stringify!(#name));
            }
        }
    };
    gen.into()
}