// These names are suffixed with _mod because having a module containing a same-name function was
// breaking my rocket::routes! call. I don't know if this is a Rust limitation or a problem with
// rocket::routes! but renaming the modules saves me from the error.
mod debug_mod;
mod approvals_mod;
mod index_mod;
mod entities_mod;

pub use index_mod::*;
pub use debug_mod::*;
pub use approvals_mod::*;
pub use entities_mod::*;

#[derive(rocket::Responder)]
pub enum ApiError {
    // #[response(status = 400)]
    // ParseError(String),

    #[response(status = 500)]
    InternalError(String)
}
