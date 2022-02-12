#![feature(let_chains)]
#![feature(trivial_bounds)] // Necessary for partial_information
#![feature(generic_associated_types)]
#![feature(result_option_inspect)] // Necessary for partial_information

#[macro_use]
extern crate diesel;
extern crate core;

pub mod ingest;
mod api;
pub mod db;
#[allow(unused_imports)]
pub mod schema;
pub mod db_types;
mod sim;
mod state;
mod event_utils;

pub use state::StateInterface;