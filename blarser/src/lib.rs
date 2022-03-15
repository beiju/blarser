#![feature(let_chains)]
#![feature(trivial_bounds)] // Necessary for partial_information
#![feature(generic_associated_types)]
#![feature(map_first_last)] // Used in ObservationStream in chron ingest


#[macro_use]
extern crate diesel;
extern crate core;

pub mod ingest;
mod api;
pub mod db;
#[allow(unused_imports)]
pub mod schema;
pub mod db_types;
pub mod sim;
pub mod state;
#[allow(dead_code)]
mod parse;

pub use state::{StateInterface, FeedStateInterface, EntityStateInterface};
