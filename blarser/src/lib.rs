#![feature(let_chains)]
#![feature(trivial_bounds)] // Necessary for partial_information
#![feature(min_specialization)] // Used for Event/Entity interaction
#![recursion_limit = "256"]

#[macro_use]
extern crate diesel;
extern crate core;

pub mod ingest;
mod api;
pub mod db;
#[allow(unused_imports)]
pub mod schema;
pub mod entity;
pub mod events;
pub mod state;
pub(crate) mod polymorphic_enum;
// #[allow(dead_code)]
// mod parse;
