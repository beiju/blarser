pub mod ingest;
// pub mod parse;
mod source;
mod eventually;
mod chronicler;

pub use source::{IngestItem, IngestError};
pub use ingest::run;
//
// pub fn all(start: &'static str) -> impl Iterator<Item=Box<dyn IngestItem>> {
//     chronicler::chronicler::ENDPOINT_NAMES.into_iter()
//         .map(|endpoint|
//             Box::new(chronicler::chronicler::versions(endpoint, start)
//                 .map(|item| Box::new(ChronUpdate { endpoint, item })))
//                 as Box<dyn Iterator<Item=Box<dyn IngestItem>>>
//         )
//         // Force the inner iterators to be started by collecting them, then turn the collection
//         // right back into an iterator to continue the chain
//         .collect::<Vec<_>>().into_iter()
//         .chain([
//             Box::new(eventually::events(start).map(|event| Box::new(event)))
//                 as Box<dyn Iterator<Item=Box<dyn IngestItem>>>
//         ])
//         .kmerge_by(|a, b| a.date() < b.date())
// }
