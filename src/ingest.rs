use std::io::{self, Write};
use std::rc::Rc;
use itertools::Itertools;
use anyhow::{Context, Error, Result};
use console::style;

use crate::chronicler;
use crate::eventually;
use crate::blaseball_state::BlaseballState;
use crate::parse::{self, IngestEvent, IngestObject};

const EXPANSION_ERA_START: &str = "2021-03-01T00:00:00Z";

pub fn ingest() -> Result<Rc<BlaseballState>> {
    println!("Starting ingest");
    let start_state = Rc::new(BlaseballState::from_chron_at_time(EXPANSION_ERA_START));
    println!("Got initial state");

    let (final_state, stored_error) = merged_feed_and_chron()
        .try_fold((start_state, None), |(latest_state, mut stored_error), object| {
            match object {
                IngestObject::Event(event) => {
                    Ok((parse::apply_event(latest_state, event), None))
                }
                IngestObject::Update { endpoint, item } => {
                    let res = parse::apply_update(&latest_state, endpoint, item.entity_id, item.data)
                        .with_context(|| format!("Failed to apply {} update from {} ({})",
                                                 &endpoint,
                                                 item.valid_from,
                                                 item.valid_from.to_rfc2822()));

                    match res {
                        Ok(()) => {}
                        Err(e) => {
                            // I would print to stderr, but CLion has ordering problems
                            // TODO Use a more robust logging solution
                            println!("{}", style(format!("{:#}", e)).red());
                            stored_error = match stored_error {
                                None => Some((e, 1)),
                                Some((stored_e, count)) if count < 25 => Some((stored_e, count + 1)),
                                Some((stored_e, _)) => return Err(stored_e),
                            }
                        }
                    }
                    Ok((latest_state, stored_error))
                }
            }
        })?;

    match stored_error {
        None => Ok(final_state),
        Some((e, _)) => Err(e)
    }
}


pub fn merged_feed_and_chron() -> impl Iterator<Item=IngestObject> {
    chronicler::ENDPOINT_NAMES.into_iter()
        .map(|endpoint|
            Box::new(chronicler::versions(endpoint, EXPANSION_ERA_START)
                .map(|item| IngestObject::Update { endpoint, item }))
                as Box<dyn Iterator<Item=IngestObject>>
        )
        // Force the inner iterators to be started by collecting them, then turn the collection
        // right back into an iterator to continue the chain
        .collect::<Vec<Box<dyn Iterator<Item=IngestObject>>>>().into_iter()
        .chain([
            Box::new(eventually::events(EXPANSION_ERA_START)
                .map(|event| IngestObject::Event(IngestEvent::FeedEvent(event))))
                as Box<dyn Iterator<Item=IngestObject>>
        ])
        .kmerge_by(|a, b| a.date() < b.date())
}