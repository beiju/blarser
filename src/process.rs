use std::rc::Rc;
use itertools::Itertools;
use anyhow::{Context, Result};
use chrono::SecondsFormat;
use log::{info, error};

use crate::ingest;
use crate::blaseball_state::BlaseballState;
use crate::parse;

const EXPANSION_ERA_START: &str = "2021-03-01T00:00:00Z";

pub fn run() -> Result<Rc<BlaseballState>> {
    info!("Starting ingest");
    let start_state = Rc::new(BlaseballState::from_chron_at_time(EXPANSION_ERA_START));
    info!("Got initial state");

    let (final_state, stored_error) = ingest::all(EXPANSION_ERA_START)
        .try_fold((start_state, None), |(latest_state, mut stored_error), object| {
            match object {
                ingest::IngestItem::FeedEvent(event) => {
                    Ok((parse::apply_feed_event(latest_state, event), None))
                }
                ingest::IngestItem::ChronUpdate { endpoint, item } => {
                    let res = parse::apply_update(&latest_state, endpoint, item.entity_id, item.data)
                        .with_context(|| format!("Failed to apply {} update from {} ({})",
                                                 &endpoint,
                                                 item.valid_from.to_rfc3339_opts(SecondsFormat::Secs, true),
                                                 item.valid_from.to_rfc2822()));

                    match res {
                        Ok(()) => {}
                        Err(e) => {
                            error!("{:#}", e);
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

