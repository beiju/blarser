use std::rc::Rc;
use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::blaseball_state::BlaseballState;


#[derive(Error, Debug)]
pub enum IngestError {}

pub trait IngestItem {
    fn date(&self) -> DateTime<Utc>;
    fn apply(&self, state: Rc<BlaseballState>) -> Result<Rc<BlaseballState>, IngestError>;
}