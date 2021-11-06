use std::rc::Rc;
use chrono::{DateTime, Utc};

use crate::blaseball_state::BlaseballState;
use crate::ingest::IngestError;

pub trait IngestItem {
    fn date(&self) -> DateTime<Utc>;
    fn apply(self: Box<Self>, state: Rc<BlaseballState>) -> Result<Rc<BlaseballState>, IngestError>;
}