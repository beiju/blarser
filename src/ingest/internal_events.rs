use std::sync::Arc;
use chrono::{DateTime, Utc};
use rocket::async_trait;
use serde_json::json;

use crate::blaseball_state as bs;
use crate::ingest::IngestItem;
use crate::ingest::data_views::{DataView, OwningEntityView};
use crate::ingest::error::IngestApplyResult;
use crate::ingest::log::IngestLogger;

pub struct StartSeasonItem {
    at_time: DateTime<Utc>,
}

impl StartSeasonItem {
    pub fn new(at_time: DateTime<Utc>) -> StartSeasonItem {
        StartSeasonItem {
            at_time
        }
    }
}

#[async_trait]
impl IngestItem for StartSeasonItem {
    fn date(&self) -> DateTime<Utc> {
        self.at_time
    }

    fn apply(&self, log: &IngestLogger, state: Arc<bs::BlaseballState>) -> IngestApplyResult {
        log.info("Season started! Adding odds to every game".to_string())?;

        let data = DataView::new(state.data.clone(),
                                 bs::Event::TimedChange(self.at_time));

        for game in data.games()? {
            game.get("lastUpdate").set("")?;
            game.get("lastUpdateFull").overwrite(json!([]))?;

            for which in ["home", "away"] {
                let active_pitcher = get_pitcher_for_game(&data, &game, which)?;
                game.get(&format!("{}BatterName", which)).set("")?;
                game.get(&format!("{}Odds", which)).set(bs::PrimitiveValue::FloatRange(0., 1.))?;
                game.get(&format!("{}Pitcher", which)).set(active_pitcher.pitcher_id)?;
                game.get(&format!("{}PitcherName", which)).set(active_pitcher.pitcher_name)?;
                game.get(&format!("{}Score", which)).set(0)?;
                game.get(&format!("{}Strikes", which)).set(3)?;
            }
        }

        let (new_data, caused_by) = data.into_inner();
        Ok(state.successor(caused_by, new_data))
    }
}


struct Pitcher {
    pitcher_id: uuid::Uuid,
    pitcher_name: String,
}

fn get_pitcher_for_game(data: &DataView, game: &OwningEntityView, home_or_away: &str) -> Result<Pitcher, bs::PathError> {
    let team_id = game.get(&format!("{}Team", home_or_away)).as_uuid()?;
    let team = data.get_team(&team_id);
    let rotation_node = team.get("rotation");
    let day = game.get("day").as_int()?;
    let rotation = rotation_node.as_array()?;
    let rotation_slot = day % (rotation.len() as i64);

    let pitcher_id = rotation.get(rotation_slot as usize)
        .expect("rotation_slot should always be valid here");

    let pitcher_id = pitcher_id.as_uuid()
        .map_err(|value| bs::PathError::UnexpectedType {
            path: bs::json_path!("team", team_id, "rotation", rotation_slot as usize),
            expected_type: "uuid",
            value,
        })?;

    // Avoid a deadlock when something else wants to read some state
    drop(rotation);

    let pitcher = data.get_player(&pitcher_id);
    let pitcher_name = pitcher.get("name").as_string()?;

    Ok(Pitcher { pitcher_id, pitcher_name })
}
