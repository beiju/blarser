use std::sync::Arc;
use chrono::{DateTime, Utc};
use futures::{stream, StreamExt, TryStreamExt};
use rocket::async_trait;
use serde_json::json;
use uuid::Uuid;

use crate::blaseball_state as bs;
use crate::blaseball_state::{EntitySet, Patch};
use crate::ingest::{IngestError, IngestItem, IngestResult};
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

    async fn apply(&self, log: &IngestLogger, state: Arc<bs::BlaseballState>) -> IngestApplyResult {
        log.info("Season started! Adding odds to every game".to_string()).await?;

        let entity_set = state.data.get("game")
            .ok_or(bs::PathError::EntityTypeDoesNotExist("game"))?;

        let diffs = get_diffs(&state, entity_set).await?;

        let caused_by = Arc::new(bs::Event::TimedChange(self.at_time));
        state.successor(caused_by, diffs).await
    }
}

async fn get_diffs(state: &Arc<bs::BlaseballState>, entity_set: &EntitySet) -> IngestResult<Vec<Patch>> {
    stream::iter(entity_set.keys().cloned())
        .then(|game_id| {
            // I don't know why but passing state as a reference into this closure fails to compile in every variation
            // I can think of, so I'm just going to copy it. At least it's behind an Arc already.
            let state = state.clone();
            async move {
                let home_pitcher = get_pitcher_for_game(&state, &game_id, "home").await?;
                let away_pitcher = get_pitcher_for_game(&state, &game_id, "away").await?;
                let s = [
                    bs::Patch {
                        path: bs::json_path!("game", game_id.clone(), "lastUpdate"),
                        change: bs::ChangeType::Set("".into()),
                    },
                    bs::Patch {
                        path: bs::json_path!("game", game_id.clone(), "lastUpdateFull"),
                        change: bs::ChangeType::Overwrite(json!([])),
                    },
                ].into_iter()
                    .chain(game_start_team_specific_diffs(&game_id, home_pitcher, "home"))
                    .chain(game_start_team_specific_diffs(&game_id, away_pitcher, "away"));

                Ok::<_, IngestError>(stream::iter(s).map(|x| Ok::<_, IngestError>(x)))
            }
        })
        .try_flatten()
        .try_collect().await
}


struct Pitcher {
    pitcher_id: uuid::Uuid,
    pitcher_name: String,
}

async fn get_pitcher_for_game(state: &bs::BlaseballState, game_id: &Uuid, home_or_away: &str) -> Result<Pitcher, bs::PathError> {
    let team_id = state.uuid_at(&bs::json_path!("game", game_id.clone(), format!("{}Team", home_or_away))).await?;
    let rotation = state.array_at(&bs::json_path!("team", team_id, "rotation")).await?;
    let day = state.int_at(&bs::json_path!("game", game_id.clone(), "day")).await?;
    let rotation_slot = day % (rotation.len() as i64);

    let pitcher_id = rotation.get(rotation_slot as usize)
        .expect("rotation_slot should always be valid here");

    let pitcher_id = pitcher_id.as_uuid().await
        .map_err(|value| bs::PathError::UnexpectedType {
            path: bs::json_path!("team", team_id, "rotation", rotation_slot as usize),
            expected_type: "uuid",
            value,
        })?;

    let pitcher_name = state.string_at(&bs::json_path!("player", pitcher_id, "name")).await?;

    Ok(Pitcher { pitcher_id, pitcher_name })
}


fn game_start_team_specific_diffs(game_id: &Uuid, active_pitcher: Pitcher, which: &'static str) -> impl Iterator<Item=bs::Patch> {
    [
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}BatterName", which)),
            change: bs::ChangeType::Set("".to_string().into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}Odds", which)),
            change: bs::ChangeType::Set(bs::PrimitiveValue::FloatRange(0., 1.)),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}Pitcher", which)),
            change: bs::ChangeType::Set(active_pitcher.pitcher_id.to_string().into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}PitcherName", which)),
            change: bs::ChangeType::Set(active_pitcher.pitcher_name.into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}Score", which)),
            change: bs::ChangeType::Set(0.into()),
        },
        bs::Patch {
            path: bs::json_path!("game", game_id.clone(), format!("{}Strikes", which)),
            change: bs::ChangeType::Set(3.into()),
        },
    ].into_iter()
}

