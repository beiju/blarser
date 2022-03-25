use chrono::{DateTime, Utc};
use diesel::QueryResult;
use itertools::{iproduct, Itertools};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::MaybeKnown;

use crate::api::EventuallyEvent;
use crate::entity::AnyEntity;
use crate::events::{AnyEvent, Event};
use crate::events::game_update::GameUpdate;
use crate::state::StateInterface;

#[derive(Serialize, Deserialize)]
pub struct HalfInning {
    game_update: GameUpdate,
    time: DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
struct StartingPitchers {
    home: (Uuid, String),
    away: (Uuid, String),
}

fn read_active_pitcher(state: &StateInterface, team_id: Uuid, day: i32) -> QueryResult<Vec<(Uuid, String)>> {
    let result = state.read_team(team_id, |team| {
        team.active_pitcher(day)
    })?
        .into_iter()
        .map(|pitcher_id| {
            state.read_player(pitcher_id, |player| {
                (pitcher_id, player.name)
            })
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect();

    Ok(result)
}

impl HalfInning {
    pub fn parse(feed_event: &EventuallyEvent, state: &StateInterface) -> QueryResult<(AnyEvent, Vec<(String, Option<Uuid>, serde_json::Value)>)> {
        let time = feed_event.created;

        let game_id = feed_event.game_id().expect("HalfInning event must have a game id");
        let (away_team, home_team): (&Uuid, &Uuid) = feed_event.team_tags.iter().collect_tuple()
            .expect("HalfInning event must have exactly two teams");

        // TODO Better to parse this, then make it available in aux info
        let is_first_half = state.read_game(game_id, |game| {
            game.inning == 0 && game.top_of_inning
        })?;

        // TODO Surely this can be done without collect()ing so much
        let effects = is_first_half.into_iter()
            .map(|is_first_half| {
                let out = if is_first_half {
                    let home_pitcher = read_active_pitcher(state, *home_team, feed_event.day)?;
                    let away_pitcher = read_active_pitcher(state, *away_team, feed_event.day)?;

                    iproduct!(home_pitcher, away_pitcher)
                        .map(|(home, away)| Some(StartingPitchers { home, away }))
                        .collect_vec()
                } else {
                    Vec::new()
                };

                Ok::<_, diesel::result::Error>(out)
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|aux| {
                ("game".to_string(), Some(game_id), serde_json::to_value(aux).unwrap())
            })
            .collect();

        let event = Self {
            game_update: GameUpdate::parse(feed_event),
            time
        };

        Ok((AnyEvent::HalfInning(event), effects))
    }
}

impl Event for HalfInning {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: AnyEntity, aux: serde_json::Value) -> AnyEntity {
        let aux: Option<StartingPitchers> = serde_json::from_value(aux)
            .expect("Failed to parse StartingPitchers from HalfInning event");

        match entity {
            AnyEntity::Game(mut game) => {
                self.game_update.forward(&mut game);

                game.top_of_inning = !game.top_of_inning;
                if game.top_of_inning {
                    game.inning += 1;
                }
                game.phase = 6;
                game.half_inning_score = 0.0;

                // The first halfInning event re-sets the data that PlayBall clears
                if let Some(starting_pitchers) = aux {
                    let (home_pitcher, home_pitcher_name) = starting_pitchers.home;
                    let (away_pitcher, away_pitcher_name) = starting_pitchers.away;

                    game.home.pitcher = Some(MaybeKnown::Known(home_pitcher));
                    game.home.pitcher_name = Some(MaybeKnown::Known(home_pitcher_name));
                    game.away.pitcher = Some(MaybeKnown::Known(away_pitcher));
                    game.away.pitcher_name = Some(MaybeKnown::Known(away_pitcher_name));
                }

                game.into()
            },
            other => panic!("HalfInning event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}