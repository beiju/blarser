use chrono::{DateTime, Utc};
use diesel::QueryResult;
use itertools::{iproduct, Itertools};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::MaybeKnown;

use crate::api::EventuallyEvent;
use crate::entity::Entity;
use crate::events::{Event, EventAux, EventTrait};
use crate::events::game_update::GameUpdate;
use crate::state::StateInterface;

#[derive(Serialize, Deserialize)]
pub struct HalfInning {
    game_update: GameUpdate,
    time: DateTime<Utc>,
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
    pub fn parse(feed_event: EventuallyEvent, state: &StateInterface) -> QueryResult<(Event, Vec<(String, Option<Uuid>, EventAux)>)> {
        let time = feed_event.created;

        let game_id = feed_event.game_id().expect("HalfInning event must have a game id");
        let (away_team, home_team): (&Uuid, &Uuid) = feed_event.team_tags.iter().collect_tuple()
            .expect("HalfInning event must have exactly two teams");

        let home_pitcher = read_active_pitcher(state, *home_team, feed_event.day)?;
        let away_pitcher = read_active_pitcher(state, *away_team, feed_event.day)?;

        let event = Self {
            game_update: GameUpdate::parse(feed_event),
            time
        };

        let effects = iproduct!(home_pitcher, away_pitcher)
            .map(|(home, away)| {
                ("game".to_string(), Some(game_id), EventAux::Pitchers { home, away })
            })
            .collect();

        Ok((event.into(), effects))
    }
}

impl EventTrait for HalfInning {
    fn time(&self) -> DateTime<Utc> {
        self.time
    }

    fn forward(&self, entity: Entity, aux: &EventAux) -> Entity {
        if let EventAux::Pitchers { home, away} = aux {
            match entity {
                Entity::Game(mut game) => {
                    self.game_update.forward(&mut game);

                    game.top_of_inning = !game.top_of_inning;
                    if game.top_of_inning {
                        game.inning += 1;
                    }
                    game.phase = 6;
                    game.half_inning_score = 0.0;

                    // The first halfInning event re-sets the data that PlayBall clears
                    if game.inning == 0 && game.top_of_inning {
                        game.home.pitcher = Some(MaybeKnown::Known(home.0));
                        game.home.pitcher_name = Some(MaybeKnown::Known(home.1.clone()));
                        game.away.pitcher = Some(MaybeKnown::Known(away.0));
                        game.away.pitcher_name = Some(MaybeKnown::Known(away.1.clone()));
                    }

                    game.into()
                },
                _ => panic!("HalfInning event does not apply to this entity")
            }
        } else {
            panic!("Wrong type of event aux");
        }
    }

    fn reverse(&self, _entity: Entity, _aux: &EventAux) -> Entity {
        todo!()
    }
}