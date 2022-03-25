use chrono::{DateTime, Utc};
use diesel::QueryResult;
use itertools::{iproduct, Itertools};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use partial_information::MaybeKnown;
use nom::{
    IResult,
    bytes::complete::{tag},
    character::complete::digit1,
    branch::alt,
    bytes::complete::{take_while1},
    character::complete::{multispace1},
    error::VerboseError,
    multi::many_till,
    sequence::terminated
};
use nom_supreme::{
    final_parser::final_parser,
    ParserExt
};

use crate::api::EventuallyEvent;
use crate::entity::AnyEntity;
use crate::events::{AnyEvent, Event};
use crate::events::game_update::GameUpdate;
use crate::state::StateInterface;

#[derive(Serialize, Deserialize)]
pub struct HalfInning {
    game_update: GameUpdate,
    time: DateTime<Utc>,
    #[serde(flatten)]
    which_inning: WhichInning,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct WhichInning {
    pub top_of_inning: bool,
    pub inning: i32,
    pub batting_team_name: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StartingPitchers {
    pub home: (Uuid, String),
    pub away: (Uuid, String),
}

pub fn parse_which_inning(input: &str) -> IResult<&str, WhichInning, VerboseError<&str>> {
    let (input, top_or_bottom) = alt((tag("Top"), tag("Bottom")))(input)?;
    let (input, _) = tag(" of ")(input)?;
    let (input, inning_str) = digit1(input)?;
    let (input, _) = tag(", ")(input)?;
    let (input, (batting_team_name, _)) = many_till(terminated(take_while1(|c: char| !c.is_whitespace()), multispace1),
                                                    tag("batting.").all_consuming())(input)?;

    let top_of_inning = match top_or_bottom {
        "Top" => true,
        "Bottom" => false,
        other => panic!("Invalid value for top_or_bottom: {}", other),
    };

    let inning: i32 = inning_str.parse().expect("Failed to parse inning number");

    Ok((input, WhichInning {
        top_of_inning,
        // Parsed inning is 1-indexed and stored inning should be 0-indexed
        inning: inning - 1,
        batting_team_name: batting_team_name.join(""),
    }))
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

        let which_inning = final_parser(parse_which_inning)(&feed_event.description)
            .expect("Error parsing inning");

        let starting_pitchers = if which_inning.inning == 0 && which_inning.top_of_inning {
            let home_pitcher = read_active_pitcher(state, *home_team, feed_event.day)?;
            let away_pitcher = read_active_pitcher(state, *away_team, feed_event.day)?;

            iproduct!(home_pitcher, away_pitcher)
                .map(|(home, away)| Some(StartingPitchers { home, away }))
                .collect_vec()
        } else {
            vec![None]
        };

        let effects = starting_pitchers.into_iter()
            .map(move |aux| {
                ("game".to_string(), Some(game_id), serde_json::to_value(aux).unwrap())
            })
            .collect();

        let event = Self {
            game_update: GameUpdate::parse(feed_event),
            time,
            which_inning,
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
            }
            other => panic!("HalfInning event does not apply to {}", other.name())
        }
    }

    fn reverse(&self, _entity: AnyEntity, _aux: serde_json::Value) -> AnyEntity {
        todo!()
    }
}