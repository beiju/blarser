use std::cmp::{max, min};
use std::collections::HashSet;
use anyhow::anyhow;
use itertools::Itertools;
use serde::Deserialize;
use uuid::Uuid;
use partial_information::MaybeKnown;

use crate::api::{EventType, EventuallyEvent};
use crate::state::events::IngestEvent;
use crate::{StateInterface};
use crate::parse::{self, Base};
use crate::sim::{GameByTeam};

impl IngestEvent for EventuallyEvent {
    fn apply(&self, state: &impl StateInterface) {
        match self.r#type {
            EventType::LetsGo => lets_go(state, self),
            EventType::PlayBall => play_ball(state, self),
            EventType::HalfInning => half_inning(state, self),
            EventType::PitcherChange => pitcher_change(state, self),
            EventType::StolenBase => stolen_base(state, self),
            EventType::Walk => walk(state, self),
            EventType::Strikeout => strikeout(state, self),
            // It's easier to combine ground out and flyout types into one function
            EventType::FlyOut => fielding_out(state, self),
            EventType::GroundOut => fielding_out(state, self),
            EventType::HomeRun => home_run(state, self),
            EventType::Hit => hit(state, self),
            EventType::GameEnd => game_end(state, self),
            EventType::BatterUp => batter_up(state, self),
            EventType::Strike => strike(state, self),
            EventType::Ball => ball(state, self),
            EventType::FoulBall => foul_ball(state, self),
            EventType::InningEnd => inning_end(state, self),
            EventType::BatterSkipped => batter_skipped(state, self),
            EventType::PeanutFlavorText => flavor_text(state, self),
            EventType::PlayerStatReroll => player_stat_reroll(state, self),
            EventType::WinCollectedRegular => win_collected_regular(state, self),
            EventType::GameOver => game_over(state, self),
            EventType::StormWarning => storm_warning(state, self),
            EventType::Snowflakes => snowflakes(state, self),
            EventType::ModExpires => mod_expires(state, self),
            EventType::ShamingRun => shaming_run(state, self),
            EventType::TeamWasShamed => team_was_shamed(state, self),
            EventType::TeamDidShame => team_did_shame(state, self),
            _ => todo!(),
        }
    }
}

fn lets_go(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect("LetsGo event must have a game id");
    state.with_game(game_id, |mut game| {
        game.game_start = true;
        game.game_start_phase = -1;
        game.home.team_batter_count = Some(-1);
        game.away.team_batter_count = Some(-1);

        game.game_update_common(event);

        Ok(vec![game])
    });
}

fn play_ball(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect("PlayBall event must have a game id");
    state.with_game(game_id, |mut game| {
        game.game_start_phase = 20;
        game.inning = -1;
        game.phase = 2;
        game.top_of_inning = false;

        // Yeah, it unsets pitchers. Why, blaseball.
        game.home.pitcher = None;
        game.home.pitcher_name = Some(MaybeKnown::Known(String::new()));
        game.away.pitcher = None;
        game.away.pitcher_name = Some(MaybeKnown::Known(String::new()));

        game.game_update_common(event);

        Ok(vec![game])
    });
}

fn half_inning(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect(concat!("HalfInning event must have a game id"));
    state.with_game(game_id, |mut game| {
        game.top_of_inning = !game.top_of_inning;
        if game.top_of_inning {
            game.inning += 1;
        }
        game.phase = 6;
        game.half_inning_score = 0.0;

        // The first halfInning event re-sets the data that PlayBall clears
        if game.inning == 0 && game.top_of_inning {
            for self_by_team in [&mut game.home, &mut game.away] {
                let pitcher_id = *state.read_team(self_by_team.team, |team| {
                    team.active_pitcher(event.day)
                }).iter().exactly_one().expect("Can't handle ambiguity in active pitcher");

                let pitcher_name = state.read_player(pitcher_id, |pitcher| {
                    pitcher.name
                }).iter().exactly_one().expect("Can't handle ambiguity in active pitcher").clone();

                self_by_team.pitcher = Some(MaybeKnown::Known(pitcher_id));
                self_by_team.pitcher_name = Some(MaybeKnown::Known(pitcher_name));
            }
        }

        game.game_update_common(event);

        Ok(vec![game])
    })
}

fn pitcher_change(state: &impl StateInterface, event: &EventuallyEvent) {
    let new_pitcher_id = event.player_id()
        .expect("PitcherChange event must have a player id");
    let new_pitcher_name = state.read_player(new_pitcher_id, |player| {
        player.name
    }).iter().exactly_one().expect("Can't handle ambiguity in player name").clone();

    let game_id = event.game_id().expect("PitcherChange event must have a game id");
    state.with_game(game_id, |mut game| {
        assert!(game.home.pitcher.is_none() || game.away.pitcher.is_none(),
                "Expected one of the pitchers to be null in PitcherChange event");

        assert!(game.home.pitcher.is_some() || game.away.pitcher.is_some(),
                "Expected only one of the pitchers to be null in PitcherChange event, not both");

        if game.home.pitcher.is_none() {
            game.home.pitcher = Some(MaybeKnown::Known(new_pitcher_id));
            game.home.pitcher_name = Some(MaybeKnown::Known(new_pitcher_name.clone()));
        } else {
            game.away.pitcher = Some(MaybeKnown::Known(new_pitcher_id));
            game.away.pitcher_name = Some(MaybeKnown::Known(new_pitcher_name.clone()));
        }

        game.game_update_common(event);

        Ok(vec![game])
    })
}

fn stolen_base(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect("StolenBase event must have a game id");
    let thief_id = event.player_id().expect("StolenBase event must have a player id");

    let read_player_vec = state.read_player(thief_id, |player| player.name);
    let thief_name = read_player_vec.iter().exactly_one().expect("Can't handle ambiguity in player name");
    let steal = parse::parse_stolen_base(thief_name, &event.description)
        .expect("Error parsing StolenBase description");

    state.with_game(game_id, |mut game| {
        match steal {
            parse::BaseSteal::Steal(base) => {
                game.apply_successful_steal(event, thief_id, base)
            }
            parse::BaseSteal::CaughtStealing(base) => {
                game.apply_caught_stealing(event, thief_id, base)
            }
        }

        Ok(vec![game])
    });
}

fn walk(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect("Walk event must have a game id");
    let event_batter_id = event.player_id()
        .expect("Walk event must have a player id");

    let (scoring_runners, _) = separate_scoring_events(&event.metadata.siblings, event_batter_id);

    state.with_game(game_id, |mut game| {
        let batter_id = game.team_at_bat().batter
            .expect("Batter must exist during Walk event");
        let batter_name = game.team_at_bat().batter_name.clone()
            .expect("Batter name must exist during Walk event");

        assert_eq!(event_batter_id, batter_id,
                   "Batter in Walk event didn't match batter in game state");

        for scoring_runner in &scoring_runners {
            game.score_runner(*scoring_runner);
        }

        let batter_mod = game.team_at_bat().batter_mod.clone();
        game.push_base_runner(batter_id, batter_name, batter_mod, Base::First);
        game.end_at_bat();
        game.game_update_pitch(event);

        Ok(vec![game])
    });
}

fn increment_consecutive_hits(state: &impl StateInterface, batter_id: Uuid) {
    state.with_player(batter_id, |mut player| {
        *player.consecutive_hits.as_mut()
            .expect("For now, all players are expected to have consecutive_hits") += 1;
        Ok(vec![player])
    });
}

fn reset_consecutive_hits(state: &impl StateInterface, batter_id: Uuid) {
    state.with_player(batter_id, |mut player| {
        *player.consecutive_hits.as_mut()
            .expect("For now, all players are expected to have consecutive_hits") = 0;
        Ok(vec![player])
    });
}

fn strikeout(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect("Strikeout event must have a game id");
    let event_batter_id = event.player_id()
        .expect("Strikeout event must have exactly one player id");

    state.with_game(game_id, |mut game| {
        let batter_id = game.team_at_bat().batter
            .expect("Batter must exist during Strikeout event");
        let batter_name = game.team_at_bat().batter_name.clone()
            .expect("Batter name must exist during Strikeout event");

        assert_eq!(event_batter_id, batter_id,
                   "Batter in Strikeout event didn't match batter in game state");

        // The result isn't used now, but it will be when double strikes are implemented
        parse::parse_strikeout(&batter_name, &event.description)
            .expect("Error parsing Strikeout description");

        game.out(event, 1);
        game.end_at_bat();

        Ok(vec![game])
    });
}

fn fielding_out(state: &impl StateInterface, event: &EventuallyEvent) {
    // Ground outs and flyouts are different event types, but the logic is so similar that it's
    // easier to combine them
    let game_id = event.game_id().expect("GroundOut/Flyout event must have a game id");

    // Need to read this before updating the game
    let batter_id = state.read_game(game_id, |game| {
        game.team_at_bat()
            .batter.expect("Batter must exist during GroundOut/FlyOut event")
    }).into_iter().exactly_one().expect("Can't handle ambiguity in player at bat");

    state.with_game(game_id, |mut game| {
        let batter_id = game.team_at_bat().batter
            .expect("Batter must exist during GroundOut/FlyOut event");
        let batter_name = game.team_at_bat().batter_name.clone()
            .expect("Batter name must exist during GroundOut/FlyOut event");

        // Verify batter id if the event has the player id; annoyingly, sometimes it doesn't
        if let Some(event_batter_id) = event.player_tags.first() {
            assert_eq!(event_batter_id, &batter_id,
                       "Batter in GroundOut/Flyout event didn't match batter in game state");
        }

        let (scoring_runners, other_events) = separate_scoring_events(&event.metadata.siblings, batter_id);

        let out = match other_events.len() {
            1 => parse::parse_simple_out(&batter_name, &other_events[0].description)
                .expect("Error parsing simple fielding out"),
            2 => parse::parse_complex_out(&batter_name, &other_events[0].description, &other_events[1].description)
                .expect("Error parsing complex fielding out"),
            more => panic!("Unexpected fielding out with {} non-score siblings", more)
        };

        let outs_added = if let parse::FieldingOut::DoublePlay = out { 2 } else { 1 };

        for runner_id in scoring_runners {
            game.score_runner(runner_id);
        }

        game.out(event, outs_added);
        game.end_at_bat();

        let games: Vec<_> = if let parse::FieldingOut::FieldersChoice(runner_name_parsed, out_at_base) = out {
            let runner_idx = game.get_baserunner_with_name(runner_name_parsed, out_at_base);
            game.remove_base_runner(runner_idx);
            // Advance runners first to ensure the batter is not allowed past first. This requires
            // putting push_base_runner in a map.
            game.advance_runners(0).into_iter()
                .update(|game| {
                    let batter_mod = game.team_at_bat().batter_mod.clone();
                    game.push_base_runner(batter_id, batter_name.clone(), batter_mod, Base::First);
                })
                // That combination of advance_runners and push_base_runner means there can be dupes
                // (runner advanced 0->1, then batter put on 0 is equivalent to runner stayed on
                //  0, then batter put on 0 and runner force advanced to 1). This is not the
                // prettiest way to fix it but it works and it was quick to implement.
                .unique_by(|g| g.bases_occupied.clone())
                .collect()
        } else if let parse::FieldingOut::DoublePlay = out {
            if game.baserunner_count > 0 {
                game.remove_each_base_runner().into_iter()
                    .flat_map(|game| game.advance_runners(0))
                    .collect()
            } else {
                game.advance_runners(0)
            }
        } else {
            game.advance_runners(0)
        };

        Ok(games)
    });

    reset_consecutive_hits(state, batter_id);
}

fn home_run(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect("HomeRun event must have a game id");
    let event_batter_id = event.player_id()
        .expect("HomeRun event must have exactly one player id");

    state.with_game(game_id, |mut game| {
        let batter_id = game.team_at_bat().batter
            .expect("Batter must exist during HomeRun event");
        let batter_name = game.team_at_bat().batter_name.clone()
            .expect("Batter name must exist during HomeRun event");

        assert_eq!(event_batter_id, batter_id,
                   "Batter in HomeRun event didn't match batter in game state");

        parse::parse_home_run(&batter_name, &event.description)
            .expect("Error parsing HomeRun description");

        game.game_update_pitch(event);
        game.end_at_bat();

        for runner_id in game.base_runners.clone() {
            game.score_runner(runner_id);
        }

        Ok(vec![game])
    });

    increment_consecutive_hits(state, event_batter_id);
}

fn hit(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect("Hit event must have a game id");
    let event_batter_id = event.player_id()
        .expect("Hit event must have exactly one player id");

    state.with_game(game_id, |mut game| {
        let batter_id = game.team_at_bat().batter
            .expect("Batter must exist during Hit event");
        let batter_name = game.team_at_bat().batter_name.clone()
            .expect("Batter name must exist during Hit event");

        assert_eq!(event_batter_id, batter_id,
                   "Batter in Hit event didn't match batter in game state");

        let hit_type = parse::parse_hit(&batter_name, &event.description)
            .expect("Error parsing Hit description");

        let (scoring_runners, _) = separate_scoring_events(&event.metadata.siblings, batter_id);
        for runner_id in scoring_runners {
            game.score_runner(runner_id);
        }

        game.game_update_pitch(event);
        game.end_at_bat();

        // Must advance runners before putting the batter on first because otherwise forced batter
        // advancement would mess things up
        let games = game.advance_runners(hit_type as i32 + 1).into_iter()
            .update(|game| {
                let batter_mod = game.team_at_bat().batter_mod.clone();
                game.push_base_runner(batter_id, batter_name.clone(), batter_mod, hit_type);
            })
            // See note in FieldersChoice
            .unique_by(|g| g.bases_occupied.clone())
            .collect();

        Ok(games)
    });

    increment_consecutive_hits(state, event_batter_id);
}

fn game_end(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect("GameEnd event must have a game id");
    let winner_id: Uuid = serde_json::from_value(
        event.metadata.other.get("winner")
            .expect("GameEnd event must have a winner in the metadata")
            .clone())
        .expect("Winner property of GameEnd event must be a uuid");

    assert!(event.team_tags.contains(&winner_id),
            "Tried to apply GameEnd event to the wrong team");

    let loser_id = event.team_id_excluding(winner_id)
        .expect("GameEnd event's gameTags must include the winner and one other team");

    update_win_streak(state, winner_id, true);
    update_win_streak(state, loser_id, false);

    state.with_game(game_id, |mut game| {
        game.phase = 7;
        game.end_phase = 3;

        game.game_update_common(event);

        Ok(vec![game])
    });

    // let (home_id, home_runs, away_runs) = state.read_game(game_id, |game| (
    //     game.home.team,
    //     game.home.score.expect("Home score must exist in GameEnd event"),
    //     game.away.score.expect("Away score must exist in GameEnd event"),
    // )).into_iter().exactly_one().expect("Can't handle ambiguity in runs scored");
    //
    // let season_id = state.read_sim(|sim| sim.season_id)
    //     .into_iter().exactly_one().expect("Can't handle ambiguity in season_id");
    //
    // let standings_id = state.read_season(season_id, |season| season.standings)
    //     .into_iter().exactly_one().expect("Can't handle ambiguity in standings");
    //
    // state.with_standings(standings_id, |mut standings| {
    //     let winner_id: Uuid = serde_json::from_value(
    //         event.metadata.other.get("winner")
    //             .expect("GameEnd event must have a winner in the metadata")
    //             .clone())
    //         .expect("Winner property of GameEnd event must be a uuid");
    //
    //     let loser_id = *event.team_tags.iter()
    //         .filter(|&id| *id != winner_id)
    //         .exactly_one()
    //         .expect("gameTags of GameEnd event must contain exactly one winner and one loser");
    //
    //     standings.games_played.insert(winner_id, 1);
    //     standings.games_played.insert(loser_id, 1);
    //     standings.wins.insert(winner_id, 1);
    //     standings.wins.insert(loser_id, 0);
    //     standings.losses.insert(winner_id, 0);
    //     standings.losses.insert(loser_id, 1);
    //     if home_id == winner_id {
    //         standings.runs.insert(winner_id, home_runs);
    //         standings.runs.insert(loser_id, away_runs);
    //     } else {
    //         standings.runs.insert(winner_id, away_runs);
    //         standings.runs.insert(loser_id, home_runs);
    //     }
    //
    //     Ok(vec![standings])
    // });
}

fn update_win_streak(state: &impl StateInterface, team_id: Uuid, team_won: bool) {
    state.with_team(team_id, |mut team| {
        let win_streak = team.win_streak.as_mut()
            .expect("GameEnd currently expects Team.win_streak to exist");

        // win_streak is a weird value whose magnitude indicates length of streak and sign indicates
        // whether it is a winning or losing streak
        if team_won {
            **win_streak = max(**win_streak, 0) + 1;
        } else {
            **win_streak = min(**win_streak, 0) - 1;
        }

        Ok(vec![team])
    });
}

fn batter_up(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect(concat!("BatterUp event must have a game id"));
    state.with_game(game_id, |mut game| {
        let (batter_count, batter_id) = *state.read_team(game.team_at_bat().team, |team| {
            let batter_count = 1 + game.team_at_bat().team_batter_count
                .expect("Team batter count must be populated during a game");
            (batter_count, team.batter_for_count(batter_count as usize))
        }).iter().exactly_one().expect("Can't handle ambiguity in team lineup order");

        let batter_name = state.read_player(batter_id, |player| { player.name })
            .iter().exactly_one().expect("Can't handle ambiguity in player name").clone();

        game.team_at_bat_mut().team_batter_count = Some(batter_count);
        game.team_at_bat_mut().batter = Some(batter_id);
        game.team_at_bat_mut().batter_name = Some(batter_name);

        game.game_update_common(event);

        Ok(vec![game])
    })
}

fn strike(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect(concat!("Strike event must have a game id"));
    state.with_game(game_id, |mut game| {
        game.at_bat_strikes += 1;
        game.game_update_pitch(event);

        Ok(vec![game])
    })
}

fn ball(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect(concat!("Ball event must have a game id"));
    state.with_game(game_id, |mut game| {
        game.at_bat_balls += 1;
        game.game_update_pitch(event);

        Ok(vec![game])
    })
}

fn foul_ball(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect(concat!("FoulBall event must have a game id"));
    state.with_game(game_id, |mut game| {
        if game.at_bat_strikes < 2 {
            game.at_bat_strikes += 1;
        }
        game.game_update_pitch(event);

        Ok(vec![game])
    })
}

fn inning_end(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect(concat!("InningEnd event must have a game id"));
    state.with_game(game_id, |mut game| {
        game.phase = 2;
        game.game_update_common(event);

        Ok(vec![game])
    })
}

fn batter_skipped(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect(concat!("BatterSkipped event must have a game id"));
    state.with_game(game_id, |mut game| {
        game.game_update_common(event);
        *game.team_at_bat_mut().team_batter_count.as_mut()
            .expect("TeamBatterCount must be populated during a game") += 1;

        Ok(vec![game])
    })
}

fn flavor_text(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect(concat!("*FlavorText event must have a game id"));
    state.with_game(game_id, |mut game| {
        game.game_update_common(event);

        Ok(vec![game])
    })
}

fn player_stat_reroll(state: &impl StateInterface, event: &EventuallyEvent) {
    let player_id = event.player_id().expect(concat!("PlayerStatReroll event must have a player id"));
    state.with_player(player_id, |mut player| {
        // This event is normally a child (or in events that use siblings, a non-first
        // sibling), but for Snow events it's a top-level event. For now I assert that it's
        // always snow.

        assert_eq!(event.description, format!("Snow fell on {}!", player.name),
                   "Unexpected top-level PlayerStatReroll event");

        // I think this is pretty close to the actual range
        player.adjust_attributes(-0.03, 0.03);

        Ok(vec![player])
    });
}

fn win_collected_regular(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect(concat!("WinCollectedRegular event must have a game id"));
    state.with_game(game_id, |mut game| {
        game.end_phase = 4;
        game.game_update_common(event);

        Ok(vec![game])
    });
}

fn game_over(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect(concat!("GameOver event must have a game id"));
    state.with_game(game_id, |mut game| {
        game.end_phase = 5;
        game.finalized = true;
        game.game_complete = true;

        if game.home.score.unwrap() > game.away.score.unwrap() {
            game.winner = Some(game.home.team);
            game.loser = Some(game.away.team);
        } else {
            game.loser = Some(game.home.team);
            game.winner = Some(game.away.team);
        };

        game.game_update_common(event);

        Ok(vec![game])
    })
}

fn storm_warning(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect(concat!("StormWarning event must have a game id"));
    state.with_game(game_id, |mut game| {
        game.game_start_phase = 11; // sure why not

        game.game_update_common(event);

        Ok(vec![game])
    })
}

fn snowflakes(state: &impl StateInterface, event: &EventuallyEvent) {
    let game_id = event.game_id().expect(concat!("Snowflakes event must have a game id"));
    let (snow_event, _) = event.metadata.siblings.split_first()
        .expect("Snowflakes event is missing metadata.siblings");

    parse::parse_snowfall(&snow_event.description)
        .expect("Error parsing Snowflakes description");

    // All this is to figure out whether a current pitcher was frozen, and if so, store which team's
    // pitcher it was
    let frozen_pitcher_teams = state.read_game(game_id, |game| {
        (
            pitcher_was_frozen(event, game.team_at_bat()),
            pitcher_was_frozen(event, game.team_fielding()),
        )
    }).into_iter().exactly_one()
        .expect("Can't handle ambiguity in whether the pitchers were frozen");

    state.with_game(game_id, |mut game| {
        game.game_update_common(event);
        game.game_start_phase = 20;

        if game.state.snowfall_events.is_none() {
            game.state.snowfall_events = Some(0)
        }
        *game.state.snowfall_events.as_mut().expect("snowfallEvents must be set in Snowflakes event") += 1;

        let frozen_players: HashSet<_> = event.metadata.siblings.iter()
            .flat_map(|event| {
                if let Some(serde_json::Value::String(mod_name)) = event.metadata.other.get("mod") {
                    if mod_name == "FROZEN" {
                        return Some(event.player_id().expect("Must have a player ID"));
                    }
                }

                None
            })
            .collect();

        if let Some(batter_id) = game.team_at_bat().batter {
            if frozen_players.contains(&batter_id) {
                game.team_at_bat_mut().batter = None;
                game.team_at_bat_mut().batter_name = Some("".to_string());
            }
        }

        if let Some(pitcher_id) = &game.team_fielding().pitcher {
            let pitcher_id = pitcher_id.known()
                .expect("Pitcher must be Known in Snowfall event");

            if frozen_players.contains(pitcher_id) {
                game.team_fielding_mut().pitcher = None;
                game.team_fielding_mut().pitcher_name = Some("".to_string().into());
            }
        }

        Ok(vec![game])
    });

    for child_event in &event.metadata.siblings {
        if child_event.r#type == EventType::AddedMod {
            let player_id = child_event.player_id().expect("AddedMod event must have a player id");
            state.with_player(player_id, |mut player| {
                player.game_attr.as_mut().expect("Everyone but Phantom Sixpack has this").push("FROZEN".to_string());

                Ok(vec![player])
            });
        }
    }

    for team_id in [frozen_pitcher_teams.0, frozen_pitcher_teams.1].into_iter().flatten() {
        state.with_team(team_id, |mut team| {
            team.rotation_slot += 1;

            Ok(vec![team])
        });
    }
}

fn pitcher_was_frozen(event: &EventuallyEvent, game_team: &GameByTeam) -> Option<Uuid> {
    game_team.pitcher.as_ref()
        .and_then(|maybe_pitcher| maybe_pitcher.known())
        .and_then(|&pitcher_id| {
            event.metadata.siblings.iter()
                .find_map(|sibling| {
                    let matches = sibling.r#type == EventType::AddedMod &&
                        sibling.metadata.other.get("mod").map(|mod_name| mod_name == "FROZEN").unwrap_or(false) &&
                        sibling.player_id().expect("ModAdded event must have a player id") == pitcher_id;

                    if matches { Some(game_team.team) } else { None }
                })
        })
}


fn mod_expires(state: &impl StateInterface, event: &EventuallyEvent) {
    let player_id = event.player_id().expect("ModExpires event must have a player id");

    #[derive(Deserialize)]
    struct ModExpiresMetadata {
        mods: Vec<String>,
        r#type: i32,
    }

    let metadata: ModExpiresMetadata = serde_json::from_value(event.metadata.other.clone())
        .expect("Failed to extract metadata from ModExpires event");

    state.with_player(player_id, |mut player| {
        let mod_list = match &metadata.r#type {
            3 => { &mut player.game_attr }
            _ => { todo!() }
        }.as_mut().expect("Tried to remove mod from nonexistent list");

        for remove_mod in &metadata.mods {
            let index = mod_list.iter().position(|m| m == remove_mod)
                .expect("Tried to remove mod that didn't exist in the list");
            mod_list.remove(index);
        }

        Ok(vec![player])
    });
}

fn shaming_run(state: &impl StateInterface, event: &EventuallyEvent) {
    // Read the away team's score before applying the event
    let game_id = event.game_id()
        .expect("Shame event must have a game id");
    let home_score_before = state.read_game(game_id, |game| {
        game.home.score.expect("homeScore must exist during a Shame event")
    }).into_iter().exactly_one()
        .expect("Can't handle ambiguity in home team score");
    let home_score_after = event.metadata.siblings.iter()
        .find(|e| e.r#type == EventType::RunsScored)
        .expect("Shame event must have a RunsScored sibling")
        .metadata.other.get("homeScore")
        .expect("RunsScored event metadata must have homeScore")
        .as_f64()
        .expect("homeScore must be a float")
        as f32;
    let shame_runs = home_score_after - home_score_before;

    // Make a new event with the shame stripped off
    let (shame_event, other_events) = event.metadata.siblings.split_first()
        .expect("Shame event must have siblings");
    let mut other_event = other_events.first().cloned()
        .expect("Shame event must have another event inside it");
    other_event.metadata.siblings = event.metadata.siblings.clone();

    other_event.apply(state);

    // I have a feeling it's not enough to consider just the shaming run, but I'll get to that when
    // I have proof of it
    // The shamed team was first in the event I looked at, hopefully that's true in general
    let shamed_team_id = *shame_event.team_tags.first()
        .expect("Shame event must have at least one team tag");

    state.with_team(shamed_team_id, |mut team| {
        team.shame_runs += shame_runs;
        Ok(vec![team])
    });
}

fn team_was_shamed(state: &impl StateInterface, event: &EventuallyEvent) {
    let team_id = event.team_id()
        .expect("TeamWasShamed event must have exactly one team id");

    team_shame_common(state, event, team_id, true);
}

fn team_did_shame(state: &impl StateInterface, event: &EventuallyEvent) {
    let team_id = event.team_id()
        .expect("TeamDidShame event must have exactly one team id");

    team_shame_common(state, event, team_id, false);
}

fn team_shame_common(state: &impl StateInterface, event: &EventuallyEvent, team_id: Uuid, was_shamed: bool) {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TeamShameMetadata {
        total_shames: i32,
        total_shamings: i32,
    }

    let metadata: TeamShameMetadata = serde_json::from_value(event.metadata.other.clone())
        .expect("Failed to parse team shame metadata");

    state.with_team(team_id, |mut team| {
        if was_shamed {
            team.total_shames += 1;
            team.season_shames += 1;
        } else {
            team.total_shamings += 1;
            team.season_shamings += 1;
        }

        if team.total_shamings != metadata.total_shamings {
            Err(anyhow!("totalShamings field in event metadata ({}) did not match team's totalShamings ({})",
                metadata.total_shamings, team.total_shamings))
        } else if team.total_shames != metadata.total_shames {
            Err(anyhow!("totalShames field in event metadata ({}) did not match team's totalShames ({})",
                metadata.total_shames, team.total_shames))
        } else {
            Ok(vec![team])
        }
    });
}

pub fn separate_scoring_events(siblings: &[EventuallyEvent], hitter_id: Uuid) -> (Vec<Uuid>, Vec<&EventuallyEvent>) {
    // The first event is never a scoring event, and it mixes up the rest of the logic because the
    // "hit" or "walk" event type is reused
    let (first, rest) = siblings.split_first()
        .expect("Event's siblings array is empty");
    let mut scores = Vec::new();
    let mut others = vec![first];

    for event in rest {
        if event.r#type == EventType::Hit || event.r#type == EventType::Walk {
            let runner_id = event.player_id_excluding(hitter_id)
                .expect("Scoring event must have a player id");
            scores.push(runner_id);
        } else if event.r#type != EventType::RunsScored {
            others.push(event);
        }
    }

    (scores, others)
}
