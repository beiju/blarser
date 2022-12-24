use log::info;
use uuid::Uuid;
use crate::events::Effect;
use crate::events::effects::GamePlayerExtrapolated;
use crate::ingest::StateGraph;
use crate::state::EntityType;

// Theses lists are very much in flux
const BATTER_MOD_PRECEDENCE: [&'static str; 1] = [
    "COFFEE_RALLY",
];
const RUNNER_MOD_PRECEDENCE: [&'static str; 2] = [
    "BLASERUNNING",
    "COFFEE_RALLY",
];

fn get_displayed_mod(state: &StateGraph, batter_id: Uuid, mods_to_display: &[&str]) -> String {
    state.query_player_unique(batter_id, |player| {
        for &mod_name in mods_to_display {
            if player.has_mod(mod_name) ||
                // Special logic for legacy items, I guess
                (mod_name == "BLASERUNNING" && player.is_wielding("AN_ACTUAL_AIRPLANE")) {
                return mod_name.to_string();
            }
        }
        String::new()
    })
}

fn game_effect_with_modified_batter(game_id: Uuid, state: &StateGraph, add: isize) -> Effect {
    let (team_id, team_batter_count) = state.query_game_unique(game_id, |game| {
        let team = game.team_at_bat();
        (team.team, team.team_batter_count.expect("Team batter count must exist here"))
    });
    let batter_id = state.query_team_unique(team_id, |team| {
        team.lineup[(team_batter_count as isize + add) as usize % team.lineup.len()]
    });
    let batter_mod = get_displayed_mod(state, batter_id, &BATTER_MOD_PRECEDENCE);

    Effect::one_id_with(EntityType::Game, game_id, GamePlayerExtrapolated::new(batter_id, batter_mod))
}

pub(crate) fn game_effect_with_batter(game_id: Uuid, state: &StateGraph) -> Effect {
    game_effect_with_modified_batter(game_id, state, 0)
}

pub(crate) fn game_effect_with_next_batter(game_id: Uuid, state: &StateGraph) -> Effect {
    game_effect_with_modified_batter(game_id, state, 1)
}


pub(crate) fn new_runner_extrapolated(game_id: Uuid, state: &StateGraph) -> GamePlayerExtrapolated {
    let batter_id = state.query_game_unique(game_id, |game| {
        game.team_at_bat().batter
            .expect("There must be a batter here")
    });
    let batter_mod = get_displayed_mod(state, batter_id, &RUNNER_MOD_PRECEDENCE);

    GamePlayerExtrapolated::new(batter_id, batter_mod)
}