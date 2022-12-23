use uuid::Uuid;
use crate::events::Effect;
use crate::events::effects::BatterIdExtrapolated;
use crate::ingest::StateGraph;
use crate::state::EntityType;

// This list is very much in flux
const BATTER_MOD_PRECEDENCE: [&'static str; 2] = [
    "COFFEE_RALLY",
    "BLASERUNNING",
];

pub fn game_effect_with_modified_batter_id(game_id: Uuid, state: &StateGraph, add: isize) -> Effect {
    let (team_id, team_batter_count) = state.query_game_unique(game_id, |game| {
        let team = game.team_at_bat();
        (team.team, team.team_batter_count)
    });
    let batter_id = team_batter_count.map(|count| {
        state.query_team_unique(team_id, |team| {
            team.lineup[(count as isize + add) as usize % team.lineup.len()]
        })
    });
    let batter_mod = if let Some(batter_id) = batter_id {
        state.query_player_unique(batter_id, |player| {
            for mod_name in BATTER_MOD_PRECEDENCE {
                if player.has_mod(mod_name) {
                    return mod_name.to_string();
                }
            }
            String::new()
        })
    } else {
        String::new()
    };

    Effect::one_id_with(EntityType::Game, game_id, BatterIdExtrapolated::new(batter_id, batter_mod))
}

pub(crate) fn game_effect_with_batter_id(game_id: Uuid, state: &StateGraph) -> Effect {
    game_effect_with_modified_batter_id(game_id, state, 0)
}

pub(crate) fn game_effect_with_next_batter_id(game_id: Uuid, state: &StateGraph) -> Effect {
    game_effect_with_modified_batter_id(game_id, state, 1)
}
