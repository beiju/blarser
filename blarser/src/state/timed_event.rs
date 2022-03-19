use anyhow::anyhow;
use uuid::Uuid;
use partial_information::MaybeKnown;
use crate::sim::{TimedEvent, TimedEventType};
use crate::state::events::IngestEvent;
use crate::StateInterface;

impl IngestEvent for TimedEvent {
    fn apply(&self, state: &impl StateInterface) {
        match self.event_type {
            TimedEventType::EarlseasonStart => earlseason_start(state),
            TimedEventType::DayAdvance => day_advance(state),
            TimedEventType::EndTopHalf(game_id) => end_top_half(game_id, state),
        }
    }
}

fn earlseason_start(state: &impl StateInterface) {
    state.with_sim(|mut sim| {
        if sim.phase == 1 {
            sim.phase = 2;
            sim.next_phase_time = sim.earlseason_date;

            Ok(vec![sim])
        } else {
            Err(anyhow!("Tried to apply EarlseasonStart event while not in Preseason phase"))
        }
    });

    state.with_each_game(|mut game| {
        for self_by_team in [&mut game.home, &mut game.away] {
            self_by_team.batter_name = Some(String::new());
            self_by_team.odds = Some(MaybeKnown::Unknown);
            self_by_team.pitcher = Some(MaybeKnown::Unknown);
            self_by_team.pitcher_name = Some(MaybeKnown::Unknown);
            self_by_team.score = Some(0.0);
            self_by_team.strikes = Some(3);
        }
        game.last_update = String::new();
        game.last_update_full = Some(Vec::new());

        Ok(vec![game])
    })
}

fn day_advance(state: &impl StateInterface) {
    // TODO Check that there are no games going to handle spillover
    state.with_sim(|mut sim| {
        sim.day += 1;

        Ok(vec![sim])
    });
}

fn end_top_half(game_id: Uuid, state: &impl StateInterface) {
    state.with_game(game_id, |mut game| {
        game.phase = 2;
        game.play_count += 1;
        game.last_update = String::new();
        game.last_update_full = Some(Vec::new());

        Ok(vec![game])
    });
}