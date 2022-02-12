use anyhow::anyhow;
use crate::sim::{TimedEvent, TimedEventType};
use crate::state::events::IngestEvent;
use crate::StateInterface;

impl IngestEvent for TimedEvent {
    fn apply(&self, state: &mut StateInterface) {
        match self.event_type {
            TimedEventType::EarlseasonStart => earlseason_start(state),
            TimedEventType::DayAdvance => day_advance(state),
            TimedEventType::EndTopHalf => todo!(),
        }
    }
}

fn earlseason_start(state: &mut StateInterface) {
    state.with_sim(|mut sim| {
        if sim.phase == 1 {
            sim.phase = 2;
            sim.next_phase_time = sim.earlseason_date;

            Ok(vec![sim])
        } else {
            Err(anyhow!("Tried to apply EarlseasonStart event while not in Preseason phase"))
        }
    });
}

fn day_advance(state: &mut StateInterface) {
    state.with_sim(|mut sim| {
        sim.day += 1;

        Ok(vec![sim])
    });
}