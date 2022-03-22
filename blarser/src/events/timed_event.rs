// fn day_advance(state: &impl StateInterface) {
//     // TODO Check that there are no games going to handle spillover
//     state.with_sim(|mut sim| {
//         sim.day += 1;
//
//         Ok(vec![sim])
//     });
// }
//
// fn end_top_half(game_id: Uuid, state: &impl StateInterface) {
//     state.with_game(game_id, |mut game| {
//         game.phase = 2;
//         game.play_count += 1;
//         game.last_update = String::new();
//         game.last_update_full = Some(Vec::new());
//
//         Ok(vec![game])
//     });
// }