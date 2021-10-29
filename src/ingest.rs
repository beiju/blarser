use std::sync::mpsc;
use std::thread;

use crate::chronicler;
use crate::chronicler_schema::ChroniclerItem;
use crate::eventually;
use crate::eventually_schema::EventuallyEvent;

const EXPANSION_ERA_START: &str = "2021-03-01T00:00:00Z";

pub enum IngestObject {
    EventuallyEvent(EventuallyEvent),
    PlayersUpdate(ChroniclerItem),
    TeamsUpdate(ChroniclerItem),
}

pub fn ingest() -> mpsc::Receiver<IngestObject> {
    let (sender, receiver) = mpsc::sync_channel(16);
    thread::spawn(move || ingest_thread(sender) );
    receiver
}

fn ingest_thread(sender: mpsc::SyncSender<IngestObject>) -> () {
    let events_recv = eventually::events(EXPANSION_ERA_START);
    let players_recv = chronicler::versions("player", EXPANSION_ERA_START);
    let teams_recv = chronicler::versions("team", EXPANSION_ERA_START);

    let mut next_event = Some(events_recv.recv().unwrap());
    let mut next_player = Some(players_recv.recv().unwrap());
    let mut next_team = Some(teams_recv.recv().unwrap());

    // what the hell have i done
    loop {
        if let Some(ref event) = next_event {
            if let Some(ref player) = next_player {
                if let Some(ref team) = next_team {
                    if event.created < player.valid_from && event.created < player.valid_from {
                        sender.send(IngestObject::EventuallyEvent(next_event.unwrap())).unwrap();
                        next_event = Some(events_recv.recv().unwrap());
                    } else if player.valid_from < event.created && player.valid_from < team.valid_from {
                        sender.send(IngestObject::PlayersUpdate(next_player.unwrap())).unwrap();
                        next_player = Some(players_recv.recv().unwrap());
                    } else if team.valid_from < event.created && team.valid_from < player.valid_from {
                        sender.send(IngestObject::TeamsUpdate(next_team.unwrap())).unwrap();
                        next_team = Some(teams_recv.recv().unwrap());
                    } else {
                        panic!("Those options should have been exhaustive");
                    }
                } else {
                    if event.created < player.valid_from {
                        sender.send(IngestObject::EventuallyEvent(next_event.unwrap())).unwrap();
                        next_event = Some(events_recv.recv().unwrap());
                    } else {
                        sender.send(IngestObject::PlayersUpdate(next_player.unwrap())).unwrap();
                        next_player = Some(players_recv.recv().unwrap());
                    }
                }
            } else if let Some(ref team) = next_team {
                if event.created < team.valid_from {
                    sender.send(IngestObject::EventuallyEvent(next_event.unwrap())).unwrap();
                    next_event = Some(events_recv.recv().unwrap());
                } else {
                    sender.send(IngestObject::TeamsUpdate(next_team.unwrap())).unwrap();
                    next_team = Some(teams_recv.recv().unwrap());
                }
            } else {
                sender.send(IngestObject::EventuallyEvent(next_event.unwrap())).unwrap();
                next_event = Some(events_recv.recv().unwrap());
            }
        } else if let Some(ref player) = next_player {
            if let Some(ref team) = next_team {
                if player.valid_from < team.valid_from {
                    sender.send(IngestObject::PlayersUpdate(next_player.unwrap())).unwrap();
                    next_player = Some(players_recv.recv().unwrap());
                } else {
                    sender.send(IngestObject::TeamsUpdate(next_team.unwrap())).unwrap();
                    next_team = Some(teams_recv.recv().unwrap());
                }
            } else {
                sender.send(IngestObject::PlayersUpdate(next_player.unwrap())).unwrap();
                next_player = Some(players_recv.recv().unwrap());
            }
        } else if let Some(_) = next_team {
            sender.send(IngestObject::TeamsUpdate(next_team.unwrap())).unwrap();
            next_team = Some(teams_recv.recv().unwrap());
        } else {
            return
        }
    }
}
