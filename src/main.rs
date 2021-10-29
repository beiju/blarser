use blarser::ingest::{ingest, IngestObject};
use std::error::Error;

fn main() -> Result<(), impl Error> {
    let recv = ingest();

    loop {
        match recv.recv() {
            Ok(IngestObject::EventuallyEvent(_)) => println!("Event"),
            Ok(IngestObject::PlayersUpdate(_)) => println!("Players"),
            Ok(IngestObject::TeamsUpdate(_)) => println!("Teams"),
            Err(e) => return Err(e),
        }
    };
}
