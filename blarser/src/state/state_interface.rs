use diesel::{PgConnection};

pub struct StateInterface<'conn> {
    pub conn: &'conn &'conn mut PgConnection,
    pub ingest_id: i32,

    // TODO: Cache parameters
}

impl<'conn> StateInterface<'conn> {
    pub fn new(c: &'conn &'conn mut PgConnection, ingest_id: i32) -> StateInterface<'conn> {
        StateInterface {
            conn: c,
            ingest_id
        }
    }
}