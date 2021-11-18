table! {
    use diesel::sql_types::*;
    use crate::db_types::*;

    ingest_logs (id) {
        id -> Int4,
        at -> Timestamp,
        ingest_id -> Int4,
        #[sql_name = "type"]
        type_ -> Log_type,
        message -> Text,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;

    ingests (id) {
        id -> Int4,
        started_at -> Timestamp,
    }
}

joinable!(ingest_logs -> ingests (ingest_id));

allow_tables_to_appear_in_same_query!(
    ingest_logs,
    ingests,
);
