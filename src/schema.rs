table! {
    use diesel::sql_types::*;
    use crate::db_types::*;

    chron_updates (id) {
        id -> Int4,
        ingest_id -> Int4,
        entity_type -> Text,
        entity_id -> Uuid,
        perceived_at -> Timestamp,
        earliest_time -> Timestamp,
        latest_time -> Timestamp,
        resolved -> Bool,
        data -> Jsonb,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;

    ingest_approvals (id) {
        id -> Int4,
        at -> Timestamp,
        chronicler_entity_type -> Varchar,
        chronicler_time -> Timestamp,
        chronicler_entity_id -> Uuid,
        message -> Text,
        approved -> Nullable<Bool>,
        explanation -> Nullable<Text>,
    }
}

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
        approval_id -> Nullable<Int4>,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;

    ingests (id) {
        id -> Int4,
        started_at -> Timestamp,
        events_parsed -> Int4,
    }
}

joinable!(ingest_logs -> ingest_approvals (approval_id));

allow_tables_to_appear_in_same_query!(
    chron_updates,
    ingest_approvals,
    ingest_logs,
    ingests,
);
