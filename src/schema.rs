table! {
    use diesel::sql_types::*;
    use crate::db_types::*;

    ingest_approvals (id) {
        id -> Int4,
        at -> Timestamp,
        ingest_id -> Int4,
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

allow_tables_to_appear_in_same_query!(
    ingest_approvals,
    ingest_logs,
    ingests,
);