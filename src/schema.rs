table! {
    use diesel::sql_types::*;
    use crate::db_types::*;

    chron_updates (id) {
        id -> Int4,
        ingest_id -> Int4,
        entity_type -> Text,
        entity_id -> Uuid,
        perceived_at -> Timestamptz,
        earliest_time -> Timestamptz,
        latest_time -> Timestamptz,
        resolved -> Bool,
        data -> Jsonb,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;

    feed_event_changes (id) {
        id -> Int4,
        feed_event_id -> Int4,
        entity_type -> Text,
        entity_id -> Nullable<Uuid>,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;

    feed_events (id) {
        id -> Int4,
        ingest_id -> Int4,
        created_at -> Timestamptz,
        data -> Jsonb,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;

    ingest_approvals (id) {
        id -> Int4,
        at -> Timestamptz,
        chronicler_entity_type -> Varchar,
        chronicler_time -> Timestamptz,
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
        at -> Timestamptz,
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
        started_at -> Timestamptz,
        events_parsed -> Int4,
    }
}

joinable!(feed_event_changes -> feed_events (feed_event_id));
joinable!(ingest_logs -> ingest_approvals (approval_id));

allow_tables_to_appear_in_same_query!(
    chron_updates,
    feed_event_changes,
    feed_events,
    ingest_approvals,
    ingest_logs,
    ingests,
);
