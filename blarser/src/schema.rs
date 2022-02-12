table! {
    use diesel::sql_types::*;
    use crate::db_types::*;
    use crate::state::Event_type;

    chron_updates (id) {
        id -> Int4,
        ingest_id -> Int4,
        entity_type -> Text,
        entity_id -> Uuid,
        perceived_at -> Timestamptz,
        earliest_time -> Timestamptz,
        latest_time -> Timestamptz,
        resolved -> Bool,
        canonical -> Bool,
        data -> Jsonb,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;
    use crate::state::Event_type;

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
    use crate::state::Event_type;

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
    use crate::state::Event_type;

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
    use crate::state::Event_type;

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
    use crate::state::Event_type;

    ingests (id) {
        id -> Int4,
        started_at -> Timestamptz,
        events_parsed -> Int4,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;
    use crate::state::Event_type;

    versions (id) {
        id -> Int4,
        ingest_id -> Int4,
        entity_type -> Text,
        entity_id -> Uuid,
        generation -> Int4,
        single_parent -> Nullable<Int4>,
        start_time -> Timestamptz,
        data -> Jsonb,
        event_type -> Event_type,
        feed_event_id -> Nullable<Uuid>,
        next_timed_event -> Nullable<Timestamptz>,
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
    versions,
);
