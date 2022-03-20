table! {
    use diesel::sql_types::*;
    use crate::db_types::*;
    use crate::state::Event_source;

    approvals (id) {
        id -> Int4,
        entity_type -> Text,
        entity_id -> Uuid,
        perceived_at -> Timestamptz,
        message -> Text,
        approved -> Nullable<Bool>,
        explanation -> Nullable<Text>,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;
    use crate::state::Event_source;

    event_effects (id) {
        id -> Int4,
        event_id -> Int4,
        entity_type -> Text,
        entity_id -> Uuid,
        aux_data -> Jsonb,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;
    use crate::state::Event_source;

    events (id) {
        id -> Int4,
        ingest_id -> Int4,
        time -> Timestamptz,
        source -> Event_source,
        data -> Jsonb,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;
    use crate::state::Event_source;

    ingests (id) {
        id -> Int4,
        started_at -> Timestamptz,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;
    use crate::state::Event_source;

    version_links (id) {
        id -> Int4,
        parent_id -> Int4,
        child_id -> Int4,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;
    use crate::state::Event_source;

    versions (id) {
        id -> Int4,
        ingest_id -> Int4,
        entity_type -> Text,
        entity_id -> Uuid,
        start_time -> Timestamptz,
        entity -> Jsonb,
        from_event -> Int4,
        event_aux_data -> Jsonb,
        observations -> Array<Timestamptz>,
        terminated -> Nullable<Text>,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;
    use crate::state::Event_source;

    versions_with_end (id) {
        id -> Int4,
        ingest_id -> Int4,
        entity_type -> Text,
        entity_id -> Uuid,
        start_time -> Timestamptz,
        end_time -> Nullable<Timestamptz>,
        entity -> Jsonb,
        from_event -> Int4,
        event_aux_data -> Jsonb,
        observations -> Array<Timestamptz>,
        terminated -> Nullable<Text>,
    }
}

joinable!(event_effects -> events (event_id));
joinable!(versions -> events (from_event));
joinable!(versions_with_end -> events (from_event));

allow_tables_to_appear_in_same_query!(
    approvals,
    event_effects,
    events,
    ingests,
    version_links,
    versions,
    versions_with_end,
);
