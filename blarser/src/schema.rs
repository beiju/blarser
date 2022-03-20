table! {
    use diesel::sql_types::*;
    use crate::db_types::*;
    use crate::state::Event_source;

    events (id) {
        id -> Int4,
        ingest_id -> Int4,
        source -> Event_source,
        data -> Jsonb,
    }
}

table! {
    use diesel::sql_types::*;
    use crate::db_types::*;
    use crate::state::Event_source;

    ingest_approvals (id) {
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

joinable!(versions -> events (from_event));

allow_tables_to_appear_in_same_query!(
    events,
    ingest_approvals,
    ingests,
    version_links,
    versions,
);
