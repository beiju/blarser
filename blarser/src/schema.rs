// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "event_source"))]
    pub struct EventSource;
}

diesel::table! {
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

diesel::table! {
    event_effects (id) {
        id -> Int4,
        event_id -> Int4,
        entity_type -> Text,
        entity_id -> Nullable<Uuid>,
        aux_data -> Jsonb,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::EventSource;

    events (id) {
        id -> Int4,
        ingest_id -> Int4,
        time -> Timestamptz,
        source -> EventSource,
        data -> Jsonb,
    }
}

diesel::table! {
    ingests (id) {
        id -> Int4,
        started_at -> Timestamptz,
    }
}

diesel::table! {
    version_links (id) {
        id -> Int4,
        parent_id -> Int4,
        child_id -> Int4,
    }
}

diesel::table! {
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

diesel::table! {
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

diesel::joinable!(event_effects -> events (event_id));
diesel::joinable!(versions -> events (from_event));
diesel::joinable!(versions_with_end -> events (from_event));

diesel::allow_tables_to_appear_in_same_query!(
    approvals,
    event_effects,
    events,
    ingests,
    version_links,
    versions,
    versions_with_end,
);
