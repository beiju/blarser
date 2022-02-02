CREATE TABLE feed_events
(
    id              SERIAL PRIMARY KEY,
    ingest_id       INT NOT NULL,

    created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    data            JSONB NOT NULL,

    CONSTRAINT ingest_fk FOREIGN KEY(ingest_id) REFERENCES ingests(id)
);

CREATE TABLE feed_event_changes
(
    id              SERIAL PRIMARY KEY,
    feed_event_id   INT NOT NULL,

    entity_type     TEXT NOT NULL,
    entity_id       UUID,

    CONSTRAINT feed_event_fk FOREIGN KEY(feed_event_id) REFERENCES feed_events(id)
);