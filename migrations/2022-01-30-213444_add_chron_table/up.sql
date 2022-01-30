CREATE TABLE chron_updates
(
    id              SERIAL PRIMARY KEY,
    ingest_id       INT NOT NULL,

    entity_type     TEXT NOT NULL,
    entity_id       UUID NOT NULL,
    perceived_at    TIMESTAMP NOT NULL,
    earliest_time   TIMESTAMP NOT NULL,
    latest_time     TIMESTAMP NOT NULL,
    resolved        BOOLEAN NOT NULL,

    data            JSONB NOT NULL,

    CONSTRAINT ingest_fk FOREIGN KEY(ingest_id) REFERENCES ingests(id)
);