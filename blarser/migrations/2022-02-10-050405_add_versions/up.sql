CREATE TABLE versions (
    id                 SERIAL PRIMARY KEY,
    ingest_id          INT NOT NULL,

    entity_type        TEXT NOT NULL,
    entity_id          UUID NOT NULL,
    start_time         TIMESTAMP WITH TIME ZONE NOT NULL,
    terminated         TEXT DEFAULT NULL,

    data               JSONB NOT NULL,

    next_timed_event   TIMESTAMP WITH TIME ZONE,

    CONSTRAINT ingest_fk FOREIGN KEY(ingest_id) REFERENCES ingests(id)
);

CREATE TABLE versions_parents (
    id                  SERIAL PRIMARY KEY,
    parent              INT NOT NULL,
    child               INT NOT NULL,
    CONSTRAINT parent_fk FOREIGN KEY(parent) REFERENCES versions(id),
    CONSTRAINT child_fk FOREIGN KEY(child) REFERENCES versions(id)
)