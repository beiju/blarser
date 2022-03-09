CREATE TYPE event_source AS ENUM ('start', 'feed', 'timed', 'chron');

CREATE TABLE events (
    id                SERIAL PRIMARY KEY,
    ingest_id         INT NOT NULL,

    event_time        TIMESTAMP WITH TIME ZONE NOT NULL,
    event_source      event_source NOT NULL,
    event_data        JSONB NOT NULL,

    CONSTRAINT ingest_fk FOREIGN KEY(ingest_id) REFERENCES ingests(id)
);


CREATE TABLE versions (
    id                 SERIAL PRIMARY KEY,
    ingest_id          INT NOT NULL,

    entity_type        TEXT NOT NULL,
    entity_id          UUID NOT NULL,
    terminated         TEXT DEFAULT NULL,

    data               JSONB NOT NULL,
    from_event         INT NOT NULL,

    next_timed_event   TIMESTAMP WITH TIME ZONE,

    CONSTRAINT ingest_fk FOREIGN KEY(ingest_id) REFERENCES ingests(id),
    CONSTRAINT from_event_fk FOREIGN KEY(from_event) REFERENCES events(id)
);

CREATE TABLE versions_parents (
    id                  SERIAL PRIMARY KEY,
    parent              INT NOT NULL,
    child               INT NOT NULL,
    CONSTRAINT parent_fk FOREIGN KEY(parent) REFERENCES versions(id),
    CONSTRAINT child_fk FOREIGN KEY(child) REFERENCES versions(id)
)