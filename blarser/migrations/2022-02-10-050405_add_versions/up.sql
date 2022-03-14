CREATE TYPE event_source AS ENUM ('start', 'feed', 'timed', 'chron');

CREATE TABLE events (
    id                SERIAL PRIMARY KEY,
    ingest_id         INT NOT NULL,

    event_time        TIMESTAMP WITH TIME ZONE NOT NULL,
    event_source      event_source NOT NULL,
    event_data        JSONB NOT NULL,

    CONSTRAINT ingest_fk FOREIGN KEY(ingest_id) REFERENCES ingests(id) ON DELETE RESTRICT
);


CREATE TABLE versions (
    id                 SERIAL PRIMARY KEY,
    ingest_id          INT NOT NULL,

    entity_type        TEXT NOT NULL,
    entity_id          UUID NOT NULL,
    terminated         TEXT DEFAULT NULL,

    data               JSONB NOT NULL,
    from_event         INT NOT NULL,

    observed_by        TIMESTAMP WITH TIME ZONE,

    next_timed_event   TIMESTAMP WITH TIME ZONE,

    CONSTRAINT ingest_fk FOREIGN KEY(ingest_id) REFERENCES ingests(id) ON DELETE RESTRICT,
    CONSTRAINT from_event_fk FOREIGN KEY(from_event) REFERENCES events(id) ON DELETE RESTRICT
);

CREATE INDEX versions_index ON versions (ingest_id, entity_type, entity_id);

CREATE TABLE versions_parents (
    id                  SERIAL PRIMARY KEY,
    parent              INT NOT NULL,
    child               INT NOT NULL,
    CONSTRAINT parent_fk FOREIGN KEY(parent) REFERENCES versions(id) ON DELETE CASCADE,
    CONSTRAINT child_fk FOREIGN KEY(child) REFERENCES versions(id) ON DELETE CASCADE
);

CREATE VIEW versions_with_range AS (
    SELECT versions.id,
           versions.ingest_id,
           versions.entity_type,
           versions.entity_id,
           versions.terminated,
           versions.data,
           versions.from_event,
           versions.next_timed_event,
           start_event.event_time,
           start_event.event_source,
           start_event.event_data,
           end_event.event_time AS end_time
    FROM versions
    INNER JOIN events start_event ON versions.from_event = start_event.id
    LEFT JOIN versions_parents vp on versions.id = vp.parent
    LEFT JOIN versions child_version on vp.child = child_version.id
    LEFT JOIN events end_event ON child_version.from_event = end_event.id
)