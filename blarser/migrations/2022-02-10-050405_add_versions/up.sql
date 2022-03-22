CREATE TYPE event_source AS ENUM ('start', 'feed', 'timed', 'manual');

CREATE TABLE events
(
    id        SERIAL PRIMARY KEY,
    ingest_id INT          NOT NULL,

    time      TIMESTAMP WITH TIME ZONE NOT NULL,
    source    event_source NOT NULL,
    data      JSONB        NOT NULL,

    CONSTRAINT ingest_fk FOREIGN KEY (ingest_id) REFERENCES ingests (id) ON DELETE CASCADE
);

CREATE TABLE event_effects
(
    id                INT PRIMARY KEY,
    event_id          INT NOT NULL,

    entity_type       TEXT NOT NULL,
    entity_id         UUID,
    aux_data          JSONB NOT NULL,

    CONSTRAINT event_fk FOREIGN KEY (event_id) REFERENCES events (id) ON DELETE CASCADE
);

-- I looked it up and Postgres doesn't create this index automatically
CREATE INDEX event_effect_index ON event_effects (event_id);

CREATE TABLE versions
(
    id             SERIAL PRIMARY KEY,
    ingest_id      INT                        NOT NULL,

    entity_type    TEXT                       NOT NULL,
    entity_id      UUID                       NOT NULL,
    start_time     TIMESTAMP WITH TIME ZONE   NOT NULL,

    entity         JSONB                      NOT NULL,
    from_event     INT                        NOT NULL,
    event_aux_data JSONB                      NOT NULL,

    observations   TIMESTAMP WITH TIME ZONE[] NOT NULL,
    terminated     TEXT DEFAULT NULL,

    CONSTRAINT ingest_fk FOREIGN KEY (ingest_id) REFERENCES ingests (id) ON DELETE CASCADE ,
    CONSTRAINT from_event_fk FOREIGN KEY (from_event) REFERENCES events (id) ON DELETE RESTRICT
);

CREATE INDEX versions_index ON versions (ingest_id, entity_type, entity_id, start_time);

CREATE TABLE version_links
(
    id        SERIAL PRIMARY KEY,
    parent_id INT NOT NULL,
    child_id  INT NOT NULL,
    UNIQUE (parent_id, child_id),
    CONSTRAINT parent_fk FOREIGN KEY (parent_id) REFERENCES versions (id) ON DELETE CASCADE,
    CONSTRAINT child_fk FOREIGN KEY (child_id) REFERENCES versions (id) ON DELETE CASCADE
);

CREATE VIEW versions_with_end AS
(
SELECT start_version.id,
       start_version.ingest_id,
       start_version.entity_type,
       start_version.entity_id,
       start_version.start_time,
       (SELECT min(end_version.start_time)
        FROM versions end_version
                 INNER JOIN version_links link ON end_version.id = link.child_id
        WHERE start_version.id = link.parent_id) AS end_time,
       start_version.entity,
       start_version.from_event,
       start_version.event_aux_data,
       start_version.observations,
       start_version.terminated
FROM versions start_version
    );