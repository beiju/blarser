CREATE TYPE event_type AS ENUM ('start', 'feed', 'manual', 'earlseason_start', 'day_start');

CREATE TABLE versions
(
    id                 SERIAL PRIMARY KEY,
    ingest_id          INT NOT NULL,

    entity_type        TEXT NOT NULL,
    entity_id          UUID NOT NULL,
    generation         INT NOT NULL,
    single_parent      INT,
    start_time         TIMESTAMP WITH TIME ZONE NOT NULL,

    data               JSONB NOT NULL,

    event_type         event_type NOT NULL,
    feed_event_id      UUID,

    next_timed_event   TIMESTAMP WITH TIME ZONE,

    CONSTRAINT ingest_fk FOREIGN KEY(ingest_id) REFERENCES ingests(id),
    CONSTRAINT single_parent_fk FOREIGN KEY(single_parent) REFERENCES versions(id),
    CONSTRAINT generation_non_negative CHECK (generation >= 0),
    CONSTRAINT feed_event_has_id CHECK ((event_type='feed') = (feed_event_id IS NOT NULL))
);