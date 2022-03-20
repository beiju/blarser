CREATE TABLE ingest_approvals
(
    id                      SERIAL PRIMARY KEY,

    entity_type             TEXT NOT NULL,
    entity_id               UUID NOT NULL,
    perceived_at            TIMESTAMP WITH TIME ZONE NOT NULL,

    message                 TEXT NOT NULL,
    approved                BOOLEAN,
    explanation             TEXT

    CONSTRAINT approvals_explained CHECK ( approved = (explanation IS NOT NULL)  )
);