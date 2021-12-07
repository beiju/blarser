CREATE TABLE ingest_approvals
(
    id                      SERIAL PRIMARY KEY,
    at                      TIMESTAMP NOT NULL,
    chronicler_entity_type  VARCHAR(255) NOT NULL,
    chronicler_time         TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    chronicler_entity_id    UUID NOT NULL,
    message                 TEXT NOT NULL,
    approved                BOOLEAN,
    explanation             TEXT
);