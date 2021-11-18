CREATE TYPE log_type AS ENUM ('debug', 'info', 'warning', 'error');


CREATE TABLE ingest_logs
(
    id         SERIAL PRIMARY KEY,
    at         TIMESTAMP NOT NULL,
    ingest_id  INT NOT NULL,
    type       log_type NOT NULL,
    message    TEXT NOT NULL,
    CONSTRAINT ingest_fk
        FOREIGN KEY(ingest_id)
            REFERENCES ingests(id)
);