CREATE TYPE log_type AS ENUM ('debug', 'info', 'warning', 'error');


CREATE TABLE ingest_logs
(
    id              SERIAL PRIMARY KEY,
    at              TIMESTAMP NOT NULL,
    ingest_id       INT NOT NULL,
    type            log_type NOT NULL,
    message         TEXT NOT NULL,
    approval_id     INT,
    CONSTRAINT ingest_fk
        FOREIGN KEY(ingest_id)
            REFERENCES ingests(id),
    CONSTRAINT approval_fk
        FOREIGN KEY(approval_id)
            REFERENCES ingest_approvals(id)
);