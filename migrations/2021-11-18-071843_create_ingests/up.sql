CREATE TABLE ingests (
   id SERIAL       PRIMARY KEY,
   started_at      TIMESTAMP NOT NULL,
   events_parsed   INT NOT NULL DEFAULT 0
)