CREATE TABLE ingests (
   id              SERIAL PRIMARY KEY,
   started_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
   events_parsed   INT NOT NULL DEFAULT 0
)