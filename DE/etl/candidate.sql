CREATE TABLE candidates(
        candidate_id      TEXT PRIMARY KEY,
  protein_id        TEXT,
  contact_map       BYTEA,
  candidate_source  TEXT,
  score             REAL DEFAULT -1.0,
);