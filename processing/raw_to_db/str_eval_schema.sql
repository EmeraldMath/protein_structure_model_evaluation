CREATE TABLE structures(
  structure_id TEXT PRIMARY KEY,
  CASP_NO      TEXT
  contact_map  BYTEA,
  score        REAL DEFAULT -1.0
);
