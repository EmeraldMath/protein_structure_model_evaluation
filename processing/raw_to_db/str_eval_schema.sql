CREATE TABLE structures(
	structure_id TEXT PRIMARY KEY,
  contact_map  TEXT[],
  score        REAL DEFAULT -1.0
);
