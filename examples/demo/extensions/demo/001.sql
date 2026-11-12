CREATE TABLE demo_items (id TEXT PRIMARY KEY, entity_id TEXT NOT NULL REFERENCES entities(id), value TEXT NOT NULL);
CREATE INDEX demo_items_entity ON demo_items(entity_id);
