PRAGMA foreign_keys = ON;
PRAGMA user_version = 1;

CREATE TABLE schema_meta (
    version INTEGER PRIMARY KEY,
    created_at TEXT NOT NULL,
    description TEXT NOT NULL
);
CREATE TABLE entities (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    title TEXT NOT NULL CHECK (length(trim(title)) > 0),
    description TEXT NOT NULL DEFAULT '',
    path TEXT UNIQUE,
    status TEXT NOT NULL,
    attributes_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(attributes_json)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX entities_kind_status ON entities(kind, status);
CREATE INDEX entities_updated ON entities(updated_at);

CREATE TABLE knowledge (
    entity_id TEXT PRIMARY KEY REFERENCES entities(id),
    knowledge_type TEXT NOT NULL CHECK (knowledge_type IN
        ('fact', 'hypothesis', 'finding', 'lesson', 'constraint', 'question')),
    body TEXT NOT NULL CHECK (length(trim(body)) > 0),
    confidence TEXT NOT NULL CHECK (confidence IN ('observed', 'reported', 'inferred', 'unknown')),
    evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
    supersedes TEXT REFERENCES knowledge(entity_id)
);
CREATE INDEX knowledge_type ON knowledge(knowledge_type, confidence);
CREATE TABLE decisions (
    entity_id TEXT PRIMARY KEY REFERENCES entities(id),
    scope TEXT NOT NULL CHECK (length(trim(scope)) > 0),
    body TEXT NOT NULL CHECK (length(trim(body)) > 0),
    authority_ref TEXT,
    evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
    supersedes TEXT REFERENCES decisions(entity_id)
);
CREATE INDEX decisions_scope ON decisions(scope);

CREATE TABLE relations (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES entities(id),
    predicate TEXT NOT NULL CHECK (predicate IN ('relates_to', 'derived_from', 'supports',
        'contradicts', 'implements', 'supersedes', 'belongs_to', 'produced_by',
        'evidence_for', 'justified_by', 'primary_building')),
    target_id TEXT NOT NULL REFERENCES entities(id),
    reason TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    created_at TEXT NOT NULL,
    CHECK (source_id <> target_id),
    UNIQUE (source_id, predicate, target_id)
);
CREATE INDEX relations_forward ON relations(source_id, predicate, target_id);
CREATE INDEX relations_reverse ON relations(target_id, predicate, source_id);
CREATE UNIQUE INDEX relations_primary_building ON relations(source_id)
    WHERE predicate = 'primary_building';

CREATE TABLE events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    created_at TEXT NOT NULL,
    before_json TEXT NOT NULL CHECK (json_valid(before_json)),
    after_json TEXT NOT NULL CHECK (json_valid(after_json)),
    result_json TEXT NOT NULL CHECK (json_valid(result_json))
);
CREATE INDEX events_time ON events(created_at);
CREATE TABLE event_entities (
    event_seq INTEGER NOT NULL REFERENCES events(seq),
    entity_id TEXT NOT NULL REFERENCES entities(id),
    PRIMARY KEY (event_seq, entity_id)
);
CREATE INDEX event_entities_history ON event_entities(entity_id, event_seq);
CREATE TRIGGER events_no_update BEFORE UPDATE ON events BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;
CREATE TRIGGER events_no_delete BEFORE DELETE ON events BEGIN
    SELECT RAISE(ABORT, 'events are append-only');
END;
CREATE TRIGGER event_entities_no_update BEFORE UPDATE ON event_entities BEGIN
    SELECT RAISE(ABORT, 'event subjects are append-only');
END;
CREATE TRIGGER event_entities_no_delete BEFORE DELETE ON event_entities BEGIN
    SELECT RAISE(ABORT, 'event subjects are append-only');
END;

CREATE VIRTUAL TABLE search_index USING fts5(entity_id UNINDEXED, title, body, tokenize='unicode61');

-- Internal derived FTS lookup for newly initialized projects. Existing v1 databases remain readable.
CREATE TABLE _sqlk_search_documents (entity_id TEXT PRIMARY KEY REFERENCES entities(id), fts_rowid INTEGER NOT NULL UNIQUE);
