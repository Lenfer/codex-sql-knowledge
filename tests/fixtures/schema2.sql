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

CREATE TABLE knowledge (
    entity_id TEXT PRIMARY KEY REFERENCES entities(id),
    knowledge_type TEXT NOT NULL CHECK (knowledge_type IN
        ('fact', 'hypothesis', 'finding', 'lesson', 'constraint', 'question')),
    body TEXT NOT NULL CHECK (length(trim(body)) > 0),
    confidence TEXT NOT NULL CHECK (confidence IN ('observed', 'reported', 'inferred', 'unknown')),
    evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
    supersedes TEXT REFERENCES knowledge(entity_id)
);

CREATE TABLE decisions (
    entity_id TEXT PRIMARY KEY REFERENCES entities(id),
    scope TEXT NOT NULL CHECK (length(trim(scope)) > 0),
    body TEXT NOT NULL CHECK (length(trim(body)) > 0),
    authority_ref TEXT,
    evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
    supersedes TEXT REFERENCES decisions(entity_id)
);

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

CREATE TABLE _event_payloads(sha256 TEXT PRIMARY KEY,codec TEXT NOT NULL CHECK(codec='zlib3'),raw_bytes INTEGER NOT NULL,payload BLOB NOT NULL);

CREATE TABLE _event_records(seq INTEGER PRIMARY KEY AUTOINCREMENT,operation_key TEXT NOT NULL UNIQUE,request_hash TEXT NOT NULL,action TEXT NOT NULL,actor TEXT NOT NULL,reason TEXT NOT NULL,source_ref TEXT NOT NULL,created_at TEXT NOT NULL,before_ref TEXT NOT NULL REFERENCES _event_payloads(sha256),after_ref TEXT NOT NULL REFERENCES _event_payloads(sha256),result_ref TEXT NOT NULL REFERENCES _event_payloads(sha256));

CREATE TABLE "event_entities"(event_seq INTEGER NOT NULL REFERENCES _event_records(seq),entity_id TEXT NOT NULL REFERENCES entities(id),PRIMARY KEY(event_seq,entity_id));

CREATE TABLE search_documents(entity_id TEXT PRIMARY KEY REFERENCES entities(id),fts_rowid INTEGER NOT NULL UNIQUE);

CREATE INDEX entities_kind_status ON entities(kind, status);

CREATE INDEX entities_updated ON entities(updated_at);

CREATE INDEX knowledge_type ON knowledge(knowledge_type, confidence);

CREATE INDEX decisions_scope ON decisions(scope);

CREATE INDEX relations_forward ON relations(source_id, predicate, target_id);

CREATE INDEX relations_reverse ON relations(target_id, predicate, source_id);

CREATE UNIQUE INDEX relations_primary_building ON relations(source_id)
    WHERE predicate = 'primary_building';

CREATE INDEX event_entities_history ON event_entities(entity_id,event_seq);

CREATE INDEX events_time ON _event_records(created_at);

CREATE INDEX entities_kind_status_id ON entities(kind,status,id);

CREATE INDEX mavis_building_kind_status ON entities(json_extract(attributes_json,'$.building_id'),kind,status,id);

CREATE VIRTUAL TABLE search_index USING fts5(entity_id UNINDEXED, title, body, tokenize='unicode61');

CREATE VIEW events AS SELECT r.seq,r.operation_key,r.request_hash,r.action,r.actor,r.reason,r.source_ref,r.created_at,
          _knowledge_unpack(b.payload) AS before_json,_knowledge_unpack(a.payload) AS after_json,_knowledge_unpack(x.payload) AS result_json
          FROM _event_records r JOIN _event_payloads b ON b.sha256=r.before_ref JOIN _event_payloads a ON a.sha256=r.after_ref JOIN _event_payloads x ON x.sha256=r.result_ref;

CREATE TRIGGER records_no_update BEFORE UPDATE ON _event_records BEGIN SELECT RAISE(ABORT,'events are append-only'); END;

CREATE TRIGGER records_no_delete BEFORE DELETE ON _event_records BEGIN SELECT RAISE(ABORT,'events are append-only'); END;

CREATE TRIGGER payloads_no_update BEFORE UPDATE ON _event_payloads BEGIN SELECT RAISE(ABORT,'event payloads are immutable'); END;

CREATE TRIGGER payloads_no_delete BEFORE DELETE ON _event_payloads BEGIN SELECT RAISE(ABORT,'event payloads are immutable'); END;

CREATE TRIGGER event_entities_no_update BEFORE UPDATE ON event_entities BEGIN SELECT RAISE(ABORT,'event subjects are append-only'); END;

CREATE TRIGGER event_entities_no_delete BEFORE DELETE ON event_entities BEGIN SELECT RAISE(ABORT,'event subjects are append-only'); END;

CREATE TRIGGER events_insert INSTEAD OF INSERT ON events BEGIN
          SELECT CASE WHEN NOT json_valid(NEW.before_json) OR NOT json_valid(NEW.after_json) OR NOT json_valid(NEW.result_json) THEN RAISE(ABORT,'invalid event JSON') END;
          INSERT OR IGNORE INTO _event_payloads VALUES (_knowledge_hash(NEW.before_json),'zlib3',length(cast(NEW.before_json AS blob)),_knowledge_pack(NEW.before_json));
          INSERT OR IGNORE INTO _event_payloads VALUES (_knowledge_hash(NEW.after_json),'zlib3',length(cast(NEW.after_json AS blob)),_knowledge_pack(NEW.after_json));
          INSERT OR IGNORE INTO _event_payloads VALUES (_knowledge_hash(NEW.result_json),'zlib3',length(cast(NEW.result_json AS blob)),_knowledge_pack(NEW.result_json));
          INSERT INTO _event_records VALUES(NEW.seq,NEW.operation_key,NEW.request_hash,NEW.action,NEW.actor,NEW.reason,NEW.source_ref,NEW.created_at,_knowledge_hash(NEW.before_json),_knowledge_hash(NEW.after_json),_knowledge_hash(NEW.result_json));
        END;

CREATE TABLE search_catalog(fts_rowid INTEGER PRIMARY KEY,entity_id TEXT NOT NULL UNIQUE REFERENCES entities(id),kind TEXT NOT NULL,title TEXT NOT NULL,status TEXT NOT NULL,path TEXT,knowledge_type TEXT,confidence TEXT,scope TEXT);
