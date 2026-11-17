#!/usr/bin/env python3
"""Project SQLite history and knowledge. Standard library only; Python >= 3.9."""
import argparse
from contextlib import contextmanager
import datetime as dt
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import sqlite3
import sys
import tempfile
import time
import uuid
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = Path(__file__).with_name('schema.sql')
VERSION = 1
ARTIFACT_KINDS = {'project', 'file', 'document', 'script', 'skill', 'source', 'model',
                  'building', 'scene', 'experiment', 'report'}
ARTIFACT_STATES = {'active', 'missing', 'deleted', 'archived', 'draft'}
KNOWLEDGE_STATES = {'proposed', 'active', 'disputed', 'superseded', 'rejected'}
DECISION_STATES = {'proposed', 'accepted', 'rejected', 'superseded', 'revoked'}
PREDICATES = {'relates_to', 'derived_from', 'supports', 'contradicts', 'implements',
              'belongs_to', 'produced_by', 'evidence_for', 'justified_by', 'primary_building'}
TABLES = ['schema_meta', 'entities', 'knowledge', 'decisions', 'relations', 'events', 'event_entities']


class MemoryError(Exception):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def require(condition, message, code='invalid_input'):
    if not condition:
        raise MemoryError(code, message)


def packed(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def timestamp():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec='microseconds')


def text(data, key):
    value = data.get(key)
    require(isinstance(value, str) and bool(value.strip()), key + ' must be nonempty text')
    return value.strip()


def durable_ref(value):
    require(isinstance(value, str) and bool(value.strip()), 'source reference is required')
    normalized = value.replace('\\', '/').lower()
    require(not any(part in normalized for part in ('.tmp/', '.report/')),
            'temporary files cannot be the durable provenance of project knowledge')
    return value.strip()


def evidence(data):
    items = data.get('evidence', [])
    require(isinstance(items, list), 'evidence must be an array')
    for item in items:
        require(isinstance(item, dict), 'evidence items must be objects with ref')
        durable_ref(item.get('ref'))
    return items


def project_path(value):
    if value is None:
        return None
    require(isinstance(value, str) and bool(value.strip()), 'path must be nonempty or null')
    p = PurePosixPath(value)
    require('\\' not in value and not p.is_absolute() and '..' not in p.parts,
            'path must be relative to the project without .. or backslashes')
    require(p.parts and p.parts[0] != '.git', 'Git internal files do not belong in the catalog')
    if p.parts[0] in {'.tmp', '.report'}:
        require(len(p.parts) >= 3, 'temporary artifacts require a daily directory and a filename')
        # Audit jobs are created by this runtime under their own reserved root.
        # Catalog support does not grant deletion: storage policies still gate it.
        if p.parts[0] == '.tmp' and p.parts[1] == 'check-jobs':
            require(len(p.parts[2]) == 32 and all(c in '0123456789abcdef' for c in p.parts[2]),
                    'invalid audit job directory')
        else:
            require(dt.date.fromisoformat(p.parts[1]).isoformat() == p.parts[1], 'invalid daily directory')
    resolved = (ROOT / str(p)).resolve()
    require(resolved == ROOT or ROOT in resolved.parents, 'path escapes the project through a symlink')
    return str(p)


class ClosingConnection(sqlite3.Connection):
    """A project connection context commits/rolls back AND closes its descriptor."""
    def close(self):
        callback=getattr(self,'_sqlk_on_close',None)
        self._sqlk_on_close=None
        try:return super().close()
        finally:
            if callback is not None:callback()

    def commit(self):
        import time
        started=time.perf_counter()
        try:return super().commit()
        finally:self._last_commit_ms=(time.perf_counter()-started)*1000

    def __exit__(self, *args):
        try:return super().__exit__(*args)
        finally:self.close()


def connect(path, writable=False):
    require(path.is_file(), 'database does not exist; explicit init or recovery is required', 'database_missing')
    db=sqlite3.connect(path.as_uri()+('?mode=rw' if writable else '?mode=ro'), uri=True, timeout=10, factory=ClosingConnection)
    try:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA busy_timeout=10000')
        version=db.execute('PRAGMA user_version').fetchone()[0]
        require(version==VERSION,'unsupported database schema version: '+str(version),'schema_version')
        require(db.execute('SELECT version FROM schema_meta').fetchone()[0]==VERSION,'schema metadata mismatch','schema_version')
        return db
    except BaseException:db.close();raise


def row(db, table, key, value):
    found = db.execute('SELECT * FROM ' + table + ' WHERE ' + key + '=?', (value,)).fetchone()
    return dict(found) if found else None


def snapshot(db, entity_id):
    entity = row(db, 'entities', 'id', entity_id)
    if entity is None:
        return None
    detail = None
    if entity['kind'] in {'knowledge', 'decision'}:
        detail = row(db, 'knowledge' if entity['kind'] == 'knowledge' else 'decisions', 'entity_id', entity_id)
    return {'entity': entity, 'detail': detail}


def reindex(db, entity_id):
    current = snapshot(db, entity_id)
    db.execute('DELETE FROM search_index WHERE entity_id=?', (entity_id,))
    entity, detail = current['entity'], current['detail']
    body = entity['description'] + '\n' + (detail['body'] if detail else '')
    db.execute('INSERT INTO search_index(entity_id,title,body) VALUES (?,?,?)',
               (entity_id, entity['title'], body))


def upsert_entity(db, data, kind, now):
    entity_id = text(data, 'id')
    old = row(db, 'entities', 'id', entity_id)
    require(old is None or old['kind'] == kind, 'entity kind cannot change')
    title = text(data, 'title')
    status = text(data, 'status')
    states = KNOWLEDGE_STATES if kind == 'knowledge' else DECISION_STATES if kind == 'decision' else ARTIFACT_STATES
    require(status in states, 'invalid status for ' + kind)
    attrs = data.get('attributes', json.loads(old['attributes_json']) if old else {})
    require(isinstance(attrs, dict), 'attributes must be an object')
    path = project_path(data.get('path', old['path'] if old else None))
    description = data.get('description', old['description'] if old else '')
    require(isinstance(description, str), 'description must be text')
    db.execute('''INSERT INTO entities VALUES (?,?,?,?,?,?,?,?,?)
        ON CONFLICT(id) DO UPDATE SET title=excluded.title, description=excluded.description,
        path=excluded.path, status=excluded.status, attributes_json=excluded.attributes_json,
        updated_at=excluded.updated_at''',
        (entity_id, kind, title, description, path, status, packed(attrs), old['created_at'] if old else now, now))
    return entity_id


def relation(db, source, predicate, target, envelope, now, relation_id=None):
    require(source != target, 'self relations are not allowed')
    require(snapshot(db, source) is not None and snapshot(db, target) is not None, 'both relation endpoints must exist')
    if predicate == 'primary_building':
        require(row(db, 'entities', 'id', target)['kind'] == 'building', 'primary_building target must be a building')
    relation_id = relation_id or 'rel-' + uuid.uuid4().hex
    db.execute('INSERT INTO relations VALUES (?,?,?,?,?,?,?)',
               (relation_id, source, predicate, target, envelope['reason'], envelope['source_ref'], now))
    return row(db, 'relations', 'id', relation_id)


def mutate(db, action, envelope, transaction=True):
    require(isinstance(envelope, dict), 'write envelope must be an object')
    for key in ('idempotency_key', 'actor', 'reason', 'source_ref'):
        envelope[key] = text(envelope, key)
    durable_ref(envelope['source_ref'])
    data = envelope.get('data')
    require(isinstance(data, dict), 'data must be an object')
    request_hash = hashlib.sha256(packed({'action': action, 'envelope': envelope}).encode()).hexdigest()
    require(transaction or db.in_transaction, 'nested mutation requires an explicit outer transaction')
    if transaction:
        db.execute('BEGIN IMMEDIATE')
    try:
        previous = row(db, 'events', 'operation_key', envelope['idempotency_key'])
        if previous:
            require(previous['request_hash'] == request_hash, 'idempotency key already used for different input', 'idempotency_conflict')
            result = json.loads(previous['result_json'])
            if transaction:
                db.rollback()
            return dict(result, replayed=True)
        now, before, touched, links = timestamp(), {}, [], []
        if action == 'link':
            source, target, predicate = text(data, 'source_id'), text(data, 'target_id'), text(data, 'predicate')
            require(predicate in PREDICATES, 'unsupported predicate; supersedes is managed by typed commands')
            touched = [source, target]
            before = {key: snapshot(db, key) for key in touched}
            links.append(relation(db, source, predicate, target, envelope, now, data.get('id')))
            result_id = links[0]['id']
        else:
            result_id = text(data, 'id')
            before[result_id] = snapshot(db, result_id)
            touched = [result_id]
            if action == 'entity':
                kind = text(data, 'kind')
                require(kind in ARTIFACT_KINDS, 'knowledge and decisions require their own commands')
                upsert_entity(db, data, kind, now)
            else:
                kind = 'knowledge' if action == 'knowledge' else 'decision'
                require(action in {'knowledge', 'decision'}, 'unsupported write command')
                items = evidence(data)
                state = text(data, 'status')
                require(state != 'superseded', 'superseded is set by accepting an explicit replacement')
                body = text(data, 'body')
                prior = before[result_id]
                if prior:
                    require(prior['entity']['status'] != 'superseded', 'a superseded record is immutable; create a new record')
                    transitions = ({'proposed': {'proposed', 'accepted', 'rejected'},
                                    'accepted': {'accepted', 'revoked'}, 'revoked': {'revoked'}, 'rejected': {'rejected'}}
                                   if kind == 'decision' else
                                   {'proposed': {'proposed', 'active', 'disputed', 'rejected'},
                                    'active': {'active', 'disputed', 'rejected'},
                                    'disputed': {'disputed', 'active', 'rejected'}, 'rejected': {'rejected'}})
                    require(state in transitions.get(prior['entity']['status'], set()),
                            'invalid lifecycle transition; create an explicit replacement record')
                if kind == 'knowledge':
                    require(data.get('knowledge_type') in {'fact', 'hypothesis', 'finding', 'lesson', 'constraint', 'question'}, 'invalid knowledge_type')
                    require(data.get('confidence') in {'observed', 'reported', 'inferred', 'unknown'}, 'invalid confidence')
                    require(state != 'active' or bool(items), 'active knowledge needs durable evidence')
                    require(prior is None or prior['entity']['status'] == 'proposed' or
                            (prior['detail']['body'] == body and prior['entity']['title'] == data.get('title') and
                             prior['detail']['knowledge_type'] == data.get('knowledge_type') and
                             prior['detail']['confidence'] == data.get('confidence')),
                            'revise active knowledge by creating a new record with supersedes')
                else:
                    authority = data.get('authority_ref')
                    require(state != 'accepted' or bool(items), 'accepted decisions need evidence')
                    if state in {'accepted', 'revoked', 'superseded'}:
                        durable_ref(authority)
                    require(prior is None or prior['entity']['status'] == 'proposed' or
                            (prior['detail']['body'] == body and prior['entity']['title'] == data.get('title') and
                             prior['detail']['scope'] == data.get('scope')),
                            'revise accepted decisions by creating a new record with supersedes')
                supersedes = data.get('supersedes', prior['detail'].get('supersedes') if prior else None)
                if prior:
                    require(supersedes == prior['detail'].get('supersedes'), 'supersedes cannot be rewritten')
                active = 'active' if kind == 'knowledge' else 'accepted'
                if supersedes and (not prior or (prior['entity']['status'] == 'proposed' and state == active)):
                    require(supersedes != result_id, 'a record cannot supersede itself')
                    old = snapshot(db, supersedes)
                    require(old is not None and old['entity']['kind'] == kind, 'superseded record has wrong type or does not exist')
                    require(state == active, 'a replacement must be active/accepted')
                    require(old['entity']['status'] == active, 'only a current active record can be superseded')
                    before[supersedes] = old
                    touched.append(supersedes)
                    db.execute('UPDATE entities SET status=?,updated_at=? WHERE id=?', ('superseded', now, supersedes))
                upsert_entity(db, data, kind, now)
                if kind == 'knowledge':
                    db.execute('''INSERT INTO knowledge VALUES (?,?,?,?,?,?) ON CONFLICT(entity_id) DO UPDATE SET
                        knowledge_type=excluded.knowledge_type, body=excluded.body, confidence=excluded.confidence,
                        evidence_json=excluded.evidence_json''',
                        (result_id, data['knowledge_type'], body, data['confidence'], packed(items), supersedes))
                else:
                    scope = text(data, 'scope')
                    db.execute('''INSERT INTO decisions VALUES (?,?,?,?,?,?) ON CONFLICT(entity_id) DO UPDATE SET
                        scope=excluded.scope, body=excluded.body, authority_ref=excluded.authority_ref,
                        evidence_json=excluded.evidence_json''',
                        (result_id, scope, body, data.get('authority_ref'), packed(items), supersedes))
                if supersedes and supersedes in touched:
                    links.append(relation(db, result_id, 'supersedes', supersedes, envelope, now))
        for entity_id in touched:
            reindex(db, entity_id)
        after = {key: snapshot(db, key) for key in touched}
        result = {'ok': True, 'id': result_id, 'operation_key': envelope['idempotency_key'], 'replayed': False}
        cur = db.execute('''INSERT INTO events(operation_key,request_hash,action,actor,reason,source_ref,
            created_at,before_json,after_json,result_json) VALUES (?,?,?,?,?,?,?,?,?,?)''',
            (envelope['idempotency_key'], request_hash, action, envelope['actor'], envelope['reason'],
             envelope['source_ref'], now, packed({'entities': before}),
             packed({'entities': after, 'relations': links}), packed(result)))
        event_seq = db.execute('SELECT seq FROM events WHERE operation_key=?', (envelope['idempotency_key'],)).fetchone()[0]
        for entity_id in set(touched):
            db.execute('INSERT INTO event_entities VALUES (?,?)', (event_seq, entity_id))
        if transaction:
            db.commit()
        return result
    except Exception:
        if transaction:
            db.rollback()
        raise


def read_query(db, sql, params, limit):
    require(1 <= limit <= 500, 'limit must be between 1 and 500')
    require(isinstance(params, (list, dict)), 'params must be an array or object')
    allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE}
    def authorize(action, arg1, arg2, database, trigger):
        if action == sqlite3.SQLITE_FUNCTION and (arg2 or '').lower() in {'load_extension', 'writefile', 'readfile'}:
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY
    deadline = time.monotonic() + 5
    db.set_authorizer(authorize)
    db.set_progress_handler(lambda: int(time.monotonic() > deadline or (getattr(db,'_sqlk_progress',None) is not None and db._sqlk_progress())), 1000)
    try:
        cursor = db.execute(sql, params)
        rows = cursor.fetchmany(limit + 1)
        return {'ok': True, 'rows': [dict(v) for v in rows[:limit]], 'truncated': len(rows) > limit}
    finally:
        db.set_progress_handler(None, 0)
        db.set_authorizer(None)


def check(db, files=True):
    problems = []
    integrity = db.execute('PRAGMA integrity_check').fetchone()[0]
    if integrity != 'ok':
        problems.append({'kind': 'sqlite_integrity', 'detail': integrity})
    for item in db.execute('PRAGMA foreign_key_check'):
        problems.append({'kind': 'foreign_key', 'detail': list(item)})
    for entity in db.execute('SELECT * FROM entities').fetchall():
        entity = dict(entity)
        last = db.execute('''SELECT ev.after_json FROM events ev JOIN event_entities ee ON ee.event_seq=ev.seq
            WHERE ee.entity_id=? ORDER BY ev.seq DESC LIMIT 1''', (entity['id'],)).fetchone()
        if not last or json.loads(last[0])['entities'].get(entity['id']) != snapshot(db, entity['id']):
            problems.append({'kind': 'history_mismatch', 'id': entity['id']})
        expected = snapshot(db, entity['id'])
        detail = expected['detail']
        if entity['kind'] in {'knowledge', 'decision'} and detail is None:
            problems.append({'kind': 'missing_detail', 'id': entity['id']})
        expected_body = entity['description'] + '\n' + (detail['body'] if detail else '')
        indexed = db.execute('SELECT title,body FROM search_index WHERE entity_id=?', (entity['id'],)).fetchall()
        if len(indexed) != 1 or tuple(indexed[0]) != (entity['title'], expected_body):
            problems.append({'kind': 'search_index_mismatch', 'id': entity['id']})
        if files and entity['path'] and entity['status'] == 'active' and not (ROOT / entity['path']).exists():
            problems.append({'kind': 'missing_file', 'id': entity['id'], 'path': entity['path']})
    return {'ok': not problems, 'problems': problems,
            'entities': db.execute('SELECT count(*) FROM entities').fetchone()[0],
            'events': db.execute('SELECT count(*) FROM events').fetchone()[0]}


@contextmanager
def read_snapshot(db):
    """Run long checks/exports on a consistent copy, without a long live read lock."""
    day = dt.datetime.now(ZoneInfo('Europe/Moscow')).date().isoformat()
    parent = ROOT / '.tmp' / day / 'project-knowledge'
    parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='read-snapshot-', dir=parent) as directory:
        replica = sqlite3.connect(str(Path(directory) / 'knowledge.sqlite'))
        replica.row_factory = sqlite3.Row
        try:
            db.backup(replica, pages=256, sleep=0.01)
            yield replica
        finally:
            replica.close()


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--db', type=Path, default=ROOT / 'knowledge.sqlite', help='explicit alternative database for tests/recovery only')
    sub = p.add_subparsers(dest='command', required=True)
    for command in ('init', 'entity', 'knowledge', 'decision', 'link'):
        sub.add_parser(command).add_argument('--input', required=True, help='JSON envelope path, or - for stdin')
    sub.add_parser('context')
    sub.add_parser('schema')
    sub.add_parser('check')
    q = sub.add_parser('query')
    q.add_argument('--sql', required=True)
    q.add_argument('--params', default='[]')
    q.add_argument('--limit', type=int, default=100)
    s = sub.add_parser('search')
    s.add_argument('--text', required=True)
    s.add_argument('--limit', type=int, default=20)
    s.add_argument('--include-inactive', action='store_true')
    h = sub.add_parser('history')
    h.add_argument('--id', required=True)
    h.add_argument('--limit', type=int, default=50)
    return p


def main():
    args = parser().parse_args()
    path = args.db.resolve()
    db = None
    try:
        if args.command == 'init':
            require(not path.exists(), 'init never overwrites an existing database', 'already_exists')
            require(path.parent.is_dir(), 'database parent directory must exist')
            payload = json.loads(sys.stdin.read() if args.input == '-' else Path(args.input).read_text(encoding='utf-8'))
            handle = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(handle)
            db = sqlite3.connect(str(path), isolation_level=None)
            db.row_factory = sqlite3.Row
            try:
                db.execute('PRAGMA foreign_keys=ON')
                db.executescript('BEGIN IMMEDIATE;\n' + SCHEMA.read_text(encoding='utf-8'))
                db.execute('INSERT INTO schema_meta VALUES (?,?,?)', (VERSION, timestamp(), 'Project knowledge baseline'))
                db.commit()
                result = mutate(db, 'entity', payload)
            except Exception:
                db.close()
                db = None
                path.unlink(missing_ok=True)
                raise
        else:
            writable = args.command in {'entity', 'knowledge', 'decision', 'link'}
            db = connect(path, writable)
            if writable:
                payload = json.loads(sys.stdin.read() if args.input == '-' else Path(args.input).read_text(encoding='utf-8'))
                result = mutate(db, args.command, payload)
            elif args.command == 'context':
                decisions = db.execute('''SELECT e.id,e.title,d.scope,d.body,d.authority_ref FROM decisions d
                    JOIN entities e ON e.id=d.entity_id WHERE e.status='accepted' ORDER BY d.scope,e.id''').fetchall()
                knowledge = db.execute('''SELECT e.id,e.title,k.body,k.confidence FROM knowledge k JOIN entities e
                    ON e.id=k.entity_id WHERE e.status='active' AND k.knowledge_type='constraint' ORDER BY e.id''').fetchall()
                result = {'ok': True, 'database': str(path), 'schema_version': VERSION,
                          'accepted_decisions': [dict(v) for v in decisions],
                          'active_constraints': [dict(v) for v in knowledge],
                          'counts': [dict(v) for v in db.execute('SELECT kind,status,count(*) AS count FROM entities GROUP BY kind,status')],
                          'recent_events': [dict(v) for v in db.execute('SELECT seq,action,reason,created_at FROM events ORDER BY seq DESC LIMIT 8')]}
            elif args.command == 'schema':
                result = {'ok': True, 'schema_version': VERSION,
                          'capabilities': {'transport': 'cli', 'mcp_server_implemented': True,
                              'artifact_kinds': sorted(ARTIFACT_KINDS), 'artifact_states': sorted(ARTIFACT_STATES),
                              'knowledge_states': sorted(KNOWLEDGE_STATES), 'decision_states': sorted(DECISION_STATES),
                              'writable_predicates': sorted(PREDICATES), 'query_max_rows': 500,
                              'query_vm_seconds': 5, 'search_mode': 'fts5-unicode61',
                              'receipts': 'events.operation_key/request_hash/result_json'},
                          'objects': [dict(v) for v in db.execute(
                    "SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")]}
            elif args.command == 'query':
                result = read_query(db, args.sql, json.loads(args.params), args.limit)
            elif args.command == 'search':
                require(1 <= args.limit <= 100, 'search limit must be between 1 and 100')
                words = args.text.split()
                require(bool(words), 'search text must not be empty')
                expression = ' OR '.join('"' + word.replace('"', '""') + '"' for word in words)
                rows = db.execute('''SELECT e.id,e.kind,e.title,e.status,e.path,k.knowledge_type,k.confidence,d.scope,
                    snippet(search_index,2,'[',']',' … ',24) AS excerpt,bm25(search_index) AS score
                    FROM search_index JOIN entities e ON e.id=search_index.entity_id
                    LEFT JOIN knowledge k ON k.entity_id=e.id LEFT JOIN decisions d ON d.entity_id=e.id
                    WHERE search_index MATCH ? ''' +
                    ('' if args.include_inactive else "AND e.status IN ('active','accepted','proposed','draft','disputed') ") +
                    'ORDER BY score,e.id LIMIT ?', (expression, args.limit)).fetchall()
                result = {'ok': True, 'rows': [dict(v) for v in rows], 'mode': 'fts5-keywords-unicode61'}
            elif args.command == 'history':
                require(1 <= args.limit <= 500, 'history limit must be between 1 and 500')
                result = {'ok': True, 'events': [dict(v) for v in db.execute('''SELECT ev.* FROM events ev
                    JOIN event_entities ee ON ee.event_seq=ev.seq WHERE ee.entity_id=? ORDER BY ev.seq DESC LIMIT ?''',
                    (args.id, args.limit))]}
            elif args.command == 'check':
                with read_snapshot(db) as replica:
                    result = check(replica)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result.get('ok') else 1
    except (MemoryError, sqlite3.Error, sqlite3.Warning, ValueError, OSError, KeyError, TypeError) as exc:
        print(json.dumps({'ok': False, 'code': getattr(exc, 'code', 'storage_error'), 'error': str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    finally:
        if db:
            db.close()


if __name__ == '__main__':
    sys.exit(main())
