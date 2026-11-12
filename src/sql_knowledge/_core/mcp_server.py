#!/usr/bin/env python3
"""Local stdio MCP adapter. All database operations use the existing CLI service.

No init, daemon, arbitrary SQL writes, model, vectors or scene geometry service.
Limits are adapter safety ceilings, not semantic verification of supplied evidence.
"""
import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from zoneinfo import ZoneInfo
from typing import Annotated, Literal

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import BaseModel, ConfigDict, Field
from . import memory, storage

ROOT = Path(__file__).resolve().parents[2]
CLI = Path(__file__).with_name('memory.py')
MAX_INPUT = 65536
MAX_OUTPUT = 524288
TIMEOUT = 20
Text = Annotated[str, Field(min_length=1, max_length=16384)]
Identifier = Annotated[str, Field(min_length=1, max_length=512)]
Rows = Annotated[int, Field(ge=1, le=100)]
READ = {'readOnlyHint': True, 'destructiveHint': False, 'openWorldHint': False}
WRITE = {'readOnlyHint': False, 'destructiveHint': True, 'idempotentHint': True, 'openWorldHint': False}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)


class EntityData(StrictModel):
    id: Identifier
    kind: Literal['project', 'file', 'document', 'script', 'skill', 'source', 'model', 'building', 'scene', 'experiment', 'report']
    title: Text
    status: Literal['active', 'missing', 'deleted', 'archived', 'draft']
    path: Text | None = None
    description: Text | None = None
    attributes: dict | None = None


class Evidence(BaseModel):
    # Evidence locators/quotes are intentionally extensible, as in the CLI.
    model_config = ConfigDict(extra='allow', strict=True)
    ref: Text


class KnowledgeData(StrictModel):
    id: Identifier
    title: Text
    status: Literal['proposed', 'active', 'disputed', 'rejected']
    knowledge_type: Literal['fact', 'hypothesis', 'finding', 'lesson', 'constraint', 'question']
    body: Text
    confidence: Literal['observed', 'reported', 'inferred', 'unknown']
    evidence: Annotated[list[Evidence], Field(max_length=100)] = []
    supersedes: Identifier | None = None


class DecisionData(StrictModel):
    id: Identifier
    title: Text
    status: Literal['proposed', 'accepted', 'rejected', 'revoked']
    scope: Identifier
    body: Text
    evidence: Annotated[list[Evidence], Field(max_length=100)] = []
    authority_ref: Text | None = None
    supersedes: Identifier | None = None


class RelationData(StrictModel):
    source_id: Identifier
    predicate: Literal['relates_to', 'derived_from', 'supports', 'contradicts', 'implements', 'belongs_to', 'produced_by', 'evidence_for', 'justified_by', 'primary_building']
    target_id: Identifier
    id: Identifier | None = None


class Envelope(StrictModel):
    idempotency_key: Identifier
    actor: Identifier
    reason: Text
    source_ref: Text


class EntityEnvelope(Envelope):
    data: EntityData


class KnowledgeEnvelope(Envelope):
    data: KnowledgeData


class DecisionEnvelope(Envelope):
    data: DecisionData


class RelationEnvelope(Envelope):
    data: RelationData


class StorageFileData(StrictModel):
    id: Identifier
    path: Text
    title: Text
    description: Text
    kind: Literal['file', 'report', 'document', 'model', 'source', 'script'] = 'file'
    retention: Literal['keep', 'disposable'] = 'keep'


class StorageFileEnvelope(Envelope):
    data: StorageFileData


class StorageIDs(StrictModel):
    entity_ids: Annotated[list[Identifier], Field(min_length=1, max_length=100)]


class StorageIDsEnvelope(Envelope):
    data: StorageIDs


class CleanupPlanData(StorageIDs):
    min_age_days: Annotated[int, Field(ge=0)] = 30


class CleanupPlanEnvelope(Envelope):
    data: CleanupPlanData


class CleanupExecuteData(StrictModel):
    plan_id: Identifier
    confirmation_token: Identifier


class CleanupExecuteEnvelope(Envelope):
    data: CleanupExecuteData


def fail(code, message):
    raise ToolError(json.dumps({'ok': False, 'code': code, 'error': message}, ensure_ascii=False))


class Service:
    def __init__(self, database):
        self.database = database.resolve()
        self.lock = threading.Lock()

    def call(self, command, *args, envelope=None):
        payload = json.dumps(envelope, ensure_ascii=False) if envelope is not None else None
        if payload is not None and len(payload.encode()) > MAX_INPUT:
            fail('limit_exceeded', 'write envelope exceeds 65536 UTF-8 bytes')
        argv = [sys.executable, '-B', str(CLI), '--db', str(self.database), command, *args]
        if payload is not None:
            argv += ['--input', '-']
        # Spool large service responses instead of retaining unlimited pipe output.
        # The child is an operation worker, not an additional persistent server.
        temp_dir = ROOT / '.tmp' / dt.datetime.now(ZoneInfo('Europe/Moscow')).date().isoformat() / 'project-knowledge-mcp'
        if self.database.is_relative_to(ROOT / '.tmp'):
            temp_dir = self.database.parent
        temp_dir.mkdir(parents=True, exist_ok=True)
        with self.lock, tempfile.TemporaryFile(dir=temp_dir) as out, tempfile.TemporaryFile(dir=temp_dir) as err:
            try:
                result = subprocess.run(argv, input=payload.encode() if payload else None,
                                        stdout=out, stderr=err, timeout=TIMEOUT)
            except subprocess.TimeoutExpired:
                fail('operation_timeout', '20 second operation limit; a write may have committed. Retry only the identical envelope/idempotency key.')
            stream = out if result.returncode == 0 or (command == 'check' and out.tell()) else err
            if stream.tell() > MAX_OUTPUT:
                fail('response_too_large', 'response exceeds 524288 UTF-8 bytes; request narrower data. No evidence was silently shortened.')
            stream.seek(0)
            raw = stream.read()
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeError):
            fail('service_error', 'CLI returned no valid JSON result')
        if result.returncode and not (command == 'check' and 'problems' in value):
            # check reports findings on stdout even when integrity status is false.
            fail(value.get('code', 'check_failed'), value.get('error', 'CLI check reported problems'))
        return value

    def query(self, sql, params=None, limit=100):
        return self.call('query', '--sql', sql, '--params', json.dumps(params or []), '--limit', str(limit))

    def storage_call(self, action, *args, **kwargs):
        # Cleanup deliberately has no public CLI command. This shares the same
        # typed mutation layer and SQLite event history with the other MCP tools.
        if len(json.dumps([args, kwargs], ensure_ascii=False).encode()) > MAX_INPUT:
            fail('limit_exceeded', 'storage request exceeds 65536 UTF-8 bytes')
        with self.lock:
            db = None
            try:
                db = memory.connect(self.database, writable=action != 'inventory')
                result = getattr(storage, action)(db, *args, **kwargs)
                if len(json.dumps(result, ensure_ascii=False).encode()) > MAX_OUTPUT:
                    fail('response_too_large', 'storage response exceeds 524288 UTF-8 bytes; select fewer IDs. Writes may have committed; preserve the original operation key.')
                return result
            except (memory.MemoryError, OSError, ValueError, KeyError, TypeError, memory.sqlite3.Error) as exc:
                fail(getattr(exc, 'code', 'storage_error'), str(exc))
            finally:
                if db is not None:
                    db.close()


def create_server(database):
    service = Service(database)
    server = FastMCP('sql-knowledge', instructions='Single local SQLite store; use typed envelopes. Evidence tools retrieve records, never invent answers. Read schema_describe for limits.')

    @server.tool(annotations=READ)
    def project_context() -> dict:
        """Load project bootstrap: accepted decisions, active constraints, counts, events."""
        return service.call('context')

    @server.tool(annotations=READ)
    def schema_describe() -> dict:
        """Describe actual schema, transport capabilities and enforced adapter limits."""
        result = service.call('schema')
        result['capabilities'].update(transport='mcp-stdio', mcp_available=True,
            max_input_bytes=MAX_INPUT, max_output_bytes=MAX_OUTPUT, operation_seconds=TIMEOUT,
            page_rows=100, graph_max_depth=5, graph_default_depth=2,
            semantic_search=False, generated_answers=False, scene_geometry=False,
            optimistic_write_revision=True, auto_init=False,
            storage_lifecycle=True, cleanup_entrypoint='MCP only',
            cleanup_max_files=100, cleanup_plan_hours=24, storage_hash_max_bytes=storage.MAX_BYTES,
            storage_operation_seconds=None,
            pagination='catalog_list: after_id plus expected_revision; stale pages rejected')
        result['database'] = str(service.database)
        return result

    @server.tool(annotations=READ)
    def query_readonly(sql: Text, params: list | dict | None = None, limit: Rows = 100) -> dict:
        """Run one bounded read-only SQL statement using the CLI SQLite authorizer."""
        if len(json.dumps(params).encode()) > MAX_INPUT:
            fail('limit_exceeded', 'parameters exceed input ceiling')
        return service.query(sql, params, limit)

    def card(entity_id):
        result = service.query('''SELECT e.*, COALESCE((SELECT max(event_seq) FROM event_entities WHERE entity_id=e.id),0) AS revision, k.knowledge_type,k.body AS knowledge_body,k.confidence,
            k.evidence_json AS knowledge_evidence_json,k.supersedes AS knowledge_supersedes,
            d.scope,d.body AS decision_body,d.authority_ref,d.evidence_json AS decision_evidence_json,
            d.supersedes AS decision_supersedes FROM entities e
            LEFT JOIN knowledge k ON k.entity_id=e.id LEFT JOIN decisions d ON d.entity_id=e.id
            WHERE e.id=?''', [entity_id], 1)
        if not result['rows']:
            fail('not_found', 'entity does not exist')
        record = result['rows'][0]
        issues = service.query('''SELECT DISTINCT e.id,e.path,e.status,e.attributes_json
            FROM entities e JOIN relations r ON
            (r.source_id=? AND r.target_id=e.id) OR (r.target_id=? AND r.source_id=e.id)
            WHERE e.status IN ('missing','deleted') OR
            json_extract(e.attributes_json,'$.storage.availability') IN ('changed','reappeared_unverified','cleanup_pending')''',
            [entity_id, entity_id])
        dependencies = issues['rows']
        for field in ('knowledge_evidence_json', 'decision_evidence_json'):
            for item in json.loads(record.get(field) or '[]'):
                ref = item['ref']
                linked = service.query('''SELECT id,path,status,attributes_json FROM entities
                    WHERE (id=? OR path=?) AND (status IN ('missing','deleted') OR
                    json_extract(attributes_json,'$.storage.availability') IN ('changed','reappeared_unverified','cleanup_pending'))''',
                    [ref.removeprefix('entity:'), ref])
                dependencies.extend(linked['rows'])
        record['availability_issues'] = list({v['id']: v for v in dependencies}.values())
        record['availability_issues_truncated'] = issues['truncated']
        return {'ok': True, 'record': record}

    @server.tool(annotations=READ)
    def catalog_get(entity_id: Identifier) -> dict:
        """Get one complete catalog card including typed details and evidence JSON."""
        return card(entity_id)

    @server.tool(annotations=READ)
    def catalog_list(kind: Identifier | None = None, status: Identifier | None = None,
                     limit: Rows = 100, after_id: Identifier | None = None,
                     expected_revision: Annotated[int, Field(ge=0)] | None = None) -> dict:
        """List by ID. Continue with after_id and returned revision; changes reject paging."""
        def revision():
            return service.query('SELECT coalesce(max(seq),0) AS revision FROM events', limit=1)['rows'][0]['revision']
        before = revision()
        if (after_id is not None and expected_revision is None) or (expected_revision is not None and before != expected_revision):
            fail('stale_page', 'continuation requires the unchanged expected_revision and the same filters')
        result = service.query('''SELECT * FROM entities WHERE (? IS NULL OR kind=?)
            AND (? IS NULL OR status=?) AND (? IS NULL OR id>?) ORDER BY id''',
            [kind, kind, status, status, after_id, after_id], limit)
        if revision() != before:
            fail('stale_page', 'database changed during page read; restart pagination')
        result.update(revision=before, next_after_id=result['rows'][-1]['id'] if result['truncated'] else None)
        return result

    @server.tool(annotations=READ)
    def history_list(entity_id: Identifier, limit: Rows = 100) -> dict:
        """Get latest entity events, newest first; explicit bounded history window."""
        return service.call('history', '--id', entity_id, '--limit', str(limit))

    @server.tool(annotations=READ)
    def relations_walk(entity_id: Identifier, depth: Annotated[int, Field(ge=1, le=5)] = 2,
                       direction: Literal['outgoing', 'incoming', 'both'] = 'both', limit: Rows = 100) -> dict:
        """Bounded graph edge retrieval; cycles terminate by node/depth UNION and depth limit."""
        card(entity_id)
        join = {'outgoing': 'r.source_id=w.id', 'incoming': 'r.target_id=w.id',
                'both': '(r.source_id=w.id OR r.target_id=w.id)'}[direction]
        target = 'r.target_id' if direction == 'outgoing' else 'r.source_id' if direction == 'incoming' else 'CASE WHEN r.source_id=w.id THEN r.target_id ELSE r.source_id END'
        result = service.query(f'''WITH RECURSIVE walk(id,depth) AS (
            SELECT ?,0 UNION SELECT {target},w.depth+1 FROM relations r JOIN walk w ON {join}
            WHERE w.depth<?) SELECT r.*,min(w.depth)+1 AS traversal_depth
            FROM relations r JOIN walk w ON {join} WHERE w.depth<? GROUP BY r.id
            ORDER BY traversal_depth,r.id''', [entity_id, depth, depth], limit)
        result.update(depth=depth, direction=direction)
        return result

    @server.tool(annotations=READ)
    def search_query(text: Text, limit: Rows = 20, include_inactive: bool = False) -> dict:
        """FTS5 keyword search. Snippets are locators; retrieve full cards for evidence."""
        return service.call('search', '--text', text, '--limit', str(limit), *(['--include-inactive'] if include_inactive else []))

    @server.tool(annotations=READ)
    def answers_evidence(entity_ids: Annotated[list[Identifier], Field(min_length=1, max_length=20)]) -> dict:
        """Retrieve full records with IDs, epistemic types and stored provenance, without generating claims."""
        result = {'ok': True, 'records': [card(i)['record'] for i in entity_ids],
                  'mode': 'stored-evidence-only', 'limitation': 'Stored evidence is not independent source verification.'}
        if len(json.dumps(result, ensure_ascii=False).encode()) > MAX_OUTPUT:
            fail('response_too_large', 'request fewer records; no evidence was silently shortened')
        return result

    def subject(entity_id, kind):
        result = card(entity_id)
        if result['record']['kind'] != kind:
            fail('wrong_kind', 'entity must have kind ' + kind)
        result['representation'] = 'v1 entity card and attributes; no geometry or separate domain tables'
        return result

    @server.tool(annotations=READ)
    def buildings_get(entity_id: Identifier) -> dict:
        """Get a v1 building card; use relations_walk for sources/models/knowledge."""
        return subject(entity_id, 'building')

    @server.tool(annotations=READ)
    def scenes_get(entity_id: Identifier) -> dict:
        """Get a v1 scene card; this does not inspect Blender geometry."""
        return subject(entity_id, 'scene')

    @server.tool(annotations=WRITE)
    def entity_write(envelope: EntityEnvelope) -> dict:
        """Typed catalog upsert through CLI, with transactional event and idempotency receipt."""
        return service.call('entity', envelope=envelope.model_dump(exclude_unset=True))

    @server.tool(annotations=WRITE)
    def knowledge_write(envelope: KnowledgeEnvelope) -> dict:
        """Typed knowledge write; active assertions require durable evidence. CLI validates lifecycle."""
        return service.call('knowledge', envelope=envelope.model_dump(exclude_unset=True))

    @server.tool(annotations=WRITE)
    def decision_write(envelope: DecisionEnvelope) -> dict:
        """Typed decision write; accepted decisions require evidence and authority_ref."""
        return service.call('decision', envelope=envelope.model_dump(exclude_unset=True))

    @server.tool(annotations=dict(WRITE, destructiveHint=False))
    def relation_write(envelope: RelationEnvelope) -> dict:
        """Typed link write using existing FK/cardinality checks; no arbitrary SQL."""
        return service.call('link', envelope=envelope.model_dump(exclude_unset=True))

    @server.tool(annotations=READ)
    def project_check() -> dict:
        """Check integrity, history, FTS and active paths; report without modifying statuses."""
        return service.call('check')

    @server.tool(annotations=READ)
    def storage_inventory(scope: Text, after_path: Text | None = None, limit: Rows = 100) -> dict:
        """List files under .tmp/.report/experiments, including unregistered paths. No content import."""
        return service.storage_call('inventory', scope, after_path, limit)

    @server.tool(annotations=WRITE)
    def storage_register(envelope: StorageFileEnvelope) -> dict:
        """Catalog a useful file with purpose, fingerprint and keep/disposable retention; default keep."""
        return service.storage_call('register', {k:v for k,v in envelope.model_dump().items() if k not in {'expected_revision','expected_revisions'} or v is not None})

    @server.tool(annotations=WRITE)
    def storage_reconcile(envelope: StorageIDsEnvelope) -> dict:
        """Reconcile selected catalog paths. Missing files retain knowledge/history; no files deleted."""
        return service.storage_call('reconcile', {k:v for k,v in envelope.model_dump().items() if k not in {'expected_revision','expected_revisions'} or v is not None})

    @server.tool(annotations=dict(WRITE, destructiveHint=False))
    def storage_cleanup_plan(envelope: CleanupPlanEnvelope) -> dict:
        """Persist exact candidates/bytes/token for selected disposable files older than min_age_days. No deletion."""
        return service.storage_call('cleanup_plan', {k:v for k,v in envelope.model_dump().items() if k not in {'expected_revision','expected_revisions'} or v is not None})

    @server.tool(annotations=WRITE)
    def storage_cleanup_execute(envelope: CleanupExecuteEnvelope) -> dict:
        """Delete only reviewed plan files, checking token, expiry, retention and hashes; journal each result."""
        return service.storage_call('cleanup_execute', {k:v for k,v in envelope.model_dump().items() if k not in {'expected_revision','expected_revisions'} or v is not None})

    return server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, default=ROOT / 'knowledge.sqlite')
    args = parser.parse_args()
    create_server(args.db).run(transport='stdio')


if __name__ == '__main__':
    main()
