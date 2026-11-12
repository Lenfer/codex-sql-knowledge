#!/usr/bin/env python3
"""Real SDK stdio integration tests. Never writes the working database.

Run: scripts/project-knowledge/.venv/bin/python -B scripts/project-knowledge/test_mcp.py
"""
import json
import asyncio
import functools
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent


def envelope(key, data):
    return {'idempotency_key': key, 'actor': 'test/mcp', 'reason': 'Isolated MCP integration check',
            'source_ref': 'test:explicit-fixture', 'data': data}


def session_test(method):
    @functools.wraps(method)
    def run(self):
        async def scenario():
            await self.asyncSetUp()
            try:
                await method(self)
            finally:
                await self.asyncTearDown()
        asyncio.run(scenario())
    return run


class MCPIntegration(unittest.TestCase):
    async def asyncSetUp(self):
        base = ROOT / '.tmp/2026-10-28/mcp-implementation'
        base.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='case-', dir=base)
        self.db = Path(self.temp.name) / 'test.sqlite'
        payload = envelope('init', {'id': 'project', 'kind': 'project', 'title': 'Test project', 'status': 'active'})
        result = subprocess.run([sys.executable, '-B', str(HERE / 'memory.py'), '--db', str(self.db),
                                 'init', '--input', '-'], input=json.dumps(payload), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.transport = stdio_client(StdioServerParameters(command=sys.executable,
            args=['-B', str(HERE / 'mcp_server.py'), '--db', str(self.db)], cwd=str(ROOT)))
        reader, writer = await self.transport.__aenter__()
        self.session_context = ClientSession(reader, writer)
        self.client = await self.session_context.__aenter__()
        self.initialized = await self.client.initialize()

    async def asyncTearDown(self):
        await self.session_context.__aexit__(None, None, None)
        await self.transport.__aexit__(None, None, None)
        self.temp.cleanup()

    async def call(self, name, arguments=None, error=False):
        result = await self.client.call_tool(name, arguments or {})
        self.assertEqual(bool(result.isError), error, str(result))
        if error:
            return '\n'.join(v.text for v in result.content if hasattr(v, 'text'))
        return result.structuredContent or json.loads(result.content[0].text)

    @session_test
    async def test_initialization_read_tools_and_boundaries(self):
        tools = {v.name: v for v in (await self.client.list_tools()).tools}
        self.assertEqual(len(tools), 21)
        self.assertTrue(tools['project_context'].annotations.readOnlyHint)
        self.assertFalse(tools['knowledge_write'].annotations.readOnlyHint)
        self.assertNotIn('project_export', tools)
        self.assertNotIn('init', tools)
        self.assertEqual((await self.call('schema_describe'))['database'], str(self.db))
        self.assertEqual((await self.call('project_context'))['schema_version'], 1)
        self.assertEqual((await self.call('catalog_get', {'entity_id': 'project'}))['record']['kind'], 'project')
        await self.call('catalog_get', {'entity_id': 'absent'}, error=True)
        await self.call('buildings_get', {'entity_id': 'project'}, error=True)
        for sql in ('DELETE FROM entities', "ATTACH ':memory:' AS x", 'PRAGMA user_version=2', 'SELECT 1; SELECT 2'):
            await self.call('query_readonly', {'sql': sql}, error=True)
        await self.call('query_readonly', {'sql': 'SELECT 1', 'limit': 101}, error=True)
        await self.call('relations_walk', {'entity_id': 'project', 'depth': 6}, error=True)
        result = await self.call('query_readonly', {'sql': 'SELECT 1 AS n UNION ALL SELECT 2', 'limit': 1})
        self.assertTrue(result['truncated'])
        self.assertTrue((await self.call('project_check'))['ok'])

    @session_test
    async def test_typed_writes_replay_authority_graph_evidence_paging_export(self):
        for entity_id, kind in [('building', 'building'), ('scene', 'scene')]:
            payload = envelope(entity_id, {'id': entity_id, 'kind': kind, 'title': entity_id, 'status': 'active'})
            self.assertFalse((await self.call('entity_write', {'envelope': payload}))['replayed'])
            self.assertTrue((await self.call('entity_write', {'envelope': payload}))['replayed'])
            payload['data']['title'] = 'Conflict'
            self.assertIn('idempotency_conflict', await self.call('entity_write', {'envelope': payload}, error=True))
        self.assertEqual((await self.call('buildings_get', {'entity_id': 'building'}))['record']['id'], 'building')
        self.assertEqual((await self.call('scenes_get', {'entity_id': 'scene'}))['record']['id'], 'scene')
        rejected = envelope('authority-rejected', {'id': 'rejected', 'title': 'No authority', 'scope': 'project',
            'status': 'accepted', 'body': 'Unapproved', 'evidence': [{'ref': 'test:claim'}]})
        await self.call('decision_write', {'envelope': rejected}, error=True)
        self.assertEqual((await self.call('query_readonly', {'sql': "SELECT id FROM entities WHERE id='rejected'"}))['rows'], [])
        rejected['idempotency_key'] = 'authority-accepted'
        rejected['data'].update(id='approved', authority_ref='test:user-authority')
        await self.call('decision_write', {'envelope': rejected})
        claim = envelope('knowledge', {'id': 'knowledge', 'title': 'Facade finding', 'status': 'active',
            'knowledge_type': 'hypothesis', 'body': 'Facade may be symmetric', 'confidence': 'inferred',
            'evidence': [{'ref': 'test:photo', 'locator': 'north facade'}]})
        await self.call('knowledge_write', {'envelope': claim})
        await self.call('relation_write', {'envelope': envelope('link', {'source_id': 'knowledge',
            'predicate': 'primary_building', 'target_id': 'building'})})
        edges = await self.call('relations_walk', {'entity_id': 'building', 'depth': 2})
        self.assertEqual(edges['rows'][0]['source_id'], 'knowledge')
        self.assertEqual((await self.call('search_query', {'text': 'Facade'}))['rows'][0]['id'], 'knowledge')
        evidence = await self.call('answers_evidence', {'entity_ids': ['knowledge', 'approved']})
        self.assertEqual(evidence['records'][0]['confidence'], 'inferred')
        self.assertIn('test:photo', evidence['records'][0]['knowledge_evidence_json'])
        page = await self.call('catalog_list', {'limit': 1})
        self.assertTrue(page['truncated'])
        await self.call('catalog_list', {'after_id': page['next_after_id']}, error=True)
        continuation = await self.call('catalog_list', {'limit': 1, 'after_id': page['next_after_id'], 'expected_revision': page['revision']})
        self.assertNotEqual(page['rows'][0]['id'], continuation['rows'][0]['id'])
        self.assertEqual(len((await self.call('history_list', {'entity_id': 'knowledge'}))['events']), 2)
        self.assertTrue((await self.call('project_check'))['ok'])
        claim['idempotency_key'] = 'update'
        claim['data']['status'] = 'disputed'
        await self.call('knowledge_write', {'envelope': claim})
        await self.call('catalog_list', {'after_id': page['next_after_id'], 'expected_revision': page['revision']}, error=True)

    @session_test
    async def test_storage_lifecycle_through_stdio(self):
        file = Path(self.temp.name) / 'useful.txt'
        file.write_text('Expensive result')
        path = file.relative_to(ROOT).as_posix()
        payload = envelope('register-file', {'id': 'useful', 'path': path, 'title': 'Useful result',
            'description': 'Isolated test result; purpose retained in SQLite', 'retention': 'disposable'})
        await self.call('storage_register', {'envelope': payload})
        self.assertTrue((await self.call('storage_register', {'envelope': payload}))['replayed'])
        inventory = await self.call('storage_inventory', {'scope': Path(self.temp.name).relative_to(ROOT).as_posix()})
        self.assertIn('useful', [v['id'] for v in inventory['files']])
        plan_payload = envelope('plan', {'entity_ids': ['useful'], 'min_age_days': 0})
        plan = await self.call('storage_cleanup_plan', {'envelope': plan_payload})
        self.assertEqual(plan['total_bytes'], len('Expensive result'))
        self.assertTrue(file.exists())
        execute = envelope('execute', {'plan_id': plan['plan_id'], 'confirmation_token': plan['confirmation_token']})
        wrong = envelope('wrong-token', dict(execute['data'], confirmation_token='wrong'))
        await self.call('storage_cleanup_execute', {'envelope': wrong}, error=True)
        result = await self.call('storage_cleanup_execute', {'envelope': execute})
        self.assertEqual(result['deleted_ids'], ['useful'])
        self.assertFalse(file.exists())
        self.assertTrue((await self.call('storage_cleanup_execute', {'envelope': execute}))['replayed'])
        self.assertEqual((await self.call('catalog_get', {'entity_id': 'useful'}))['record']['status'], 'deleted')
        file.write_text('A new file at the same path')
        await self.call('storage_reconcile', {'envelope': envelope('reappeared', {'entity_ids': ['useful']})})
        record = (await self.call('catalog_get', {'entity_id': 'useful'}))['record']
        self.assertEqual(json.loads(record['attributes_json'])['storage']['availability'], 'reappeared_unverified')
        payload['idempotency_key'] = 'reregister'
        payload['data']['retention'] = 'keep'
        await self.call('storage_register', {'envelope': payload})
        await self.call('knowledge_write', {'envelope': envelope('finding', {'id': 'finding', 'title': 'Finding',
            'status': 'active', 'knowledge_type': 'finding', 'body': 'Preserved result of verification',
            'confidence': 'observed', 'evidence': [{'ref': 'entity:useful'}]})})
        file.unlink()  # Explicit simulation of a manual external deletion.
        await self.call('storage_reconcile', {'envelope': envelope('manual-loss', {'entity_ids': ['useful']})})
        record = (await self.call('catalog_get', {'entity_id': 'finding'}))['record']
        self.assertEqual(record['status'], 'active')
        self.assertEqual(record['availability_issues'][0]['id'], 'useful')
        self.assertEqual(record['availability_issues'][0]['status'], 'missing')
        self.assertTrue((await self.call('project_check'))['ok'])

    @session_test
    async def test_response_limits_and_check_reports(self):
        await self.call('query_readonly', {'sql': 'SELECT hex(zeroblob(300000)) AS large'}, error=True)
        payload = envelope('oversized', {'id': 'big', 'kind': 'file', 'title': 'Big', 'status': 'draft',
                                       'attributes': {'body': 'x' * 65536}})
        await self.call('entity_write', {'envelope': payload}, error=True)
        payload = envelope('missing', {'id': 'missing', 'kind': 'file', 'title': 'Missing file', 'status': 'active',
                                    'path': 'mcp-test-deliberately-absent.file'})
        await self.call('entity_write', {'envelope': payload})
        report = await self.call('project_check')
        self.assertFalse(report['ok'])
        self.assertEqual(report['problems'][0]['kind'], 'missing_file')
        absent = Path(self.temp.name) / 'absent.sqlite'
        async with stdio_client(StdioServerParameters(command=sys.executable,
                args=['-B', str(HERE / 'mcp_server.py'), '--db', str(absent)])) as (reader, writer):
            async with ClientSession(reader, writer) as session:
                await session.initialize()
                result = await session.call_tool('project_context')
                self.assertTrue(result.isError)
                self.assertIn('database_missing', result.content[0].text)
        self.assertFalse(absent.exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
