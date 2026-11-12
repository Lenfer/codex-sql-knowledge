#!/usr/bin/env python3
"""Behavioral checks in disposable, explicitly selected SQLite databases."""
import datetime as dt
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
CLI = Path(__file__).with_name('memory.py')


class KnowledgeWorkflow(unittest.TestCase):
    def setUp(self):
        day = dt.datetime.now(ZoneInfo('Europe/Moscow')).date().isoformat()
        base = ROOT / '.tmp' / day / 'project-knowledge-authoring' / 'tests'
        base.mkdir(parents=True, exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(prefix='case-', dir=base)
        self.path = Path(self.directory.name) / 'test.sqlite'
        self.seq = 0
        self.run_cli('init', data={'id': 'project', 'kind': 'project', 'title': 'Test project', 'status': 'active'})

    def tearDown(self):
        self.directory.cleanup()

    def run_cli(self, command, *args, data=None, key=None, succeeds=True):
        argv = [sys.executable, '-B', str(CLI), '--db', str(self.path), command, *args]
        payload = None
        if data is not None:
            self.seq += 1
            payload = json.dumps({'idempotency_key': key or 'op-' + str(self.seq), 'actor': 'test',
                                  'reason': 'isolated behavioral check', 'source_ref': 'test:explicit-input', 'data': data})
            argv.extend(['--input', '-'])
        result = subprocess.run(argv, input=payload, text=True, capture_output=True)
        self.assertEqual(result.returncode == 0, succeeds, result.stdout + result.stderr)
        return json.loads(result.stdout.strip() or result.stderr.strip())

    def decision(self, entity_id, **changes):
        data = {'id': entity_id, 'title': 'Decision ' + entity_id, 'status': 'accepted', 'scope': 'project',
                'body': 'Authorized test rule ' + entity_id, 'authority_ref': 'test:user-confirmation',
                'evidence': [{'ref': 'test:user-confirmation', 'quote': 'Apply this rule.'}]}
        data.update(changes)
        return data

    def test_idempotency_and_conflict(self):
        data = {'id': 'artifact', 'kind': 'file', 'title': 'Artifact', 'status': 'draft'}
        first = self.run_cli('entity', data=data, key='same')
        self.assertFalse(first['replayed'])
        self.assertTrue(self.run_cli('entity', data=data, key='same')['replayed'])
        data['title'] = 'Different'
        self.assertEqual(self.run_cli('entity', data=data, key='same', succeeds=False)['code'], 'idempotency_conflict')
        self.assertEqual(self.run_cli('check')['events'], 2)

    def test_authority_required_and_atomic_failure(self):
        data = self.decision('unapproved', authority_ref=None)
        self.run_cli('decision', data=data, succeeds=False)
        rows = self.run_cli('query', '--sql', "SELECT * FROM entities WHERE id='unapproved'")['rows']
        self.assertEqual(rows, [])
        self.assertEqual(self.run_cli('check')['events'], 1)

    def test_replace_decision_then_revoke(self):
        self.run_cli('decision', data=self.decision('old'))
        replacement = self.decision('new', supersedes='old')
        self.run_cli('decision', data=replacement)
        rows = self.run_cli('query', '--sql', "SELECT id,status FROM entities WHERE kind='decision' ORDER BY id")['rows']
        self.assertEqual(rows, [{'id': 'new', 'status': 'accepted'}, {'id': 'old', 'status': 'superseded'}])
        self.assertEqual(len(self.run_cli('history', '--id', 'old')['events']), 2)
        replacement['status'] = 'revoked'
        self.run_cli('decision', data=replacement)
        self.run_cli('decision', data=self.decision('old'), succeeds=False)
        replacement['status'] = 'accepted'
        self.run_cli('decision', data=replacement, succeeds=False)
        self.run_cli('check')

    def test_accepted_scope_and_downgrade_cannot_bypass_replacement(self):
        self.run_cli('decision', data=self.decision('immutable'))
        self.run_cli('decision', data=self.decision('immutable', scope='expanded unauthorized scope'), succeeds=False)
        self.run_cli('decision', data=self.decision('immutable', status='proposed', evidence=[], authority_ref=None), succeeds=False)
        row = self.run_cli('query', '--sql', "SELECT scope FROM decisions WHERE entity_id='immutable'")['rows'][0]
        self.assertEqual(row['scope'], 'project')
        self.run_cli('check')

    def test_cardinality_fk_and_rollback(self):
        for entity_id, kind in [('model', 'model'), ('b1', 'building'), ('b2', 'building')]:
            self.run_cli('entity', data={'id': entity_id, 'kind': kind, 'title': entity_id, 'status': 'active'})
        link = {'source_id': 'model', 'predicate': 'primary_building', 'target_id': 'b1'}
        self.run_cli('link', data=link)
        before = self.run_cli('check')['events']
        link['target_id'] = 'b2'
        self.run_cli('link', data=link, succeeds=False)
        link['target_id'] = 'does-not-exist'
        self.run_cli('link', data=link, succeeds=False)
        self.assertEqual(self.run_cli('check')['events'], before)

    def test_missing_then_deleted_keeps_history(self):
        data = {'id': 'gone', 'kind': 'file', 'title': 'Gone file', 'status': 'active', 'path': 'nonexistent-test-artifact.xyz'}
        self.run_cli('entity', data=data)
        problem = self.run_cli('check', succeeds=False)['problems'][0]
        self.assertEqual(problem['kind'], 'missing_file')
        rows = self.run_cli('query', '--sql', "SELECT status FROM entities WHERE id='gone'")['rows']
        self.assertEqual(rows[0]['status'], 'active')
        data['status'] = 'deleted'
        self.run_cli('entity', data=data)
        self.assertEqual(len(self.run_cli('history', '--id', 'gone')['events']), 2)
        self.run_cli('check')

    def test_hypothesis_is_not_silently_promoted(self):
        data = {'id': 'k1', 'title': 'Проверка фасада', 'status': 'active', 'body': 'Возможно, фасад симметричен.',
                'knowledge_type': 'hypothesis', 'confidence': 'inferred', 'evidence': [{'ref': 'test:photo-analysis'}]}
        self.run_cli('knowledge', data=data)
        self.assertEqual(self.run_cli('search', '--text', 'ФАСАДА')['rows'][0]['id'], 'k1')
        data['knowledge_type'] = 'fact'
        self.run_cli('knowledge', data=data, succeeds=False)
        data.update(id='k2', title='Проверенный фасад', body='На снимке видны симметричные проёмы.',
                    confidence='observed', supersedes='k1')
        self.run_cli('knowledge', data=data)
        self.run_cli('check')

    def test_temporary_catalog_allowed_but_provenance_requires_durable_ref(self):
        data = {'id': 'temp', 'kind': 'file', 'title': 'Temporary', 'status': 'draft', 'path': '.tmp/2026-10-28/a.html'}
        self.run_cli('entity', data=data)
        data = {'id': 'k', 'title': 'Ephemeral claim', 'status': 'active', 'body': 'Claim',
                'knowledge_type': 'fact', 'confidence': 'observed', 'evidence': [{'ref': '.report/2026-10-28/a.pdf'}]}
        self.run_cli('knowledge', data=data, succeeds=False)
        self.assertEqual(self.run_cli('check')['events'], 2)

    def test_read_only_sql_and_limits(self):
        for sql in ("DELETE FROM entities", "ATTACH ':memory:' AS other", 'PRAGMA user_version=2',
                    'SELECT 1; SELECT 2', "SELECT load_extension('missing')"):
            self.run_cli('query', '--sql', sql, succeeds=False)
        rows = self.run_cli('query', '--sql', 'WITH x AS (SELECT 1 AS v UNION ALL SELECT 2) SELECT * FROM x', '--limit', '1')
        self.assertTrue(rows['truncated'])
        self.run_cli('check')

    def test_events_are_append_only_and_external_changes_detected(self):
        with sqlite3.connect(self.path) as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM events')
            db.execute("UPDATE entities SET title='Unlogged external edit' WHERE id='project'")
        problems = self.run_cli('check', succeeds=False)['problems']
        self.assertIn('history_mismatch', {v['kind'] for v in problems})

    def test_binary_copy_and_no_implicit_init(self):
        self.run_cli('decision', data=self.decision('saved'))
        self.run_cli('decision', data=self.decision('z-old'))
        self.run_cli('decision', data=self.decision('a-new', supersedes='z-old'))
        # SQL export is intentionally absent, even when explicitly requested.
        rejected = subprocess.run([sys.executable, str(CLI), '--db', str(self.path), 'export'], capture_output=True, text=True)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn('invalid choice', rejected.stderr)
        self.assertFalse(list(Path(self.directory.name).glob('*.sql')))
        restored = Path(self.directory.name) / 'restored.sqlite'
        with sqlite3.connect(self.path) as source, sqlite3.connect(restored) as target:
            source.backup(target)
        original = self.path
        self.path = restored
        self.assertEqual(self.run_cli('check')['events'], 4)
        self.assertIn('saved', {v['id'] for v in self.run_cli('search', '--text', 'Authorized')['rows']})
        self.assertEqual(self.run_cli('query', '--sql', "SELECT supersedes FROM decisions WHERE entity_id='a-new'")['rows'][0]['supersedes'], 'z-old')
        self.run_cli('init', data={'id': 'never', 'kind': 'project', 'title': 'Never', 'status': 'active'}, succeeds=False)
        self.path = Path(self.directory.name) / 'absent.sqlite'
        self.run_cli('context', succeeds=False)
        self.assertFalse(self.path.exists())
        self.path = original


if __name__ == '__main__':
    unittest.main(verbosity=2)
