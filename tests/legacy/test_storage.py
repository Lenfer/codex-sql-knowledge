#!/usr/bin/env python3
"""Destructive lifecycle checks on isolated files/DB, never the working catalog."""
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import memory as m
import storage as s


class StorageLifecycle(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='knowledge-lifecycle-')
        self.root = Path(self.temp.name).resolve()
        self.patch = patch.object(m, 'ROOT', self.root)
        self.patch.start()
        self.db = sqlite3.connect(self.root / 'test.sqlite')
        self.db.row_factory = sqlite3.Row
        self.db.executescript(m.SCHEMA.read_text())
        self.db.execute('INSERT INTO schema_meta VALUES (1,?,?)', (m.timestamp(), 'Isolated fixture'))
        self.db.commit()
        self.n = 0

    def tearDown(self):
        self.db.close()
        self.patch.stop()
        self.temp.cleanup()

    def env(self, data, key=None):
        self.n += 1
        return {'actor': 'test', 'source_ref': 'test:fixture', 'reason': 'Isolated behavior check',
                'idempotency_key': key or 'test-' + str(self.n), 'data': data}

    def file(self, name='a.txt', retention='disposable', scope='.tmp/2026-10-28'):
        path = scope + '/' + name
        file = self.root / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text('Costly generated data ' + name)
        s.register(self.db, self.env({'id': name, 'path': path, 'title': name,
            'description': 'Generated result with durable purpose', 'retention': retention}))
        return file

    def plan(self, *ids, age=0):
        return s.cleanup_plan(self.db, self.env({'entity_ids': list(ids), 'min_age_days': age}))

    def execute_env(self, plan):
        return self.env({'plan_id': plan['plan_id'], 'confirmation_token': plan['confirmation_token']})

    def test_keep_age_and_experiments(self):
        a = self.file(retention='keep')
        b = self.file('b.txt', scope='experiments/2026-10-28_23__fixture/output')
        plan = self.plan('a.txt', 'b.txt', age=30)
        self.assertEqual(plan['candidates'], [])
        self.assertEqual(len(plan['excluded']), 2)
        plan = self.plan('a.txt', 'b.txt')
        self.assertEqual([v['id'] for v in plan['candidates']], ['b.txt'])
        s.cleanup_execute(self.db, self.execute_env(plan))
        self.assertTrue(a.exists())
        self.assertFalse(b.exists())
        self.assertTrue(m.check(self.db)['ok'])

    def test_changed_batch_never_starts_deletion(self):
        a, b = self.file(), self.file('b.txt')
        plan = self.plan('a.txt', 'b.txt')
        b.write_text('Changed since planning')
        with self.assertRaises(m.MemoryError):
            s.cleanup_execute(self.db, self.execute_env(plan))
        self.assertTrue(a.exists())
        self.assertTrue(b.exists())

    def test_repin_invalidates_plan(self):
        file = self.file()
        plan = self.plan('a.txt')
        s.register(self.db, self.env({'id': 'a.txt', 'path': file.relative_to(self.root).as_posix(),
            'title': 'Keep it', 'description': 'Now valuable', 'retention': 'keep'}))
        with self.assertRaises(m.MemoryError):
            s.cleanup_execute(self.db, self.execute_env(plan))
        self.assertTrue(file.exists())

    def test_symlink_and_outside_paths_refused(self):
        file = self.file()
        plan = self.plan('a.txt')
        original = self.root / 'outside.txt'
        original.write_text('Do not delete')
        file.unlink()
        file.symlink_to(original)
        with self.assertRaises((m.MemoryError, OSError)):
            s.cleanup_execute(self.db, self.execute_env(plan))
        self.assertTrue(original.exists())
        for path in ('../outside', 'source/a/b.txt', '.tmp/invalid/file', '.tmp/2026-10-28/test.sqlite'):
            with self.assertRaises((m.MemoryError, ValueError)):
                s.managed_path(path)
        file.unlink()
        directory = file.parent
        moved = directory.with_name('moved')
        directory.rename(moved)
        directory.symlink_to(moved, target_is_directory=True)
        with self.assertRaises(OSError):
            s.fingerprint(file.relative_to(self.root).as_posix())

    def test_manual_loss_history_and_reappearance(self):
        file = self.file()
        file.unlink()
        env = self.env({'entity_ids': ['a.txt']})
        self.assertEqual(s.reconcile(self.db, env)['records'][0]['availability'], 'missing')
        self.assertTrue(s.reconcile(self.db, env)['replayed'])
        card = m.row(self.db, 'entities', 'id', 'a.txt')
        self.assertEqual(card['status'], 'missing')
        self.assertIn('Generated result', card['description'])
        file.write_text('Other file')
        self.assertEqual(s.reconcile(self.db, self.env({'entity_ids': ['a.txt']}))['records'][0]['availability'],
                         'reappeared_unverified')
        self.assertTrue(m.check(self.db)['ok'])

    def test_same_key_conflict_and_replay(self):
        file = self.file()
        plan = self.plan('a.txt')
        env = self.execute_env(plan)
        self.assertEqual(s.cleanup_execute(self.db, env)['deleted_ids'], ['a.txt'])
        self.assertTrue(s.cleanup_execute(self.db, env)['replayed'])
        env['data']['confirmation_token'] = 'another'
        with self.assertRaises(m.MemoryError):
            s.cleanup_execute(self.db, env)
        self.assertFalse(file.exists())

    def test_interrupted_after_unlink_recovery_keeps_uncertainty(self):
        file = self.file()
        plan = self.plan('a.txt')
        env = self.execute_env(plan)
        original_write = s.write
        def failure(db, envelope, suffix, data, transaction=True, entity_id=None):
            if suffix == 'result':
                raise OSError('simulated crash after unlink before commit')
            return original_write(db, envelope, suffix, data, transaction, entity_id=entity_id)
        with patch.object(s, 'write', side_effect=failure):
            with self.assertRaises(OSError):
                s.cleanup_execute(self.db, env)
        self.assertFalse(file.exists())
        result = s.cleanup_execute(self.db, env)
        self.assertEqual(result['missing_ids'], ['a.txt'])
        self.assertEqual(result['deleted_ids'], [])
        self.assertTrue(m.check(self.db)['ok'])

    def test_missing_experiment_directory_can_be_reconciled(self):
        data = {'id': 'experiment', 'kind': 'experiment', 'title': 'Absent experiment',
                'status': 'active', 'path': 'experiments/2026-10-28_23__absent'}
        m.mutate(self.db, 'entity', self.env(data))
        result = s.reconcile(self.db, self.env({'entity_ids': ['experiment']}))
        self.assertEqual(result['records'][0]['availability'], 'missing')
        self.assertTrue(m.check(self.db)['ok'])

    def test_registration_can_resume_after_receipt_failure(self):
        file = self.root / '.report/2026-10-28/result.txt'
        file.parent.mkdir(parents=True)
        file.write_text('Saved expensive result')
        env = self.env({'id': 'result', 'path': file.relative_to(self.root).as_posix(),
                        'title': 'Result', 'description': 'Purpose'})
        with patch.object(s, 'finish', side_effect=OSError('simulated interruption')):
            with self.assertRaises(OSError):
                s.register(self.db, env)
        file.unlink()
        result = s.register(self.db, env)
        self.assertEqual(result['id'], 'result')
        self.assertTrue(s.register(self.db, env)['replayed'])
        self.assertEqual(m.row(self.db, 'entities', 'id', 'result')['status'], 'active')
        s.reconcile(self.db, self.env({'entity_ids': ['result']}))
        self.assertTrue(m.check(self.db)['ok'])

    def test_inventory_pages_and_symlink_exclusion(self):
        self.file('a.txt')
        self.file('b.txt')
        path = self.root / '.tmp/2026-10-28/link.txt'
        path.symlink_to(self.root / '.tmp/2026-10-28/a.txt')
        first = s.inventory(self.db, '.tmp', limit=1)
        self.assertTrue(first['truncated'])
        second = s.inventory(self.db, '.tmp', after_path=first['next_after_path'], limit=1)
        self.assertEqual(second['files'][0]['id'], 'b.txt')
        self.assertFalse(second['truncated'])
        with self.assertRaises(m.MemoryError):
            s.inventory(self.db, '../')

    def test_expired_plan_does_not_delete(self):
        file = self.file()
        plan = self.plan('a.txt')
        card = m.row(self.db, 'entities', 'id', plan['plan_id'])
        data = s.card_data(card)
        data['attributes']['expires_at'] = '2000-01-01T00:00:00+00:00'
        m.mutate(self.db, 'entity', self.env(data))
        with self.assertRaises(m.MemoryError):
            s.cleanup_execute(self.db, self.execute_env(plan))
        self.assertTrue(file.exists())

    def test_binary_backup_retains_loss_and_receipts(self):
        file = self.file()
        file.unlink()
        s.reconcile(self.db, self.env({'entity_ids': ['a.txt']}))
        restored = sqlite3.connect(self.root / 'restored.sqlite')
        self.db.backup(restored)
        restored.row_factory = sqlite3.Row
        self.assertEqual(m.row(restored, 'entities', 'id', 'a.txt')['status'], 'missing')
        self.assertTrue(m.check(restored)['ok'])
        restored.close()


if __name__ == '__main__':
    unittest.main(verbosity=2)
