"""Regression: runtime-created audit paths can be catalogued, never arbitrary roots."""
from pathlib import Path
import unittest
from unittest.mock import patch
from sql_knowledge._core import memory, compressed_memory

class AuditPaths(unittest.TestCase):
    def test_reserved_audit_root(self):
        for module in (memory, compressed_memory):
            with patch.object(module, 'ROOT', Path('/tmp/sqlk-audit-path-fixture').resolve()):
                path = '.tmp/check-jobs/' + 'a' * 32 + '/snapshot.sqlite'
                self.assertEqual(module.project_path(path), path)

    def test_invalid_ids_traversal_and_wrong_root(self):
        for module in (memory, compressed_memory):
            with patch.object(module, 'ROOT', Path('/tmp/sqlk-audit-path-fixture').resolve()):
                for path in ['.tmp/check-jobs/x', '.tmp/check-jobs/' + 'A'*32,
                             '.tmp/check-jobs/../knowledge.sqlite', '.report/check-jobs/' + 'a'*32]:
                    with self.assertRaises((module.MemoryError, ValueError)):
                        module.project_path(path)

if __name__ == '__main__': unittest.main()
