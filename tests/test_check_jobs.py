import importlib
import sqlite3
import tempfile
import unittest
import time
from pathlib import Path
from sql_knowledge import check_jobs as jobs, extensions
from sql_knowledge.engine import Project
from tests.helpers import fixture, env


class CheckJobs(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.project=fixture(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def finish(self, job):
        for _ in range(500):
            out=jobs.step(self.project,job['job_id'],batch_size=1)
            if out['status']!='running': return jobs.result(self.project,job['job_id'])
            if out['phase']=='physical': time.sleep(.02)
        self.fail('job failed to finish')

    def test_batch_restart_snapshot(self):
        job=jobs.start(self.project,files=False)
        first=jobs.step(self.project,job['job_id'],1)
        self.assertEqual(first['checked'],1)
        self.project.operation('entity',envelope=env('later',dict(id='later',kind='document',title='Later',status='active')))
        importlib.reload(jobs)
        self.project=Project(str(self.root))
        out=self.finish(job)
        self.assertEqual(out['status'],'complete')
        self.assertTrue(out['ok'],out)
        self.assertEqual(out['snapshot_cursor'],job['snapshot_cursor'])

    def test_corrupt_fts_history_mapping(self):
        with sqlite3.connect(self.project.database) as db:
            db.execute("UPDATE entities SET title='broken' WHERE id='asset'")
            db.execute("DELETE FROM search_index WHERE entity_id='asset'")
            db.execute("DELETE FROM _sqlk_search_documents WHERE entity_id='asset'")
        out=self.finish(jobs.start(self.project,files=False))
        kinds={r['kind'] for r in out['problems']}
        self.assertTrue({'history_mismatch','search_index_mismatch','search_mapping_mismatch'}<=kinds,out)
        self.assertFalse(out['ok'])

    def test_cancel_and_binding(self):
        job=jobs.start(self.project,False)
        self.assertEqual(jobs.cancel(self.project,job['job_id'])['status'],'cancelled')
        self.assertEqual(jobs.step(self.project,job['job_id'])['checked'],0)
        old=self.project.id
        self.project.id='foreign'
        with self.assertRaises(ValueError): jobs.result(self.project,job['job_id'])
        self.project.id=old
        with self.assertRaises(ValueError): jobs.result(self.project,'../escape')

    def test_snapshot_tamper(self):
        job=jobs.start(self.project,False)
        path=self.root/'.tmp/check-jobs'/job['job_id']/'snapshot.sqlite'
        with sqlite3.connect(path) as db: db.execute("UPDATE entities SET title='tampered'")
        with self.assertRaisesRegex(ValueError,'fingerprint'): jobs.step(self.project,job['job_id'])

    def test_missing_entity_and_subject(self):
        with sqlite3.connect(self.project.database) as db:
            db.execute('DROP TRIGGER event_entities_no_delete')
            db.execute("DELETE FROM event_entities WHERE entity_id='asset'")
            db.execute("DELETE FROM entities WHERE id='asset'")
        out=self.finish(jobs.start(self.project,False))
        kinds={r['kind'] for r in out['problems']}
        self.assertTrue({'history_entity_missing','event_subject_missing','search_index_orphan'}<=kinds,out)

    def test_extension_deleted_row_and_frozen_definition(self):
        self.project=fixture(self.root/'extended',True)
        extensions.migrate(self.project)
        extensions.upsert(self.project,'demo','demo_items',dict(id='x',entity_id='asset',value='proof'),env('ext'))
        with sqlite3.connect(self.project.database) as db: db.execute('DELETE FROM demo_items')
        job=jobs.start(self.project,False)
        (self.project.root/'extensions/demo/001.sql').write_text('bad migration after snapshot')
        out=self.finish(job)
        self.assertIn('extension_row_missing',{r['kind'] for r in out['problems']},out)


if __name__=='__main__': unittest.main()
