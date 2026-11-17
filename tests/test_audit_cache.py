import asyncio
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from sql_knowledge import audit_cache as cache, check_jobs as jobs
from sql_knowledge.coordinator import Writer
from sql_knowledge.engine import Project
from tests.helpers import fixture,env
from tests.test_identity_cas import project as schema_project


class AuditCache(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name).resolve()
        self.p=fixture(self.root)

    def tearDown(self):self.tmp.cleanup()

    def completed(self,p=None,problems=(),index=0):
        """Isolated completed state; actual end-to-end audit is tested separately."""
        p=p or self.p;job=jobs.start(p,False);ident=job['job_id']
        with closing(sqlite3.connect(p.root/'.tmp/check-jobs'/ident/'state.sqlite')) as db:
            meta=json.loads(db.execute('SELECT body FROM metadata').fetchone()[0])
            meta.update(status='complete',phase='done',completed_at=f'2026-11-17T12:00:{index:02d}+00:00')
            db.execute('UPDATE metadata SET body=?',(json.dumps(meta),))
            db.executemany('INSERT INTO problems(body) VALUES (?)',[(json.dumps(v),) for v in problems]);db.commit()
        return ident

    def test_large_error_report_survives_automatic_release_both_schemas(self):
        for version in (1,2):
            p=schema_project(self.root/str(version),version)
            problems=[dict(kind='history_mismatch',id=f'record-{i}',detail='evidence '*30) for i in range(1001)]
            problems[3]['detail']='x'*70000
            ident=self.completed(p,problems)
            result=cache.maintain(p,ident)
            self.assertEqual(len(result['released']),1,result)
            self.assertFalse((p.root/'.tmp/check-jobs'/ident).exists())
            found=[]
            for offset in range(0,1001,100):
                report=jobs.result(p,ident,offset,100);self.assertFalse(report['ok'])
                found.extend(report['problems'])
            self.assertEqual(found,problems)
            self.assertEqual(jobs.step(p,ident)['status'],'complete')
            self.assertFalse(cache.maintain(p)['released'])

    def test_retention_20_errors_1_success_really_deletes_service_rows(self):
        errors=[];successes=[]
        for index in range(23):
            ident=self.completed(problems=[dict(kind='bad',id=str(index))],index=index)
            errors.append(ident);self.assertEqual(len(cache.maintain(self.p,ident)['released']),1)
        for index in range(3):
            ident=self.completed(index=index);successes.append(ident);cache.maintain(self.p,ident)
        rows=cache.reports(self.p)['reports']
        self.assertEqual({r['job_id'] for r in rows},{*errors[-20:],successes[-1]})
        with cache.store(self.p) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reports').fetchone()[0],21)
            self.assertEqual(db.execute('SELECT count(*) FROM problems').fetchone()[0],20)
            self.assertEqual(db.execute('SELECT count(*) FROM cleanup').fetchone()[0],0)
        with self.assertRaisesRegex(ValueError,'expired'):jobs.result(self.p,errors[0])
        with closing(self.p.connect()) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM events WHERE actor LIKE 'audit%'").fetchone()[0],0)

    def test_incomplete_foreign_and_unknown_jobs_not_touched(self):
        ident=jobs.start(self.p,False)['job_id'];path=self.root/'.tmp/check-jobs'/ident/'state.sqlite'
        with closing(sqlite3.connect(path)) as db:original=json.loads(db.execute('SELECT body FROM metadata').fetchone()[0])
        for fields in [dict(status='running'),dict(status='failed'),dict(status='cancelled'),dict(status='unknown'),
                       dict(status='complete',project_id='foreign'),dict(status='complete',root='/foreign'),
                       dict(status='complete',instance_id='foreign'),dict(status='complete',job_id='f'*32)]:
            with closing(sqlite3.connect(path)) as db:db.execute('UPDATE metadata SET body=?',(json.dumps(dict(original,**fields)),));db.commit()
            self.assertFalse(cache.maintain(self.p)['released'],fields)
            self.assertTrue(path.exists())

    def test_busy_lock_unknown_and_symlink_fail_closed(self):
        ident=self.completed();root=self.root/'.tmp/check-jobs'/ident
        with (root/'snapshot.sqlite').open('rb'):
            result=cache.maintain(self.p,ident);self.assertFalse(result['released']);self.assertTrue(result['skipped'])
        with cache.guard(self.p,ident):
            self.assertFalse(cache.maintain(self.p,ident)['released'])
        (root/'keep').write_text('keep')
        self.assertFalse(cache.maintain(self.p,ident)['released']);(root/'keep').unlink()
        (root/'definitions').symlink_to(self.root,target_is_directory=True)
        self.assertFalse(cache.maintain(self.p,ident)['released']);(root/'definitions').unlink()
        self.assertTrue(root.exists())
        self.assertEqual(len(cache.maintain(self.p,ident)['released']),1)

    def test_catalog_keep_is_never_reclassified(self):
        ident=self.completed();path='.tmp/check-jobs/'+ident
        self.p.operation('entity',envelope=env('keep',dict(id='keep',kind='file',title='Keep',status='active',path=path,
                            attributes={'storage':{'retention':'keep'}})))
        self.assertFalse(cache.maintain(self.p,ident)['released'])
        self.assertTrue((self.root/path).exists())

    def test_save_failure_preserves_all_files(self):
        ident=self.completed();writer=Writer(self.root,self.p.id,self.p.instance_id)
        try:
            with patch('sql_knowledge.audit_cache._save_report',side_effect=OSError('disk full')):
                result=writer.execute('audit_cache',dict(action='maintain',job_id=ident))
            self.assertFalse(result['released']);self.assertIn('disk full',result['skipped'][0]['reason'])
            self.assertTrue((self.root/'.tmp/check-jobs'/ident/'snapshot.sqlite').exists())
        finally:writer.close()

    def test_interrupted_unlink_retry_and_late_keep_fingerprint(self):
        for change in ('none','keep','file','report'):
            with self.subTest(change=change):
                p=fixture(self.root/change);ident=self.completed(p)
                writer=Writer(p.root,p.id,p.instance_id);unlink=os.unlink;count=0
                def interrupted(*args,**kwargs):
                    nonlocal count
                    if 'dir_fd' in kwargs:
                        count+=1
                        if count==2:raise OSError('interrupted')
                    return unlink(*args,**kwargs)
                try:
                    with patch('sql_knowledge.audit_cache.os.unlink',side_effect=interrupted):
                        self.assertFalse(writer.execute('audit_cache',dict(action='maintain',job_id=ident))['released'])
                    path=p.root/'.tmp/check-jobs'/ident
                    if change=='keep':
                        p.operation('entity',envelope=env('late-keep',dict(id='keep',kind='file',title='Keep',status='active',
                                path='.tmp/check-jobs/'+ident,attributes={'storage':{'retention':'keep'}})))
                    elif change=='file':
                        (path/'snapshot.sqlite').write_bytes(b'changed')
                    elif change=='report':
                        with cache.store(p,writable=True) as db:
                            db.execute('UPDATE reports SET report_sha256=?',('0'*64,));db.commit()
                    result=writer.execute('audit_cache',dict(action='maintain',job_id=ident))
                    if change=='none':
                        self.assertEqual(len(result['released']),1,result);self.assertFalse(path.exists())
                        self.assertTrue(jobs.result(p,ident)['ok'])
                    else:self.assertFalse(result['released']);self.assertTrue(path.exists())
                finally:writer.close()

    def test_report_store_symlink_or_wrong_binding_never_deletes(self):
        ident=self.completed();folder=self.root/'.tmp/check-reports';folder.mkdir()
        external=self.root/'external';external.write_text('keep')
        (folder/'reports.sqlite').symlink_to(external)
        with self.assertRaises(Exception):cache.maintain(self.p,ident)
        self.assertEqual(external.read_text(),'keep')
        (folder/'reports.sqlite').unlink()
        with cache.store(self.p,writable=True) as db:
            db.execute('UPDATE binding SET body=?',(json.dumps(['foreign','/foreign','foreign']),));db.commit()
        with self.assertRaises(ValueError):cache.maintain(self.p,ident)
        self.assertTrue((self.root/'.tmp/check-jobs'/ident).exists())

    def test_admission_bound_and_resume(self):
        manifest=self.root/'knowledge.toml';manifest.write_text(manifest.read_text()+'\n[audit_cache]\nmax_jobs=1\nmax_bytes=1073741824\n')
        p=Project(self.root);job=jobs.start(p,False)
        with self.assertRaisesRegex(ValueError,'admission limit'):jobs.start(p,False)
        self.assertEqual(jobs.step(p,job['job_id'],1)['status'],'running')

    def test_parallel_maintenance_uses_one_coordinator(self):
        from concurrent.futures import ThreadPoolExecutor
        ident=self.completed()
        with ThreadPoolExecutor(max_workers=4) as pool:
            results=list(pool.map(lambda _:cache.maintain(self.p),range(4)))
        self.assertEqual(sum(len(r['released']) for r in results),1)
        self.assertEqual(len(cache.reports(self.p)['reports']),1)
        self.assertFalse((self.root/'.tmp/check-jobs'/ident).exists())

    def test_old_marker_only_folders_do_not_consume_job_slots(self):
        manifest=self.root/'knowledge.toml'
        manifest.write_text(manifest.read_text()+'\n[audit_cache]\nmax_jobs=1\n')
        self.p=Project(self.root)
        for index in range(8):
            folder=self.root/'.tmp/check-jobs'/format(index,'032x');folder.mkdir(parents=True)
            (folder/'physical.done').write_text('complete')
        self.assertEqual(jobs.start(self.p,False)['status'],'running')
        self.assertEqual(len(list((self.root/'.tmp/check-jobs').glob('*/physical.done'))),8)

    def test_no_implicit_limit_on_existing_incomplete_jobs(self):
        ids=[jobs.start(self.p,False)['job_id'] for _ in range(9)]
        self.assertEqual(len(ids),9)
        self.assertEqual(cache.maintain(self.p)['released'],[])
        self.assertTrue(all((self.root/'.tmp/check-jobs'/ident/'state.sqlite').exists() for ident in ids))


class AuditCacheMCP(unittest.IsolatedAsyncioTestCase):
    async def test_real_mcp_completion_and_startup_cleanup(self):
        from mcp import ClientSession,StdioServerParameters
        from mcp.client.stdio import stdio_client
        with tempfile.TemporaryDirectory() as td:
            p=fixture(Path(td));params=StdioServerParameters(command=sys.executable,args=['-m','sql_knowledge.mcp','--project',str(p.root)])
            async with stdio_client(params) as streams:
                async with ClientSession(*streams) as session:
                    await session.initialize()
                    async def call(name,**args):
                        r=await session.call_tool(name,args);self.assertFalse(r.isError,r)
                        return r.structuredContent or json.loads(r.content[0].text)
                    job=await call('project_check_start',files=False)
                    for _ in range(100):
                        result=await call('project_check_step',job_id=job['job_id'],batch_size=100)
                        if result['status']!='running':break
                        await asyncio.sleep(.02)
                    self.assertTrue(result['ok'],result)
                    await call('project_check_maintenance')
                    report=await call('project_check_report',job_id=job['job_id'])
                    self.assertTrue(report['retained']);self.assertTrue(report['ok'])
                    self.assertFalse((p.root/'.tmp/check-jobs'/job['job_id']).exists())
                    self.assertEqual(len((await call('project_check_reports'))['reports']),1)
            # A legacy complete job left on disk is picked up on the next MCP startup.
            helper=AuditCache();helper.p=p
            old=helper.completed(p,index=59)
            async with stdio_client(params) as streams:
                async with ClientSession(*streams) as session:
                    await session.initialize()
                    for _ in range(100):
                        if not (p.root/'.tmp/check-jobs'/old).exists():break
                        await asyncio.sleep(.02)
                    self.assertFalse((p.root/'.tmp/check-jobs'/old).exists())

    async def test_periodic_runs_without_model_or_tools(self):
        stop=asyncio.Event();calls=[]
        def maintenance(project):
            calls.append(project)
            if len(calls)==3:stop.set()
        with patch('sql_knowledge.audit_cache.maintain',side_effect=maintenance):
            await cache.periodic('isolated-project',stop,interval=.01)
        self.assertEqual(len(calls),3)
