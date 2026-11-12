"""Delegated OS leases preserve existing explicit SQLite transaction scope."""
import concurrent.futures,os,signal,subprocess,sys,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
from sql_knowledge.engine import Project,initialize
from sql_knowledge.coordinator import CoordinatorError,submit,paths,set_fence
from sql_knowledge.legacy import bind,connect
from tests.helpers import env,ROOT

class LegacyLeaseTests(unittest.TestCase):
 def setUp(self):
  (ROOT/'work').mkdir(exist_ok=True)
  self.tmp=tempfile.TemporaryDirectory(dir=ROOT/'work');self.root=Path(self.tmp.name)
  initialize(str(self.root),'project');self.optin();self.p=Project(self.root)
 def optin(self):
  with (self.root/'knowledge.toml').open('a') as f:f.write('\n[compatibility]\nlegacy_writes = true\n')
 def tearDown(self):
  try:
   if paths(self.root)[0].exists():submit(Project(self.root),'shutdown',{})
   deadline=time.monotonic()+5
   while paths(self.root)[0].exists() and time.monotonic()<deadline:time.sleep(.01)
  finally:self.tmp.cleanup()
 def write(self,key):return self.p.operation('entity',envelope=env(key,dict(id=key,kind='report',status='active',title=key)))
 def count(self,key):
  with self.p.connect() as db:return db.execute('SELECT count(*) FROM entities WHERE id=?',(key,)).fetchone()[0]
 def test_old_embedded_sqlite_refused_before_grant(self):
  with self.p.connect() as db:before=db.execute('SELECT count(*) FROM events').fetchone()[0]
  with patch('sql_knowledge.legacy.sqlite3.sqlite_version_info',(3,50,4)),patch('sql_knowledge.legacy.submit') as grant:
   with self.assertRaisesRegex(CoordinatorError,'globally installed SQL Knowledge Python') as refused:connect(self.root,True)
   self.assertEqual(refused.exception.code,'sqlite_version');grant.assert_not_called()
   with connect(self.root) as db:self.assertEqual(db.execute('SELECT count(*) FROM events').fetchone()[0],before)
  self.assertFalse(paths(self.root)[0].exists())
 def test_multiwrite_atomic_and_typed_writer_waits_for_close(self):
  m=bind(self.root);db=m.connect(writable=True);db.execute('BEGIN IMMEDIATE')
  for key in ['legacy1','legacy2']:m.mutate(db,'entity',env(key,dict(id=key,kind='report',status='active',title=key)),transaction=False)
  self.assertEqual(self.count('legacy1'),0)
  with concurrent.futures.ThreadPoolExecutor() as pool:
   pending=pool.submit(self.write,'typed');time.sleep(.15);self.assertFalse(pending.done())
   db.commit();time.sleep(.1);self.assertFalse(pending.done())
   db.close();self.assertTrue(pending.result(timeout=8)['ok'])
  self.assertEqual(self.count('legacy1'),1);self.assertEqual(self.count('legacy2'),1)
 def test_context_rollback_releases_lease(self):
  m=bind(self.root)
  with self.assertRaisesRegex(RuntimeError,'abort'):
   with m.connect(writable=True) as db:
    db.execute('BEGIN IMMEDIATE');m.mutate(db,'entity',env('rollback',dict(id='rollback',kind='report',status='active',title='Rollback')),transaction=False)
    raise RuntimeError('abort')
  self.assertEqual(self.count('rollback'),0);self.assertTrue(self.write('after-rollback')['ok'])
 def test_killed_client_rolls_back_and_releases_os_lock(self):
  code="""import sys,time
from sql_knowledge.legacy import bind
m=bind(sys.argv[1]);db=m.connect(writable=True);db.execute('BEGIN IMMEDIATE')
m.mutate(db,'entity',dict(idempotency_key='crash',actor='test',reason='crash test',source_ref='test:crash',data=dict(id='crash',kind='report',status='active',title='Crash')),transaction=False)
print('ready',flush=True)
time.sleep(30)
"""
  child=subprocess.Popen([sys.executable,'-B','-c',code,str(self.root)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  try:
   self.assertEqual(child.stdout.readline().strip(),'ready');child.kill();child.wait(timeout=3)
   self.assertTrue(self.write('after-crash')['ok']);self.assertEqual(self.count('crash'),0)
  finally:
   if child.poll() is None:child.kill();child.wait()
   child.stdout.close();child.stderr.close()
 def test_optin_fence_and_guard(self):
  manifest=self.root/'knowledge.toml';manifest.write_text(manifest.read_text().replace('legacy_writes = true','legacy_writes = false'))
  with self.assertRaises(CoordinatorError) as disabled:connect(self.root,True)
  self.assertEqual(disabled.exception.code,'legacy_disabled')
  manifest.write_text(manifest.read_text().replace('legacy_writes = false','legacy_writes = true'))
  set_fence(self.p)
  with self.assertRaises(CoordinatorError) as fenced:connect(self.root,True)
  self.assertEqual(fenced.exception.code,'maintenance')
 def test_manifest_change_while_queued_rejects_lease(self):
  first=connect(self.root,True)
  with concurrent.futures.ThreadPoolExecutor() as pool:
   pending=pool.submit(connect,self.root,True);time.sleep(.15)
   manifest=self.root/'knowledge.toml';manifest.write_text(manifest.read_text().replace('legacy_writes = true','legacy_writes = false'))
   first.close()
   with self.assertRaises(CoordinatorError) as stale:pending.result(timeout=8)
   self.assertEqual(stale.exception.code,'stale_contract')
 def test_abandoned_expired_grant_does_not_leak_lock(self):
  first=connect(self.root,True)
  with concurrent.futures.ThreadPoolExecutor() as pool:
   pending=pool.submit(lambda:connect(self.root,True,timeout=.1))
   time.sleep(1.25)
   with self.assertRaises(CoordinatorError):pending.result(timeout=2)
   first.close()
  self.assertTrue(self.write('after-abandoned-grant')['ok'])
 def test_schema2_uses_runtime_codecs(self):
  from tests.test_identity_cas import project
  root=self.root/'compressed';project(root,version=2)
  with (root/'knowledge.toml').open('a') as f:f.write('\n[compatibility]\nlegacy_writes = true\n')
  try:
   m=bind(root)
   with m.connect(writable=True) as db:
    self.assertEqual(db.execute("SELECT _knowledge_unpack(_knowledge_pack('roundtrip'))").fetchone()[0],'roundtrip')
    db.execute('BEGIN IMMEDIATE')
    m.mutate(db,'entity',env('compressed',dict(id='compressed',kind='report',status='active',title='Compressed')),transaction=False)
    db.commit()
   with Project(root).connect() as db:self.assertIsNotNone(db.execute("SELECT 1 FROM events WHERE operation_key='compressed'").fetchone())
  finally:
   submit(Project(root),'shutdown',{})
   deadline=time.monotonic()+5
   while paths(root)[0].exists() and time.monotonic()<deadline:time.sleep(.01)
 def test_guard_authorization_is_on_leased_connection(self):
  # Represents the cutover guard, without changing the production guard schema.
  with connect(self.root,True) as db:
   db.execute("CREATE TRIGGER fixture_guard BEFORE INSERT ON entities BEGIN SELECT CASE WHEN _sqlk_writer_authorized()!=1 THEN RAISE(ABORT,'unauthorized') END; END")
  with self.p.connect(True) as db:
   with self.assertRaisesRegex(Exception,'unauthorized'):db.execute("INSERT INTO entities(id,kind,title,status,created_at,updated_at) VALUES ('bad','report','Bad','active','now','now')")
  with connect(self.root,True) as db:
   bind(self.root).mutate(db,'entity',env('guarded',dict(id='guarded',kind='report',status='active',title='Guarded')))
  self.assertTrue(self.write('typed-guarded')['ok'])

if __name__=='__main__':unittest.main()
