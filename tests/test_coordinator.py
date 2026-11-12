"""Real IPC/process fault regressions on tiny isolated projects.

Includes kill-before-commit and lost-response-after-commit; does not simulate
power loss, disk failure, or kill exactly between fsync and the receipt reply.
"""
import concurrent.futures,json,os,shutil,signal,socket,sqlite3,subprocess,sys,tempfile,time,unittest,uuid
from pathlib import Path
from sql_knowledge.engine import Project,initialize
from sql_knowledge.storage_keys import step_key
from sql_knowledge.coordinator import submit,paths,CoordinatorError,contract
from tests.helpers import env,ROOT
from sql_knowledge import extensions as ex


def request(key,eid=None,revision=None):
 e=env(key,dict(id=eid or key,kind='report',title=key,status='active'))
 if revision is not None:e['expected_revision']=revision
 return e


def wait_for(predicate,seconds=8):
 deadline=time.monotonic()+seconds
 while time.monotonic()<deadline:
  value=predicate()
  if value:return value
  time.sleep(.01)
 raise AssertionError('condition did not complete within deadline')


class CoordinatorRegressions(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);initialize(str(self.root),'project');self.p=Project(self.root)
 def tearDown(self):
  try:
   if paths(self.root)[0].exists():
    submit(self.p,'shutdown',{})
    wait_for(lambda:not paths(self.root)[0].exists())
  finally:self.temp.cleanup()
 def call(self,e,client_id=None,timeout=8):
  try:return submit(Project(self.root),'core',dict(command='entity',envelope=e),client_id=client_id,timeout=timeout)
  except Exception as exc:return dict(ok=False,code=getattr(exc,'code','exception'),error=str(exc))
 def parallel(self,requests):
  with concurrent.futures.ThreadPoolExecutor(max_workers=len(requests)) as pool:return list(pool.map(self.call,requests))
 def count(self,key):
  with self.p.connect() as db:return db.execute('SELECT count(*) FROM events WHERE operation_key=?',(key,)).fetchone()[0]
 def test_twenty_simultaneous_processes_share_one_writer(self):
  gate=self.root/'gate';code='''import json,sys,time
from pathlib import Path
from sql_knowledge.engine import Project
root=Path(sys.argv[1]);p=Project(root)
print('ready',flush=True)
while not (root/'gate').exists():time.sleep(.005)
i=sys.argv[2];r=p.operation('entity',envelope=dict(idempotency_key='child'+i,actor='test',reason='process singleton',source_ref='test:process',data=dict(id='child'+i,kind='report',status='active',title='child'+i)))
print(json.dumps(r),flush=True)
'''
  children=[]
  try:
   for i in range(20):children.append(subprocess.Popen([sys.executable,'-B','-c',code,str(self.root),str(i)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True))
   for child in children:self.assertEqual(child.stdout.readline().strip(),'ready')
   gate.write_text('go');results=[]
   for child in children:
    out,err=child.communicate(timeout=25);self.assertEqual(child.returncode,0,err);results.append(json.loads(out))
   self.assertEqual(len({r['_writer']['pid'] for r in results}),1);self.assertTrue(all(r['ok'] for r in results))
   with self.p.connect() as db:self.assertEqual(db.execute("SELECT count(*) FROM events WHERE operation_key LIKE 'child%'").fetchone()[0],20)
  finally:
   for child in children:
    if child.poll() is None:child.kill();child.communicate()
 def test_twenty_cas_contenders_have_one_winner(self):
  initial=self.p.operation('entity',envelope=request('initial','item'));results=self.parallel([request('cas'+str(i),'item',initial['revision']) for i in range(20)])
  self.assertEqual(sum(r['ok'] for r in results),1);self.assertEqual(sum(r.get('code')=='revision_conflict' for r in results),19)
  with self.p.connect() as db:self.assertEqual(db.execute("SELECT count(*) FROM events WHERE operation_key LIKE 'cas%'").fetchone()[0],1)
 def test_twenty_different_objects_and_identical_retries(self):
  results=self.parallel([request('different'+str(i)) for i in range(20)]);self.assertTrue(all(r['ok'] for r in results),results)
  same=request('identical');results=self.parallel([same for _ in range(20)])
  self.assertEqual(sum(r.get('replayed',False) for r in results),19);self.assertTrue(all(r['ok'] for r in results),results);self.assertEqual(self.count('identical'),1)
  self.assertEqual(len({r['revision'] for r in results}),1)
 def test_killed_writer_before_commit_safe_retry(self):
  self.p.operation('entity',envelope=request('warmup'));pid=submit(self.p,'status',{})['pid'];lock=self.p.connect(True);lock.execute('BEGIN IMMEDIATE');key='kill-before-commit'
  with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
   future=pool.submit(self.call,request(key))
   try:
    wait_for(lambda:submit(self.p,'status',{})['active']);os.kill(pid,signal.SIGKILL)
    result=future.result(timeout=5);self.assertFalse(result['ok']);self.assertEqual(self.count(key),0)
   finally:lock.rollback();lock.close()
  first=self.p.operation('entity',envelope=request(key));again=self.p.operation('entity',envelope=request(key))
  self.assertFalse(first['replayed']);self.assertTrue(again['replayed']);self.assertEqual(first['revision'],again['revision']);self.assertEqual(self.count(key),1)
 def test_lost_response_after_commit_uses_receipt(self):
  self.p.operation('entity',envelope=request('warmup'));key='response-lost';path=paths(self.root)[0]
  message=dict(project_id=self.p.id,instance_id=self.p.instance_id,manifest_contract=contract(self.p.config),kind='core',payload=dict(command='entity',envelope=request(key)),client_id='lost-client',deadline=time.time()+8)
  with socket.socket(socket.AF_UNIX) as stream:
   stream.connect(str(path));stream.sendall(json.dumps(message).encode()+b'\n')
  wait_for(lambda:self.count(key)==1)
  replay=self.p.operation('entity',envelope=request(key));self.assertTrue(replay['replayed']);self.assertEqual(self.count(key),1)
 def test_rogue_writer_busy_is_bounded_and_reads_work(self):
  self.p.operation('entity',envelope=request('warmup'));lock=self.p.connect(True);lock.execute('BEGIN IMMEDIATE')
  try:
   with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
    started=time.monotonic();future=pool.submit(self.call,request('busy'))
    wait_for(lambda:submit(self.p,'status',{})['active']);read=time.monotonic();self.assertTrue(self.p.operation('context')['ok']);self.assertLess(time.monotonic()-read,1)
    result=future.result(timeout=5);elapsed=time.monotonic()-started
   self.assertEqual(result.get('code'),'sqlite_busy',result);self.assertLess(elapsed,4);self.assertEqual(self.count('busy'),0)
  finally:lock.rollback();lock.close()
 def test_per_client_queue_overflow_and_reader_latency(self):
  self.p.operation('entity',envelope=request('warmup'));lock=self.p.connect(True);lock.execute('BEGIN IMMEDIATE')
  with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
   futures=[pool.submit(self.call,request('queued'+str(i)),'one-flood-client',3) for i in range(50)]
   try:
    wait_for(lambda:submit(self.p,'status',{})['rejected']>0)
    started=time.monotonic();self.assertTrue(self.p.operation('context')['ok']);self.assertLess(time.monotonic()-started,1)
   finally:lock.rollback();lock.close()
   results=[future.result(timeout=8) for future in futures]
  self.assertTrue(any(r.get('code')=='queue_full' for r in results),results)
  status=submit(self.p,'status',{});self.assertLessEqual(status['peak_depth'],33);self.assertEqual(status['depth'],0)

 def test_migration_serializes_with_queued_core_write(self):
  self.p.operation('entity',envelope=request('warmup'))
  shutil.copytree(ROOT/'examples/demo/extensions',self.root/'extensions')
  with (self.root/'knowledge.toml').open('a') as manifest:manifest.write('[extensions.demo]\nversion = 1\npath = "extensions/demo/extension.toml"\n')
  q=Project(self.root);lock=q.connect(True);lock.execute('BEGIN IMMEDIATE')
  with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
   migration=pool.submit(ex.migrate,q)
   try:
    wait_for(lambda:submit(q,'status',{})['active'])
    write=pool.submit(self.call,request('after-migration'))
    written=write.result(timeout=2)
    self.assertEqual(written.get('code'),'maintenance')
    with q.connect() as db:self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='demo_items'").fetchone())
   finally:lock.rollback();lock.close()
   migrated=migration.result(timeout=8);written=write.result(timeout=8)
  self.assertTrue(migrated['ok']);self.assertFalse(written['ok'])
  with self.assertRaisesRegex(CoordinatorError,'contract changed'):self.p.operation('entity',envelope=request('stale-client'))
  self.assertTrue(q.operation('entity',envelope=request('after-migration'))['ok'])
  row=ex.upsert(q,'demo','demo_items',dict(id='after',entity_id='after-migration',value='committed'),env('after-extension'))
  self.assertTrue(row['ok']);self.assertTrue(q.operation('check')['ok'])

 def test_shutdown_fences_until_explicit_resume(self):
  self.p.operation('entity',envelope=request('warmup'))
  submit(self.p,'shutdown',{});wait_for(lambda:not paths(self.root)[0].exists())
  result=self.call(request('blocked'))
  self.assertEqual(result.get('code'),'maintenance');self.assertEqual(self.count('blocked'),0)
  self.assertTrue(submit(self.p,'resume',{})['resumed'])
  self.assertTrue(self.p.operation('entity',envelope=request('resumed'))['ok'])
 def test_preparation_preserves_client_fifo_and_other_client_progress(self):
  self.p.operation('entity',envelope=request('warmup'))
  path=self.root/'experiments/2026-11-12_00__fifo/file.dat';path.parent.mkdir(parents=True)
  with path.open('wb') as stream:stream.truncate(256*1024*1024)
  envelope=env('fifo-register',dict(id='fifo-file',path=path.relative_to(self.root).as_posix(),title='file',description='FIFO hashing probe'))
  with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
   hashed=pool.submit(submit,self.p,'storage',dict(action='register',args=[envelope]),30,'shared')
   wait_for(lambda:submit(self.p,'status',{})['preparing']>0)
   later=pool.submit(self.call,request('later'),'shared')
   other=pool.submit(self.call,request('other'),'other')
   results=[hashed.result(timeout=15),later.result(timeout=15),other.result(timeout=15)]
  self.assertTrue(all(r['ok'] for r in results),results)
  file_key=step_key('fifo-register','register','file')
  with self.p.connect() as db:
   events=dict(db.execute("SELECT operation_key,seq FROM events WHERE operation_key IN (?,?,?)",('other','later',file_key)))
  self.assertLess(events['other'],events[file_key]);self.assertLess(events[file_key],events['later'])
