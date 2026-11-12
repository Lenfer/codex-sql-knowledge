"""Storage CAS and recovery at persisted suboperation boundaries, both schemas."""
import asyncio,json,sys,tempfile,time,unittest
from contextlib import closing
from pathlib import Path
from mcp import ClientSession,StdioServerParameters
from mcp.client.stdio import stdio_client
from sql_knowledge.engine import Project,module,BASE
from sql_knowledge.storage_keys import step_key
from sql_knowledge.coordinator import Writer,prepare_storage,submit,paths
from tests.test_identity_cas import project
from tests.helpers import ROOT,env


def file_request(p,key='register',ident='file',disposable=False):
 path='.report/2026-11-12/'+ident.strip()+'.txt';file=p.root/path;file.parent.mkdir(parents=True,exist_ok=True);file.write_text('isolated fixture '+ident)
 return env(key,dict(id=ident,path=path,title='Fixture',description='Storage test',kind='file',retention='disposable' if disposable else 'keep'))


def revision(p,ident):
 with closing(p.connect()) as db:return p.revision(db,ident)


def keys(p):
 with closing(p.connect()) as db:return [r[0] for r in db.execute('SELECT operation_key FROM events ORDER BY seq')]


def run(writer,p,action,envelope):
 request=dict(action=action,args=[envelope]);return writer.execute('storage',request,prepare_storage(p,request))


class StorageRecovery(unittest.TestCase):
 def test_missing_cas_canonical_id_has_no_side_effects(self):
  with tempfile.TemporaryDirectory() as td:
   for version in (1,2):
    p=project(Path(td)/str(version),version);w=Writer(p.root,p.id,p.instance_id)
    try:
     original=file_request(p,disposable=True);run(w,p,'register',original);before=keys(p)
     for ident in ['file',' file ']:
      changed=dict(original,idempotency_key='missing-'+ident,data=dict(original['data'],id=ident,title='Overwrite'))
      with self.assertRaises(Exception) as caught:run(w,p,'register',changed)
      self.assertEqual(caught.exception.code,'revision_required');self.assertEqual(keys(p),before)
     for action in ('reconcile','cleanup_plan'):
      with self.assertRaises(Exception) as caught:run(w,p,action,env('missing-'+action,dict(entity_ids=[' file '],**({'min_age_days':0} if action=='cleanup_plan' else {}))))
      self.assertEqual(caught.exception.code,'revision_required');self.assertEqual(keys(p),before)
     plan=run(w,p,'cleanup_plan',dict(env('valid-plan',dict(entity_ids=['file'],min_age_days=0)),expected_revisions={'file':revision(p,'file')}));before=keys(p)
     with self.assertRaises(Exception) as caught:run(w,p,'cleanup_execute',env('missing-execute',dict(plan_id=plan['plan_id'],confirmation_token=plan['confirmation_token'])))
     self.assertEqual(caught.exception.code,'revision_required');self.assertEqual(keys(p),before);self.assertTrue((p.root/original['data']['path']).exists())
     valid=dict(original,idempotency_key='canonical-map',data=dict(original['data'],id=' file '),expected_revisions={' file ':revision(p,'file')})
     self.assertTrue(run(w,p,'register',valid)['ok'])
    finally:w.close()
 def test_explicit_creation_revisions_are_honored(self):
  from sql_knowledge import extensions as ex
  with tempfile.TemporaryDirectory() as td:
   for version in (1,2):
    p=project(Path(td)/str(version),version,True);before=keys(p)
    data=dict(id='new',kind='report',title='New',status='active')
    with self.assertRaises(Exception) as caught:p.operation('entity',envelope=dict(env('new-stale',data),expected_revision=9))
    self.assertEqual(caught.exception.code,'revision_conflict');self.assertEqual(before,keys(p))
    p.operation('entity',envelope=dict(env('new-zero',data),expected_revision=0));ex.migrate(p);before=keys(p)
    row=dict(id='new-row',entity_id='new',value='first')
    with self.assertRaises(Exception) as caught:ex.upsert(p,'demo','demo_items',row,dict(env('new-extension-stale'),expected_revision=0))
    self.assertEqual(caught.exception.code,'revision_conflict');self.assertEqual(before,keys(p))
    self.assertTrue(ex.upsert(p,'demo','demo_items',row,dict(env('new-extension-current'),expected_revision=revision(p,'new')))['ok'])
 def test_register_resume_after_file_commit_and_changed_payload_rejected(self):
  with tempfile.TemporaryDirectory() as td:
   for version in (1,2):
    p=project(Path(td)/str(version),version);w=Writer(p.root,p.id,p.instance_id);request=file_request(p,ident=' file ')
    w.storage.finish=lambda *a:(_ for _ in ()).throw(RuntimeError('fault after file commit'))
    with self.assertRaisesRegex(RuntimeError,'fault'):run(w,p,'register',request)
    self.assertIn(step_key('register','register','file'),keys(p));self.assertNotIn(step_key('register','register','receipt'),keys(p));w.close();w=Writer(p.root,p.id,p.instance_id)
    try:
     before=keys(p)
     changed=dict(request,data=dict(request['data'],title='Changed'))
     with self.assertRaises(Exception) as caught:run(w,p,'register',changed)
     self.assertEqual(caught.exception.code,'idempotency_conflict');self.assertEqual(before,keys(p))
     result=run(w,p,'register',request);self.assertTrue(result['ok']);self.assertEqual(keys(p).count(step_key('register','register','file')),1);self.assertEqual(keys(p).count(step_key('register','register','receipt')),1)
     again=run(w,p,'register',request);self.assertTrue(again['replayed']);self.assertEqual(keys(p).count(step_key('register','register','file')),1)
    finally:w.close()
 def test_existing_registration_resumes_after_intent_only(self):
  with tempfile.TemporaryDirectory() as td:
   for version in (1,2):
    p=project(Path(td)/str(version),version);w=Writer(p.root,p.id,p.instance_id);first=file_request(p);run(w,p,'register',first)
    request=dict(first,idempotency_key='register-update',data=dict(first['data'],title='Updated'),expected_revisions={'file':revision(p,'file')});original=w.storage.write
    def fail_file(db,envelope,suffix,data,transaction=True):
     if suffix=='file':raise RuntimeError('fault before file commit')
     return original(db,envelope,suffix,data,transaction)
    w.storage.write=fail_file
    with self.assertRaisesRegex(RuntimeError,'fault'):run(w,p,'register',request)
    self.assertIn(step_key('register-update','register','start'),keys(p));self.assertNotIn(step_key('register-update','register','file'),keys(p));w.close();w=Writer(p.root,p.id,p.instance_id)
    try:
     self.assertTrue(run(w,p,'register',request)['ok']);self.assertEqual(keys(p).count(step_key('register-update','register','file')),1)
    finally:w.close()
 def test_reconcile_resumes_only_unfinished_targets(self):
  with tempfile.TemporaryDirectory() as td:
   for version in (1,2):
    p=project(Path(td)/str(version),version);w=Writer(p.root,p.id,p.instance_id)
    for ident in ('one','two'):run(w,p,'register',file_request(p,'register-'+ident,ident))
    request=dict(env('reconcile',dict(entity_ids=[' one ','two'])),expected_revisions={ident:revision(p,ident) for ident in ('one','two')});original=w.storage.write
    def fail_second(db,envelope,suffix,data,transaction=True,entity_id=None):
     if entity_id=='two':raise RuntimeError('fault before second target')
     return original(db,envelope,suffix,data,transaction,entity_id=entity_id)
    w.storage.write=fail_second
    with self.assertRaisesRegex(RuntimeError,'fault'):run(w,p,'reconcile',request)
    self.assertIn(step_key('reconcile','reconcile','target','one'),keys(p));self.assertNotIn(step_key('reconcile','reconcile','target','two'),keys(p));w.close();w=Writer(p.root,p.id,p.instance_id)
    try:
     before=keys(p);changed=dict(request,data=dict(entity_ids=['one']))
     with self.assertRaises(Exception) as caught:run(w,p,'reconcile',changed)
     self.assertEqual(caught.exception.code,'idempotency_conflict');self.assertEqual(before,keys(p))
     result=run(w,p,'reconcile',request);self.assertTrue(result['ok']);self.assertEqual(keys(p).count(step_key('reconcile','reconcile','target','one')),1);self.assertEqual(keys(p).count(step_key('reconcile','reconcile','target','two')),1);self.assertEqual(keys(p).count(step_key('reconcile','reconcile','receipt')),1)
    finally:w.close()
 def test_cleanup_resume_validates_own_intent_and_rejects_foreign_change(self):
  with tempfile.TemporaryDirectory() as td:
   for version in (1,2):
    for foreign in (False,True):
     p=project(Path(td)/(str(version)+str(foreign)),version);w=Writer(p.root,p.id,p.instance_id);registered=file_request(p,disposable=True);run(w,p,'register',registered)
     plan=run(w,p,'cleanup_plan',dict(env('plan',dict(entity_ids=['file'],min_age_days=0)),expected_revisions={'file':revision(p,'file')}))
     request=dict(env('delete',dict(plan_id=plan['plan_id'],confirmation_token=plan['confirmation_token'])),expected_revisions={'file':revision(p,'file')})
     # Crash before the second BEGIN, after intent's commit.
     class CrashConnection:
      def __init__(self,real):self.real=real;self.begins=0
      def __getattr__(self,name):return getattr(self.real,name)
      def execute(self,sql,*args):
       if sql=='BEGIN IMMEDIATE':
        self.begins+=1
        if self.begins==3:raise RuntimeError('fault after intent commit')
       return self.real.execute(sql,*args)
     real=w.db;w.db=CrashConnection(real)
     with self.assertRaisesRegex(RuntimeError,'fault'):run(w,p,'cleanup_execute',request)
     w.db=real;w.close();self.assertIn(step_key('delete','execute','intent','file'),keys(p));self.assertTrue((p.root/registered['data']['path']).exists())
     if foreign:
      p.operation('entity',envelope=dict(env('foreign',dict(id='file',kind='file',title='External update',status='active')),expected_revision=revision(p,'file')))
     w=Writer(p.root,p.id,p.instance_id)
     try:
      before=keys(p)
      if foreign:
       with self.assertRaises(Exception) as caught:run(w,p,'cleanup_execute',request)
       self.assertEqual(caught.exception.code,'revision_conflict');self.assertTrue((p.root/registered['data']['path']).exists());self.assertEqual(before,keys(p))
      else:
       result=run(w,p,'cleanup_execute',request);self.assertEqual(result['deleted_ids'],['file']);self.assertFalse((p.root/registered['data']['path']).exists());self.assertTrue(run(w,p,'cleanup_execute',request)['replayed'])
     finally:w.close()


class HistoricalStorageMCP(unittest.IsolatedAsyncioTestCase):
 async def test_all_four_legacy_storage_shapes_replay_over_real_mcp(self):
  for version in (1,2):
   with tempfile.TemporaryDirectory() as td:
    p=project(Path(td)/'project',version);oldmodel=module('sql_knowledge._core.old_shapes',ROOT/'tests/fixtures/legacy_runtime/mcp_server.py');oldstorage=module('sql_knowledge._core.old_storage',ROOT/'tests/fixtures/legacy_runtime/storage.py');oldstorage.m=p.memory
    register=file_request(p,'old-register',disposable=True);requests=[]
    with closing(p.connect(True)) as db:
     oldstorage.register(db,oldmodel.StorageFileEnvelope.model_validate(register).model_dump());requests.append(('storage_register',register))
     reconcile=env('old-reconcile',dict(entity_ids=['file']));oldstorage.reconcile(db,oldmodel.StorageIDsEnvelope.model_validate(reconcile).model_dump());requests.append(('storage_reconcile',reconcile))
     planenv=env('old-plan',dict(entity_ids=['file'],min_age_days=0));plan=oldstorage.cleanup_plan(db,oldmodel.CleanupPlanEnvelope.model_validate(planenv).model_dump());requests.append(('storage_cleanup_plan',planenv))
     execute=env('old-execute',dict(plan_id=plan['plan_id'],confirmation_token=plan['confirmation_token']));oldstorage.cleanup_execute(db,oldmodel.CleanupExecuteEnvelope.model_validate(execute).model_dump());requests.append(('storage_cleanup_execute',execute))
    before=keys(p)
    try:
     with (p.root/'mcp.stderr.log').open('w') as err:
      async with stdio_client(StdioServerParameters(command=sys.executable,args=['-B','-m','sql_knowledge.mcp','--project',str(p.root)]),errlog=err) as streams:
       async with ClientSession(*streams) as session:
        await session.initialize()
        for tool,envelope in requests:
         result=await session.call_tool(tool,dict(envelope=envelope));self.assertFalse(result.isError,(tool,result));self.assertEqual(before,keys(p))
         changed=dict(envelope,reason='Changed payload');result=await session.call_tool(tool,dict(envelope=changed));self.assertTrue(result.isError,tool);self.assertIn('idempotency_conflict',str(result));self.assertEqual(before,keys(p))
    finally:
     if paths(p.root)[0].exists():submit(p,'shutdown',{})
