"""Observable Astra2 regressions on both schemas, using real writer lifecycle."""
import hashlib,json,tempfile,time,unittest
from pathlib import Path
from contextlib import closing
from unittest.mock import patch
from sql_knowledge import coordinator as c
from sql_knowledge.engine import Project,module,BASE
from tests.test_identity_cas import project
from tests.test_storage_recovery import file_request,revision,run,keys
from tests.helpers import ROOT,env

class ReceiptFollowup(unittest.TestCase):
 def test_stop_during_connect_never_acknowledges_mutation(self):
  for schema in (1,2):
   for connect_attempt in (1,2):
    with self.subTest(schema=schema,connect_attempt=connect_attempt),tempfile.TemporaryDirectory() as td:
     p=Project(project(Path(td)/'project',schema).root);c.submit(p,'status',{});original=c._exchange;attempts=[]
     def scheduled(path,request,timeout):
      if request['kind']=='core':
       attempts.append(1)
       if len(attempts)<connect_attempt:raise ConnectionRefusedError('transient before maintenance')
       c.submit(p,'shutdown',{});end=time.monotonic()+3
       while path.exists() and time.monotonic()<end:time.sleep(.01)
       self.assertFalse(path.exists())
      return original(path,request,timeout)
     with patch.object(c,'_exchange',scheduled):
      with self.assertRaises(c.CoordinatorError) as caught:p.operation('entity',envelope=env('race',dict(id='card',kind='report',title='Card',status='active')))
     self.assertEqual(caught.exception.code,'maintenance')
     with closing(p.connect()) as db:
      self.assertIsNone(db.execute("SELECT id FROM entities WHERE id='card'").fetchone());self.assertIsNone(db.execute("SELECT seq FROM events WHERE operation_key='race'").fetchone())
     self.assertTrue(c.submit(p,'status',{})['maintenance'])
 def test_reserved_and_unicode_ids_have_durable_equal_replay(self):
  for schema in (1,2):
   with tempfile.TemporaryDirectory() as td:
    p=project(Path(td)/'project',schema);w=c.Writer(p.root,p.id,p.instance_id)
    try:
     for ident in ('invoice','start','receipt','a:b','雪'):
      with self.subTest(schema=schema,ident=ident):
       run(w,p,'register',file_request(p,'register-'+ident,ident));key='reconcile-'+ident
       request=dict(env(key,dict(entity_ids=[ident])),expected_revisions={ident:revision(p,ident)})
       first=run(w,p,'reconcile',request);before=keys(p);second=run(w,p,'reconcile',request)
       clean=lambda value:{k:v for k,v in value.items() if k not in ('_writer','replayed')}
       self.assertEqual(clean(first),clean(second));self.assertTrue(second['replayed']);self.assertEqual(before,keys(p));self.assertIn('related_ids',first['records'][0])
       op='storage-op-'+hashlib.sha256(key.encode()).hexdigest()[:32]
       with closing(p.connect()) as db:
        row=db.execute('SELECT status,attributes_json FROM entities WHERE id=?',(op,)).fetchone();self.assertEqual(row['status'],'active');self.assertEqual(json.loads(row['attributes_json'])['result'],clean(first))
    finally:w.close()
 def test_resume_preserves_related_ids_contract(self):
  for schema in (1,2):
   with tempfile.TemporaryDirectory() as td:
    p=project(Path(td)/'project',schema);w=c.Writer(p.root,p.id,p.instance_id)
    for ident in ('one','two'):run(w,p,'register',file_request(p,'register-'+ident,ident))
    p.operation('link',envelope=dict(env('linked',dict(source_id='one',target_id='two',predicate='relates_to')),expected_revisions={ident:revision(p,ident) for ident in ('one','two')}))
    request=dict(env('partial',dict(entity_ids=['one','two'])),expected_revisions={ident:revision(p,ident) for ident in ('one','two')});original=w.storage.write
    def stop_second(db,envelope,suffix,data,*args,**kwargs):
     if data['id']=='two':raise RuntimeError('interrupted after first target')
     return original(db,envelope,suffix,data,*args,**kwargs)
    w.storage.write=stop_second
    with self.assertRaisesRegex(RuntimeError,'interrupted'):run(w,p,'reconcile',request)
    w.close();w=c.Writer(p.root,p.id,p.instance_id)
    try:
     result=run(w,p,'reconcile',request);self.assertEqual([r['related_ids'] for r in result['records']],[['two'],['one']]);before=keys(p);again=run(w,p,'reconcile',request);self.assertEqual(result['records'],again['records']);self.assertEqual(before,keys(p))
    finally:w.close()
 def test_ambiguous_legacy_partial_is_explicit_refusal_without_writes(self):
  for schema in (1,2):
   with tempfile.TemporaryDirectory() as td:
    p=project(Path(td)/'project',schema);w=c.Writer(p.root,p.id,p.instance_id);run(w,p,'register',file_request(p,'register','receipt'));w.close()
    request=env('old-partial',dict(entity_ids=['receipt']));old=module('sql_knowledge._core.legacy_storage',ROOT/'tests/fixtures/legacy_runtime/storage.py');old.m=p.memory
    old.finish=lambda *args:(_ for _ in ()).throw(RuntimeError('legacy interrupted'))
    with closing(p.connect(True)) as db:
     with self.assertRaisesRegex(RuntimeError,'legacy interrupted'):old.reconcile(db,request)
    before=keys(p);w=c.Writer(p.root,p.id,p.instance_id)
    try:
     with self.assertRaises(Exception) as caught:run(w,p,'reconcile',request)
     self.assertEqual(caught.exception.code,'legacy_storage_collision');self.assertEqual(before,keys(p))
    finally:w.close()

 def test_unambiguous_legacy_partial_reconcile_resumes(self):
  for schema in (1,2):
   with tempfile.TemporaryDirectory() as td:
    p=project(Path(td)/'project',schema);w=c.Writer(p.root,p.id,p.instance_id);run(w,p,'register',file_request(p,'register','invoice'));w.close()
    request=dict(env('old-normal',dict(entity_ids=['invoice'])),expected_revisions={'invoice':revision(p,'invoice')})
    old=module('sql_knowledge._core.legacy_normal',ROOT/'tests/fixtures/legacy_runtime/storage.py');old.m=p.memory;old.finish=lambda *a:(_ for _ in ()).throw(RuntimeError('interrupted'))
    with closing(p.connect(True)) as db:
     with self.assertRaisesRegex(RuntimeError,'interrupted'):old.reconcile(db,request)
    w=c.Writer(p.root,p.id,p.instance_id)
    try:
     result=run(w,p,'reconcile',request);self.assertEqual(result['records'],[dict(id='invoice',availability='present',related_ids=[])]);before=keys(p);self.assertEqual(run(w,p,'reconcile',request)['records'],result['records']);self.assertEqual(before,keys(p))
    finally:w.close()

 def test_legacy_cleanup_resumes_after_unlink_without_false_deletion_claim(self):
  for schema in (1,2):
   with tempfile.TemporaryDirectory() as td:
    p=project(Path(td)/'project',schema);w=c.Writer(p.root,p.id,p.instance_id);registered=file_request(p,'register','invoice',True);run(w,p,'register',registered)
    plan=run(w,p,'cleanup_plan',dict(env('new-plan',dict(entity_ids=['invoice'],min_age_days=0)),expected_revisions={'invoice':revision(p,'invoice')}));w.close()
    request=dict(env('old-delete',dict(plan_id=plan['plan_id'],confirmation_token=plan['confirmation_token'])),expected_revisions={'invoice':revision(p,'invoice')})
    old=module('sql_knowledge._core.legacy_cleanup',ROOT/'tests/fixtures/legacy_runtime/storage.py');old.m=p.memory;original=old.write
    def interrupt(db,envelope,suffix,data,transaction=True):
     if suffix.startswith('result-'):raise RuntimeError('after unlink')
     return original(db,envelope,suffix,data,transaction)
    old.write=interrupt
    with closing(p.connect(True)) as db:
     with self.assertRaisesRegex(RuntimeError,'after unlink'):old.cleanup_execute(db,request)
    self.assertFalse((p.root/registered['data']['path']).exists());w=c.Writer(p.root,p.id,p.instance_id)
    try:
     result=run(w,p,'cleanup_execute',request);self.assertEqual(result['deleted_ids'],[]);self.assertEqual(result['missing_ids'],['invoice']);before=keys(p);again=run(w,p,'cleanup_execute',request);self.assertTrue(again['replayed']);self.assertEqual(before,keys(p))
    finally:w.close()
