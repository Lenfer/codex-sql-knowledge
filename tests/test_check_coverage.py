import tempfile,time,sqlite3,unittest,json,hashlib,zlib
from pathlib import Path
from contextlib import closing
from tests.helpers import fixture,env
from sql_knowledge import check_jobs as jobs
class Coverage(unittest.TestCase):
 def audit(self,p):
  job=jobs.start(p,False)
  for _ in range(200):
   job=jobs.step(p,job['job_id'],10)
   if job['status']!='running':return jobs.result(p,job['job_id'])
   if job['phase']=='physical':time.sleep(.02)
  self.fail('check did not finish')
 def test_relations_receipts_and_original_unchanged(self):
  with tempfile.TemporaryDirectory() as td:
   p=fixture(Path(td))
   with p.connect() as db:rev={i:p.revision(db,i) for i in ['project','asset']}
   p.operation('link',envelope=dict(env('link',dict(source_id='project',target_id='asset',predicate='relates_to',id='link')),expected_revisions=rev))
   self.assertTrue(self.audit(p)['ok'])
   with closing(sqlite3.connect(p.database)) as db:
    db.execute("UPDATE relations SET source_ref='test:tampered'");db.execute('DROP TRIGGER events_no_update');db.execute("UPDATE events SET result_json='{}' WHERE operation_key='link'");db.commit()
   report=self.audit(p);kinds={r['kind'] for r in report['problems']}
   self.assertIn('relation_history_mismatch',kinds);self.assertIn('receipt_mismatch',kinds)
   self.assertEqual(report['status'],'complete');self.assertFalse(report['ok'])
 def test_removed_extension_declaration_cannot_pass_either_audit(self):
  from tests.test_identity_cas import project
  from sql_knowledge import extensions as ex
  from sql_knowledge.engine import Project
  for version in (1,2):
   with tempfile.TemporaryDirectory() as td:
    p=project(Path(td)/'p',version,True);ex.migrate(p);ex.upsert(p,'demo','demo_items',dict(id='row',entity_id='project',value='original'),env('extension'))
    with closing(p.connect(True)) as db:db.execute("UPDATE demo_items SET value='tampered'");db.commit()
    manifest=p.root/'knowledge.toml';manifest.write_text(manifest.read_text().split('[extensions.demo]')[0]);p=Project(p.root,_local=True)
    for report in (p.operation('check'),self.audit(p)):
     kinds={r['kind'] for r in report['problems']};self.assertFalse(report['ok']);self.assertIn('extension_undeclared',kinds);self.assertIn('extension_table_undeclared',kinds);self.assertIn('extension_history_undeclared',kinds)
 def test_unknown_history_and_live_table_independently_detected(self):
  from tests.test_identity_cas import project
  from sql_knowledge import extensions as ex
  from sql_knowledge.engine import Project
  for version in (1,2):
   with tempfile.TemporaryDirectory() as td:
    p=project(Path(td)/'p',version,True);ex.migrate(p);ex.upsert(p,'demo','demo_items',dict(id='row',entity_id='project',value='original'),env('extension'))
    manifest=p.root/'knowledge.toml';manifest.write_text(manifest.read_text().split('[extensions.demo]')[0]);p=Project(p.root,_local=True)
    with closing(p.connect(True)) as db:db.execute('DROP TABLE demo_items');db.execute('DELETE FROM _sqlk_migrations');db.commit()
    for report in (p.operation('check'),self.audit(p)):
     self.assertFalse(report['ok']);self.assertIn('extension_history_undeclared',{r['kind'] for r in report['problems']})
    with closing(p.connect(True)) as db:db.execute('CREATE TABLE rogue_items(id TEXT PRIMARY KEY)');db.commit()
    for report in (p.operation('check'),self.audit(p)):
     self.assertFalse(report['ok']);self.assertIn('extension_table_undeclared',{r['kind'] for r in report['problems']})
 def test_wrong_kind_core_details_are_not_hidden_from_full_audit(self):
  from tests.test_identity_cas import project
  for version in (1,2):
   for tables in [('knowledge',),('decisions',),('knowledge','decisions')]:
    with self.subTest(version=version,tables=tables),tempfile.TemporaryDirectory() as td:
     p=project(Path(td)/'p',version)
     p.operation('entity',envelope=env('asset',dict(id='asset',kind='report',title='Asset',status='active')))
     self.assertTrue(self.audit(p)['ok'])
     with closing(p.connect(True)) as db:
      if 'knowledge' in tables:db.execute("INSERT INTO knowledge VALUES ('asset','finding','Hidden knowledge','observed','[]',NULL)")
      if 'decisions' in tables:db.execute("INSERT INTO decisions VALUES ('asset','hidden','Hidden decision',NULL,'[]',NULL)")
      db.commit();self.assertEqual(list(db.execute('PRAGMA foreign_key_check')),[])
     report=self.audit(p);self.assertEqual(report['status'],'complete');self.assertFalse(report['ok'])
     found=[r for r in report['problems'] if r['kind']=='detail_kind_mismatch']
     self.assertEqual({r['table'] for r in found},set(tables));self.assertEqual(len(found),len(tables));self.assertTrue(all(r['id']=='asset' for r in found))
 def test_reverse_detail_keysets_visit_null_missing_and_negative_rows(self):
  from tests.test_identity_cas import project
  for version in (1,2):
   with self.subTest(version=version),tempfile.TemporaryDirectory() as td:
    p=project(Path(td)/'p',version)
    with closing(p.connect(True)) as db:
     db.execute('PRAGMA foreign_keys=OFF')
     db.execute("INSERT INTO knowledge(rowid,entity_id,knowledge_type,body,confidence,evidence_json) VALUES (-5,NULL,'finding','Null target','observed','[]')")
     db.execute("INSERT INTO knowledge VALUES ('missing','finding','Missing target','observed','[]',NULL)")
     db.execute("INSERT INTO decisions(rowid,entity_id,scope,body,evidence_json) VALUES (-8,NULL,'hidden','Null decision','[]')")
     db.commit()
    job=jobs.start(p,False)
    for _ in range(300):
     job=jobs.step(p,job['job_id'],1)
     if job['status']!='running':break
     if job['phase']=='physical':time.sleep(.02)
    report=jobs.result(p,job['job_id']);self.assertEqual(report['status'],'complete');self.assertFalse(report['ok'])
    found=[r for r in report['problems'] if r['kind']=='detail_entity_missing'];self.assertEqual(len(found),3)
    self.assertEqual({(r['table'],r['id']) for r in found},{('knowledge',None),('knowledge','missing'),('decisions',None)})
 def test_receipt_id_and_event_before_continuity(self):
  from tests.test_identity_cas import project
  for version in (1,2):
   for corruption,expected in [('receipt_id','receipt_id_mismatch'),('before_continuity','event_continuity_mismatch')]:
    with self.subTest(version=version,corruption=corruption),tempfile.TemporaryDirectory() as td:
     p=project(Path(td)/'p',version)
     result=p.operation('entity',envelope=env('asset',dict(id='asset',kind='report',title='Asset',status='active')))
     p.operation('entity',envelope=dict(env('update',dict(id='asset',kind='report',title='Updated',status='active')),expected_revision=result['revision']))
     column='result_json' if corruption=='receipt_id' else 'before_json'
     with closing(p.connect(True)) as db:
      payload=json.loads(db.execute('SELECT '+column+" FROM events WHERE operation_key='update'").fetchone()[0])
      if corruption=='receipt_id':payload['id']='wrong-id'
      else:payload['entities']['asset']['entity']['title']='Wrong previous state'
      raw=json.dumps(payload,sort_keys=True,separators=(',',':'))
      if version==1:
       db.execute('DROP TRIGGER events_no_update');db.execute('UPDATE events SET '+column+"=? WHERE operation_key='update'",(raw,))
      else:
       digest=hashlib.sha256(raw.encode()).hexdigest();db.execute('INSERT OR IGNORE INTO _event_payloads VALUES (?,?,?,?)',(digest,'zlib3',len(raw.encode()),zlib.compress(raw.encode(),3)))
       db.execute('DROP TRIGGER records_no_update');db.execute('UPDATE _event_records SET '+('result_ref' if corruption=='receipt_id' else 'before_ref')+"=? WHERE operation_key='update'",(digest,))
      db.commit()
     report=self.audit(p);self.assertEqual(report['status'],'complete');self.assertFalse(report['ok']);self.assertIn(expected,{r['kind'] for r in report['problems']})
 def test_valid_typed_receipts_and_supersession_continuity_pass(self):
  from tests.test_identity_cas import project
  from sql_knowledge import extensions as ex
  for version in (1,2):
   with self.subTest(version=version),tempfile.TemporaryDirectory() as td:
    p=project(Path(td)/'p',version,True)
    for action,status in [('knowledge','active'),('decision','accepted')]:
     common=dict(title='Original',status=status,body='Durable body',evidence=[dict(ref='test:proof')])
     common.update(dict(knowledge_type='finding',confidence='observed') if action=='knowledge' else dict(scope='test',authority_ref='test:authorization'))
     old=action+'-old';new=action+'-new';first=p.operation(action,envelope=env(old,dict(common,id=old)))
     p.operation(action,envelope=dict(env(new,dict(common,id=new,title='Replacement',supersedes=old)),expected_revisions={old:first['revision']}))
    ex.migrate(p);ex.upsert(p,'demo','demo_items',dict(id='extension-row',entity_id='project',value='proof'),env('extension'))
    self.assertTrue(self.audit(p)['ok'])
if __name__=='__main__':unittest.main()
