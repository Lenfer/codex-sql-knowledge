import asyncio,json,os,shutil,sqlite3,tempfile,time,unittest,sys
from pathlib import Path
from contextlib import closing
from sql_knowledge.coordinator import submit,paths
from sql_knowledge.engine import Project,initialize
from sql_knowledge import extensions as ex
from tests.helpers import ROOT,fixture,env
from mcp import ClientSession,StdioServerParameters
from mcp.client.stdio import stdio_client

def schema2(root):
 root.mkdir()
 with closing(sqlite3.connect(root/'knowledge.sqlite')) as db:
  db.executescript((ROOT/'tests/fixtures/schema2.sql').read_text())
  db.execute('PRAGMA user_version=2');db.execute("INSERT INTO schema_meta VALUES (2,'test','review regression')");db.execute("INSERT INTO entities VALUES ('project','project','Fixture','',NULL,'active','{}','now','now')");db.commit()
 (root/'knowledge.toml').write_text('format_version = 1\nproject_id = "project"\nschema_version = 2\n[extensions.demo]\nversion = 1\npath = "extensions/demo/extension.toml"\n');shutil.copytree(ROOT/'examples/demo/extensions',root/'extensions')
 p=Project(root);p.operation('entity',envelope=dict(env('seed',dict(id='project',kind='project',status='active',title='Fixture')),expected_revision=0));p.operation('entity',envelope=env('asset',dict(id='asset',kind='report',status='active',title='Gold proof')));return p

def revision(p,eid):
 with p.connect() as db:return p.revision(db,eid)

def shutdown(p):
 submit(p,'shutdown',{})
 end=time.monotonic()+5
 while paths(p.root)[0].exists() and time.monotonic()<end:time.sleep(.01)
 if paths(p.root)[0].exists():raise AssertionError('coordinator did not drain')

def backup(p,destination):
 with p.connect() as db,closing(sqlite3.connect(destination)) as copy:db.backup(copy)

def upgrade(p,required=True):
 f=p.root/'extensions/demo/extension.toml';s=f.read_text().replace('version = 1','version = 2').replace('["001.sql"]','["001.sql", "002.sql"]');s=s.replace('columns = ["id", "entity_id", "value"]','columns = ["id", "entity_id", "value", "score"]')
 if required:s=s.replace('required = ["id", "entity_id", "value"]','required = ["id", "entity_id", "value", "score"]')
 f.write_text(s);(f.parent/'002.sql').write_text('ALTER TABLE demo_items ADD COLUMN score INTEGER NOT NULL DEFAULT 0;');f=p.root/'knowledge.toml';f.write_text(f.read_text().replace('version = 1\npath','version = 2\npath'));q=Project(p.root);ex.migrate(q);return q

class ReviewRegressions(unittest.TestCase):
 def test_identity_guard_both_schemas(self):
  with tempfile.TemporaryDirectory() as td:
   for version in [1,2]:
    p=fixture(Path(td)/'v1') if version==1 else schema2(Path(td)/'v2')
    with p.connect() as db:before=db.execute('SELECT count(*) FROM events').fetchone()[0]
    for data in [dict(id='second',kind='project',status='active',title='Other'),dict(id='project',kind='project',status='archived',title='Close'),dict(id='project',kind='report',status='active',title='Change')]:
     with self.assertRaises(ValueError):p.operation('entity',envelope=env('deny'+data['title'],data))
    with p.connect() as db:self.assertEqual(db.execute('SELECT count(*) FROM events').fetchone()[0],before)
    p.operation('entity',envelope=dict(env('rename',dict(id='project',kind='project',status='active',title='Renamed')),expected_revision=revision(p,'project')));self.assertEqual(Project(p.root).operation('context')['project_id'],'project')
 def test_live_handle_rejects_foreign_database_restore(self):
  with tempfile.TemporaryDirectory() as td:
   p=fixture(Path(td)/'a');initialize(str(Path(td)/'b'),'other');other=Project(Path(td)/'b');saved=Path(td)/'correct.sqlite';foreign=Path(td)/'foreign.sqlite'
   backup(p,saved);backup(other,foreign);shutdown(p);os.replace(foreign,p.database)
   with self.assertRaisesRegex(ValueError,'identity'):p.operation('context')
   with self.assertRaisesRegex(ValueError,'identity'):p.operation('entity',envelope=env('bad',dict(id='bad',kind='report',title='No',status='active')))
   os.replace(saved,p.database)
   with self.assertRaisesRegex(ValueError,'identity'):p.operation('context')
   self.assertEqual(Project(p.root).operation('context')['project_id'],'project')
 def test_schema2_extension_event_subject_and_replay(self):
  with tempfile.TemporaryDirectory() as td:
   p=schema2(Path(td)/'v2');ex.migrate(p);row=dict(id='one',entity_id='asset',value='first')
   ex.upsert(p,'demo','demo_items',row,env('ext1'));ex.upsert(p,'demo','demo_items',dict(row,value='second'),dict(env('ext2'),expected_revision=revision(p,'asset')));self.assertTrue(ex.upsert(p,'demo','demo_items',row,env('ext1'))['replayed'])
   self.assertEqual([x['operation_key'] for x in p.operation('history',dict(id='asset'))['events']],['ext2','ext1','asset']);self.assertEqual([x['operation_key'] for x in p.operation('history',dict(id='project'))['events']],['seed']);self.assertTrue(p.operation('check')['ok'])
 def test_extension_audit_and_legitimate_migration(self):
  with tempfile.TemporaryDirectory() as td:
   p=fixture(Path(td),True);ex.migrate(p);row=dict(id='one',entity_id='asset',value='kept');ex.upsert(p,'demo','demo_items',row,env('one'));p=upgrade(p);self.assertTrue(p.operation('check')['ok'])
   with p.connect(True) as db:db.execute("UPDATE demo_items SET score=9 WHERE id='one'")
   self.assertIn('extension_history_mismatch',{x['kind'] for x in p.operation('check')['problems']})
   with p.connect(True) as db:db.execute("DELETE FROM demo_items WHERE id='one'")
   self.assertIn('extension_row_missing',{x['kind'] for x in p.operation('check')['problems']})
   f=p.root/'extensions/demo/001.sql';f.write_text(f.read_text()+'-- tampered')
   self.assertIn('extension_schema',{x['kind'] for x in p.operation('check')['problems']})
 def test_composite_and_nontext_pk_roll_back(self):
  for typ in ['composite','integer']:
   with tempfile.TemporaryDirectory() as td:
    p=fixture(Path(td),True);f=p.root/'extensions/demo/001.sql';s=f.read_text();s=s.replace('id TEXT PRIMARY KEY','id INTEGER PRIMARY KEY') if typ=='integer' else s.replace('id TEXT PRIMARY KEY','id TEXT').replace('value TEXT NOT NULL);','value TEXT NOT NULL,PRIMARY KEY(id,entity_id));');f.write_text(s)
    with self.assertRaisesRegex(ValueError,'TEXT primary key'):ex.migrate(p)
    with p.connect() as db:self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='demo_items'").fetchone())
 def test_receipt_before_new_required_fields(self):
  with tempfile.TemporaryDirectory() as td:
   p=fixture(Path(td),True);ex.migrate(p);row=dict(id='one',entity_id='asset',value='kept');ex.upsert(p,'demo','demo_items',row,env('old'));q=upgrade(p)
   self.assertTrue(ex.upsert(q,'demo','demo_items',row,env('old'))['replayed'])
   with self.assertRaises(Exception):ex.upsert(q,'demo','demo_items',dict(row,value='different'),env('old'))
   with self.assertRaises(ValueError):ex.upsert(q,'demo','demo_items',row,env('new'))
   with self.assertRaises(ValueError):ex.query(p,'demo','demo_items')
   self.assertEqual(ex.query(q,'demo','demo_items')['rows'][0]['score'],0)
 def test_fts_mapping_and_orphan_audit(self):
  with tempfile.TemporaryDirectory() as td:
   p=fixture(Path(td));p.operation('entity',envelope=dict(env('update',dict(id='asset',kind='report',title='Silver',status='active')),expected_revision=revision(p,'asset')));self.assertEqual(p.operation('search',dict(text='Gold'))['rows'],[]);self.assertTrue(p.operation('check')['ok'])
   with p.connect(True) as db:db.execute("UPDATE _sqlk_search_documents SET fts_rowid=999 WHERE entity_id='asset'")
   self.assertIn('search_mapping_mismatch',{x['kind'] for x in p.operation('check')['problems']})

class ReviewMCP(unittest.IsolatedAsyncioTestCase):
 async def test_generic_schema_and_restart_after_migration(self):
  with tempfile.TemporaryDirectory() as td:
   p=fixture(Path(td),True);ex.migrate(p)
   async def session(upgrade_now=False):
    with (p.root/'stderr.txt').open('a') as log:
     async with stdio_client(StdioServerParameters(command=sys.executable,args=['-m','sql_knowledge.mcp','--project',td]),errlog=log) as io:
      async with ClientSession(*io) as c:
       await c.initialize();tools={t.name:t for t in (await c.list_tools()).tools};text=json.dumps(tools['entity_write'].inputSchema);self.assertNotIn('building',text);self.assertNotIn('scene',text);self.assertNotIn('primary_building',json.dumps(tools['relation_write'].inputSchema))
       if upgrade_now:upgrade(p)
       r=await c.call_tool('extension_query',dict(extension='demo',table='demo_items'));self.assertEqual(bool(r.isError),upgrade_now)
   await session(True);await session(False)
