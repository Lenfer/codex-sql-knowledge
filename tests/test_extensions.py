import json,sqlite3,tempfile,unittest
from pathlib import Path
from sql_knowledge.engine import Project,initialize
from sql_knowledge import extensions as ex
from tests.helpers import fixture,env
class Extensions(unittest.TestCase):
 def setUp(self):self.t=tempfile.TemporaryDirectory();self.root=Path(self.t.name);self.p=fixture(self.root,True)
 def tearDown(self):self.t.cleanup()
 def test_pending_migration_and_replay(self):
  with self.assertRaisesRegex(ValueError,'pending'):ex.query(self.p,'demo','demo_items')
  self.assertEqual(ex.migrate(self.p)['applied'],[['demo',1]]);self.assertEqual(ex.migrate(self.p)['applied'],[])
  row=dict(id='one',entity_id='asset',value='proof')
  self.assertFalse(ex.upsert(self.p,'demo','demo_items',row,env('one'))['replayed']);self.assertTrue(ex.upsert(self.p,'demo','demo_items',row,env('one'))['replayed'])
  with self.assertRaises(Exception):ex.upsert(self.p,'demo','demo_items',dict(row,value='changed'),env('one'))
  self.assertEqual(ex.query(self.p,'demo','demo_items')['rows'],[row]);self.assertTrue(self.p.operation('check')['ok'])
  with self.p.connect() as db:self.assertEqual(db.execute("SELECT count(*) FROM events WHERE action='extension_upsert'").fetchone()[0],1)
 def test_fields_fk_types_and_namespace(self):
  ex.migrate(self.p)
  for row in [dict(id='x',entity_id='missing',value='x'),dict(id='x',entity_id='asset',value=2),dict(id='x',entity_id='asset',value='x',unknown=True)]:
   with self.assertRaises(Exception):ex.upsert(self.p,'demo','demo_items',row,env('bad'))
  for table in ['entities','demo_items;DROP TABLE entities']:
   with self.assertRaises(Exception):ex.query(self.p,'demo',table)
  with self.p.connect() as db:self.assertEqual(db.execute('SELECT count(*) FROM demo_items').fetchone()[0],0)
 def test_migration_atomic_and_core_protected(self):
  f=self.root/'extensions/demo/001.sql';f.write_text(f.read_text()+"UPDATE entities SET title='corrupt';")
  with self.assertRaisesRegex(Exception,'not authorized'):ex.migrate(self.p)
  with self.p.connect() as db:
   self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='demo_items'").fetchone());self.assertEqual(db.execute("SELECT title FROM entities WHERE id='asset'").fetchone()[0],'Gold proof')
 def test_checksum_and_version_and_two_projects(self):
  ex.migrate(self.p);f=self.root/'extensions/demo/001.sql';original=f.read_text();f.write_text(original+'-- modified')
  with self.assertRaisesRegex(ValueError,'checksum'):ex.query(self.p,'demo','demo_items')
  f.write_text(original)
  with tempfile.TemporaryDirectory() as tmp:
   other=fixture(Path(tmp))
   with self.assertRaises(ValueError):ex.query(other,'demo','demo_items')
  data=(self.root/'knowledge.toml').read_text().replace('project_id = "project"','project_id = "wrong"');(self.root/'knowledge.toml').write_text(data)
  with self.assertRaisesRegex(ValueError,'identity'):Project(str(self.root))
 def test_add_column_preserves_data(self):
  ex.migrate(self.p);ex.upsert(self.p,'demo','demo_items',dict(id='one',entity_id='asset',value='kept'),env('one'))
  f=self.root/'extensions/demo/extension.toml';s=f.read_text().replace('version = 1','version = 2').replace('migrations = ["001.sql"]','migrations = ["001.sql", "002.sql"]').replace('"entity_id", "value"]','"entity_id", "value", "score"]',1);f.write_text(s)
  (f.parent/'002.sql').write_text('ALTER TABLE demo_items ADD COLUMN score INTEGER NOT NULL DEFAULT 0;')
  f=self.root/'knowledge.toml';f.write_text(f.read_text().replace('version = 1\npath','version = 2\npath'));self.p=Project(str(self.root));ex.migrate(self.p)
  self.assertEqual(ex.query(self.p,'demo','demo_items')['rows'][0],dict(id='one',entity_id='asset',value='kept',score=0))
if __name__=='__main__':unittest.main()
