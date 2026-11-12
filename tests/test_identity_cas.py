"""Pinned identity, atomic revisions, portable schema2, and SQLite defaults."""
import hashlib,json,os,shutil,sqlite3,tempfile,unittest
from pathlib import Path
from sql_knowledge.engine import Project,initialize
from sql_knowledge import extensions as ex
from tests.helpers import ROOT,env


def project(root,version=1,extension=False):
 if version==1:initialize(str(root),'project')
 else:
  root.mkdir()
  with sqlite3.connect(root/'knowledge.sqlite') as db:
   db.executescript((ROOT/'tests/fixtures/schema2.sql').read_text())
   db.execute('PRAGMA user_version=2');db.execute("INSERT INTO schema_meta VALUES(2,'fixture-created','portable schema2')")
   db.execute("INSERT INTO entities VALUES('project','project','Fixture','',NULL,'active','{}','fixture-created','fixture-created')")
  (root/'knowledge.toml').write_text('format_version = 1\nproject_id = "project"\nschema_version = 2\n')
 if extension:
  shutil.copytree(ROOT/'examples/demo/extensions',root/'extensions')
  with (root/'knowledge.toml').open('a') as f:f.write('[extensions.demo]\nversion = 1\npath = "extensions/demo/extension.toml"\n')
 p=Project(root,_local=True)
 if version==2:p.operation('entity',envelope=dict(env('seed',dict(id='project',kind='project',status='active',title='Fixture')),expected_revision=0))
 return p


def update(p,key,eid='item',**extra):
 return dict(env(key,dict(id=eid,kind='report',status='active',title=key)),**extra)


class IdentityCAS(unittest.TestCase):
 def test_portable_fixture_hash(self):
  path=ROOT/'tests/fixtures/schema2.sql';meta=json.loads(path.with_suffix('.metadata.json').read_text())
  self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),meta['sha256'])
 def test_whitespace_and_atomic_conflict_both_schemas(self):
  with tempfile.TemporaryDirectory() as td:
   for version in (1,2):
    p=project(Path(td)/str(version),version)
    for n,data in enumerate([dict(id='second ',kind=' project ',status=' active ',title='Other'),dict(id=' project ',kind='project',status='archived',title='Close')]):
     with self.assertRaises(ValueError):p.operation('entity',envelope=env('invalid'+str(n),data))
    with p.connect() as db:
     n=db.execute('SELECT count(*) FROM events').fetchone()[0];rev=p.revision(db,'project')
    rename=dict(env('rename',dict(id=' project ',kind=' project ',status=' active ',title='Renamed')),expected_revision=rev)
    p.operation('entity',envelope=rename)
    result=p.operation('entity',envelope=update(p,'create'))
    for request,code in [(update(p,'missing'),'revision_required'),(update(p,'stale',expected_revision=result['revision']-1),'revision_conflict')]:
     with self.assertRaises(Exception) as caught:p.operation('entity',envelope=request)
     self.assertEqual(caught.exception.code,code)
    good=update(p,'good',expected_revision=result['revision']);changed=p.operation('entity',envelope=good)
    self.assertGreater(changed['revision'],result['revision']);self.assertEqual(p.operation('entity',envelope=good),dict(changed,replayed=True))
    replay=p.operation('entity',envelope=update(p,'create'));self.assertTrue(replay['replayed']);self.assertEqual(replay['revision'],result['revision'])
    with p.connect() as db:self.assertEqual(db.execute('SELECT count(*) FROM events').fetchone()[0],n+3)
 def test_manifest_canonicalization_and_generation(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td)/'new';initialize(str(root),' project ');p=Project(root,_local=True)
   self.assertEqual(p.id,'project')
   with p.connect() as db:
    self.assertEqual(db.execute('PRAGMA journal_mode').fetchone()[0],'wal');self.assertEqual(db.execute('PRAGMA synchronous').fetchone()[0],2)
    self.assertEqual(db.execute("SELECT value FROM _sqlk_meta WHERE key='instance_id'").fetchone()[0],p.instance_id)
   manifest=root/'knowledge.toml';manifest.write_text(manifest.read_text().replace('"project"','" project "'))
   self.assertEqual(Project(root).instance_id,p.instance_id)
   manifest.write_text(manifest.read_text().replace(p.instance_id,'foreign-generation'))
   with self.assertRaisesRegex(ValueError,'identity'):p.operation('context')
 def test_foreign_database_and_manifest_reject_existing_handles(self):
  for version in (1,2):
   with tempfile.TemporaryDirectory() as td:
    root=Path(td);p=project(root/'a',version);q=project(root/'b',version)
    # Even a same-ID independently initialized database is a new generation.
    with q.connect(True) as db:db.execute("UPDATE schema_meta SET created_at='foreign-generation'")
    with p.connect() as db:pass
    q=Project(q.root,_local=True)
    replacement=root/'replacement.sqlite'
    with q.connect() as src,sqlite3.connect(replacement) as dst:src.backup(dst)
    os.replace(replacement,p.database);shutil.copyfile(q.root/'knowledge.toml',p.root/'knowledge.toml')
    for action in [lambda:p.operation('context'),lambda:p.operation('entity',envelope=update(p,'foreign')),lambda:ex.migrate(p)]:
     with self.assertRaisesRegex(ValueError,'identity'):action()
 def test_legacy_instance_derivation_does_not_write(self):
  with tempfile.TemporaryDirectory() as td:
   p=project(Path(td)/'legacy',2);before=p.database.read_bytes();q=Project(p.root)
   self.assertEqual(p.instance_id,q.instance_id);self.assertTrue(p.instance_id.startswith('legacy-'));self.assertEqual(before,p.database.read_bytes())
 def test_persistent_connection_and_relation_cas(self):
  with tempfile.TemporaryDirectory() as td:
   p=project(Path(td)/'p');db=p.connect(True);p._writer_connection=db
   try:
    one=p.operation('entity',envelope=update(p,'one','one'));two=p.operation('entity',envelope=update(p,'two','two'))
    command=env('link',dict(source_id=' one ',target_id='two',predicate='supports'))
    with self.assertRaises(Exception):p.operation('link',envelope=command)
    command['expected_revisions']={'one':one['revision'],'two':two['revision']}
    result=p.operation('link',envelope=command);self.assertEqual(p.revision(db,'one'),result['revision']);self.assertEqual(db.execute('SELECT 1').fetchone()[0],1)
   finally:db.close()
 def test_extension_cas_replay_and_default_affinity(self):
  with tempfile.TemporaryDirectory() as td:
   for version in (1,2):
    p=project(Path(td)/str(version),version,True);item=p.operation('entity',envelope=update(p,'item'));ex.migrate(p)
    row=dict(id='row',entity_id='item',value='first');first=ex.upsert(p,'demo','demo_items',row,env('extension'))
    with self.assertRaises(Exception) as caught:ex.upsert(p,'demo','demo_items',dict(row,value='second'),env('missing'))
    self.assertEqual(caught.exception.code,'revision_required')
    stale=dict(env('stale'),expected_revision=item['revision'])
    with self.assertRaises(Exception) as caught:ex.upsert(p,'demo','demo_items',dict(row,value='second'),stale)
    self.assertEqual(caught.exception.code,'revision_conflict')
    second=ex.upsert(p,'demo','demo_items',dict(row,value='second'),dict(env('second'),expected_revision=first['revision']))
    self.assertGreater(second['revision'],first['revision']);self.assertEqual(ex.upsert(p,'demo','demo_items',row,env('extension'))['revision'],first['revision'])
    with p.connect() as db:
     for typ,default,value in [('TEXT','0','0'),('INTEGER',"'02'",2),('NUMERIC',"'3.0e+5'",300000),('REAL','7',7.0),('TEXT','NULL',None),('TEXT',"'hello'",'hello')]:
      self.assertEqual(ex.default_with_affinity(db,dict(type=typ,dflt_value=default)),value)
    self.assertTrue(p.operation('check')['ok'])
