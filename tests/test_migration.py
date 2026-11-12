"""Tiny legacy fixtures: migration losslessness, vocabulary, replay and refusal."""
import asyncio,json,shutil,sqlite3,sys,tempfile,unittest
from contextlib import closing
from pathlib import Path
from mcp import ClientSession,StdioServerParameters
from mcp.client.stdio import stdio_client
from sql_knowledge.engine import BASE,Project,module
from sql_knowledge.migration import migrate,verify,file_hash,stop_writer,audit
from sql_knowledge.coordinator import submit
from sql_knowledge import extensions
from tests.helpers import ROOT,env


def legacy(root):
 root.mkdir();memory=module('legacy_migration',BASE/'memory.py');memory.ROOT=root
 oldmodel=module('sql_knowledge._core.legacy_model',ROOT/'tests/fixtures/legacy_runtime/mcp_server.py')
 envelope=oldmodel.EntityEnvelope.model_validate(env('old-building',dict(id='house',kind='building',title='Дом',status='active',attributes={'address':'Мурино','location':{'lat':60.0,'lon':30.0}}))).model_dump(exclude_unset=True)
 with closing(sqlite3.connect(root/'knowledge.sqlite')) as db:
  db.row_factory=sqlite3.Row;db.executescript((BASE/'schema.sql').read_text().split('-- Internal derived FTS lookup')[0]);db.execute("INSERT INTO schema_meta VALUES(1,'legacy','legacy fixture')");db.commit()
  memory.mutate(db,'entity',env('project',dict(id='project-mavis-murino',kind='project',title='MAVIS',status='active')))
  memory.mutate(db,'entity',envelope)
  memory.mutate(db,'entity',env('scene',dict(id='scene',kind='scene',title='Scene',status='active')))
 (root/'knowledge.sqlite').chmod(0o400)
 return root/'knowledge.sqlite',envelope


def revision(p,eid):
 with closing(p.connect()) as db:return p.revision(db,eid)


class Migration(unittest.TestCase):
 def test_lossless_and_safe_repeat_and_wrong_identity(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);source,_=legacy(root/'legacy');before=file_hash(source)
   with self.assertRaisesRegex(ValueError,'identity'):migrate(source,root/'bad','wrong')
   self.assertFalse((root/'bad').exists());out=root/'migrated';result=migrate(source,out,'project-mavis-murino')
   self.assertTrue(result['ok']);self.assertEqual(result['verification']['appended_extension_events'],2);self.assertEqual(file_hash(source),before)
   with self.assertRaises(FileExistsError):migrate(source,out,'project-mavis-murino')
   self.assertTrue(verify(source,out/'project','project-mavis-murino')['ok'])
   checked=audit(out/'project',root/'audit.json');self.assertTrue(checked['ok']);self.assertTrue(checked['scratch_removed'])
   with closing(sqlite3.connect(out/'project/knowledge.sqlite')) as db:
    with self.assertRaisesRegex(sqlite3.OperationalError,'_sqlk_writer_authorized'):db.execute("UPDATE entities SET title='old writer' WHERE id='house'")
    # Deliberate corruption tests can simulate a privileged external writer.
    db.create_function('_sqlk_writer_authorized',0,lambda:1);db.execute("UPDATE entities SET title='tampered' WHERE id='house'");db.commit()
   self.assertFalse(verify(source,out/'project','project-mavis-murino')['ok']);self.assertEqual(file_hash(source),before)
 def test_unknown_tables_refused_without_output(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);source,_=legacy(root/'legacy')
   source.chmod(0o600)
   with closing(sqlite3.connect(source)) as db:db.execute('CREATE TABLE unknown_subject(id TEXT)');db.commit()
   source.chmod(0o400)
   with self.assertRaisesRegex(ValueError,'unknown source tables'):migrate(source,root/'out','project-mavis-murino')
   self.assertFalse((root/'out').exists())
 def test_local_policy_and_kind_owned_extension(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);source,_=legacy(root/'legacy');out=root/'out';migrate(source,out,'project-mavis-murino');p=Project(out/'project');submit(p,'resume',{})
   try:
    for target in ['scene','house']:
     row=dict(id='mismatch',entity_id=target,import_revision=0,provenance_ref='test:fixture')
     if target=='scene':
      with self.assertRaisesRegex(Exception,'entity kind'):extensions.upsert(p,'mavis','mavis_buildings',row,env('wrong-owner'))
    p.operation('entity',envelope=env('other-building',dict(id='house2',kind='building',title='House2',status='active')))
    relation=dict(source_id='scene',target_id='house',predicate='primary_building')
    p.operation('link',envelope=dict(env('first-primary',relation),expected_revisions={e:revision(p,e) for e in ('scene','house')}))
    with self.assertRaisesRegex(Exception,'one target'):
     p.operation('link',envelope=dict(env('second-primary',dict(relation,target_id='house2')),expected_revisions={e:revision(p,e) for e in ('scene','house2')}))
    (root/'legacy/knowledge.toml').write_text('format_version = 1\nproject_id = \"project-mavis-murino\"\nschema_version = 1\n')
    self.assertNotIn('building',Project(root/'legacy').memory.ARTIFACT_KINDS)
   finally:stop_writer(p)
 def test_null_cas_compatibility_does_not_erase_real_revision(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);source,old=legacy(root/'legacy');out=root/'out';migrate(source,out,'project-mavis-murino');p=Project(out/'project');submit(p,'resume',{})
   try:
    for shape in [old,dict(old,expected_revision=None),dict(old,expected_revision=None,expected_revisions=None)]:self.assertTrue(p.operation('entity',envelope=shape)['replayed'])
    with self.assertRaises(Exception):p.operation('entity',envelope=dict(old,expected_revision=123))
    request=dict(env('intermediate',dict(id='extra',kind='report',title='Extra',status='active')),expected_revision=None,expected_revisions=None)
    p.operation('entity',envelope=request);self.assertTrue(p.operation('entity',envelope={k:v for k,v in request.items() if k not in ('expected_revision','expected_revisions')})['replayed'])
   finally:stop_writer(p)


class MigrationMCP(unittest.IsolatedAsyncioTestCase):
 async def test_domain_schema_writes_and_old_receipt_over_stdio(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);source,old=legacy(root/'legacy');out=root/'out';migrate(source,out,'project-mavis-murino');p=Project(out/'project');submit(p,'resume',{})
   try:
    with (root/'mcp.stderr.log').open('w') as err:
     async with stdio_client(StdioServerParameters(command=sys.executable,args=['-B','-m','sql_knowledge.mcp','--project',str(p.root)]),errlog=err) as streams:
      async with ClientSession(*streams) as session:
       await session.initialize();tools=await session.list_tools();tool=next(t for t in tools.tools if t.name=='entity_write')
       self.assertIn('building',str(tool.inputSchema));replay=await session.call_tool('entity_write',dict(envelope=old));self.assertFalse(replay.isError,str(replay));self.assertTrue(json.loads(replay.content[0].text)['replayed'])
       write=await session.call_tool('entity_write',dict(envelope=env('new-house',dict(id='new-house',kind='building',title='New house',status='active'))));self.assertFalse(write.isError,str(write))
       rows=await session.call_tool('extension_query',dict(extension='mavis',table='mavis_buildings'));self.assertFalse(rows.isError,str(rows))
   finally:stop_writer(p)
