"""Offline SQLite migration rehearsal. Never replaces or installs a live project."""
from pathlib import Path
from contextlib import closing
import argparse,datetime,fcntl,hashlib,json,os,shutil,sqlite3,sys,time,uuid
from .engine import Project
from . import extensions

CORE={'schema_meta':'version','entities':'id','knowledge':'entity_id','decisions':'entity_id','relations':'id','events':'seq','event_entities':'event_seq,entity_id'}
PROFILE=Path(__file__).resolve().parent/'profiles/mavis'

def readonly(path):
 db=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=5)
 db.execute('PRAGMA query_only=ON');return db

def file_hash(path):
 digest=hashlib.sha256()
 with Path(path).open('rb') as f:
  for block in iter(lambda:f.read(1024*1024),b''):digest.update(block)
 return digest.hexdigest()

def backup(source,destination,seconds=60):
 source=Path(source).resolve();destination=Path(destination).resolve()
 if destination.exists() or destination==source:raise ValueError('backup destination must be new')
 destination.parent.mkdir(parents=True,exist_ok=True);started=time.monotonic()
 with closing(readonly(source)) as src,closing(sqlite3.connect(destination)) as dest:
  def progress(status,remaining,total):
   if time.monotonic()-started>seconds:raise TimeoutError('backup time budget exceeded')
  src.backup(dest,pages=256,progress=progress,sleep=.02)
 destination.chmod(0o600)
 return dict(method='SQLite backup API, source mode=ro/query_only',elapsed_seconds=time.monotonic()-started,bytes=destination.stat().st_size,sha256=file_hash(destination))

def table_digest(db,table,order,cutoff=None):
 clause=''
 if cutoff is not None and table in ('events','event_entities'):
  clause=' WHERE '+('seq' if table=='events' else 'event_seq')+'<='+str(int(cutoff))
 digest=hashlib.sha256();count=0
 for row in db.execute('SELECT * FROM '+table+clause+' ORDER BY '+order):
  raw=json.dumps(tuple(row),ensure_ascii=False,separators=(',',':')).encode();digest.update(len(raw).to_bytes(8,'big'));digest.update(raw);count+=1
 return dict(rows=count,sha256=digest.hexdigest())

def fingerprints(path,cutoff=None):
 with closing(readonly(path)) as db:
  result={table:table_digest(db,table,order,cutoff) for table,order in CORE.items()}
  digest=hashlib.sha256();count=0
  for row in db.execute('SELECT rowid,entity_id,title,body FROM search_index ORDER BY rowid'):
   raw=json.dumps(tuple(row),ensure_ascii=False,separators=(',',':')).encode();digest.update(len(raw).to_bytes(8,'big'));digest.update(raw);count+=1
  result['fts']=dict(rows=count,sha256=digest.hexdigest())
  return result

def inspect(path,expected_id):
 with closing(readonly(path)) as db:
  if db.execute('PRAGMA user_version').fetchone()[0]!=1:raise ValueError('rehearsal accepts legacy schema1 only')
  if db.execute("SELECT id FROM entities WHERE kind='project' AND status='active'").fetchall()!=[(expected_id,)]:raise ValueError('source project identity mismatch')
  from .engine import BASE
  with closing(sqlite3.connect(':memory:')) as model:
   model.executescript((BASE/'schema.sql').read_text())
   for table in CORE:
    actual=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()
    expected=model.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()
    if not actual or ''.join(actual[0].split())!=''.join(expected[0].split()):raise ValueError('unsupported source DDL: '+table)
  allowed=set(CORE)|{'search_index','search_index_data','search_index_idx','search_index_content','search_index_docsize','search_index_config','sqlite_sequence'}
  tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
  if tables-allowed:raise ValueError('unknown source tables: '+repr(sorted(tables-allowed)))
  if db.execute('PRAGMA foreign_key_check').fetchone():raise ValueError('source foreign keys invalid')
  if db.execute('PRAGMA quick_check').fetchall()!=[('ok',)]:raise ValueError('source quick_check failed')
  return dict(project_id=expected_id,schema_version=1,revision=db.execute('SELECT coalesce(max(seq),0) FROM events').fetchone()[0],entities=db.execute('SELECT count(*) FROM entities').fetchone()[0],events=db.execute('SELECT count(*) FROM events').fetchone()[0],kinds=dict(db.execute('SELECT kind,count(*) FROM entities GROUP BY kind')))

def stop_writer(project):
 from .coordinator import submit,paths
 result=submit(project,'shutdown',{})
 lock=paths(project.root)[1];deadline=time.monotonic()+10
 with lock.open('a+b') as handle:
  while True:
   try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB);break
   except BlockingIOError:
    if time.monotonic()>deadline:raise TimeoutError('writer did not drain')
    time.sleep(.02)
 with closing(sqlite3.connect(project.database)) as db:
  state=list(db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone())
  if state[0]:raise ValueError('writer checkpoint busy')
 return dict(shutdown=result,checkpoint=state)

def _string(value):return value if isinstance(value,str) else None

def _number(value):return value if type(value) in (int,float) else None

def install_write_guard(db):
 """Fail closed for pre-migration writers; not a hostile-local-user boundary."""
 tables=['entities','knowledge','decisions','relations','events','event_entities','_sqlk_search_documents']
 for table in tables:
  for operation in ('INSERT','UPDATE','DELETE'):
   name='_sqlk_guard_'+table+'_'+operation.lower()
   db.execute('CREATE TRIGGER IF NOT EXISTS '+name+' BEFORE '+operation+' ON '+table+" BEGIN SELECT CASE WHEN _sqlk_writer_authorized()!=1 THEN RAISE(ABORT,'SQL Knowledge requires coordinator') END; END")
 return len(tables)*3

def seed_domain(project,source_sha,revision):
 rows=[]
 with closing(project.connect()) as db:
  records=[dict(r) for r in db.execute("SELECT id,kind,attributes_json FROM entities WHERE kind IN ('building','scene') ORDER BY id")]
 for record in records:
  attrs=json.loads(record['attributes_json']);eid=record['id'];row=dict(id=eid,entity_id=eid,import_revision=revision,provenance_ref='sha256:'+source_sha)
  if record['kind']=='building':
   location=attrs.get('location') if isinstance(attrs.get('location'),dict) else {}
   row.update(address=_string(attrs.get('address')),native_house_id=_string(attrs.get('native_yandex_house_id')),native_object_id=_string(attrs.get('native_yandex_3d_object_id',attrs.get('yandex_3d_object_id'))),latitude=_number(location.get('latitude',location.get('lat',attrs.get('lat')))),longitude=_number(location.get('longitude',location.get('lon',attrs.get('lon')))))
   table='mavis_buildings'
  else:
   value=attrs.get('terrain_parameters');row.update(scope=_string(attrs.get('scope')),pipeline_stage=_string(attrs.get('pipeline_stage')),terrain_parameters_json=json.dumps(value,ensure_ascii=False,sort_keys=True) if value is not None else None);table='mavis_scenes'
  envelope=dict(idempotency_key='mavis-import:'+source_sha[:16]+':'+table+':'+eid,actor='sql-knowledge-migration',reason='Add project-owned extension record; preserve all legacy core data',source_ref='sha256:'+source_sha)
  rows.append(dict(table=table,id=eid,result=extensions.upsert(project,'mavis',table,row,envelope)))
 return rows

def verify(input_db,project_root,expected_id,baseline=None):
 info=inspect(input_db,expected_id);baseline=baseline or fingerprints(input_db)
 project=Project(Path(project_root).resolve());actual=fingerprints(project.database,info['revision'])
 differences=[name for name in baseline if actual[name]!=baseline[name]]
 with closing(project.connect()) as db:
  guards=db.execute("SELECT count(*) FROM sqlite_master WHERE type='trigger' AND name GLOB '_sqlk_guard_*'").fetchone()[0]
  if guards!=21:differences.append('missing write guards')
  integrity=[r[0] for r in db.execute('PRAGMA integrity_check')];fk=[list(r) for r in db.execute('PRAGMA foreign_key_check')]
  spec=extensions.specs(project)['mavis'];extensions.verify(project,db,spec)
  expected=info['kinds'].get('building',0)+info['kinds'].get('scene',0)
  appended=db.execute('SELECT count(*) FROM events WHERE seq>?',(info['revision'],)).fetchone()[0]
  imports=db.execute('SELECT count(*) FROM mavis_buildings').fetchone()[0]+db.execute('SELECT count(*) FROM mavis_scenes').fetchone()[0]
  if appended!=expected or imports!=expected:differences.append('extension seed count')
  if db.execute("SELECT count(*) FROM events WHERE seq>? AND action!='extension_upsert'",(info['revision'],)).fetchone()[0]:differences.append('unexpected appended action')
  for table,kind in [('mavis_buildings','building'),('mavis_scenes','scene')]:
   if db.execute('SELECT count(*) FROM '+table+' x JOIN entities e ON e.id=x.entity_id WHERE e.kind!=?',(kind,)).fetchone()[0]:differences.append('wrong extension ownership')
  searches=[]
  for text in ['Мурино','Шувалова','несуществующиймаркер987654321']:
   expression=' OR '.join('"'+w.replace('"','""')+'"' for w in text.split())
   with closing(readonly(input_db)) as src:
    expected_ids=[r[0] for r in src.execute("SELECT e.id FROM search_index JOIN entities e ON e.id=search_index.entity_id WHERE search_index MATCH ? AND e.status IN ('active','accepted','proposed','draft','disputed') ORDER BY bm25(search_index),e.id LIMIT 20",(expression,))]
   observed=[r['id'] for r in project.operation('search',dict(text=text,limit=20))['rows']]
   searches.append(dict(query=text,expected_ids=expected_ids,observed_ids=observed,passed=observed==expected_ids))
 result=dict(ok=not differences and integrity==['ok'] and not fk and all(s['passed'] for s in searches),source=info,baseline=baseline,observed_original_prefix=actual,differences=differences,integrity=integrity,foreign_keys=fk,write_guard_triggers=guards,appended_extension_events=appended,extension_rows=imports,searches=searches)
 return result

def migrate(input_db,output,expected_id,profile=PROFILE):
 input_db=Path(input_db).resolve();output=Path(output).resolve();profile=Path(profile).resolve()
 if output.exists():raise FileExistsError('output exists; use verify or a new empty destination')
 if output==input_db.parent or output.is_relative_to(input_db.parent):raise ValueError('output must be outside source project')
 if sqlite3.sqlite_version_info<(3,51,3):raise ValueError('patched SQLite>=3.51.3 required')
 if input_db.stat().st_mode&0o222:raise ValueError('input must be a sealed read-only snapshot; run snapshot first')
 info=inspect(input_db,expected_id);sha=file_hash(input_db);baseline=fingerprints(input_db)
 if shutil.disk_usage(output.parent).free<input_db.stat().st_size*3+1024**3:raise ValueError('insufficient migration scratch budget')
 output.mkdir();(output/'baseline.json').write_text(json.dumps(dict(input_sha256=sha,info=info,fingerprints=baseline),ensure_ascii=False,indent=2))
 project_root=output/'project';project_root.mkdir();backup(input_db,project_root/'knowledge.sqlite')
 shutil.copytree(profile,project_root/'extensions/mavis');instance=uuid.uuid4().hex
 manifest='format_version = 1\nproject_id = '+json.dumps(expected_id)+'\nschema_version = 1\ninstance_id = '+json.dumps(instance)+'\n[extensions.mavis]\nversion = 1\npath = "extensions/mavis/extension.toml"\n'
 (project_root/'knowledge.toml').write_text(manifest)
 with closing(sqlite3.connect(project_root/'knowledge.sqlite')) as db:
  db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
  db.execute('CREATE TABLE _sqlk_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
  db.executemany('INSERT INTO _sqlk_meta VALUES (?,?)',[('instance_id',instance),('migration_source_sha256',sha),('migration_source_revision',str(info['revision']))])
  db.execute('CREATE TABLE _sqlk_search_documents(entity_id TEXT PRIMARY KEY REFERENCES entities(id),fts_rowid INTEGER NOT NULL UNIQUE)')
  db.execute('INSERT INTO _sqlk_search_documents SELECT entity_id,rowid FROM search_index');install_write_guard(db);db.commit()
 project=Project(project_root)
 try:
  migration=extensions.migrate(project);seeds=seed_domain(project,sha,info['revision']);verification=verify(input_db,project_root,expected_id,baseline)
  result=dict(ok=verification['ok'],input_sha256=sha,profile_sha256={p.name:file_hash(p) for p in profile.iterdir() if p.is_file()},source=info,project_root=str(project_root),migration=migration,seed_results=seeds,verification=verification,production_installed=False)
 finally:shutdown=stop_writer(project)
 result['shutdown']=shutdown
 if file_hash(input_db)!=sha:raise ValueError('input snapshot changed during migration')
 (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
 if not result['ok']:raise ValueError('migration verification failed; see result.json')
 return result

def audit(project_root, report_path):
 """Explicit full audit with teardown of this operation's own completed scratch."""
 from . import check_jobs
 project=Project(Path(project_root).resolve());report_path=Path(report_path).resolve()
 if report_path.exists():raise FileExistsError('audit report exists')
 started=time.monotonic();current=check_jobs.start(project,files=False)
 while current['status']=='running':
  current=check_jobs.step(project,current['job_id'],10000)
  if current['phase']=='physical':time.sleep(.05)
 result=check_jobs.result(project,current['job_id'],0,1000);problems=list(result['problems'])
 for offset in range(1000,result['problems_count'],1000):problems.extend(check_jobs.result(project,current['job_id'],offset,1000)['problems'])
 result.update(problems=problems,elapsed_seconds=time.monotonic()-started,scope='logical/physical database audit; original file references checked separately')
 folder=project.root/'.tmp/check-jobs'/current['job_id']
 with closing(sqlite3.connect(folder/'state.sqlite')) as state:result['job_metadata']=json.loads(state.execute('SELECT body FROM metadata').fetchone()[0])
 report_path.write_text(json.dumps(result,ensure_ascii=False,indent=2))
 try:
  with (folder/'physical.lock').open('a+b') as handle:
   deadline=time.monotonic()+3
   while True:
    try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB);break
    except BlockingIOError:
     if time.monotonic()>deadline:raise
     time.sleep(.02)
   result['scratch_bytes_removed']=sum(p.stat().st_size for p in folder.rglob('*') if p.is_file())
   shutil.rmtree(folder)
  result['scratch_removed']=True
 except BlockingIOError:result.update(scratch_removed=False,scratch_reason='physical worker still holds lock')
 report_path.write_text(json.dumps(result,ensure_ascii=False,indent=2));return result

def main():
 parser=argparse.ArgumentParser(description=__doc__);commands=parser.add_subparsers(dest='command',required=True)
 p=commands.add_parser('snapshot');p.add_argument('--source-db',type=Path,required=True);p.add_argument('--output-db',type=Path,required=True)
 p=commands.add_parser('audit');p.add_argument('--project-root',type=Path,required=True);p.add_argument('--report',type=Path,required=True)
 for command in ('migrate','verify'):
  p=commands.add_parser(command);p.add_argument('--input-db',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--expected-project-id',required=True)
 args=parser.parse_args()
 if args.command=='snapshot':
  result=backup(args.source_db,args.output_db);args.output_db.chmod(0o400);result['sealed_readonly']=True
 elif args.command=='audit':result=audit(args.project_root,args.report)
 elif args.command=='migrate':result=migrate(args.input_db,args.output,args.expected_project_id)
 else:
  baseline=json.loads((args.output/'baseline.json').read_text())
  if file_hash(args.input_db)!=baseline['input_sha256']:raise ValueError('input snapshot differs from migration baseline')
  result=verify(args.input_db,args.output/'project',args.expected_project_id,baseline['fingerprints'])
 print(json.dumps(result,ensure_ascii=False,indent=2));return 0 if result.get('ok',True) else 1
if __name__=='__main__':raise SystemExit(main())
