"""Explicit project binding. No implicit initialization, SQL export or global cwd state."""
from pathlib import Path
from contextlib import closing
import argparse,hashlib,importlib.util,json,os,sqlite3,tomllib,uuid
from . import compressed
from .storage_keys import step_key
BASE=Path(__file__).parent/'_core'
def module(name,path):
 spec=importlib.util.spec_from_file_location(name+'_'+uuid.uuid4().hex,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
class Project:
 def __init__(self,root,expected_id=None,expected_instance=None,_local=False):
  if not Path(root).is_absolute():raise ValueError('project root must be absolute')
  self.root=Path(root).resolve();self.database=self.root/'knowledge.sqlite'
  manifest=self.root/'knowledge.toml'
  if manifest.is_symlink() or self.database.is_symlink():raise ValueError('manifest/database symlinks are not allowed')
  self.config=tomllib.loads(manifest.read_text())
  if self.config.get('format_version')!=1 or type(self.config.get('schema_version')) is not int or self.config['schema_version'] not in [1,2]:raise ValueError('unsupported manifest/schema version')
  self.id=self.config.get('project_id')
  if not isinstance(self.id,str) or not self.id.strip():raise ValueError('project_id required')
  self.id=self.id.strip();self.expected_id=expected_id.strip() if isinstance(expected_id,str) else self.id;self.instance_id=expected_instance;self._local=_local
  self._file_identity=None
  if self.config['schema_version']==1:self.memory=module('core_memory',BASE/'memory.py');self.memory.ROOT=self.root
  else:self.memory=compressed.install(compressed.load_base(),self.root,None)
  self.memory.SCHEMA=BASE/'schema.sql'
  # Core owns generic kinds. Subject-specific data lives in extension tables/attributes.
  from .extensions import install_legacy_policy
  install_legacy_policy(self)
  raw_connect=self.memory.connect;raw_mutate=self.memory.mutate
  def binding(db):
   manifest=tomllib.loads((self.root/'knowledge.toml').read_text())
   if self.database.is_symlink() or (self.root/'knowledge.toml').is_symlink():raise ValueError('Project identity symlink replacement')
   stat=self.database.stat();file_identity=(stat.st_dev,stat.st_ino)
   if self._file_identity is not None and file_identity!=self._file_identity:raise ValueError('Project identity database replacement; restart required')
   ids=[r[0] for r in db.execute("SELECT id FROM entities WHERE kind='project' AND status='active'")]
   if ids!=[self.expected_id] or str(manifest.get('project_id','')).strip()!=self.expected_id:raise ValueError('Project identity mismatch on active connection')
   if manifest.get('schema_version')!=self.config['schema_version']:raise ValueError('Project identity schema replacement')
   has_meta=db.execute("SELECT 1 FROM sqlite_master WHERE name='_sqlk_meta' AND type='table'").fetchone()
   if has_meta:
    token=db.execute("SELECT value FROM _sqlk_meta WHERE key='instance_id'").fetchone()
    if not token:raise ValueError('Project identity generation missing')
    token=token[0]
   else:
    creation=db.execute('SELECT created_at FROM entities WHERE id=?',(self.expected_id,)).fetchone()[0]
    meta=[list(r) for r in db.execute('SELECT * FROM schema_meta ORDER BY version')]
    token='legacy-'+hashlib.sha256(json.dumps([self.expected_id,creation,meta],sort_keys=True).encode()).hexdigest()
   declared=manifest.get('instance_id')
   if (declared is not None and declared!=token) or (self.instance_id is not None and self.instance_id!=token):raise ValueError('Project identity generation mismatch')
   self.instance_id=token;self._file_identity=file_identity
  self.binding=binding
  def bound_connect(path,writable=False):
   db=raw_connect(path,writable)
   try:
    db.create_function('_sqlk_writer_authorized',0,lambda:int(bool(writable and self._local)))
    db.execute('PRAGMA synchronous=FULL');binding(db)
    from .runtime import track_connection
    track_connection(db,self,writable)
    return db
   except BaseException:db.close();raise
  def bound_mutate(db,action,envelope,transaction=True):
   if transaction:db.execute('BEGIN IMMEDIATE')
   try:
    binding(db)
    # Metadata canonicalization matches the frozen writer's receipt hashing.
    event=dict(envelope)
    for key in ('idempotency_key','actor','reason','source_ref'):event[key]=self.memory.text(event,key)
    request=hashlib.sha256(self.memory.packed(dict(action=action,envelope=event)).encode()).hexdigest()
    prior=db.execute('SELECT seq,request_hash,result_json FROM events WHERE operation_key=?',(event['idempotency_key'],)).fetchone()
    if prior:
     compatible={request}
     # Absent/null CAS metadata has identical semantics. Earlier MCP versions
     # serialized these two additive fields differently; never erase non-null CAS.
     nullable=[key for key in ('expected_revision','expected_revisions') if event.get(key) is None]
     for mask in range(1<<len(nullable)):
      alternative=dict(event)
      for index,key in enumerate(nullable):
       if mask&(1<<index):alternative[key]=None
       else:alternative.pop(key,None)
      compatible.add(hashlib.sha256(self.memory.packed(dict(action=action,envelope=alternative)).encode()).hexdigest())
     self.memory.require(prior['request_hash'] in compatible,'idempotency key already used for different input','idempotency_conflict')
     result=dict(json.loads(prior['result_json']),replayed=True,revision=prior['seq'])
    else:
     data=envelope.get('data',{})
     if action=='entity' and data.get('kind','').strip() in self._legacy_kind_owners:
      from .extensions import verify
      verify(self,db,self._legacy_kind_owners[data['kind'].strip()])
     if action in ['entity','knowledge','decision']:
      eid=self.memory.text(data,'id')
      if eid==self.id and (action!='entity' or self.memory.text(data,'kind')!='project' or self.memory.text(data,'status')!='active'):raise ValueError('Identifying project must remain an active project')
      if action=='entity' and eid!=self.id and self.memory.text(data,'kind')=='project' and self.memory.text(data,'status')=='active':raise ValueError('A second active project is not allowed')
     if getattr(self,'_storage_internal',False):
      key=getattr(self,'_storage_key','');targets=getattr(self,'_storage_targets',[])
      storage_action={'cleanup_plan':'plan','cleanup_execute':'execute'}.get(self._storage_action,self._storage_action)
      if event['idempotency_key'] in [step_key(key,storage_action,phase,version=self._storage_key_version) for phase in ('start','plan')]:
       for target in targets:self.validate_storage_cas(db,self.memory.text({'id':target},'id'),allow_progress=False)
      if action=='entity' and eid in {self.memory.text({'id':target},'id') for target in targets}:self.validate_storage_cas(db,eid)
     if getattr(self,'_cas_public',False) and not getattr(self,'_storage_internal',False):self.validate_cas(db,action,envelope)
     result=raw_mutate(db,action,envelope,transaction=False)
     result=dict(result,revision=db.execute('SELECT seq FROM events WHERE operation_key=?',(event['idempotency_key'],)).fetchone()[0])
    binding(db)
    if transaction:db.commit()
    return result
   except BaseException:
    if transaction:db.rollback()
    raise
  self.memory.connect=bound_connect;self.memory.mutate=bound_mutate
  self.memory.storage_revision_guard=lambda db,eid:self.validate_storage_cas(db,eid) if getattr(self,'_storage_internal',False) else None
  db=self.connect();db.close()
 def revision(self,db,entity_id):
  return db.execute('SELECT COALESCE(MAX(event_seq),0) FROM event_entities WHERE entity_id=?',(entity_id,)).fetchone()[0]
 def check_revision(self,db,entity_id,expected):
  self.memory.require(expected is not None,'expected_revision required for '+entity_id,'revision_required')
  self.memory.require(type(expected) is int and expected==self.revision(db,entity_id),'revision conflict for '+entity_id,'revision_conflict')
 def validate_storage_cas(self,db,eid,allow_progress=True):
  m=self.memory;expected={}
  m.require(db.in_transaction,'storage revision guard requires an explicit transaction')
  values=getattr(self,'_storage_expected',{}) or {}
  m.require(isinstance(values,dict),'expected_revisions must be an object')
  for key,value in values.items():
   canonical=m.text({'id':key},'id')
   m.require(canonical not in expected,'duplicate canonical expected revision ID')
   expected[canonical]=value
  prior=m.snapshot(db,eid);wanted=expected.get(eid)
  if prior is None:
   if wanted is not None:m.require(type(wanted) is int and wanted==0,'revision conflict for new '+eid,'revision_conflict')
   return
  m.require(wanted is not None,'expected_revision required for '+eid,'revision_required')
  current=self.revision(db,eid)
  if type(wanted) is int and current==wanted:return
  # Cleanup has two target commits. Resume only this operation's verified
  # receipts, with no foreign event or unrecorded row edit since original CAS.
  if allow_progress and type(wanted) is int and current>wanted and getattr(self,'_storage_action',None)=='cleanup_execute':
   key=getattr(self,'_storage_key','');original=getattr(self,'_storage_envelope',None)
   events=db.execute('SELECT e.* FROM events e JOIN event_entities x ON e.seq=x.event_seq WHERE x.entity_id=? AND e.seq>? ORDER BY e.seq',(eid,wanted)).fetchall()
   valid=bool(events) and isinstance(original,dict)
   latest=None
   for ev in events:
    if ev['operation_key'] not in [step_key(key,'execute',phase,eid,self._storage_key_version) for phase in ('intent','result')]:valid=False;break
    saved=json.loads(ev['after_json']).get('entities',{}).get(eid)
    if not saved:valid=False;break
    entity=saved['entity'];attributes=json.loads(entity['attributes_json'])
    operation_id='storage-op-'+hashlib.sha256(key.encode()).hexdigest()[:32]
    if attributes.get('storage',{}).get('cleanup_operation')!=operation_id:valid=False;break
    data={k:entity[k] for k in ('id','kind','title','description','path','status')};data['attributes']=attributes
    event=dict(original,data=data,idempotency_key=ev['operation_key'])
    for field in ('idempotency_key','actor','reason','source_ref'):event[field]=m.text(event,field)
    request=hashlib.sha256(m.packed(dict(action='entity',envelope=event)).encode()).hexdigest()
    if request!=ev['request_hash']:valid=False;break
    latest=saved
   if valid and latest==prior:return
  m.require(False,'revision conflict for '+eid,'revision_conflict')
 def validate_cas(self,db,action,envelope):
  data=envelope.get('data',{});expected=envelope.get('expected_revisions') or {}
  self.memory.require(isinstance(expected,dict),'expected_revisions must be an object')
  if action=='link':
   for field in ('source_id','target_id'):
    eid=self.memory.text(data,field)
    if self.memory.snapshot(db,eid) is not None:self.check_revision(db,eid,expected.get(eid))
  else:
   eid=self.memory.text(data,'id');prior=self.memory.snapshot(db,eid)
   wanted=envelope.get('expected_revision',expected.get(eid))
   if prior is not None:self.check_revision(db,eid,wanted)
   elif wanted is not None:self.memory.require(type(wanted) is int and wanted==0,'revision conflict for new '+eid,'revision_conflict')
   supersedes=data.get('supersedes',prior['detail'].get('supersedes') if prior and action!='entity' else None)
   active='active' if action=='knowledge' else 'accepted'
   if supersedes and (not prior or (prior['entity']['status']=='proposed' and data.get('status')==active)):
    self.check_revision(db,supersedes,expected.get(supersedes))
 def connect(self,writable=False):return self.memory.connect(self.database,writable)
 def operation(self,command,args=None,envelope=None):
  if envelope is not None and not self._local:
   from .coordinator import submit
   return submit(self,'core',{'command':command,'envelope':envelope})
  args=args or {};m=self.memory;borrowed=envelope is not None and getattr(self,'_writer_connection',None) is not None;db=self._writer_connection if borrowed else self.connect(command in ['entity','knowledge','decision','link'])
  try:
   if envelope is not None:
    if command not in ['entity','knowledge','decision','link']:raise ValueError('unsupported write command')
    self._cas_public=True
    try:return m.mutate(db,command,envelope)
    finally:self._cas_public=False
   db.execute('BEGIN')
   if command=='query':return m.read_query(db,args['sql'],json.loads(args.get('params','[]')),int(args.get('limit',100)))
   if command=='context':return dict(ok=True,database=str(self.database),project_id=self.id,schema_version=m.VERSION,accepted_decisions=[dict(r) for r in db.execute("SELECT e.id,e.title,d.scope,d.body,d.authority_ref FROM decisions d JOIN entities e ON e.id=d.entity_id WHERE e.status='accepted' ORDER BY d.scope,e.id")],active_constraints=[dict(r) for r in db.execute("SELECT e.id,e.title,k.body,k.confidence FROM knowledge k JOIN entities e ON e.id=k.entity_id WHERE e.status='active' AND k.knowledge_type='constraint' ORDER BY e.id")],counts=[dict(r) for r in db.execute('SELECT kind,status,count(*) AS count FROM entities GROUP BY kind,status')],recent_events=[dict(r) for r in db.execute('SELECT seq,action,reason,created_at FROM events ORDER BY seq DESC LIMIT 8')])
   if command=='schema':return dict(ok=True,schema_version=m.VERSION,project_id=self.id,capabilities=dict(query_max_rows=500,query_vm_seconds=5,search_mode='fts5-unicode61',sql_dump=False,auto_init=False),extensions=self.config.get('extensions',{}),objects=[dict(r) for r in db.execute("SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")])
   if command=='search':
    limit=int(args.get('limit',20));text=args.get('text','');m.require(1<=limit<=100 and bool(text.split()),'nonempty text and limit1..100 required')
    expression=' OR '.join('"'+w.replace('"','""')+'"' for w in text.split())
    if m.VERSION==2:
     sql="SELECT c.entity_id AS id,c.kind,c.title,c.status,c.path,c.knowledge_type,c.confidence,c.scope,snippet(search_index,2,'[',']',' … ',24) AS excerpt,bm25(search_index) AS score FROM search_index CROSS JOIN search_catalog c ON c.fts_rowid=search_index.rowid WHERE search_index MATCH ? ";status='c.status';idcol='c.entity_id'
    else:
     sql="SELECT e.id,e.kind,e.title,e.status,e.path,k.knowledge_type,k.confidence,d.scope,snippet(search_index,2,'[',']',' … ',24) AS excerpt,bm25(search_index) AS score FROM search_index JOIN entities e ON e.id=search_index.entity_id LEFT JOIN knowledge k ON k.entity_id=e.id LEFT JOIN decisions d ON d.entity_id=e.id WHERE search_index MATCH ? ";status='e.status';idcol='e.id'
    if not args.get('include_inactive',False):sql+="AND "+status+" IN ('active','accepted','proposed','draft','disputed') "
    sql+='ORDER BY score,'+idcol+' LIMIT ?';return dict(ok=True,rows=[dict(r) for r in db.execute(sql,(expression,limit))],mode='fts5-keywords-unicode61')
   if command=='history':
    limit=int(args.get('limit',100));m.require(1<=limit<=500,'history limit1..500')
    return dict(ok=True,events=[dict(r) for r in db.execute('SELECT e.* FROM event_entities x JOIN events e ON e.seq=x.event_seq WHERE x.entity_id=? ORDER BY e.seq DESC LIMIT ?',(args['id'],limit))])
   if command=='check':
    result=m.check(db,files=args.get('files',True))
    from .extensions import check_extensions
    result=check_extensions(self,db,result)
    if len(result['problems'])>50:
     folder=self.root/'.tmp/check-reports';folder.mkdir(parents=True,exist_ok=True);rid=uuid.uuid4().hex;raw=json.dumps(result,ensure_ascii=False).encode();(folder/(rid+'.json')).write_bytes(raw)
     return dict(result,problems=result['problems'][:50],problem_count=len(result['problems']),truncated=True,report_id=rid,report_sha256=hashlib.sha256(raw).hexdigest())
    return result
   raise ValueError('unsupported command '+command)
  finally:
   if not borrowed:db.close()
def initialize(root,project_id,title='SQL Knowledge project'):
 if not isinstance(project_id,str) or not project_id.strip():raise ValueError('project_id required')
 project_id=project_id.strip()
 if sqlite3.sqlite_version_info<(3,51,3):raise ValueError('WAL requires patched SQLite >=3.51.3')
 instance_id=uuid.uuid4().hex
 root=Path(root)
 if not root.is_absolute():raise ValueError('project root must be absolute')
 root=root.resolve();root.mkdir(parents=True,exist_ok=True);database=root/'knowledge.sqlite';manifest=root/'knowledge.toml'
 if database.exists() or manifest.exists():raise ValueError('init never overwrites a database or manifest')
 fd=os.open(database,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.close(fd)
 m=module('init_memory',BASE/'memory.py');m.ROOT=root
 try:
  with closing(sqlite3.connect(database)) as db:
   db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA synchronous=FULL')
   db.row_factory=sqlite3.Row;db.executescript((BASE/'schema.sql').read_text());db.execute('CREATE TABLE _sqlk_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)');db.execute('INSERT INTO _sqlk_meta VALUES (?,?)',('instance_id',instance_id));db.execute('INSERT INTO schema_meta VALUES (1,?,?)',(m.timestamp(),'SQL Knowledge core'));db.commit()
   m.mutate(db,'entity',dict(idempotency_key='project-init',actor='sql-knowledge-init',reason='Explicit project initialization',source_ref='project:explicit-init',data=dict(id=project_id,kind='project',title=title,status='active')))
  with manifest.open('x') as f:f.write('format_version = 1\nproject_id = '+json.dumps(project_id)+'\nschema_version = 1\ninstance_id = '+json.dumps(instance_id)+'\n')
 except BaseException:
  database.unlink(missing_ok=True);raise
 return dict(ok=True,database=str(database),project_id=project_id)
