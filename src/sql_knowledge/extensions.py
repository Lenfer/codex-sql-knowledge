"""Declarative, namespaced project extensions. Explicit transactional migrations."""
from pathlib import Path
import hashlib,json,re,sqlite3,tomllib
NAME=re.compile(r'^[a-z][a-z0-9_]{0,62}$')
def identifier(value):
 if not isinstance(value,str) or not NAME.fullmatch(value):raise ValueError('invalid SQL identifier')
 return value
def resolve(root,path):
 value=(root/path).resolve()
 if not value.is_relative_to(root) or Path(path).is_absolute():raise ValueError('extension path escapes project')
 return value
def specs(project):
 result={}
 for name,ref in project.config.get('extensions',{}).items():
  identifier(name);path=resolve(project.root,ref['path']);spec=tomllib.loads(path.read_text());namespace=identifier(spec.get('namespace'));version=spec.get('version')
  if spec.get('name')!=name or type(version) is not int or version<1 or ref.get('version')!=version or version!=len(spec.get('migrations',[])):raise ValueError('extension version/name mismatch')
  if namespace!=name:raise ValueError('extension namespace must equal its registered name')
  kinds=spec.get('legacy_artifact_kinds',[])
  if not isinstance(kinds,list) or len(set(kinds))!=len(kinds):raise ValueError('invalid legacy kinds')
  for kind in kinds:
   identifier(kind)
   if kind in {'project','file','document','script','skill','source','model','experiment','report','knowledge','decision'}:raise ValueError('extension cannot redefine core kind')
  predicates=spec.get('legacy_predicates',[])
  if not isinstance(predicates,list) or len({p['name'] for p in predicates})!=len(predicates):raise ValueError('duplicate legacy predicate')
  for predicate in predicates:
   identifier(predicate['name']);identifier(predicate['target_kind'])
   if type(predicate.get('unique_source',False)) is not bool:raise ValueError('unique_source must be boolean')
  tables=spec.get('tables',[])
  if len({t['name'] for t in tables})!=len(tables):raise ValueError('duplicate extension table')
  for t in tables:
   identifier(t['name'])
   if t.get('entity_kind') is not None:identifier(t['entity_kind'])
   if not t['name'].startswith(namespace+'_'):raise ValueError('table must have extension namespace prefix')
   for c in t['columns']:identifier(c)
   if len(set(t['columns']))!=len(t['columns']) or t.get('primary_key') not in t['columns'] or t.get('entity_column') not in t['columns']:raise ValueError('invalid table key/entity declaration')
   if not set(t.get('required',[]))<=set(t['columns']):raise ValueError('unknown required field')
  result[name]=dict(spec,path=path)
 return result
def install_legacy_policy(project):
 """Explicit project-owned compatibility vocabulary; never rewrite legacy rows."""
 from .compressed import BASE_KINDS,BASE_PREDICATES
 definitions=specs(project);owners={};rules={};kinds=set(BASE_KINDS);predicates=set(BASE_PREDICATES)
 for spec in definitions.values():
  for kind in spec.get('legacy_artifact_kinds',[]):
   if kind in kinds:raise ValueError('legacy kind declared by multiple extensions')
   kinds.add(kind);owners[kind]=spec
  for rule in spec.get('legacy_predicates',[]):
   name=rule['name']
   if name in predicates or name=='supersedes':raise ValueError('extension cannot redefine existing predicate')
   predicates.add(name);rules[name]=(spec,rule)
 for spec,rule in rules.values():
  if rule['target_kind'] not in kinds:raise ValueError('legacy predicate target kind not declared')
 project.memory.ARTIFACT_KINDS=kinds;project.memory.PREDICATES=predicates
 project._legacy_kind_owners=owners
 original=project.memory.relation
 def relation(db,source,predicate,target,envelope,now,relation_id=None):
  if predicate in rules:
   spec,rule=rules[predicate];verify(project,db,spec)
   record=project.memory.row(db,'entities','id',target)
   project.memory.require(record and record['kind']==rule['target_kind'],'legacy predicate target has wrong kind')
   if rule.get('unique_source'):
    project.memory.require(not db.execute('SELECT 1 FROM relations WHERE source_id=? AND predicate=?',(source,predicate)).fetchone(),'legacy predicate allows only one target per source')
  return original(db,source,predicate,target,envelope,now,relation_id)
 project.memory.relation=relation

def ledger(db):db.execute('CREATE TABLE IF NOT EXISTS _sqlk_migrations(extension TEXT NOT NULL, version INTEGER NOT NULL, sha256 TEXT NOT NULL, PRIMARY KEY(extension,version))')
def statements(text):
 buffer=''
 for line in text.splitlines(keepends=True):
  for ch in line:
   buffer+=ch
   if ch==';' and sqlite3.complete_statement(buffer):yield buffer;buffer=''
 if buffer.strip() and not buffer.strip().startswith('--'):raise ValueError('migration requires complete SQL statements')
def digest(spec,index):
 f=resolve(spec['path'].parent,spec['migrations'][index]);raw=f.read_bytes();return hashlib.sha256(raw).hexdigest(),raw.decode()
def migration_authorizer(namespace):
 prefix=namespace+'_'
 deny={sqlite3.SQLITE_ATTACH,sqlite3.SQLITE_DETACH,sqlite3.SQLITE_PRAGMA,sqlite3.SQLITE_TRANSACTION,sqlite3.SQLITE_CREATE_TRIGGER,sqlite3.SQLITE_CREATE_VIEW,sqlite3.SQLITE_CREATE_VTABLE,sqlite3.SQLITE_DROP_TABLE,sqlite3.SQLITE_DROP_INDEX,sqlite3.SQLITE_CREATE_TEMP_TABLE,sqlite3.SQLITE_CREATE_TEMP_INDEX,sqlite3.SQLITE_CREATE_TEMP_TRIGGER,sqlite3.SQLITE_CREATE_TEMP_VIEW,sqlite3.SQLITE_DROP_VIEW}
 def auth(action,a,b,database,trigger):
  if action in deny:return sqlite3.SQLITE_DENY
  if action==sqlite3.SQLITE_ALTER_TABLE:return sqlite3.SQLITE_OK if (b or '').startswith(prefix) else sqlite3.SQLITE_DENY
  if action in [sqlite3.SQLITE_CREATE_TABLE,sqlite3.SQLITE_CREATE_INDEX]:return sqlite3.SQLITE_OK if ((a or '').startswith(prefix) or (a or '').startswith('sqlite_autoindex_'+prefix)) and (action!=sqlite3.SQLITE_CREATE_INDEX or (b or '').startswith(prefix)) else sqlite3.SQLITE_DENY
  if action in [sqlite3.SQLITE_INSERT,sqlite3.SQLITE_UPDATE,sqlite3.SQLITE_DELETE] and a not in ['sqlite_master','sqlite_schema'] :return sqlite3.SQLITE_DENY
  if action==sqlite3.SQLITE_FUNCTION and str(b).lower() in ['load_extension','writefile','readfile']:return sqlite3.SQLITE_DENY
  return sqlite3.SQLITE_OK
 return auth
def verify(project,db,spec):
 exists=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='_sqlk_migrations'").fetchone()
 rows=db.execute('SELECT version,sha256 FROM _sqlk_migrations WHERE extension=? ORDER BY version',(spec['name'],)).fetchall() if exists else []
 if len(rows)!=spec['version']:raise ValueError('pending extension migrations; run explicit migrate')
 for i,row in enumerate(rows):
  if row[0]!=i+1 or row[1]!=digest(spec,i)[0]:raise ValueError('applied migration checksum mismatch')
 for t in spec['tables']:
  columns=db.execute('PRAGMA table_info('+identifier(t['name'])+')').fetchall();actual={r['name']:r for r in columns}
  pk=[r for r in columns if r['pk']]
  if not set(t['columns'])<=set(actual) or len(pk)!=1 or pk[0]['name']!=t['primary_key'] or pk[0]['type'].upper()!='TEXT' or actual[t['entity_column']]['type'].upper()!='TEXT':raise ValueError('declared table requires one TEXT primary key and TEXT entity FK')
  if not any(r['table']=='entities' and r['from']==t['entity_column'] and r['to']=='id' for r in db.execute('PRAGMA foreign_key_list('+t['name']+')')):raise ValueError('extension table must reference core entity')
def migrate(project):
 if not project._local:
  from .coordinator import submit
  return submit(project,'migrate',{})
 definitions=specs(project);borrowed=getattr(project,'_writer_connection',None) is not None;db=project._writer_connection if borrowed else project.connect(True);applied=[]
 try:
  db.execute('BEGIN IMMEDIATE');project.binding(db);ledger(db)
  for name,spec in definitions.items():
   rows=db.execute('SELECT version,sha256 FROM _sqlk_migrations WHERE extension=? ORDER BY version',(name,)).fetchall()
   if len(rows)>spec['version']:raise ValueError('extension downgrade prohibited')
   for i,row in enumerate(rows):
    if row[0]!=i+1 or row[1]!=digest(spec,i)[0]:raise ValueError('applied migration checksum mismatch')
   for i in range(len(rows),spec['version']):
    sha,sql=digest(spec,i);db.set_authorizer(migration_authorizer(spec['namespace']))
    try:
     for statement in statements(sql):db.execute(statement)
    finally:db.set_authorizer(None)
    db.execute('INSERT INTO _sqlk_migrations VALUES (?,?,?)',(name,i+1,sha));applied.append([name,i+1])
   verify(project,db,spec)
  if db.execute('PRAGMA foreign_key_check').fetchone():raise ValueError('foreign key violations')
  project.binding(db);db.commit();return dict(ok=True,applied=applied)
 except BaseException:db.rollback();raise
 finally:
  if not borrowed:db.close()
def table_spec(project,extension,table,db):
 spec=specs(project).get(extension)
 if spec is None:raise ValueError('extension not enabled')
 verify(project,db,spec)
 t=next((t for t in spec['tables'] if t['name']==table),None)
 if t is None:raise ValueError('table not declared')
 return t
def upsert(project,extension,table,row,envelope):
 if not project._local:
  from .coordinator import submit
  return submit(project,'extension',dict(extension=extension,table=table,row=row,envelope=envelope))
 m=project.memory;borrowed=getattr(project,'_writer_connection',None) is not None;db=project._writer_connection if borrowed else project.connect(True)
 try:
  db.execute('BEGIN IMMEDIATE');project.binding(db)
  if not isinstance(row,dict) or not isinstance(envelope,dict):raise ValueError('row/envelope must be objects')
  payload=dict(extension=extension,table=table,row=row);event=dict(envelope,data=payload)
  for field in ['idempotency_key','actor','reason','source_ref']:event[field]=m.text(event,field)
  m.durable_ref(event['source_ref']);key=event['idempotency_key'];request=hashlib.sha256(m.packed(dict(action='extension_upsert',envelope=event)).encode()).hexdigest()
  prior=db.execute('SELECT seq,request_hash,result_json FROM events WHERE operation_key=?',(key,)).fetchone()
  if prior:
   m.require(prior['request_hash']==request,'idempotency key reused with different content','idempotency_conflict');db.rollback();return dict(json.loads(prior['result_json']),replayed=True,revision=prior['seq'])
  t=table_spec(project,extension,table,db)
  if set(row)-set(t['columns']) or not set(t['required'])<=set(row):raise ValueError('invalid extension row fields')
  for field in [t['primary_key'],t['entity_column']]:
   if not isinstance(row.get(field),str) or not row[field]:raise ValueError('row key/entity must be nonempty strings')
  infos={x['name']:x for x in db.execute('PRAGMA table_info('+table+')')}
  for k,v in row.items():
   typ=infos[k]['type'].upper()
   if v is None:
    if infos[k]['notnull']:raise ValueError('null for required column '+k)
   elif ('INT' in typ and type(v) is not int) or (any(x in typ for x in ['CHAR','TEXT','CLOB']) and not isinstance(v,str)) or (any(x in typ for x in ['REAL','FLOA','DOUB']) and type(v) not in [int,float]) or isinstance(v,(list,dict)):raise ValueError('field type mismatch '+k)
  before=db.execute('SELECT * FROM '+table+' WHERE '+t['primary_key']+'=?',(row[t['primary_key']],)).fetchone()
  if before or envelope.get('expected_revision') is not None:project.check_revision(db,row[t['entity_column']],envelope.get('expected_revision'))
  if before and before[t['entity_column']]!=row[t['entity_column']]:raise ValueError('extension row ownership is immutable')
  entity=m.snapshot(db,row[t['entity_column']]);m.require(entity is not None,'linked entity missing')
  if t.get('entity_kind'):m.require(entity['entity']['kind']==t['entity_kind'],'extension requires entity kind '+t['entity_kind'])
  columns=list(row);assign=[c for c in columns if c!=t['primary_key']]
  sql='INSERT INTO '+table+'('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+') ON CONFLICT('+t['primary_key']+') '+('DO UPDATE SET '+','.join(c+'=excluded.'+c for c in assign) if assign else 'DO NOTHING')
  db.execute(sql,tuple(row[c] for c in columns));after=db.execute('SELECT * FROM '+table+' WHERE '+t['primary_key']+'=?',(row[t['primary_key']],)).fetchone();result=dict(ok=True,id=row[t['primary_key']],operation_key=key,replayed=False)
  current=m.snapshot(db,row[t['entity_column']]);left=dict(entities={row[t['entity_column']]:current},extension=dict(table=table,row=dict(before) if before else None));right=dict(entities={row[t['entity_column']]:current},extension=dict(table=table,row=dict(after)))
  cur=db.execute('INSERT INTO events(operation_key,request_hash,action,actor,reason,source_ref,created_at,before_json,after_json,result_json) VALUES (?,?,?,?,?,?,?,?,?,?)',(key,request,'extension_upsert',event['actor'],event['reason'],event['source_ref'],m.timestamp(),m.packed(left),m.packed(right),m.packed(result)));seq=db.execute('SELECT seq FROM events WHERE operation_key=?',(key,)).fetchone()[0];db.execute('INSERT INTO event_entities VALUES (?,?)',(seq,row[t['entity_column']]));project.binding(db);db.commit();return dict(result,revision=seq)
 except BaseException:db.rollback();raise
 finally:
  if not borrowed:db.close()
def query(project,extension,table,limit=100,after_id=None):
 if type(limit) is not int or not 1<=limit<=100:raise ValueError('limit1..100 required')
 with project.connect() as db:
  t=table_spec(project,extension,table,db);key=t['primary_key'];rows=[dict(r) for r in db.execute('SELECT '+','.join(t['columns'])+' FROM '+table+(' WHERE '+key+'>?' if after_id else '')+' ORDER BY '+key+' LIMIT ?',([after_id] if after_id else [])+[limit+1])];return dict(ok=True,rows=rows[:limit],next_after_id=rows[limit-1][key] if len(rows)>limit else None)

def default_with_affinity(db,info):
 # Inserting into a typed temporary column applies exactly SQLite's affinity,
 # including NUMERIC conversions that CAST does not model.
 name='_sqlk_default_'+__import__('uuid').uuid4().hex
 typ=info['type']
 if any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_ (),.' for c in typ):raise ValueError('unsupported extension column type')
 try:
  db.execute('CREATE TEMP TABLE '+name+'(value '+typ+')')
  if info['dflt_value'] is None:return None
  db.execute('INSERT INTO '+name+' VALUES ('+info['dflt_value']+')')
  return db.execute('SELECT value FROM '+name).fetchone()[0]
 finally:db.execute('DROP TABLE IF EXISTS temp.'+name)

def inventory_problems(db,definitions,history=True):
 """Check extension inventory in both directions, even without a manifest."""
 problems=[];declared=set(definitions);tables={t['name'] for spec in definitions.values() for t in spec['tables']}
 if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='_sqlk_migrations'").fetchone():
  for (name,) in db.execute('SELECT DISTINCT extension FROM _sqlk_migrations'):
   if name not in declared:problems.append(dict(kind='extension_undeclared',extension=name))
 core={'schema_meta','entities','knowledge','decisions','relations','events','event_entities','search_index','_sqlk_search_documents','search_documents','search_catalog','_event_records','_event_payloads','_sqlk_meta','_sqlk_migrations','search_index_data','search_index_idx','search_index_content','search_index_docsize','search_index_config'}
 for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"):
  if name not in core and name not in tables:problems.append(dict(kind='extension_table_undeclared',table=name))
 if history:
  seen=set()
  for (raw,) in db.execute("SELECT after_json FROM events WHERE action='extension_upsert'"):
   table=json.loads(raw).get('extension',{}).get('table')
   if table not in tables and table not in seen:problems.append(dict(kind='extension_history_undeclared',table=table));seen.add(table)
 return problems

def check_extensions(project,db,result):
 problems=list(result['problems'])
 try:definitions=specs(project)
 except Exception as exc:definitions={};problems.append(dict(kind='extension_manifest',detail=str(exc)))
 problems.extend(inventory_problems(db,definitions))
 for name,spec in definitions.items():
  try:verify(project,db,spec)
  except Exception as exc:problems.append(dict(kind='extension_schema',extension=name,detail=str(exc)));continue
  tables={t['name']:t for t in spec['tables']};expected={}
  # Reverse history once per enabled extension. Table namespaces prevent collisions.
  for ev in db.execute("SELECT after_json FROM events WHERE action='extension_upsert' ORDER BY seq DESC"):
   ext=json.loads(ev[0]).get('extension',{});table=ext.get('table');row=ext.get('row')
   if table in tables and isinstance(row,dict):
    key=(table,row.get(tables[table]['primary_key']))
    if key not in expected:expected[key]=row
  actual_keys=set()
  for table,t in tables.items():
   infos={r['name']:r for r in db.execute('PRAGMA table_info('+table+')')}
   for record in db.execute('SELECT * FROM '+table):
    actual=dict(record);key=(table,actual[t['primary_key']]);actual_keys.add(key);old=expected.get(key)
    if old is None:problems.append(dict(kind='extension_history_missing',extension=name,table=table,id=key[1]));continue
    comparison=dict(old)
    for column,info in infos.items():
     if column not in comparison:
      comparison[column]=default_with_affinity(db,info)
    if comparison!=actual:problems.append(dict(kind='extension_history_mismatch',extension=name,table=table,id=key[1]))
  for table,key in set(expected)-actual_keys:problems.append(dict(kind='extension_row_missing',extension=name,table=table,id=key))
 return dict(result,ok=not problems,problems=problems)
