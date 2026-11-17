"""Per-project local writer. RAM queue is bounded, receipts provide retry safety.

The lock is held for the process lifetime. Readers never visit this service.
Only typed operations are accepted; no SQL execution IPC endpoint exists.
"""
import array,argparse,asyncio,collections,concurrent.futures,fcntl,hashlib,json,os,socket,sqlite3,stat,subprocess,sys,time,uuid,threading
from pathlib import Path
MAX_REQUEST=131072
MAX_QUEUE=256
MAX_BYTES=4*1024*1024
MAX_CLIENT=32
class CoordinatorError(ValueError):
 def __init__(self,code,message):super().__init__(message);self.code=code

def paths(root):
 folder=Path('/private/tmp')/('sql-knowledge-'+str(os.getuid()))
 try:folder.mkdir(mode=0o700)
 except FileExistsError:pass
 info=folder.lstat()
 if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:raise CoordinatorError('unsafe_ipc','IPC directory must be owned by user and mode0700')
 key=hashlib.sha256(str(Path(root).resolve()).encode()).hexdigest()[:32]
 return folder/(key+'.sock'),folder/(key+'.lock'),folder/(key+'.start')

def transaction_path(root):return paths(root)[1].with_suffix(".transaction")

def legacy_enabled(config):return config.get("compatibility",{}).get("legacy_writes") is True

def contract(config):
 return hashlib.sha256(json.dumps(config,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def fence_path(project):return project.root/'.tmp/coordinator/maintenance.json'
def set_fence(project):
 path=fence_path(project);path.parent.mkdir(parents=True,exist_ok=True)
 raw=json.dumps(dict(project_id=project.id,instance_id=project.instance_id,since=time.time()))
 temporary=path.with_suffix('.'+uuid.uuid4().hex+'.tmp');temporary.write_text(raw);temporary.chmod(0o600);temporary.replace(path)

def _exchange_raw(path,request,timeout):
 raw=json.dumps(request,separators=(',',':'),ensure_ascii=False).encode()+b'\n'
 if len(raw)>MAX_REQUEST:raise CoordinatorError('limit_exceeded','IPC request exceeds128KiB')
 with socket.socket(socket.AF_UNIX) as sock:
  sock.settimeout(timeout);sock.connect(str(path));sock.sendall(raw);data=bytearray();fds=[]
  try:
   while not data.endswith(b'\n'):
    if request.get('kind')=='legacy_lease':
     block,ancillary,flags,_=sock.recvmsg(65536,socket.CMSG_SPACE(array.array('i').itemsize*8))
     for level,kind,value in ancillary:
      if level==socket.SOL_SOCKET and kind==socket.SCM_RIGHTS:
       received=array.array('i');received.frombytes(value[:len(value)-len(value)%received.itemsize]);fds.extend(received)
     if flags&socket.MSG_CTRUNC:raise CoordinatorError('invalid_lease','truncated lease descriptor')
    else:block=sock.recv(65536)
    if not block:raise CoordinatorError('outcome_unknown','writer disconnected; retry identical operation key')
    data.extend(block)
    if len(data)>524288:raise CoordinatorError('response_too_large','response exceeds512KiB; write may have committed')
   result=json.loads(data)
   if not result.get('ok'):raise CoordinatorError(result.get('code','operation_failed'),result.get('error','operation failed'))
   if request.get('kind')=='legacy_lease':
    if len(fds)!=1:raise CoordinatorError('invalid_lease','writer did not provide exactly one lease descriptor')
    result['_lease_fd']=fds.pop();os.set_inheritable(result['_lease_fd'],False)
   return result
  finally:
   for fd in fds:os.close(fd)

def _exchange(path,request,timeout):
 try:return _exchange_raw(path,request,timeout)
 except (socket.timeout,ConnectionResetError,BrokenPipeError) as exc:
  raise CoordinatorError('outcome_unknown','response lost/timed out; retry identical operation key') from exc

def submit(project,kind,payload,timeout=8,client_id=None):
 # Validate existing live binding before contacting an already running writer.
 with project.connect():pass
 path,lock,start=paths(project.root)
 fence=fence_path(project)
 if kind=='resume':
  with start.open('a+b') as guard,lock.open('a+b') as writer_lock:
   fcntl.flock(guard,fcntl.LOCK_EX)
   try:fcntl.flock(writer_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
   except BlockingIOError:raise CoordinatorError('draining','writer still active; wait before resume')
   if fence.exists():
    saved=json.loads(fence.read_text())
    if saved['project_id']!=project.id or saved['instance_id']!=project.instance_id:raise CoordinatorError('identity_mismatch','maintenance fence belongs to another generation')
    fence.unlink()
   return dict(ok=True,resumed=True)
 def fenced_result():
  if kind not in ['status','shutdown']:raise CoordinatorError('maintenance','project is fenced for maintenance; explicit writer-start required')
  return dict(ok=True,maintenance=True,writer_pid=None)
 if fence.exists():
  if kind not in ['status','shutdown'] or not path.exists():return fenced_result()
 request=dict(project_id=project.id,instance_id=project.instance_id,kind=kind,payload=payload,manifest_contract=contract(project.config),client_id=client_id or getattr(project,'client_id',None) or str(os.getpid()),deadline=time.time()+min(timeout,30))
 try:return _exchange(path,request,timeout+1)
 except (FileNotFoundError,ConnectionRefusedError):
  if fence.exists():return fenced_result()
 with start.open('a+b') as guard:
  deadline=time.monotonic()+5
  while True:
   try:fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB);break
   except BlockingIOError:
    if time.monotonic()>deadline:raise CoordinatorError('startup_timeout','coordinator startup busy')
    time.sleep(.02)
  try:
   try:return _exchange(path,request,timeout+1)
   except (FileNotFoundError,ConnectionRefusedError):
    if fence.exists():return fenced_result()
   log=project.root/'.tmp/coordinator';log.mkdir(parents=True,exist_ok=True)
   with (log/'stderr.log').open('ab') as err:
    child=subprocess.Popen([sys.executable,'-B','-m','sql_knowledge.coordinator','--project',str(project.root),'--expected-id',project.id,'--expected-instance',project.instance_id],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=err,start_new_session=True,close_fds=True)
    threading.Thread(target=child.wait,daemon=True).start()
   while time.monotonic()<deadline:
    if child.poll() is not None:
     tail=(log/'stderr.log').read_text(errors='replace')[-1000:]
     raise CoordinatorError('startup_failed','writer exited '+str(child.returncode)+': '+tail)
    try:return _exchange(path,request,timeout+1)
    except (FileNotFoundError,ConnectionRefusedError):time.sleep(.025)
   raise CoordinatorError('startup_timeout','writer not ready within5s; pid='+str(child.pid)+'; inspect .tmp/coordinator/stderr.log')
  finally:fcntl.flock(guard,fcntl.LOCK_UN)

def error(exc):return dict(ok=False,code=getattr(exc,'code','sqlite_busy' if isinstance(exc,sqlite3.OperationalError) and ('locked' in str(exc) or 'busy' in str(exc)) else 'operation_failed'),error=str(exc))

class Writer:
 def __init__(self,root,identity,instance):
  from .engine import Project,BASE,module
  self.project=Project(root,expected_id=identity,expected_instance=instance,_local=True)
  self.db=self.project.connect(True);self.db.execute('PRAGMA synchronous=FULL');self.db.execute('PRAGMA busy_timeout=250');self.project._writer_connection=self.db
  self.file_identity=self.project.database.stat();self.storage=module('sql_knowledge._core.writer_storage',BASE/'storage.py');self.storage.m=self.project.memory
 def validate_path(self):
  now=self.project.database.stat()
  if (now.st_dev,now.st_ino)!=(self.file_identity.st_dev,self.file_identity.st_ino):raise CoordinatorError('identity_mismatch','live database replaced; stop writer and verify restore')
  self.project.binding(self.db)
  import tomllib
  self.project.config=tomllib.loads((self.project.root/'knowledge.toml').read_text())
 def execute(self,kind,payload,prepared=None,deadline=None):
  with transaction_path(self.project.root).open('a+b') as guard:
   fcntl.flock(guard,fcntl.LOCK_EX)
   if deadline is not None and deadline<time.time():raise CoordinatorError("queue_timeout","deadline expired while waiting for transaction lease")
   return self._execute(kind,payload,prepared)
 def legacy_lease(self,request):
  fd=os.open(transaction_path(self.project.root),os.O_CREAT|os.O_RDWR,0o600)
  try:
   fcntl.flock(fd,fcntl.LOCK_EX)
   self.validate_path()
   if fence_path(self.project).exists():raise CoordinatorError('maintenance','project fenced before lease grant')
   if request['deadline']<time.time():raise CoordinatorError('queue_timeout','deadline expired before lease grant')
   if request.get('manifest_contract')!=contract(self.project.config):raise CoordinatorError('stale_contract','manifest changed before lease grant')
   if not legacy_enabled(self.project.config):raise CoordinatorError('legacy_disabled','manifest must opt in to legacy writes')
   return dict(ok=True,_lease_fd=fd)
  except BaseException:os.close(fd);raise
 def _execute(self,kind,payload,prepared=None):
  from . import extensions
  self.validate_path();start=time.perf_counter();busy=0
  while True:
   try:
    if kind=='core':result=self.project.operation(payload['command'],envelope=payload['envelope'])
    elif kind=='extension':result=extensions.upsert(self.project,**payload)
    elif kind=='migrate':result=extensions.migrate(self.project)
    elif kind=='checkpoint':
     result=dict(ok=True,checkpoint=list(self.db.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()))
    elif kind=='storage':result=self.storage_execute(payload,prepared or {})
    elif kind=='audit_cache':
     from .audit_cache import dispatch
     result=dispatch(self.project,payload)
    else:raise CoordinatorError('invalid_operation','unsupported writer operation')
    return dict(result,_writer=dict(elapsed_ms=(time.perf_counter()-start)*1000,busy_retries=busy,pid=os.getpid(),commit_ms=getattr(self.db,'_last_commit_ms',None)))
   except sqlite3.OperationalError as exc:
    self.db.rollback()
    if not ('locked' in str(exc) or 'busy' in str(exc)) or time.perf_counter()-start>1.5:raise
    busy+=1;time.sleep(min(.02*busy,.1))
 def storage_execute(self,payload,prepared):
  action=payload['action'];args=payload.get('args',[]);env=args[0]
  # Per-substep CAS and receipt recovery run inside the actual transactions.
  data=env['data'];ids=data.get('entity_ids',[]) if action!='register' else [data['id']]
  if action=='cleanup_execute':
   row=self.db.execute('SELECT attributes_json FROM entities WHERE id=?',(str(data['plan_id']).strip(),)).fetchone()
   if row:ids=[item['id'] for item in json.loads(row[0])['cleanup_plan']['candidates']]
  self.project._storage_expected=env.get('expected_revisions') or {}
  self.project._storage_targets=[str(i).strip() for i in ids]
  op='storage-op-'+hashlib.sha256(env['idempotency_key'].encode()).hexdigest()[:32]
  intent=self.db.execute('SELECT attributes_json FROM entities WHERE id=?',(op,)).fetchone()
  self.project._storage_key_version=json.loads(intent[0]).get('storage_key_version',1) if intent else 2
  self.project._storage_key=env['idempotency_key'];self.project._storage_action=action;self.project._storage_envelope=env
  original=self.storage.fingerprint
  def cached(path):
   fp=prepared.get(path)
   if fp is None:raise CoordinatorError('file_changed','file not prepared; retry same key')
   if fp.get('missing'):raise FileNotFoundError(path)
   with self.storage.parent_fd(path) as (directory,name):
    info=os.stat(name,dir_fd=directory,follow_symlinks=False)
   for a,b in [('st_dev','device'),('st_ino','inode'),('st_size','size'),('st_mtime_ns','mtime_ns'),('st_ctime_ns','ctime_ns')]:
    if getattr(info,a)!=fp[b]:raise CoordinatorError('file_changed','file changed after preparation')
   return fp
  self.storage.fingerprint=cached;self.project._storage_internal=True
  try:return getattr(self.storage,action)(self.db,*args,**payload.get('kwargs',{}))
  finally:
   self.storage.fingerprint=original;self.project._storage_internal=False
   for name in ['_storage_expected','_storage_targets','_storage_key','_storage_action','_storage_envelope','_storage_key_version']:
    if hasattr(self.project,name):delattr(self.project,name)
 def close(self):self.db.close()

def prepare_storage(project,payload):
 from .engine import BASE,module
 storage=module('sql_knowledge._core.prepare_storage',BASE/'storage.py');storage.m=project.memory
 action=payload['action'];env=payload['args'][0];paths_to_hash=[]
 with project.connect() as db:
  op='storage-op-'+hashlib.sha256(env['idempotency_key'].encode()).hexdigest()[:32]
  old=db.execute('SELECT attributes_json FROM entities WHERE id=?',(op,)).fetchone()
  if old and 'result' in json.loads(old[0]):return {}
  if action=='register':paths_to_hash=[env['data']['path']]
  elif action in ['reconcile','cleanup_plan']:
   for ident in env['data']['entity_ids']:
    row=db.execute('SELECT path,attributes_json FROM entities WHERE id=?',(str(ident).strip(),)).fetchone()
    if row and (action=='cleanup_plan' or json.loads(row[1]).get('storage',{}).get('fingerprint')):paths_to_hash.append(row[0])
  elif action=='cleanup_execute':
   row=db.execute('SELECT attributes_json FROM entities WHERE id=?',(env['data']['plan_id'],)).fetchone()
   if row:paths_to_hash=[item['path'] for item in json.loads(row[0])['cleanup_plan']['candidates']]
 result={}
 for path in paths_to_hash:
  try:result[path]=storage.fingerprint(path)
  except FileNotFoundError:result[path]={'missing':True}
 return result

async def serve(root,identity,instance):
 from .engine import Project
 project=Project(root,expected_id=identity,expected_instance=instance,_local=True)
 path,lock,_=paths(root)
 with lock.open('a+b') as guard:
  try:fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError:return
  if fence_path(project).exists():return
  path.unlink(missing_ok=True)
  pool=concurrent.futures.ThreadPoolExecutor(max_workers=1,thread_name_prefix='sqlk-writer')
  prep_pool=concurrent.futures.ThreadPoolExecutor(max_workers=2,thread_name_prefix='sqlk-prepare')
  loop=asyncio.get_running_loop();writer=await loop.run_in_executor(pool,Writer,root,identity,instance)
  queues=collections.OrderedDict();wake=asyncio.Event();stop=asyncio.Event()
  state=dict(depth=0,bytes=0,peak_depth=0,completed=0,active=False,preparing=0,accepted=0,rejected=0,last=time.monotonic(),maintenance=False)
  async def pause():
   wake.clear()
   try:await asyncio.wait_for(wake.wait(),.1)
   except TimeoutError:pass
  async def consume():
   while not stop.is_set() or state['depth']:
    picked=None
    for _ in range(len(queues)):
     client,queue=queues.popitem(last=False);job=queue[0]
     if job['ready'] and (job['request']['kind']!='migrate' or state['depth']==1):
      picked=queue.popleft()
      if queue:queues[client]=queue
      break
     queues[client]=queue
    if picked is None:
     if not state['depth'] and not state['active'] and time.monotonic()-state['last']>15:stop.set()
     await pause();continue
    job=picked;req=job['request'];state['depth']-=1;state['bytes']-=job['size'];state['active']=True
    try:
     if job.get('error'):result=job['error']
     elif req['deadline']<time.time():result=dict(ok=False,code='queue_timeout',error='deadline expired before mutation')
     else:
      try:
       if req['kind']=='legacy_lease':result=await loop.run_in_executor(pool,writer.legacy_lease,req)
       else:result=await loop.run_in_executor(pool,writer.execute,req['kind'],req['payload'],job['prepared'],req['deadline'])
      except Exception as exc:result=error(exc)
     result.setdefault('_writer',{}).update(queue_wait_ms=max(0,(time.perf_counter()-job['queued'])*1000-result.get('_writer',{}).get('elapsed_ms',0)-job.get('preparation_ms',0)),preparation_ms=job.get('preparation_ms',0),queue_depth=state['depth'])
     if not job['future'].done():job['future'].set_result(result)
    finally:
     if req['kind']=='migrate':state['maintenance']=False
     state['active']=False;state['completed']+=1;state['last']=time.monotonic();wake.set()
  async def handle(reader,stream):
   state['last']=time.monotonic()
   try:
    raw=await asyncio.wait_for(reader.readline(),2)
    if len(raw)>MAX_REQUEST:raise CoordinatorError('limit_exceeded','IPC request too large')
    req=json.loads(raw)
    if req.get('project_id')!=identity or req.get('instance_id')!=instance:raise CoordinatorError('identity_mismatch','coordinator identity/generation mismatch')
    if req['kind']=='status':
     from .runtime import readers_status
     result=dict(ok=True,pid=os.getpid(),**state,readers=readers_status(project.root))
    elif req['kind']=='shutdown':
     set_fence(project);stop.set();wake.set();result=dict(ok=True,draining=True,maintenance=True)
    else:
     if stop.is_set():raise CoordinatorError('draining','writer is stopping')
     if state['maintenance'] or fence_path(project).exists():raise CoordinatorError('maintenance','admission closed for lifecycle maintenance')
     if req['kind'] not in ['core','extension','migrate','storage','checkpoint','legacy_lease','audit_cache']:raise CoordinatorError('invalid_operation','unknown operation')
     import tomllib
     actual=tomllib.loads((project.root/'knowledge.toml').read_text())
     if req.get('manifest_contract')!=contract(actual):raise CoordinatorError('stale_contract','manifest contract changed; restart client')
     client=str(req['client_id']);size=len(raw)
     if not isinstance(req.get('deadline'),(int,float)) or req['deadline']>time.time()+31:raise CoordinatorError('invalid_input','invalid deadline')
     if state['depth']>=MAX_QUEUE or state['bytes']+size>MAX_BYTES or len(queues.get(client,[]))>=MAX_CLIENT:
      state['rejected']+=1;raise CoordinatorError('queue_full','bounded queue full; retry_after_ms=100')
     job=dict(request=req,future=loop.create_future(),size=size,queued=time.perf_counter(),prepared=None,ready=req['kind']!='storage')
     queues.setdefault(client,collections.deque()).append(job);state['depth']+=1;state['bytes']+=size;state['accepted']+=1;state['peak_depth']=max(state['peak_depth'],state['depth'])
     if req['kind']=='migrate':state['maintenance']=True
     wake.set()
     if req['kind']=='storage':
      state['preparing']+=1;started=time.perf_counter()
      try:job['prepared']=await loop.run_in_executor(prep_pool,prepare_storage,project,req['payload'])
      except Exception as exc:job['error']=error(exc)
      finally:job['preparation_ms']=(time.perf_counter()-started)*1000;state['preparing']-=1;job['ready']=True;wake.set()
     result=await asyncio.shield(job['future'])
    lease_fd=result.pop('_lease_fd',None)
    if lease_fd is None:
     stream.write(json.dumps(result,ensure_ascii=False,separators=(',',':')).encode()+b'\n');await stream.drain()
    else:
     try:
      response=json.dumps(result,separators=(',',':')).encode()+b'\n'
      sent=stream.get_extra_info('socket')._sock.sendmsg([response],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[lease_fd]))])
      if sent<len(response):stream.write(response[sent:]);await stream.drain()
     finally:os.close(lease_fd)
   except Exception as exc:
    try:stream.write(json.dumps(error(exc)).encode()+b'\n');await stream.drain()
    except Exception:pass
   finally:
    stream.close()
    try:await stream.wait_closed()
    except Exception:pass

  server=await asyncio.start_unix_server(handle,path=str(path),limit=MAX_REQUEST+1);os.chmod(path,0o600)
  consumer=asyncio.create_task(consume())
  try:await stop.wait()
  finally:
   server.close();await server.wait_closed();wake.set();await consumer
   await loop.run_in_executor(pool,writer.close);pool.shutdown();prep_pool.shutdown();path.unlink(missing_ok=True)

def main():
 p=argparse.ArgumentParser();p.add_argument('--project',required=True);p.add_argument('--expected-id',required=True);p.add_argument('--expected-instance',required=True);a=p.parse_args();asyncio.run(serve(a.project,a.expected_id,a.expected_instance))
if __name__=='__main__':main()
