"""Cancellation/deadline and bounded telemetry for short-lived live readers."""
import contextvars,json,os,threading,time,uuid
from pathlib import Path
CANCEL=contextvars.ContextVar('sqlk_cancel',default=None)
_LOCK=threading.RLock()
_READERS={}
_NONCE=uuid.uuid4().hex

def _publish(root):
 folder=Path(root)/'.tmp/readers';folder.mkdir(parents=True,exist_ok=True)
 path=folder/(str(os.getpid())+'-'+_NONCE+'.json')
 rows=[r for r in _READERS.values() if r['root']==str(root)]
 if not rows:path.unlink(missing_ok=True);return
 temporary=path.with_suffix('.'+uuid.uuid4().hex+'.tmp')
 temporary.write_text(json.dumps(dict(pid=os.getpid(),readers=rows)));temporary.replace(path)

def track_connection(db,project,writable=False):
 if writable:return
 token=uuid.uuid4().hex;started=time.monotonic();cancel=CANCEL.get()
 db.execute('PRAGMA busy_timeout=250')
 db._sqlk_progress=lambda:int(time.monotonic()-started>5 or (cancel is not None and cancel.is_set()))
 db.set_progress_handler(db._sqlk_progress,1000)
 with _LOCK:
  _READERS[token]=dict(token=token,root=str(project.root),started=time.time(),deadline_seconds=5)
  try:_publish(project.root)
  except OSError:pass
 def closed():
  with _LOCK:
   _READERS.pop(token,None)
   try:_publish(project.root)
   except OSError:pass
 db._sqlk_on_close=closed

def readers_status(root):
 records=[];now=time.time();folder=Path(root)/'.tmp/readers'
 for path in folder.glob('*.json') if folder.exists() else []:
  try:
   data=json.loads(path.read_text())
   try:os.kill(data['pid'],0)
   except ProcessLookupError:path.unlink(missing_ok=True);continue
   for row in data['readers']:records.append(dict(pid=data['pid'],age_seconds=max(0,now-row['started']),deadline_seconds=row['deadline_seconds'],overdue=now-row['started']>row['deadline_seconds']))
  except (OSError,ValueError,KeyError,TypeError):continue
 return dict(active=len(records),oldest_seconds=max((r['age_seconds'] for r in records),default=0),snapshots=records)
