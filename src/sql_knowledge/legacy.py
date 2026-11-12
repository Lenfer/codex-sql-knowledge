"""Explicit, local-only compatibility for existing multi-statement writers.

Opt in with [compatibility] legacy_writes=true in knowledge.toml. The coordinator
queues an exclusive transaction flock and delegates its open description using
SCM_RIGHTS. There is no SQL RPC, TTL, or early unlock: SQLite closes first.
"""
import os,sqlite3,threading
from pathlib import Path
from .engine import Project
from .coordinator import submit,legacy_enabled,CoordinatorError,contract,fence_path

class Connection:
 """Forward SQLite attributes; keep the OS lease until SQLite is closed."""
 def __init__(self,db,fd,project):
  object.__setattr__(self,'_db',db);object.__setattr__(self,'_fd',fd);object.__setattr__(self,'_project',project)
 def __getattr__(self,name):return getattr(self._db,name)
 def __setattr__(self,name,value):
  if name.startswith('_'):object.__setattr__(self,name,value)
  else:setattr(self._db,name,value)
 def close(self):
  if self._fd is None:return
  # Do not release the lease when close itself fails (e.g. wrong thread).
  self._db.close();fd=self._fd;self._fd=None;os.close(fd)
 def __enter__(self):self._db.__enter__();return self
 def __exit__(self,*args):
  try:return self._db.__exit__(*args)
  finally:self.close()
 def __del__(self):
  try:self.close()
  except Exception:pass


def connect(root,writable=False,*,timeout=8):
 """Open a guarded connection; writable leases require explicit manifest opt-in."""
 if writable and sqlite3.sqlite_version_info<(3,51,3):
  raise CoordinatorError('sqlite_version','legacy WAL writes require patched SQLite >=3.51.3; run the script with the globally installed SQL Knowledge Python, not an older embedded Python such as Blender')
 project=Project(Path(root))
 if not writable:return project.connect()
 if not legacy_enabled(project.config):raise CoordinatorError('legacy_disabled','manifest must opt in to legacy writes')
 result=submit(project,'legacy_lease',{},timeout=timeout);fd=result['_lease_fd']
 db=None
 try:
  # Local authorization is constructed only after the coordinator delegated lock.
  local=Project(project.root,expected_id=project.id,expected_instance=project.instance_id,_local=True)
  if contract(local.config)!=contract(project.config):raise CoordinatorError('stale_contract','manifest changed after lease grant')
  if fence_path(local).exists():raise CoordinatorError('maintenance','project fenced after lease grant')
  db=local.connect(True)
  return Connection(db,fd,local)
 except BaseException:
  if db is not None:db.close()
  os.close(fd);raise

_BOUND={}
_BIND_LOCK=threading.RLock()
def bind(root):
 with _BIND_LOCK:return _bind(root)

def _bind(root):
 """Return the cached memory API for a legacy shim, with queued writable connect.

Existing functions/constants and MemoryError remain from one bound runtime.
connect(path=None,writable=False) only accepts this project's database path.
mutate(...,transaction=False) supports callers' explicit BEGIN/COMMIT blocks.
"""
 root=Path(root).resolve();key=str(root)
 if key not in _BOUND:
  project=Project(root);memory=project.memory
  def bound_connect(path=None,writable=False):
   if path is not None and Path(path).resolve()!=project.database:raise ValueError('legacy database path differs from explicit project root')
   return connect(root,writable)
  memory.connect=bound_connect
  _BOUND[key]=memory
 return _BOUND[key]

def mutate(db,action,envelope,transaction=True):
 """Compatibility mutation for users calling connect directly."""
 if not isinstance(db,Connection):raise ValueError('legacy writable connection required')
 return bind(db._project.root).mutate(db,action,envelope,transaction=transaction)
