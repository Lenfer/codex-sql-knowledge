"""Automatic, bounded audit cache maintenance; no model calls or OS service.

Only complete audits are eligible. Reports use a disposable service SQLite store,
not catalog entities/events. Save and verify a report before unlinking snapshots.
"""
from contextlib import contextmanager, closing
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import stat
import subprocess
import time
import datetime as dt

ERROR_REPORTS = 20
SUCCESS_REPORTS = 1
MAINTENANCE_SECONDS = 300
STORE_PARTS = ['.tmp', 'check-reports']
MAX_FILES = 512
ROOT_FILES = {'cancelled', 'heartbeat', 'physical.lock', 'physical.done', 'physical.done.tmp'}
ROOT_FILES |= {base + suffix for base in ('snapshot.sqlite', 'state.sqlite', 'physical.sqlite')
               for suffix in ('', '-wal', '-shm', '-journal')}


class CacheError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def require(condition, message, code='audit_cache_unsafe'):
    if not condition:
        raise CacheError(code, message)


def packed(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def valid_id(job_id):
    require(isinstance(job_id, str) and len(job_id) == 32
            and all(c in '0123456789abcdef' for c in job_id), 'invalid audit job ID')
    return job_id


@contextmanager
def directory(project, parts, create=False):
    """All traversal uses directory descriptors and refuses symlink components."""
    fd = os.open(project.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts:
            require(part not in ('', '.', '..') and '/' not in part, 'unsafe directory part')
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=fd)
                except FileExistsError:
                    pass
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


@contextmanager
def guard(project, name, exclusive=False):
    require(name == 'admission' or valid_id(name), 'invalid lifecycle lock')
    with directory(project, ['.tmp', 'check-jobs', '.locks'], create=True) as parent:
        lock_name = 'admission' if name == 'admission' else f'{int(name, 16) % 64:02d}'
        fd = os.open(lock_name + '.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=parent)
    try:
        require(stat.S_ISREG(os.fstat(fd).st_mode), 'lifecycle lock is not regular')
        try:
            fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CacheError('audit_cache_busy', 'audit job is in use; retry after the active call') from exc
        yield
    finally:
        os.close(fd)


def admission(project):
    """Bound new snapshot admission only; never runs Disk Budget or deletes files."""
    options = project.config.get('audit_cache', {})
    max_jobs = options.get('max_jobs')
    max_bytes = options.get('max_bytes')
    if max_jobs is None and max_bytes is None:
        return  # report rotation must not introduce an implicit audit-start quota
    require((max_jobs is None or type(max_jobs) is int and max_jobs > 0)
            and (max_bytes is None or type(max_bytes) is int and max_bytes > 0),
            'audit_cache max_jobs/max_bytes must be positive integers')
    count = size = 0
    with directory(project, ['.tmp', 'check-jobs'], create=True) as base:
        for name in os.listdir(base):
            if name.startswith('.'):
                continue
            path = project.root / '.tmp/check-jobs' / name
            if path.is_symlink():
                raise CacheError('audit_cache_unsafe', 'symlink in audit cache; inspect manually')
            # Historical manual cleanup can leave only heartbeat/done/lock markers.
            # Preserve these remnants and account their bytes, but count only
            # directories still holding a state/snapshot as on-disk audit jobs.
            if (path/'state.sqlite').exists() or (path/'snapshot.sqlite').exists():
                count += 1
            for root, dirs, files in os.walk(path, followlinks=False):
                for filename in files:
                    info = (Path(root) / filename).lstat()
                    size += info.st_size
    # Scratch indexes need headroom. This is admission accounting, not a disk quota.
    estimate = 2 * project.database.stat().st_size
    require((max_jobs is None or count < max_jobs) and (max_bytes is None or size + estimate <= max_bytes),
            f'audit cache admission limit: {count}/{max_jobs} jobs, {size} bytes; '
            'completed caches need maintenance; inspect protected caches or adjust [audit_cache]; '
            'running jobs can still resume', 'audit_cache_full')


def _fingerprint(parent, name):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
    with os.fdopen(fd, 'rb') as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode), 'only regular audit files are eligible')
        sha = hashlib.sha256()
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            sha.update(block)
        after = os.fstat(stream.fileno())
        keys = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        require(all(getattr(before, k) == getattr(after, k) for k in keys), 'audit file changed while hashing')
        return dict(sha256=sha.hexdigest(), **{k: getattr(after, k) for k in keys})


def inventory(project, job_id):
    files, dirs = [], []
    def walk(parent, prefix=''):
        for name in sorted(os.listdir(parent)):
            relative = prefix + name
            info = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                require(relative == 'definitions' or relative.startswith('definitions/'),
                        'unexpected directory in audit cache')
                dirs.append(relative)
                fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                try:
                    walk(fd, relative + '/')
                finally:
                    os.close(fd)
            else:
                require(stat.S_ISREG(info.st_mode) and (relative in ROOT_FILES or
                        relative.startswith('definitions/')), 'unexpected or nonregular audit cache member')
                files.append(dict(path=relative, fingerprint=_fingerprint(parent, name)))
                require(len(files) <= MAX_FILES, 'audit cache exceeds bounded cleanup manifest')
    with directory(project, ['.tmp', 'check-jobs', valid_id(job_id)]) as fd:
        root = os.fstat(fd)
        walk(fd)
    return dict(root=[root.st_dev, root.st_ino], files=files, directories=dirs)


def closed(project, job_id, members):
    executable = shutil.which('lsof')
    require(executable is not None, 'lsof required to verify inactivity', 'activity_unknown')
    paths = [str(project.root / '.tmp/check-jobs' / job_id / f['path']) for f in members
             if (project.root / '.tmp/check-jobs' / job_id / f['path']).exists()]
    if not paths:
        return
    result = subprocess.run([executable, '-t', '--', *paths], capture_output=True, text=True, timeout=15)
    require(result.returncode == 1 and not result.stdout.strip() and not result.stderr.strip(),
            'audit files are open or inactivity cannot be verified', 'audit_cache_busy')


def _binding(project):
    with closing(project.connect()):
        pass
    return [project.id, str(project.root), project.instance_id]


@contextmanager
def store(project, writable=False):
    """Disposable service state, separate from knowledge and append-only events."""
    binding = _binding(project)
    path = project.root.joinpath(*STORE_PARTS, 'reports.sqlite')
    if writable:
        with directory(project, STORE_PARTS, create=True) as parent:
            fd = os.open('reports.sqlite', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600, dir_fd=parent)
            require(stat.S_ISREG(os.fstat(fd).st_mode), 'report store must be regular')
            os.close(fd)
    else:
        with directory(project, STORE_PARTS):
            require(not path.is_symlink(), 'report store symlink is forbidden')
    for suffix in ('-journal', '-wal', '-shm'):
        require(not path.with_name(path.name+suffix).is_symlink(), 'report journal symlink is forbidden')
    with closing(sqlite3.connect(path.as_uri() + ('?mode=rw' if writable else '?mode=ro'), uri=True, timeout=2)) as db:
        db.row_factory = sqlite3.Row
        if writable:
            tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            require(not tables or 'binding' in tables,'refuse to initialize over a foreign report database')
            db.execute('PRAGMA synchronous=FULL')
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('PRAGMA auto_vacuum=FULL')
            db.executescript('''
              CREATE TABLE IF NOT EXISTS binding(body TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS reports(
                job_id TEXT PRIMARY KEY, completed_at TEXT NOT NULL, ok INTEGER NOT NULL,
                body TEXT NOT NULL, report_sha256 TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS problems(
                job_id TEXT NOT NULL REFERENCES reports(job_id) ON DELETE CASCADE,
                seq INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(job_id,seq));
              CREATE TABLE IF NOT EXISTS cleanup(
                job_id TEXT PRIMARY KEY REFERENCES reports(job_id) ON DELETE RESTRICT,
                body TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS maintenance(
                id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL);
            ''')
            row = db.execute('SELECT body FROM binding').fetchone()
            if row is None:
                db.execute('INSERT INTO binding VALUES (?)', (packed(binding),));db.commit()
            else:
                require(json.loads(row[0]) == binding, 'report store belongs to another project/generation')
        else:
            row = db.execute('SELECT body FROM binding').fetchone()
            require(row and json.loads(row[0]) == binding, 'report store belongs to another project/generation')
        yield db


def _meta(project, job_id):
    path = project.root / '.tmp/check-jobs' / valid_id(job_id) / 'state.sqlite'
    with directory(project, ['.tmp', 'check-jobs', job_id]):
        require(not path.is_symlink(), 'audit state symlink')
        with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as db:
            meta = json.loads(db.execute('SELECT body FROM metadata').fetchone()[0])
    require([meta.get('project_id'), meta.get('root'), meta.get('instance_id')] == _binding(project)
            and meta.get('job_id') == job_id, 'foreign audit project, generation or job ID')
    require(meta.get('status') == 'complete', 'incomplete audits are protected', 'audit_cache_incomplete')
    return meta


def _protect(project, job_id):
    prefix = '.tmp/check-jobs/' + job_id
    with closing(project.connect()) as db:
        for row in db.execute('SELECT * FROM entities WHERE path=? OR substr(path,1,?)=?',
                              (prefix, len(prefix)+1, prefix+'/')):
            attrs = json.loads(row['attributes_json'])
            require(row['status'] in {'deleted', 'missing'}
                    and attrs.get('storage', {}).get('retention') != 'keep',
                    'audit has a catalog owner; preserve and reconcile explicitly', 'audit_cache_protected')
        # Recognize a keep/live marker from a previously registered audit card too.
        for row in db.execute('SELECT attributes_json FROM entities WHERE id IN (?,?)',
                              ('audit-cache-'+job_id, 'disk-budget-check-job-'+job_id)):
            attrs = json.loads(row[0])
            require(attrs.get('storage', {}).get('retention') != 'keep'
                    and attrs.get('audit_cache', {}).get('retention') != 'keep'
                    and not attrs.get('protected') and not attrs.get('live_consumers')
                    and not attrs.get('disk_budget', {}).get('protected'),
                    'audit is keep/live/protected', 'audit_cache_protected')


def _digest_report(db, job_id, summary):
    digest = hashlib.sha256(packed(summary).encode())
    count = 0
    for row in db.execute('SELECT body FROM problems WHERE job_id=? ORDER BY seq', (job_id,)):
        digest.update(b'\n');digest.update(row[0].encode());count += 1
    require(count == summary['problems_count'], 'report problem count mismatch')
    return digest.hexdigest()


def _verify_report(db, job_id):
    row = db.execute('SELECT * FROM reports WHERE job_id=?', (job_id,)).fetchone()
    require(row is not None, 'saved audit report is missing')
    summary = json.loads(row['body'])
    require(_digest_report(db, job_id, summary) == row['report_sha256'], 'saved report fingerprint mismatch')
    return summary


def _save_report(project, db, job_id, meta, members):
    """Save report + recoverable unlink manifest atomically, before any unlink."""
    state_path = project.root/'.tmp/check-jobs'/job_id/'state.sqlite'
    with closing(sqlite3.connect(state_path.as_uri()+'?mode=ro', uri=True)) as state:
        state.execute('BEGIN')
        require(json.loads(state.execute('SELECT body FROM metadata').fetchone()[0]) == meta,
                'audit changed before report save')
        count = state.execute('SELECT count(*) FROM problems').fetchone()[0]
        finished = meta.get('completed_at') or dt.datetime.fromtimestamp(state_path.stat().st_mtime,dt.timezone.utc).isoformat()
        summary = dict(job_id=job_id, completed_at=finished, revision=meta['snapshot_cursor']['revision'],
            status='complete', phase=meta['phase'], snapshot_cursor=meta['snapshot_cursor'],
            checked=meta['checked'], files=meta['files'], progress=dict(phase=meta['phase'],cursor=meta['cursor']),
            problems_count=count, ok=count==0, algorithm_version=meta['algorithm_version'])
        db.execute('BEGIN IMMEDIATE')
        try:
            require(db.execute('SELECT 1 FROM reports WHERE job_id=?',(job_id,)).fetchone() is None,
                    'report already exists without cleanup intent')
            db.execute('INSERT INTO reports VALUES (?,?,?,?,?)', (job_id,finished,int(count==0),packed(summary),''))
            # Audit problems already contain reasons/IDs, not copies of catalog cards.
            # Stream all rows, including >1000 problems; do not truncate error evidence.
            for seq,body in state.execute('SELECT seq,body FROM problems ORDER BY seq'):
                value=json.loads(body)
                db.execute('INSERT INTO problems VALUES (?,?,?)',(job_id,seq,packed(value)))
            digest = _digest_report(db,job_id,summary)
            db.execute('UPDATE reports SET report_sha256=? WHERE job_id=?',(digest,job_id))
            intent=dict(binding=_binding(project),job_id=job_id,members=members,report_sha256=digest)
            db.execute('INSERT INTO cleanup VALUES (?,?)',(job_id,packed(intent)))
            db.commit()
        except BaseException:
            db.rollback();raise
    _verify_report(db,job_id)


def _release(project, db, job_id):
    with guard(project,job_id,exclusive=True):
        _protect(project,job_id)
        row=db.execute('SELECT body FROM cleanup WHERE job_id=?',(job_id,)).fetchone()
        root=project.root/'.tmp/check-jobs'/job_id
        if row:
            intent=json.loads(row[0])
            require(intent['binding']==_binding(project),'foreign cleanup intent')
            summary=_verify_report(db,job_id)
            saved=db.execute('SELECT report_sha256 FROM reports WHERE job_id=?',(job_id,)).fetchone()[0]
            require(saved==intent['report_sha256'],'report replaced since cleanup intent')
            expected=intent['members']
        else:
            meta=_meta(project,job_id)
            expected=inventory(project,job_id)
            closed(project,job_id,expected['files'])
            require(meta==_meta(project,job_id),'audit metadata changed')
            _save_report(project,db,job_id,meta,expected)
            summary=_verify_report(db,job_id)
        if root.exists():
            current=inventory(project,job_id)
            require(current['root']==expected['root'],'audit directory replaced')
            wanted={f['path']:f for f in expected['files']}
            require(all(wanted.get(f['path'])==f for f in current['files'])
                    and set(current['directories'])<=set(expected['directories']),
                    'audit cache changed after report save')
            closed(project,job_id,current['files'])
            if (root/'state.sqlite').exists():
                _meta(project,job_id)
            _protect(project,job_id)
            with directory(project,['.tmp','check-jobs',job_id]) as fd:
                require([os.fstat(fd).st_dev,os.fstat(fd).st_ino]==expected['root'],'late audit folder replacement')
                # Remove state after all other files. A committed intent handles a
                # crash after any unlink, including after state/directory removal.
                for item in sorted(expected['files'],key=lambda f:f['path']=='state.sqlite'):
                    parts=list(PurePosixPath(item['path']).parts)
                    try:
                        with directory(project,['.tmp','check-jobs',job_id,*parts[:-1]]) as parent:
                            require(_fingerprint(parent,parts[-1])==item['fingerprint'],'late audit file replacement')
                            os.unlink(parts[-1],dir_fd=parent)
                    except FileNotFoundError:
                        pass  # durable intent proves the exact earlier file set
                for path in sorted(expected['directories'],key=lambda p:p.count('/'),reverse=True):
                    try:os.rmdir(path,dir_fd=fd)
                    except FileNotFoundError:pass
            with directory(project,['.tmp','check-jobs']) as parent:
                os.rmdir(job_id,dir_fd=parent)
        db.execute('DELETE FROM cleanup WHERE job_id=?',(job_id,));db.commit()
        return dict(job_id=job_id,deleted_bytes=sum(f['fingerprint']['st_size'] for f in expected['files']),
                    report_ok=summary['ok'])


def _prune(db):
    deleted=0
    for ok,keep in ((0,ERROR_REPORTS),(1,SUCCESS_REPORTS)):
        # A rare interrupted cleanup keeps its report until release can resume.
        # It is a pending operation, not an additional retained finished report.
        ids=[r[0] for r in db.execute('''SELECT job_id FROM reports WHERE ok=?
              AND job_id NOT IN (SELECT job_id FROM cleanup)
              ORDER BY completed_at DESC,job_id DESC LIMIT -1 OFFSET ?''',(ok,keep))]
        for ident in ids:
            db.execute('DELETE FROM reports WHERE job_id=?',(ident,));deleted+=1
    db.commit()
    return deleted


def dispatch(project, payload):
    """Called only by the coordinator while holding its project transaction lease."""
    require(payload.get('action')=='maintain','unsupported audit maintenance action')
    selected=payload.get('job_id')
    if selected is not None:valid_id(selected)
    output=dict(ok=True,released=[],skipped=[],pruned_reports=0,retention=dict(errors=ERROR_REPORTS,successes=SUCCESS_REPORTS))
    with store(project,writable=True) as db:
        with directory(project,['.tmp','check-jobs'],create=True) as parent:
            candidates=set(os.listdir(parent))
        candidates.update(r[0] for r in db.execute('SELECT job_id FROM cleanup'))
        candidates=[selected] if selected else sorted(v for v in candidates if len(v)==32 and all(c in '0123456789abcdef' for c in v))
        attempted=0
        for job_id in candidates:
            if selected is None:
                # Do not hash or open incomplete jobs' snapshots during a sweep.
                if db.execute('SELECT 1 FROM cleanup WHERE job_id=?',(job_id,)).fetchone() is None:
                    try:_meta(project,job_id)
                    except Exception:continue
            try:
                output['released'].append(_release(project,db,job_id))
            except Exception as exc:
                db.rollback()
                output['skipped'].append(dict(job_id=job_id,code=getattr(exc,'code','audit_cache_error'),reason=str(exc)))
            attempted+=1
            if attempted>=16:break
        output['pruned_reports']=_prune(db)
        db.execute('INSERT OR REPLACE INTO maintenance VALUES (1,?)',
                   (packed(dict(at=dt.datetime.now(dt.timezone.utc).isoformat(),**output)),));db.commit()
    return output


def maintain(project, job_id=None):
    from .coordinator import submit
    return submit(project,'audit_cache',dict(action='maintain',job_id=job_id),timeout=120)


def finalize(project, job_id):
    """A failed service save/cleanup never turns a complete audit into a false pass."""
    try:
        result=maintain(project,job_id)
        return dict(cache_state='released' if result['released'] else 'pending',maintenance=result)
    except Exception as exc:
        return dict(cache_state='pending',maintenance_error=str(exc))


def saved_result(project, job_id, offset=0, limit=100):
    valid_id(job_id)
    require(type(offset) is int and offset>=0 and type(limit) is int and 1<=limit<=1000,'invalid report page')
    try:
        with store(project) as db:
            row=db.execute('SELECT body FROM reports WHERE job_id=?',(job_id,)).fetchone()
            if row is None:return None
            summary=_verify_report(db,job_id)
            problems=[json.loads(r[0]) for r in db.execute('SELECT body FROM problems WHERE job_id=? ORDER BY seq LIMIT ? OFFSET ?',(job_id,limit,offset))]
            pending=db.execute('SELECT 1 FROM cleanup WHERE job_id=?',(job_id,)).fetchone() is not None
            return dict(summary,problems=problems,offset=offset,limit=limit,retained=True,
                        cache_state='pending' if pending else 'released')
    except FileNotFoundError:
        return None


def reports(project):
    try:
        with store(project) as db:
            rows=[json.loads(r[0]) for r in db.execute('SELECT body FROM reports ORDER BY completed_at DESC,job_id DESC')]
            last=db.execute('SELECT body FROM maintenance WHERE id=1').fetchone()
            return dict(ok=True,reports=rows,retention=dict(errors=ERROR_REPORTS,successes=SUCCESS_REPORTS),
                        last_maintenance=json.loads(last[0]) if last else None)
    except FileNotFoundError:
        return dict(ok=True,reports=[],retention=dict(errors=ERROR_REPORTS,successes=SUCCESS_REPORTS),last_maintenance=None)


async def periodic(project, stop, interval=MAINTENANCE_SECONDS):
    import asyncio
    import logging
    while not stop.is_set():
        try:
            await asyncio.to_thread(maintain,project)
        except Exception as exc:
            logging.getLogger(__name__).warning('Audit maintenance deferred: %s',exc)
        try:
            await asyncio.wait_for(stop.wait(),timeout=interval)
        except TimeoutError:
            pass
