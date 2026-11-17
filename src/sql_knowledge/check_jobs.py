"""Resumable logical audits. Only the explicit job's private files are written."""
from contextlib import closing
from pathlib import Path
import hashlib
import json
import sqlite3
import time
import os
import subprocess
import sys
import threading
import fcntl
import uuid

from . import compressed, extensions, audit_cache

ALGORITHM_VERSION = 4


def _dump(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _signature(path):
    s = path.stat()
    return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns]


def _folder(project, job_id):
    if not isinstance(job_id, str) or len(job_id) != 32 or any(c not in '0123456789abcdef' for c in job_id):
        raise ValueError('invalid check job id')
    path = project.root / '.tmp/check-jobs' / job_id
    for part in (project.root/'.tmp', project.root/'.tmp/check-jobs', path):
        if part.is_symlink():
            raise ValueError('audit cache symlink is forbidden')
    return path


class _LockedState(sqlite3.Connection):
    def close(self):
        try:
            super().close()
        finally:
            guard = getattr(self, '_lifecycle_guard', None)
            if guard is not None:
                self._lifecycle_guard = None
                guard.__exit__(None, None, None)


def _problem(state, kind, **values):
    state.execute('INSERT INTO problems(body) VALUES (?)', (_dump(dict(kind=kind, **values)),))


def _meta(state):
    return json.loads(state.execute('SELECT body FROM metadata').fetchone()[0])


def _save(state, meta):
    state.execute('UPDATE metadata SET body=?', (_dump(meta),))


def _public(state, meta):
    count = state.execute('SELECT count(*) FROM problems').fetchone()[0]
    return dict(job_id=meta['job_id'], status=meta['status'], phase=meta['phase'],
                snapshot_cursor=meta['snapshot_cursor'], checked=meta['checked'],
                progress=dict(phase=meta['phase'], cursor=meta['cursor']),
                problems_count=count, ok=meta['status'] == 'complete' and count == 0,
                files=meta['files'])


def _open(project, job_id):
    # Revalidate the actual live connection on *every* call, including result/cancel.
    with closing(project.connect()):
        pass
    folder = _folder(project, job_id)
    if (folder/'state.sqlite').is_symlink() or (folder/'snapshot.sqlite').is_symlink():
        raise ValueError('audit database symlink is forbidden')
    guard = audit_cache.guard(project, job_id)
    guard.__enter__()
    try:
        state = sqlite3.connect((folder/'state.sqlite').as_uri()+'?mode=rw', uri=True,
                                timeout=2, factory=_LockedState)
        state._lifecycle_guard = guard
    except BaseException:
        guard.__exit__(None, None, None)
        raise
    try:
        state.execute('BEGIN IMMEDIATE')
        meta = _meta(state)
        if (meta['job_id'] != job_id or meta['project_id'] != project.id or meta['root'] != str(project.root)
                or meta['instance_id'] != getattr(project, 'instance_id', None)):
            raise ValueError('check job project/instance binding mismatch')
        if meta['algorithm_version'] != ALGORITHM_VERSION:
            raise ValueError('check job algorithm version mismatch')
        if _signature(folder / 'snapshot.sqlite') != meta['snapshot_signature']:
            raise ValueError('check job snapshot fingerprint mismatch')
        return folder, state, meta
    except BaseException:
        state.close()
        raise


def start(project, files=True):
    """Backup is synchronous; subsequent logical execution is batched and resumable."""
    with audit_cache.guard(project, 'admission', exclusive=True):
        audit_cache.admission(project)
        return _start(project, files)


def _start(project, files=True):
    job_id = uuid.uuid4().hex
    folder = _folder(project, job_id)
    folder.mkdir(parents=True, mode=0o700)
    snapshot = folder / 'snapshot.sqlite'
    with closing(project.connect()) as source, closing(sqlite3.connect(snapshot)) as dest:
        source.backup(dest, pages=256)
        dest.execute('PRAGMA journal_mode=DELETE')
    with closing(sqlite3.connect(snapshot)) as db:
        compressed.register_codecs(db)
        revision = db.execute('SELECT coalesce(max(seq),0) FROM events').fetchone()[0]
    sha = hashlib.sha256()
    with snapshot.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            sha.update(block)
    meta = dict(job_id=job_id, project_id=project.id, root=str(project.root),
                instance_id=getattr(project, 'instance_id', None), algorithm_version=ALGORITHM_VERSION,
                snapshot_signature=_signature(snapshot),
                snapshot_cursor=dict(snapshot_id=job_id, sha256=sha.hexdigest(), revision=revision,
                                     algorithm_version=ALGORITHM_VERSION),
                files=bool(files), status='running', phase='events', cursor=0, checked=0,
                config=project.config, extensions=[])
    with closing(sqlite3.connect(folder / 'state.sqlite')) as state:
        state.executescript('''CREATE TABLE metadata(body TEXT NOT NULL);
          CREATE TABLE problems(seq INTEGER PRIMARY KEY, body TEXT NOT NULL);
          CREATE TABLE latest(id TEXT PRIMARY KEY, body TEXT NOT NULL);
          CREATE TABLE relation_latest(id TEXT PRIMARY KEY,body TEXT NOT NULL);
          CREATE TABLE docs(rowid INTEGER PRIMARY KEY, id TEXT NOT NULL, title TEXT, body TEXT);
          CREATE INDEX docs_entity ON docs(id);
          CREATE TABLE extension_latest(tab TEXT, id TEXT, body TEXT, PRIMARY KEY(tab,id));''')
        # Capture manifests/migrations now; later checks never read evolving source files.
        try:
            definitions = extensions.specs(project)
            with closing(sqlite3.connect(snapshot)) as db:
                db.row_factory = sqlite3.Row
                compressed.register_codecs(db)
                for problem in extensions.inventory_problems(db, definitions, history=False):
                    _problem(state, **problem)
                for name, spec in definitions.items():
                    captured = folder / 'definitions' / name
                    captured.mkdir(parents=True)
                    copied = dict(spec, path=captured / 'extension.toml')
                    for i, migration in enumerate(spec['migrations']):
                        _, sql = extensions.digest(spec, i)
                        path = extensions.resolve(captured, migration)
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_text(sql)
                    try:
                        extensions.verify(project, db, copied)
                        meta['extensions'].extend(spec['tables'])
                    except Exception as exc:
                        _problem(state, 'extension_schema', extension=name, detail=str(exc))
        except Exception as exc:
            _problem(state, 'extension_manifest', detail=str(exc))
        state.execute('INSERT INTO metadata VALUES (?)', (_dump(meta),))
        state.commit()
        return _public(state, meta)


def _advance(meta, phase, cursor=0):
    meta.update(phase=phase, cursor=cursor)


def step(project, job_id, batch_size=1000):
    if type(batch_size) is not int or not 1 <= batch_size <= 10000:
        raise ValueError('batch_size must be 1..10000')
    if not (_folder(project,job_id)/'state.sqlite').exists():
        report=audit_cache.saved_result(project,job_id,0,1)
        if report is None:
            raise audit_cache.CacheError('audit_report_unavailable','audit report expired or job does not exist')
        return {k:v for k,v in report.items() if k not in ('problems','offset','limit')}
    result=_step(project,job_id,batch_size)
    if result['status']=='complete':
        result.update(audit_cache.finalize(project,job_id))
    return result


def _step(project, job_id, batch_size):
    folder, state, meta = _open(project, job_id)
    try:
        db = sqlite3.connect((folder/'snapshot.sqlite').as_uri()+'?mode=ro', uri=True)
    except BaseException:
        state.close();raise
    db.row_factory = sqlite3.Row
    compressed.register_codecs(db)
    try:
        if meta['status'] != 'running':
            return _public(state, meta)
        phase, cursor = meta['phase'], meta['cursor']
        rows = []
        if phase == 'events':
            rows = db.execute('SELECT seq,action,before_json,after_json,operation_key,request_hash,result_json FROM events WHERE seq>? ORDER BY seq LIMIT ?', (cursor,batch_size)).fetchall()
            tables = {t['name']: t for t in meta['extensions']}
            for ev in rows:
                payload = json.loads(ev['after_json'])
                before = json.loads(ev['before_json']).get('entities', {})
                after = payload.get('entities', {})
                if set(before) != set(after):
                    _problem(state,'event_entity_set_mismatch',event_seq=ev['seq'])
                for eid, body in before.items():
                    previous = state.execute('SELECT body FROM latest WHERE id=?',(eid,)).fetchone()
                    # The first observed legacy event may start from an existing
                    # untracked baseline. Later before images must join history.
                    if previous and json.loads(previous[0]) != body:
                        _problem(state,'event_continuity_mismatch',id=eid,event_seq=ev['seq'])
                receipt=json.loads(ev['result_json'])
                if not isinstance(receipt,dict) or receipt.get('operation_key')!=ev['operation_key'] or receipt.get('ok') is not True:
                    _problem(state,'receipt_mismatch',event_seq=ev['seq'])
                expected_ids = None
                if ev['action'] == 'entity':
                    expected_ids = set(after)
                elif ev['action'] in {'knowledge','decision'}:
                    expected_ids = {eid for eid, body in after.items() if body and body.get('entity',{}).get('kind') == ev['action'] and body['entity'].get('status') != 'superseded'}
                elif ev['action'] == 'link':
                    expected_ids = {rel['id'] for rel in payload.get('relations',[])}
                elif ev['action'] == 'extension_upsert':
                    extension = payload.get('extension',{}); definition = tables.get(extension.get('table'))
                    if definition and isinstance(extension.get('row'),dict):
                        expected_ids = {extension['row'].get(definition['primary_key'])}
                if expected_ids is not None and (not isinstance(receipt,dict) or receipt.get('id') not in expected_ids):
                    _problem(state,'receipt_id_mismatch',event_seq=ev['seq'])
                if len(ev['request_hash'])!=64 or any(c not in '0123456789abcdef' for c in ev['request_hash']):
                    _problem(state,'receipt_hash_invalid',event_seq=ev['seq'])
                for rel in payload.get('relations',[]):
                    state.execute('INSERT OR REPLACE INTO relation_latest VALUES (?,?)',(rel['id'],_dump(rel)))
                for eid, body in payload.get('entities', {}).items():
                    state.execute('INSERT OR REPLACE INTO latest VALUES (?,?)', (eid,_dump(body)))
                    if not db.execute('SELECT 1 FROM event_entities WHERE event_seq=? AND entity_id=?', (ev['seq'],eid)).fetchone():
                        _problem(state,'event_subject_missing',id=eid,event_seq=ev['seq'])
                for subject in db.execute('SELECT entity_id FROM event_entities WHERE event_seq=?',(ev['seq'],)):
                    if subject[0] not in payload.get('entities',{}): _problem(state,'event_subject_mismatch',id=subject[0],event_seq=ev['seq'])
                ext = payload.get('extension', {})
                if ext and ext.get('table') not in tables:
                    _problem(state,'extension_history_undeclared',table=ext.get('table'),event_seq=ev['seq'])
                t = tables.get(ext.get('table'))
                if t and isinstance(ext.get('row'),dict):
                    body = ext['row']
                    state.execute('INSERT OR REPLACE INTO extension_latest VALUES (?,?,?)', (t['name'],body[t['primary_key']],_dump(body)))
                meta['cursor'] = ev['seq']
            if not rows: _advance(meta,'fts')
        elif phase == 'fts':
            rows = db.execute('SELECT rowid,entity_id,title,body FROM search_index WHERE rowid>? ORDER BY rowid LIMIT ?', (cursor,batch_size)).fetchall()
            for r in rows:
                state.execute('INSERT INTO docs VALUES (?,?,?,?)',tuple(r))
                if not db.execute('SELECT 1 FROM entities WHERE id=?',(r['entity_id'],)).fetchone():
                    _problem(state,'search_index_orphan',id=r['entity_id'])
                meta['cursor'] = r['rowid']
            if not rows: _advance(meta,'entities','')
        elif phase == 'entities':
            rows = db.execute('SELECT * FROM entities WHERE id>? ORDER BY id LIMIT ?', (cursor,batch_size)).fetchall()
            mapping = 'search_documents' if project.memory.VERSION == 2 else '_sqlk_search_documents'
            has_mapping = db.execute('SELECT 1 FROM sqlite_master WHERE name=?',(mapping,)).fetchone()
            for e in rows:
                eid = e['id']; current = project.memory.snapshot(db,eid)
                last = state.execute('SELECT body FROM latest WHERE id=?',(eid,)).fetchone()
                if not last or json.loads(last[0]) != current: _problem(state,'history_mismatch',id=eid)
                d = current['detail']
                if e['kind'] in {'knowledge','decision'} and d is None: _problem(state,'missing_detail',id=eid)
                body = e['description']+'\n'+(d['body'] if d else '')
                indexed = state.execute('SELECT rowid,title,body FROM docs WHERE id=? ORDER BY rowid',(eid,)).fetchmany(2)
                if [(r[1],r[2]) for r in indexed] != [(e['title'],body)]: _problem(state,'search_index_mismatch',id=eid)
                rid = None
                if has_mapping:
                    mapped = db.execute('SELECT fts_rowid FROM '+mapping+' WHERE entity_id=?',(eid,)).fetchone()
                    rid = mapped[0] if mapped else None
                    if [r[0] for r in indexed] != [rid]: _problem(state,'search_mapping_mismatch',id=eid)
                if project.memory.VERSION == 2:
                    actual = db.execute('SELECT * FROM search_catalog WHERE entity_id=?',(eid,)).fetchone()
                    expected = (rid,eid,e['kind'],e['title'],e['status'],e['path'],d.get('knowledge_type') if d else None,d.get('confidence') if d else None,d.get('scope') if d else None)
                    if not actual or tuple(actual) != expected: _problem(state,'search_catalog_mismatch',id=eid)
                if meta['files'] and e['path'] and e['status']=='active' and not (project.root/e['path']).exists(): _problem(state,'missing_file',id=eid,path=e['path'])
                meta['cursor'] = eid
            if not rows: _advance(meta,'knowledge_details',None)
        elif phase in {'knowledge_details','decision_details'}:
            # Reverse coverage is necessary: snapshot() selects detail by entity
            # kind and therefore cannot see a row attached to the wrong kind.
            # rowid keysets also visit NULL/empty entity IDs and negative rowids.
            table, expected = ('knowledge','knowledge') if phase == 'knowledge_details' else ('decisions','decision')
            sql = 'SELECT d.rowid AS detail_rowid,d.entity_id,e.kind FROM '+table+' d LEFT JOIN entities e ON e.id=d.entity_id'
            if cursor is None:
                rows = db.execute(sql+' ORDER BY d.rowid LIMIT ?', (batch_size,)).fetchall()
            else:
                rows = db.execute(sql+' WHERE d.rowid>? ORDER BY d.rowid LIMIT ?', (cursor,batch_size)).fetchall()
            for row in rows:
                if row['kind'] is None:
                    _problem(state,'detail_entity_missing',table=table,id=row['entity_id'])
                elif row['kind'] != expected:
                    _problem(state,'detail_kind_mismatch',table=table,id=row['entity_id'],actual_kind=row['kind'],expected_kind=expected)
                meta['cursor'] = row['detail_rowid']
            if not rows:
                if phase == 'knowledge_details': _advance(meta,'decision_details',None)
                else: _advance(meta,'orphans','')
        elif phase == 'orphans':
            rows = state.execute('SELECT id FROM latest WHERE id>? ORDER BY id LIMIT ?', (cursor,batch_size)).fetchall()
            for r in rows:
                if not db.execute('SELECT 1 FROM entities WHERE id=?',(r[0],)).fetchone(): _problem(state,'history_entity_missing',id=r[0])
                meta['cursor'] = r[0]
            if not rows: _advance(meta,'relations','')
        elif phase == 'relations':
            rows=db.execute('SELECT * FROM relations WHERE id>? ORDER BY id LIMIT ?',(cursor,batch_size)).fetchall()
            for row in rows:
                old=state.execute('SELECT body FROM relation_latest WHERE id=?',(row['id'],)).fetchone()
                if not old or json.loads(old[0])!=dict(row):_problem(state,'relation_history_mismatch',id=row['id'])
                meta['cursor']=row['id']
            if not rows:_advance(meta,'relations_missing','')
        elif phase == 'relations_missing':
            rows=state.execute('SELECT id FROM relation_latest WHERE id>? ORDER BY id LIMIT ?',(cursor,batch_size)).fetchall()
            for row in rows:
                if not db.execute('SELECT 1 FROM relations WHERE id=?',(row[0],)).fetchone():_problem(state,'relation_missing',id=row[0])
                meta['cursor']=row[0]
            if not rows:_advance(meta,'mapping',0)
        elif phase == 'mapping':
            table = 'search_documents' if project.memory.VERSION == 2 else '_sqlk_search_documents'
            if db.execute('SELECT 1 FROM sqlite_master WHERE name=?',(table,)).fetchone():
                rows = db.execute('SELECT entity_id,fts_rowid FROM '+table+' WHERE fts_rowid>? ORDER BY fts_rowid LIMIT ?', (cursor,batch_size)).fetchall()
                for r in rows:
                    if not db.execute('SELECT 1 FROM entities WHERE id=?',(r[0],)).fetchone(): _problem(state,'search_mapping_orphan',id=r[0])
                    meta['cursor'] = r[1]
            if not rows: _advance(meta,'catalog',0)
        elif phase == 'catalog':
            if project.memory.VERSION == 2:
                rows=db.execute('SELECT fts_rowid,entity_id FROM search_catalog WHERE fts_rowid>? ORDER BY fts_rowid LIMIT ?',(cursor,batch_size)).fetchall()
                for r in rows:
                    if not db.execute('SELECT 1 FROM entities WHERE id=?',(r[1],)).fetchone(): _problem(state,'search_catalog_orphan',id=r[1])
                    meta['cursor']=r[0]
            if not rows: _advance(meta,'extensions','')
        elif phase == 'extensions':
            index = meta.get('extension_index',0)
            if index >= len(meta['extensions']): _advance(meta,'physical')
            else:
                t = meta['extensions'][index]; table=t['name']; key=t['primary_key']
                rows = db.execute('SELECT * FROM '+table+' WHERE '+key+'>? ORDER BY '+key+' LIMIT ?', (cursor,batch_size)).fetchall()
                for r in rows:
                    old = state.execute('SELECT body FROM extension_latest WHERE tab=? AND id=?',(table,r[key])).fetchone()
                    if not old: _problem(state,'extension_history_missing',table=table,id=r[key])
                    else:
                        comparison=json.loads(old[0]); infos=db.execute('PRAGMA table_info('+table+')').fetchall()
                        # SQLite itself applies column affinity to historical omitted defaults.
                        for info in infos:
                            if info['name'] not in comparison:
                                comparison[info['name']]=extensions.default_with_affinity(db,info)
                        if comparison != dict(r): _problem(state,'extension_history_mismatch',table=table,id=r[key])
                    meta['cursor']=r[key]
                if not rows: _advance(meta,'extension_missing','')
        elif phase == 'extension_missing':
            t=meta['extensions'][meta['extension_index'] if 'extension_index' in meta else 0];table=t['name'];key=t['primary_key']
            rows=state.execute('SELECT id FROM extension_latest WHERE tab=? AND id>? ORDER BY id LIMIT ?',(table,cursor,batch_size)).fetchall()
            for r in rows:
                if not db.execute('SELECT 1 FROM '+table+' WHERE '+key+'=?',(r[0],)).fetchone(): _problem(state,'extension_row_missing',table=table,id=r[0])
                meta['cursor']=r[0]
            if not rows:
                meta['extension_index']=meta.get('extension_index',0)+1
                _advance(meta,'extensions','')
        elif phase == 'physical':
            output = folder / 'physical.sqlite'
            done = folder / 'physical.done'
            if done.exists():
                with closing(sqlite3.connect(output)) as physical:
                    rows = physical.execute('SELECT seq,body FROM problems WHERE seq>? ORDER BY seq LIMIT ?', (cursor,batch_size)).fetchall()
                    for r in rows:
                        state.execute('INSERT INTO problems(body) VALUES (?)',(r[1],))
                        meta['cursor']=r[0]
                if not rows:
                    meta['status']='complete' if done.read_text()=='complete' else 'failed'
                    _advance(meta,'done')
            elif (not meta.get('physical_pid') or not _alive(meta['physical_pid']) or
                  time.time()-max(meta.get('physical_started',0),(folder/'heartbeat').stat().st_mtime if (folder/'heartbeat').exists() else 0)>10):
                # Restart an interrupted physical worker; no partial physical pass is accepted.
                process=subprocess.Popen([sys.executable,'-m','sql_knowledge.check_jobs','--physical',str(folder)],
                                         stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
                                         start_new_session=True)
                threading.Thread(target=process.wait,daemon=True).start()
                meta['physical_pid']=process.pid
                meta['physical_started']=time.time()
        if meta['status']=='complete' and 'completed_at' not in meta:
            import datetime
            meta['completed_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
        meta['checked'] += len(rows)
        _save(state,meta);state.commit()
        return _public(state,meta)
    except Exception as exc:
        state.rollback()
        meta=_meta(state)
        _problem(state,'check_phase_error',phase=meta['phase'],detail=str(exc))
        meta['status']='failed'
        _save(state,meta);state.commit()
        return _public(state,meta)
    finally:
        db.close();state.close()


def result(project, job_id, offset=0, limit=100):
    if type(offset) is not int or offset<0 or type(limit) is not int or not 1<=limit<=1000:
        raise ValueError('offset>=0 and limit1..1000 required')
    saved=audit_cache.saved_result(project,job_id,offset,limit)
    if saved is not None:
        return saved
    folder = _folder(project, job_id)
    if not (folder/'state.sqlite').exists():
        raise audit_cache.CacheError('audit_report_unavailable','audit report expired or job does not exist')
    _,state,meta=_open(project,job_id)
    try:
        return dict(_public(state,meta),problems=[json.loads(r[0]) for r in state.execute('SELECT body FROM problems ORDER BY seq LIMIT ? OFFSET ?',(limit,offset))],offset=offset,limit=limit)
    finally: state.close()


def cancel(project, job_id):
    if not (_folder(project,job_id)/'state.sqlite').exists():
        return result(project,job_id,0,1)
    _,state,meta=_open(project,job_id)
    try:
        if meta['status']=='running':
            meta['status']='cancelled'
            (_folder(project,job_id)/'cancelled').touch()
        _save(state,meta);state.commit()
        return _public(state,meta)
    finally: state.close()


def _alive(pid):
    try:
        os.kill(pid,0)
        return True
    except ProcessLookupError:
        return False


def _physical_body(folder):
    """Independent worker: never opens the live DB; bounds memory and wall time."""
    folder=Path(folder)
    with closing(sqlite3.connect((folder/'snapshot.sqlite').as_uri()+'?mode=ro',uri=True)) as db, closing(sqlite3.connect(folder/'physical.sqlite')) as output:
        compressed.register_codecs(db)
        output.execute('CREATE TABLE IF NOT EXISTS problems(seq INTEGER PRIMARY KEY,body TEXT)')
        output.execute('DELETE FROM problems')
        deadline=time.monotonic()+3600
        db.set_progress_handler(lambda: int((folder/'cancelled').exists() or time.monotonic()>deadline),10000)
        status='complete'
        try:
            for row in db.execute('PRAGMA integrity_check'):
                if row[0]!='ok': _problem(output,'sqlite_integrity',detail=row[0])
            for row in db.execute('PRAGMA foreign_key_check'):
                _problem(output,'foreign_key',detail=list(row))
        except Exception as exc:
            _problem(output,'physical_incomplete',detail=str(exc));status='failed'
        output.commit()
        temporary=folder/'physical.done.tmp'
        temporary.write_text(status)
        temporary.replace(folder/'physical.done')


def _physical(folder):
    folder=Path(folder)
    # A live PID alone is insufficient after PID reuse. The OS lock also prevents
    # two recovery workers from writing the same output when a lease is stale.
    from types import SimpleNamespace
    project = SimpleNamespace(root=folder.parents[2])
    with audit_cache.guard(project, folder.name), (folder/'physical.lock').open('a+b') as guard:
        try:fcntl.flock(guard,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:return
        stopped=threading.Event()
        def heartbeat():
            while not stopped.is_set():
                (folder/'heartbeat').touch()
                stopped.wait(1)
        thread=threading.Thread(target=heartbeat,daemon=True);thread.start()
        try:_physical_body(folder)
        finally:stopped.set();thread.join(timeout=2)


if __name__=='__main__':
    if len(sys.argv)==3 and sys.argv[1]=='--physical': _physical(sys.argv[2])
