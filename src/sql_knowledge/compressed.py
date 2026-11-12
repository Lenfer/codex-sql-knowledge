"""Experimental common runtime: explicit project, optional domain extension.

Compatibility reader/writer for the archived compressed schema2; no SQL dumps.
Physical v2 stores immutable, deduplicated compressed event payloads.
"""
from pathlib import Path
from contextlib import contextmanager
from types import SimpleNamespace
from collections import OrderedDict
import hashlib,importlib.util,json,os,sqlite3,tempfile,uuid,zlib,gzip

HERE=Path(__file__).resolve().parent
FROZEN=HERE/'_core/compressed_memory.py'
BASE_KINDS={'project','file','document','script','skill','source','model','experiment','report'}
BASE_PREDICATES={'relates_to','derived_from','supports','contradicts','implements','belongs_to','produced_by','evidence_for','justified_by'}

def load_base():
    spec=importlib.util.spec_from_file_location('knowledge_compat_'+uuid.uuid4().hex,FROZEN)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

def register_codecs(db):
    cache=OrderedDict();cache_bytes=0
    def unpack(blob):
        nonlocal cache_bytes
        if blob in cache:
            value,size=cache.pop(blob);cache[blob]=(value,size);return value
        raw=zlib.decompress(blob);value=raw.decode();size=len(raw)
        if size<=16*1024*1024:
            while cache and cache_bytes+size>16*1024*1024:
                _,(_,removed)=cache.popitem(last=False);cache_bytes-=removed
            cache[blob]=(value,size);cache_bytes+=size
        return value
    db.create_function('_knowledge_unpack',1,unpack,deterministic=True)
    db.create_function('_knowledge_pack',1,lambda text:zlib.compress(text.encode(),3),deterministic=True)
    db.create_function('_knowledge_hash',1,lambda text:hashlib.sha256(text.encode()).hexdigest(),deterministic=True)

def install(m,root,extension):
    m.ROOT=root;m.VERSION=2
    m.ARTIFACT_KINDS=BASE_KINDS|({'building','scene'} if extension=='mavis' else set())
    m.PREDICATES=BASE_PREDICATES|({'primary_building'} if extension=='mavis' else set())
    original_connect=m.connect
    def connect(path,writable=False):
        db=original_connect(path,writable);register_codecs(db);return db
    m.connect=connect
    original_snapshot=m.read_snapshot
    @contextmanager
    def decoded_snapshot(db):
        with original_snapshot(db) as replica:
            register_codecs(replica)
            yield replica
    m.read_snapshot=decoded_snapshot
    def relation(db,source,predicate,target,envelope,now,relation_id=None):
        m.require(source!=target,'self relations are not allowed')
        m.require(m.snapshot(db,source) is not None and m.snapshot(db,target) is not None,'both relation endpoints must exist')
        if extension=='mavis':
            spec=importlib.util.spec_from_file_location('mavis_validation',HERE/'extensions/mavis.py');ext=importlib.util.module_from_spec(spec);spec.loader.exec_module(ext);ext.validate_relation(m,db,predicate,target)
        relation_id=relation_id or 'rel-'+uuid.uuid4().hex
        db.execute('INSERT INTO relations VALUES (?,?,?,?,?,?,?)',(relation_id,source,predicate,target,envelope['reason'],envelope['source_ref'],now))
        return m.row(db,'relations','id',relation_id)
    m.relation=relation
    def reindex(db,eid):
        current=m.snapshot(db,eid)
        existing=db.execute('SELECT fts_rowid FROM search_documents WHERE entity_id=?',(eid,)).fetchone()
        if existing:
            docid=existing[0];db.execute('DELETE FROM search_index WHERE rowid=?',(docid,))
        else:
            docid=db.execute('SELECT coalesce(max(fts_rowid),0)+1 FROM search_documents').fetchone()[0]
            db.execute('INSERT INTO search_documents VALUES (?,?)',(eid,docid))
        entity,detail=current['entity'],current['detail']
        body=entity['description']+'\n'+(detail['body'] if detail else '')
        db.execute('INSERT INTO search_index(rowid,entity_id,title,body) VALUES (?,?,?,?)',(docid,eid,entity['title'],body))
    def update_search_catalog(db,eid):
        current=m.snapshot(db,eid);e,d=current['entity'],current['detail']
        rid=db.execute('SELECT fts_rowid FROM search_documents WHERE entity_id=?',(eid,)).fetchone()[0]
        db.execute('INSERT OR REPLACE INTO search_catalog VALUES (?,?,?,?,?,?,?,?,?)',
          (rid,eid,e['kind'],e['title'],e['status'],e['path'],d.get('knowledge_type') if d else None,d.get('confidence') if d else None,d.get('scope') if d else None))
    def catalog_reindex(db,eid):
        reindex(db,eid);update_search_catalog(db,eid)
    m.reindex=catalog_reindex
    def search(db,expression):
        spec=importlib.util.spec_from_file_location('search_provider',HERE/'runtime/search.py');provider=importlib.util.module_from_spec(spec);spec.loader.exec_module(provider)
        return provider.query(db,expression)
    m.search=search
    def check(db,files=True):
        problems=[];integrity=db.execute('PRAGMA integrity_check').fetchone()[0]
        if integrity!='ok':problems.append({'kind':'sqlite_integrity','detail':integrity})
        for item in db.execute('PRAGMA foreign_key_check'):problems.append({'kind':'foreign_key','detail':list(item)})
        cached={r['entity_id']:tuple(r) for r in db.execute('SELECT * FROM search_catalog')}
        docids={r[0]:r[1] for r in db.execute('SELECT entity_id,fts_rowid FROM search_documents')}
        indexed={}
        for row in db.execute('SELECT entity_id,title,body FROM search_index'):
            indexed.setdefault(row[0],[]).append((row[1],row[2]))
        for entity in db.execute('SELECT * FROM entities').fetchall():
            last=db.execute('SELECT ev.after_json FROM event_entities ee JOIN events ev ON ee.event_seq=ev.seq WHERE ee.entity_id=? ORDER BY ee.event_seq DESC LIMIT 1',(entity['id'],)).fetchone()
            current=m.snapshot(db,entity['id'])
            if not last or json.loads(last[0])['entities'].get(entity['id'])!=current:problems.append({'kind':'history_mismatch','id':entity['id']})
            detail=current['detail']
            expected=(docids.get(entity['id']),entity['id'],entity['kind'],entity['title'],entity['status'],entity['path'],detail.get('knowledge_type') if detail else None,detail.get('confidence') if detail else None,detail.get('scope') if detail else None)
            if cached.get(entity['id'])!=expected:problems.append({'kind':'search_catalog_mismatch','id':entity['id']})
            if entity['kind'] in {'knowledge','decision'} and detail is None:problems.append({'kind':'missing_detail','id':entity['id']})
            body=entity['description']+'\n'+(detail['body'] if detail else '')
            if indexed.get(entity['id'],[])!=[(entity['title'],body)]:problems.append({'kind':'search_index_mismatch','id':entity['id']})
            if files and entity['path'] and entity['status']=='active' and not (root/entity['path']).exists():problems.append({'kind':'missing_file','id':entity['id'],'path':entity['path']})
        return {'ok':not problems,'problems':problems,'entities':db.execute('SELECT count(*) FROM entities').fetchone()[0],'events':db.execute('SELECT count(*) FROM events').fetchone()[0]}
    m.check=check
    return m

def open_project(root,project_id,extension='mavis',keep_connection=False):
    root=Path(root).resolve();path=root/'knowledge.sqlite'
    if not path.is_file():raise ValueError('Existing explicit project database required')
    m=install(load_base(),root,extension)
    db=m.connect(path)
    try:
        rows=db.execute("SELECT id FROM entities WHERE kind='project' AND status='active'").fetchall()
        if [r[0] for r in rows]!=[project_id]:raise ValueError('Project identity mismatch')
    finally:
        if not keep_connection:db.close()
    return SimpleNamespace(root=root,database=path,project_id=project_id,extension=extension,memory=m,connection=db if keep_connection else None)
