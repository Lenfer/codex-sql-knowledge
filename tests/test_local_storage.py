"""Opt-in policy confinement, private imports, queued CAS and receipt safety."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from sql_knowledge.engine import Project
from sql_knowledge.local_storage import Backend, configured, STORAGE, BUDGET
from tests.test_identity_cas import project
from tests.helpers import env

POLICY = '''import memory as m
import hashlib
import json
import sys
from pathlib import Path
sys.path.insert(0, 'must-not-be-added')
def register(db, envelope):
    from budget import marker
    key = envelope['idempotency_key']
    op_id = 'storage-op-' + hashlib.sha256(key.encode()).hexdigest()[:32]
    op = m.row(db, 'entities', 'id', op_id)
    if op is None:
        digest=hashlib.sha256(m.packed(dict(action='register',envelope=envelope)).encode()).hexdigest()
        m.mutate(db,'entity',dict(envelope,idempotency_key=key+':start',data=dict(id=op_id,kind='report',title='Receipt',status='draft',attributes=dict(request_hash=digest))))
    old = m.row(db, 'events', 'operation_key', key+':file')
    if old:
        return dict(ok=True, replayed=True)
    result = m.mutate(db, 'entity', dict(envelope, idempotency_key=key+':file', data=dict(envelope['data'],kind='file',status='active',attributes={})))
    return dict(result, marker=marker, root=str(m.ROOT))
def inventory(db, scope, after_path=None, limit=100):
    return dict(ok=True, root=str(m.ROOT), scope=scope)
'''
BUDGET_SOURCE = "import memory as m\nimport sys\nsys.path.insert(0, 'must-not-be-added-budget')\nmarker='disk-budget-policy'\n"


def fixture(root, compat=False):
    project(root)
    for path, source in [(STORAGE, POLICY), (BUDGET, BUDGET_SOURCE)]:
        file = root/path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(source)
    with (root/'knowledge.toml').open('a') as f:
        f.write('\n[compatibility]\nlegacy_writes=true\nstorage_module='+json.dumps(STORAGE)+'\n')
        for name,path in [('storage_sha256',STORAGE),('storage_budget_sha256',BUDGET)]:
            f.write(name+'='+json.dumps(hashlib.sha256((root/path).read_bytes()).hexdigest())+'\n')
        if compat:
            f.write('storage_legacy_cas=true\n')
    return Project(root)


def request(key='register', title='File'):
    return env(key, dict(id='file',title=title,path='.report/2026-11-12/file',description='Fixture'))


class LocalStorage(unittest.TestCase):
    def test_default_no_backend(self):
        with tempfile.TemporaryDirectory() as td:
            self.assertIsNone(configured(project(Path(td)/'p')))

    def test_policy_invoked_with_private_bindings(self):
        with tempfile.TemporaryDirectory() as td:
            p=fixture(Path(td)/'p');backend=Backend(p)
            before=list(sys.path);memory=sys.modules.get('memory');budget=sys.modules.get('budget')
            result=backend.call('register',request())
            self.assertEqual(result['marker'],'disk-budget-policy')
            self.assertEqual(result['root'],str(p.root))
            self.assertEqual(before,sys.path)
            self.assertIs(sys.modules.get('memory'),memory)
            self.assertIs(sys.modules.get('budget'),budget)
            self.assertEqual(backend.call('inventory','.report')['scope'],'.report')

    def test_missing_stale_and_explicit_cas(self):
        with tempfile.TemporaryDirectory() as td:
            p=fixture(Path(td)/'p');backend=Backend(p);backend.call('register',request())
            with closing(p.connect()) as db:
                revision=p.revision(db,'file');before=db.execute('SELECT count(*) FROM events').fetchone()[0]
            for wanted,code in [(None,'revision_required'),(revision+1,'revision_conflict')]:
                item=request('update-'+str(wanted))
                if wanted is not None:item['expected_revisions']={'file':wanted}
                with self.assertRaises(Exception) as error:backend.call('register',item)
                self.assertEqual(error.exception.code,code)
            with closing(p.connect()) as db:self.assertEqual(before,db.execute('SELECT count(*) FROM events').fetchone()[0])
            item=request('update');item['expected_revision']=revision
            self.assertTrue(backend.call('register',item)['ok'])
            # Own persisted substep can resume with original expected revision.
            self.assertTrue(backend.call('register',item)['replayed'])
            changed=dict(item,data=dict(item['data'],title='Changed'))
            with self.assertRaises(Exception) as error:backend.call('register',changed)
            self.assertEqual(error.exception.code,'idempotency_conflict')

    def test_legacy_cas_requires_explicit_optin(self):
        with tempfile.TemporaryDirectory() as td:
            p=fixture(Path(td)/'p',compat=True);backend=Backend(p)
            backend.call('register',request())
            self.assertTrue(backend.call('register',request('legacy-update'))['ok'])

    def test_policy_source_hash_inode_manifest_and_symlinks(self):
        for mode in ('bytes','inode','manifest','symlink'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as td:
                p=fixture(Path(td)/'p');backend=Backend(p);path=p.root/STORAGE
                if mode=='bytes':path.write_text(POLICY+'\n# changed\n')
                elif mode=='inode':
                    temp=path.with_suffix('.new');temp.write_bytes(path.read_bytes());temp.replace(path)
                elif mode=='manifest':
                    with (p.root/'knowledge.toml').open('a') as f:f.write('storage_legacy_cas=true\n')
                else:
                    alternate=path.with_suffix('.other');alternate.write_bytes(path.read_bytes());path.unlink();path.symlink_to(alternate)
                with self.assertRaises(Exception) as error:backend.call('inventory','.report')
                self.assertEqual(error.exception.code,'storage_policy_changed')

    def test_unpinned_or_arbitrary_module_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            p=fixture(Path(td)/'p')
            for key,value in [('storage_module','other.py'),('storage_sha256','0'*64)]:
                original=p.config['compatibility'][key];p.config['compatibility'][key]=value
                with self.assertRaises(Exception):Backend(p)
                p.config['compatibility'][key]=original
            with self.assertRaises(Exception):Backend(p).call('__dict__')
