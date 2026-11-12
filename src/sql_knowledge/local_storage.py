"""Explicit, hash-pinned project storage policy compatibility.

Only the historical storage and DiskBudget locations are supported. Modules get
private import bindings; they never claim process-global ``memory``/``budget``.
The legacy writable connection owns the coordinator lease for the entire policy
call, including filesystem work and its multiple SQLite commits.
"""
import ast
import builtins
from contextlib import closing
import hashlib
import json
from pathlib import Path
import types

STORAGE = 'scripts/project-knowledge/storage.py'
BUDGET = 'scripts/disk-budget/budget.py'
ACTIONS = {'inventory', 'register', 'reconcile', 'cleanup_plan', 'cleanup_execute'}


class PolicyError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


class Backend:
    def __init__(self, project):
        self.project = project
        self.config = dict(project.config.get('compatibility', {}))
        if self.config.get('storage_module') != STORAGE:
            raise PolicyError('storage_policy', 'only scripts/project-knowledge/storage.py is supported')
        if self.config.get('legacy_writes') is not True:
            raise PolicyError('storage_policy', 'local storage requires explicit legacy_writes=true')
        self.files = {}
        self._pin(STORAGE, 'storage_sha256')
        self._pin(BUDGET, 'storage_budget_sha256')

    def _read(self, relative):
        path = self.project.root / relative
        for parent in [path, *path.parents]:
            if parent == self.project.root:
                break
            if parent.is_symlink():
                raise PolicyError('storage_policy_changed', 'policy symlinks are forbidden')
        if not path.is_file() or self.project.root not in path.resolve().parents:
            raise PolicyError('storage_policy_changed', 'policy must be a project-contained regular file')
        info = path.stat()
        raw = path.read_bytes()
        return raw, (info.st_dev, info.st_ino), hashlib.sha256(raw).hexdigest()

    def _pin(self, relative, field):
        raw, identity, digest = self._read(relative)
        if self.config.get(field) != digest:
            raise PolicyError('storage_policy_changed', field + ' must pin the current policy SHA256')
        self.files[relative] = (raw, identity, digest)

    def verify(self):
        import tomllib
        now = tomllib.loads((self.project.root / 'knowledge.toml').read_text()).get('compatibility', {})
        if now != self.config:
            raise PolicyError('storage_policy_changed', 'storage compatibility changed; restart MCP')
        for relative, original in self.files.items():
            if self._read(relative) != original:
                raise PolicyError('storage_policy_changed', 'storage policy changed/replaced; restart MCP')

    def _load(self, relative, memory, budget=None):
        module = types.ModuleType('_sqlk_private_' + Path(relative).stem)
        module.__file__ = str(self.project.root / relative)
        importer = builtins.__import__
        def isolated(name, globals=None, locals=None, fromlist=(), level=0):
            if level == 0 and name == 'memory':
                return memory
            if level == 0 and name == 'budget':
                if budget is None:
                    raise PolicyError('storage_policy', 'unexpected recursive budget import')
                return budget
            return importer(name, globals, locals, fromlist, level)
        module.__dict__['__builtins__'] = dict(vars(builtins), __import__=isolated)
        tree = ast.parse(self.files[relative][0], filename=module.__file__)
        class PrivateImports(ast.NodeTransformer):
            def visit_Expr(self, node):
                # Historical modules add their sibling import directories. Their
                # two supported local imports are now bound explicitly above.
                call = node.value
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and ast.unparse(call.func) == 'sys.path.insert':
                    return ast.copy_location(ast.Pass(), node)
                return self.generic_visit(node)
        tree = ast.fix_missing_locations(PrivateImports().visit(tree))
        exec(compile(tree, module.__file__, 'exec'), module.__dict__)
        return module

    def call(self, action, *args, **kwargs):
        if action not in ACTIONS:
            raise PolicyError('invalid_operation', 'unsupported local storage operation')
        self.verify()
        from .legacy import bind
        memory = bind(self.project.root)
        budget = self._load(BUDGET, memory)
        policy = self._load(STORAGE, memory, budget)
        original_write = policy.write if hasattr(policy, 'write') else None
        if original_write is not None:
            def checked_write(db, envelope, suffix, data, transaction=True):
                payload = dict(envelope, data=data, idempotency_key=envelope['idempotency_key']+':'+suffix)
                for field in ('idempotency_key','actor','reason','source_ref'):
                    payload[field] = memory.text(payload, field)
                prior = memory.row(db, 'events', 'operation_key', payload['idempotency_key'])
                if prior:
                    digest = hashlib.sha256(memory.packed(dict(action='entity',envelope=payload)).encode()).hexdigest()
                    memory.require(prior['request_hash'] == digest, 'storage substep key reused with different input', 'idempotency_conflict')
                return original_write(db, envelope, suffix, data, transaction=transaction)
            policy.write = checked_write
        with closing(memory.connect(self.project.database, writable=action != 'inventory')) as db:
            # A queued writable connection has acquired the lease before this
            # second check; a queued request never executes stale source bytes.
            self.verify()
            if action != 'inventory':
                if len(args) != 1 or kwargs or not isinstance(args[0], dict):
                    raise PolicyError('invalid_input', 'storage write requires one typed envelope')
                self._cas(db, memory, action, args[0])
            return getattr(policy, action)(db, *args, **kwargs)

    def _cas(self, db, memory, action, envelope):
        """Fail before creating receipts; recognize only audited own progress."""
        data = envelope.get('data', {})
        ids = [data['id']] if action == 'register' else data.get('entity_ids', [])
        if action == 'cleanup_execute':
            row = memory.row(db, 'entities', 'id', data.get('plan_id'))
            if row:
                ids = [v['id'] for v in json.loads(row['attributes_json'])['cleanup_plan']['candidates']]
        key = memory.text(envelope, 'idempotency_key')
        op = memory.row(db, 'entities', 'id', 'storage-op-' + hashlib.sha256(key.encode()).hexdigest()[:32])
        if op is None:
            suffixes = ['start','receipt']
            if action == 'register':suffixes += ['file']
            elif action == 'reconcile':suffixes += list(ids)
            elif action == 'cleanup_plan':suffixes += ['plan']
            elif action == 'cleanup_execute':suffixes += [phase+'-'+ident for ident in ids for phase in ('intent','result')]
            for suffix in suffixes:
                memory.require(memory.row(db,'events','operation_key',key+':'+suffix) is None, 'storage substep exists without its operation receipt', 'idempotency_conflict')
        if op:
            saved = json.loads(op['attributes_json'])
            request = hashlib.sha256(memory.packed(dict(action={'cleanup_plan':'plan','cleanup_execute':'execute'}.get(action,action), envelope=envelope)).encode()).hexdigest()
            memory.require(saved['request_hash'] == request, 'operation key reused with different input', 'idempotency_conflict')
            if 'result' in saved:
                return
        expected = envelope.get('expected_revisions') or {}
        memory.require(isinstance(expected, dict), 'expected_revisions must be an object')
        normalized = {}
        for name, value in expected.items():
            canonical = memory.text({'id': name}, 'id')
            memory.require(canonical not in normalized, 'duplicate canonical expected revision ID')
            normalized[canonical] = value
        if action == 'register' and envelope.get('expected_revision') is not None:
            normalized[memory.text(data, 'id')] = envelope['expected_revision']
        compat = self.config.get('storage_legacy_cas') is True
        for ident in ids:
            ident = memory.text({'id': ident}, 'id')
            snapshot = memory.snapshot(db, ident)
            wanted = normalized.get(ident)
            if snapshot is None:
                memory.require(wanted is None or type(wanted) is int and wanted == 0, 'revision conflict for new '+ident, 'revision_conflict')
                continue
            if wanted is None and compat:
                continue
            memory.require(wanted is not None, 'expected_revision required for '+ident, 'revision_required')
            current = self.project.revision(db, ident)
            if type(wanted) is int and wanted == current:
                continue
            valid = type(wanted) is int and 0 <= wanted < current and op is not None
            latest = None
            events = db.execute('SELECT e.* FROM events e JOIN event_entities x ON x.event_seq=e.seq WHERE x.entity_id=? AND e.seq>? ORDER BY e.seq', (ident, wanted if type(wanted) is int else current)).fetchall()
            allowed = {'register': [key+':file'], 'reconcile': [key+':'+ident], 'cleanup_execute': [key+':intent-'+ident, key+':result-'+ident]}.get(action, [])
            for event in events:
                after = json.loads(event['after_json']).get('entities', {}).get(ident)
                if event['operation_key'] not in allowed or not after:
                    valid = False
                    break
                entity = after['entity']
                item = {k:entity[k] for k in ('id','kind','title','description','path','status')}
                item['attributes'] = json.loads(entity['attributes_json'])
                payload = dict(envelope, data=item, idempotency_key=event['operation_key'])
                for field in ('idempotency_key','actor','reason','source_ref'):
                    payload[field] = memory.text(payload, field)
                digest = hashlib.sha256(memory.packed(dict(action='entity',envelope=payload)).encode()).hexdigest()
                if digest != event['request_hash']:
                    valid = False
                    break
                latest = after
            memory.require(valid and bool(events) and latest == snapshot, 'revision conflict for '+ident, 'revision_conflict')


def configured(project):
    config = project.config.get('compatibility', {})
    return Backend(project) if 'storage_module' in config else None
