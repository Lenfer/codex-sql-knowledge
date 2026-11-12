"""Managed artifact lifecycle, exposed only by MCP; metadata stays in entities/events.

No recursive deletion. Filesystem and SQLite cannot share a transaction: persist
intent first; an interrupted deletion can be resumed or reconciled as missing.
"""
import contextlib
import datetime as dt
import hashlib
import json
import os
from pathlib import PurePosixPath
import stat

from . import memory as m
from sql_knowledge.storage_keys import step_key

MANAGED = {'.tmp', '.report', 'experiments'}
MAX_BYTES = 512 * 1024 * 1024


def managed_path(value):
    path = m.project_path(value)
    parts = PurePosixPath(path).parts
    m.require(parts[0] in MANAGED and len(parts) >= 3,
              'managed files must be below .tmp/<date>, .report/<date> or experiments/<experiment>')
    if parts[0] == 'experiments':
        # Existing project template YYYY-MM-DD_HH__tag, optional collision suffix.
        prefix = parts[1].split('__', 1)
        m.require(len(prefix) == 2 and bool(prefix[1]), 'invalid experiment directory')
        dt.datetime.strptime(prefix[0], '%Y-%m-%d_%H')
    m.require(not any(v in {'.git', '.aws', '.codex'} for v in parts), 'private/internal directory')
    m.require(not path.endswith(('.sqlite', '.sqlite-wal', '.sqlite-shm', '.sqlite-journal')),
              'database files cannot be lifecycle-managed')
    return path


@contextlib.contextmanager
def parent_fd(path):
    """Resolve every component without following symlinks, including during unlink."""
    parts = PurePosixPath(managed_path(path)).parts
    fd = os.open(str(m.ROOT), os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in parts[:-1]:
            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        yield fd, parts[-1]
    finally:
        os.close(fd)


def fingerprint(path):
    with parent_fd(path) as (directory, name):
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            m.require(stat.S_ISREG(before.st_mode), 'only regular files can be managed')
            m.require(before.st_size <= MAX_BYTES, 'file exceeds 512 MiB lifecycle hashing limit')
            digest = hashlib.sha256()
            size = 0
            while True:
                block = stream.read(1024 * 1024)
                if not block:
                    break
                size += len(block)
                m.require(size <= MAX_BYTES, 'file grew beyond hashing limit')
                digest.update(block)
            after = os.fstat(stream.fileno())
            keys = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
            m.require(all(getattr(before, k) == getattr(after, k) for k in keys),
                      'file changed during hashing', 'file_changed')
    return {'sha256': digest.hexdigest(), 'size': size, 'mtime_ns': after.st_mtime_ns,
            'device': after.st_dev, 'inode': after.st_ino, 'ctime_ns': after.st_ctime_ns}


def envelope_check(envelope):
    for field in ('actor', 'reason', 'source_ref', 'idempotency_key'):
        m.text(envelope, field)
    m.durable_ref(envelope['source_ref'])
    return OperationEnvelope(envelope)


class OperationEnvelope(dict):
    # Context is not serialized into the public envelope or its historical hash.
    action = None
    key_version = 2


def operation_key(envelope, phase, entity_id=None):
    if phase == 'file': entity_id = None
    return step_key(envelope['idempotency_key'], envelope.action, phase, entity_id, envelope.key_version)


def verified_step(db, envelope, phase, entity_id=None, data=None):
    previous = m.row(db, 'events', 'operation_key', operation_key(envelope, phase, entity_id))
    if previous is None:
        return None
    saved = json.loads(previous['after_json']).get('entities', {})
    expected_id = data['id'] if data else entity_id
    image = saved.get(expected_id)
    code = 'legacy_storage_collision' if envelope.key_version == 1 else 'idempotency_conflict'
    m.require(previous['action'] == 'entity' and image is not None,
              'storage step key belongs to another operation or entity', code)
    payload_data = data or card_data(image['entity'])
    payload = dict(envelope, data=payload_data, idempotency_key=previous['operation_key'])
    for field in ('idempotency_key', 'actor', 'reason', 'source_ref'):
        payload[field] = m.text(payload, field)
    digest = hashlib.sha256(m.packed({'action': 'entity', 'envelope': payload}).encode()).hexdigest()
    m.require(previous['request_hash'] == digest,
              'storage step receipt does not match its operation payload', code)
    m.require(json.loads(previous['result_json']).get('id') == expected_id,
              'storage step receipt identifies another entity', code)
    return previous


def write(db, envelope, suffix, data, transaction=True, entity_id=None):
    payload = dict(envelope, data=data, idempotency_key=operation_key(envelope, suffix, entity_id))
    previous = verified_step(db, envelope, suffix, entity_id, data)
    if previous:
        return dict(json.loads(previous['result_json']), replayed=True)
    return m.mutate(db, 'entity', payload, transaction=transaction)


def card_data(card, **changes):
    result = {k: card[k] for k in ('id', 'kind', 'title', 'description', 'path', 'status')}
    result['attributes'] = json.loads(card['attributes_json'])
    result.update(changes)
    return result


def receipt(db, key, action, envelope):
    """Store operation inputs/result as an ordinary catalog card with audited history."""
    operation_id = 'storage-op-' + hashlib.sha256(key.encode()).hexdigest()[:32]
    old = m.row(db, 'entities', 'id', operation_id)
    request_hash = hashlib.sha256(m.packed({'action': action, 'envelope': envelope}).encode()).hexdigest()
    envelope.action = action
    if old:
        attrs = json.loads(old['attributes_json'])
        m.require(attrs.get('request_hash') == request_hash, 'operation key reused with different input',
                  'idempotency_conflict')
        if 'result' in attrs:
            return operation_id, request_hash, attrs
        envelope.key_version = attrs.get('storage_key_version', 1)
        m.require(envelope.key_version in (1, 2) and attrs.get('storage_action', action) == action,
                  'unknown storage intent key policy', 'legacy_storage_collision')
        if envelope.key_version == 1 and action == 'reconcile':
            ids = [m.text({'id': value}, 'id') for value in envelope['data']['entity_ids']]
            m.require(not {'start', 'receipt'}.intersection(ids),
                      'ambiguous legacy reconciliation requires explicit recovery; history unchanged',
                      'legacy_storage_collision')
        return operation_id, request_hash, None
    write(db, envelope, 'start', {'id': operation_id, 'kind': 'report',
        'title': 'Storage operation ' + action, 'status': 'draft',
        'description': 'MCP lifecycle operation intent; retry identical envelope after interruption.',
        'attributes': {'request_hash': request_hash, 'storage_key_version': 2, 'storage_action': action}})
    return operation_id, request_hash, None


def finish(db, envelope, operation_id, request_hash, result):
    attrs = json.loads(m.row(db, 'entities', 'id', operation_id)['attributes_json'])
    attrs.update(request_hash=request_hash, result=result)
    write(db, envelope, 'receipt', {'id': operation_id, 'kind': 'report',
        'title': 'Storage operation ' + operation_id, 'status': 'active',
        'description': 'MCP lifecycle operation receipt; no file content.',
        'attributes': attrs})
    return result


def register(db, envelope):
    """Register one useful file, preserving its semantic description and retention."""
    envelope = envelope_check(envelope)
    operation_id, request_hash, prior = receipt(db, envelope['idempotency_key'], 'register', envelope)
    if prior:
        return dict(prior['result'], replayed=True)
    completed = verified_step(db, envelope, 'file', entity_id=m.text(envelope['data'], 'id'))
    if completed:
        saved = json.loads(completed['after_json'])['entities'][m.text(envelope['data'], 'id')]['entity']
        fp = json.loads(saved['attributes_json'])['storage']['fingerprint']
        return finish(db, envelope, operation_id, request_hash,
                      dict(json.loads(completed['result_json']), path=saved['path'], fingerprint=fp))
    data = envelope['data']
    path = managed_path(m.text(data, 'path'))
    fp = fingerprint(path)
    entity_id = m.text(data, 'id')
    old = m.row(db, 'entities', 'id', entity_id)
    m.require(old is None or old['path'] == path, 'registration cannot silently move a catalog artifact')
    attrs = json.loads(old['attributes_json']) if old else {}
    retention = data.get('retention', 'keep')
    m.require(retention in {'keep', 'disposable'}, 'retention must be keep or disposable')
    attrs['storage'] = {'retention': retention, 'availability': 'present',
                        'fingerprint': fp, 'observed_at': m.timestamp()}
    result = write(db, envelope, 'file', {'id': entity_id, 'kind': data.get('kind', 'file'),
        'title': m.text(data, 'title'), 'description': m.text(data, 'description'),
        'path': path, 'status': 'active', 'attributes': attrs})
    return finish(db, envelope, operation_id, request_hash, dict(result, path=path, fingerprint=fp))


def inventory(db, scope, after_path=None, limit=100):
    """Discover file paths only, without reading content or creating catalog entries."""
    m.require(1 <= limit <= 100, 'limit must be 1..100')
    p = PurePosixPath(scope)
    m.require(p.parts and p.parts[0] in MANAGED and not p.is_absolute() and '..' not in p.parts
              and '\\' not in scope, 'invalid inventory scope')
    start = m.ROOT / str(p)
    for parent in [start, *start.parents]:
        if parent == m.ROOT:
            break
        m.require(not parent.is_symlink(), 'inventory cannot follow symlinks')
    found = []
    visited = 0
    for directory, dirs, files in os.walk(start, followlinks=False):
        dirs[:] = sorted(d for d in dirs if not (m.ROOT / directory / d).is_symlink())
        for name in sorted(files):
            visited += 1
            m.require(visited <= 10000, 'inventory scope exceeds 10000 files; use a narrower scope')
            path = (m.ROOT / directory / name).relative_to(m.ROOT).as_posix()
            if after_path and path <= after_path:
                continue
            absolute = m.ROOT / path
            if absolute.is_symlink():
                continue
            info = absolute.stat()
            if not stat.S_ISREG(info.st_mode):
                continue
            card = db.execute('SELECT id,status FROM entities WHERE path=?', (path,)).fetchone()
            found.append({'path': path, 'size': info.st_size,
                          'id': card['id'] if card else None, 'status': card['status'] if card else 'unregistered'})
    found.sort(key=lambda v: v['path'])
    return {'ok': True, 'files': found[:limit], 'truncated': len(found) > limit,
            'next_after_path': found[limit - 1]['path'] if len(found) > limit else None,
            'scope_exists': start.exists()}


def selected(db, ids):
    m.require(isinstance(ids, list) and 1 <= len(ids) <= 100 and len(set(ids)) == len(ids),
              'select 1..100 distinct catalog IDs')
    ids = [m.text({'id': value}, 'id') for value in ids]
    m.require(len(set(ids)) == len(ids), 'select distinct canonical catalog IDs')
    result = []
    for entity_id in ids:
        card = m.row(db, 'entities', 'id', entity_id)
        m.require(card and card['path'], 'catalog ID must have a file path: ' + entity_id)
        result.append(card)
    return result


def related_ids(db, entity_id):
    return [r[0] for r in db.execute(
        'SELECT source_id FROM relations WHERE target_id=? UNION SELECT target_id FROM relations WHERE source_id=? ORDER BY 1',
        (entity_id, entity_id))]


def reconcile(db, envelope):
    envelope = envelope_check(envelope)
    op_id, request_hash, prior = receipt(db, envelope['idempotency_key'], 'reconcile', envelope)
    if prior:
        return dict(prior['result'], replayed=True)
    output = []
    for original in selected(db, envelope['data']['entity_ids']):
        completed = verified_step(db, envelope, 'target', original['id'])
        if completed:
            saved = json.loads(completed['after_json'])['entities'][original['id']]['entity']
            state = json.loads(saved['attributes_json'])['storage']['availability']
            output.append({'id': original['id'], 'availability': state, 'related_ids': related_ids(db, original['id'])})
            continue
        path = m.project_path(original['path'])
        # For managed regular files check identity as well as presence. Other catalog
        # paths (sources, directories, code) are checked for existence, never deleted.
        fp = None
        try:
            if PurePosixPath(path).parts[0] in MANAGED and json.loads(original['attributes_json']).get('storage', {}).get('fingerprint'):
                fp = fingerprint(path)
                present = True
            else:
                (m.ROOT / path).stat()
                present = True
        except FileNotFoundError:
            present = False
        db.execute('BEGIN IMMEDIATE')
        try:
            card = m.row(db, 'entities', 'id', original['id'])
            m.require(card == original, 'catalog changed; retry reconciliation with a new key', 'catalog_changed')
            data = card_data(card)
            storage = data['attributes'].setdefault('storage', {})
            previous_fp = storage.get('fingerprint')
            state = ('changed' if fp and previous_fp and fp != previous_fp else 'present') if present else 'missing'
            storage.update(availability=state, observed_at=m.timestamp())
            if fp:
                storage['observed_fingerprint'] = fp
            if not present:
                storage['loss_reason'] = 'absent_on_disk; cause unknown (possibly manual removal)'
                if data['status'] != 'deleted':
                    data['status'] = 'missing'
            elif data['status'] in {'missing', 'deleted'}:
                # A reappearing path is not proof of restored identity.
                storage['availability'] = state = 'reappeared_unverified'
            write(db, envelope, 'target', data, transaction=False, entity_id=card['id'])
            db.commit()
        except Exception:
            db.rollback()
            raise
        affected = related_ids(db, card['id'])
        output.append({'id': card['id'], 'availability': state, 'related_ids': affected})
    return finish(db, envelope, op_id, request_hash, {'ok': True, 'records': output})


def cleanup_plan(db, envelope):
    envelope = envelope_check(envelope)
    op_id, request_hash, prior = receipt(db, envelope['idempotency_key'], 'plan', envelope)
    if prior:
        return dict(prior['result'], replayed=True)
    age = envelope['data'].get('min_age_days', 30)
    m.require(type(age) is int and age >= 0, 'min_age_days must be a nonnegative integer')
    cutoff = dt.datetime.now(dt.timezone.utc).timestamp() - age * 86400
    candidates, excluded = [], []
    for card in selected(db, envelope['data']['entity_ids']):
        managed_path(card['path'])
        attrs = json.loads(card['attributes_json'])
        storage = attrs.get('storage', {})
        if storage.get('retention', 'keep') != 'disposable' or card['status'] != 'active':
            excluded.append({'id': card['id'], 'reason': 'protected_or_inactive'})
            continue
        try:
            fp = fingerprint(card['path'])
        except FileNotFoundError:
            excluded.append({'id': card['id'], 'reason': 'missing_on_disk; reconcile first'})
            continue
        if fp['mtime_ns'] / 1e9 > cutoff:
            excluded.append({'id': card['id'], 'reason': 'too_recent'})
            continue
        m.require(storage.get('fingerprint') == fp, 'file changed since registration; review and register again', 'file_changed')
        candidates.append({'id': card['id'], 'path': card['path'], 'fingerprint': fp,
                           'updated_at': card['updated_at'], 'size': fp['size']})
    plan_id = 'cleanup-plan-' + op_id.removeprefix('storage-op-')
    existing_plan = m.row(db, 'entities', 'id', plan_id)
    if existing_plan:
        return finish(db, envelope, op_id, request_hash,
                      json.loads(existing_plan['attributes_json'])['cleanup_plan'])
    token = hashlib.sha256(m.packed(candidates).encode()).hexdigest()
    result = {'ok': True, 'plan_id': plan_id, 'confirmation_token': token,
              'candidates': candidates, 'excluded': excluded,
              'total_bytes': sum(v['size'] for v in candidates)}
    write(db, envelope, 'plan', {'id': plan_id, 'kind': 'report', 'status': 'active',
        'title': 'MCP cleanup plan', 'description': 'Explicit reviewed list of files; no recursive removal.',
        'attributes': {'cleanup_plan': result, 'expires_at':
            (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=24)).isoformat()}})
    return finish(db, envelope, op_id, request_hash, result)


def cleanup_execute(db, envelope):
    envelope = envelope_check(envelope)
    op_id, request_hash, prior = receipt(db, envelope['idempotency_key'], 'execute', envelope)
    if prior:
        return dict(prior['result'], replayed=True)
    plan_id = m.text(envelope['data'], 'plan_id')
    plan_card = m.row(db, 'entities', 'id', plan_id)
    m.require(plan_card is not None, 'cleanup plan does not exist')
    attrs = json.loads(plan_card['attributes_json'])
    plan = attrs.get('cleanup_plan')
    m.require(plan and plan['confirmation_token'] == m.text(envelope['data'], 'confirmation_token'),
              'confirmation token does not match plan')
    m.require(dt.datetime.now(dt.timezone.utc) < dt.datetime.fromisoformat(attrs['expires_at']),
              'cleanup plan expired; create a new plan', 'plan_expired')
    deleted, skipped = [], []
    # Preflight entire batch before first unlink. Resuming the same operation is
    # allowed only for its own persisted intents/completions.
    for item in plan['candidates']:
        card = m.row(db, 'entities', 'id', item['id'])
        storage = json.loads(card['attributes_json']).get('storage', {})
        if storage.get('cleanup_operation') == op_id and card['status'] in {'deleted', 'missing'}:
            continue
        m.require(storage.get('retention') == 'disposable' and card['status'] == 'active',
                  'candidate is now protected or inactive', 'stale_plan')
        own_intent = storage.get('cleanup_operation') == op_id
        m.require(own_intent or card['updated_at'] == item['updated_at'], 'catalog changed since plan', 'stale_plan')
        try:
            m.require(fingerprint(item['path']) == item['fingerprint'], 'file changed since plan', 'stale_plan')
        except FileNotFoundError:
            m.require(own_intent, 'file disappeared before cleanup; reconcile first', 'stale_plan')
    for item in plan['candidates']:
        db.execute('BEGIN IMMEDIATE')
        try:
            card = m.row(db, 'entities', 'id', item['id'])
            data = card_data(card)
            storage = data['attributes']['storage']
            if storage.get('cleanup_operation') == op_id and card['status'] in {'deleted', 'missing'}:
                (deleted if card['status'] == 'deleted' else skipped).append(item['id'])
                db.rollback()
                continue
            if storage.get('cleanup_operation') != op_id:
                m.require(card['updated_at'] == item['updated_at'], 'catalog changed before intent', 'stale_plan')
                storage.update(cleanup_operation=op_id, cleanup_plan_id=plan_id, availability='cleanup_pending')
                write(db, envelope, 'intent', data, transaction=False, entity_id=item['id'])
            db.commit()
        except Exception:
            db.rollback()
            raise
        db.execute('BEGIN IMMEDIATE')
        try:
            guard = getattr(m, 'storage_revision_guard', None)
            if guard: guard(db, item['id'])
            current = m.row(db, 'entities', 'id', item['id'])
            data = card_data(current)
            storage = data['attributes']['storage']
            m.require(storage.get('cleanup_operation') == op_id and storage.get('retention') == 'disposable'
                      and current['path'] == item['path'] and current['status'] == 'active',
                      'candidate changed during cleanup', 'stale_plan')
            try:
                m.require(fingerprint(item['path']) == item['fingerprint'], 'file changed during cleanup', 'stale_plan')
                with parent_fd(item['path']) as (directory, name):
                    info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                    m.require(stat.S_ISREG(info.st_mode) and info.st_ino == item['fingerprint']['inode']
                              and info.st_dev == item['fingerprint']['device']
                              and info.st_mtime_ns == item['fingerprint']['mtime_ns']
                              and info.st_ctime_ns == item['fingerprint']['ctime_ns'], 'file replaced before unlink', 'stale_plan')
                    os.unlink(name, dir_fd=directory)
            except FileNotFoundError:
                # Persisted intent exists, but cause of absence cannot be proven
                # after a process crash. Preserve this distinction from deletion.
                storage.update(availability='missing', loss_reason='absent_after_cleanup_intent; deletion unconfirmed')
                data['status'] = 'missing'
                skipped.append(item['id'])
            else:
                data['status'] = 'deleted'
                storage.update(availability='deleted', loss_reason='deleted_by_mcp', deleted_at=m.timestamp())
                deleted.append(item['id'])
            write(db, envelope, 'result', data, transaction=False, entity_id=item['id'])
            db.commit()
        except Exception:
            db.rollback()
            raise
    return finish(db, envelope, op_id, request_hash, {'ok': True, 'plan_id': plan_id,
                  'deleted_ids': deleted, 'missing_ids': skipped, 'directories_removed': False})
