"""Versioned, unambiguous keys for storage suboperations; no database mutation."""
import hashlib,json

def step_key(key,action,phase,entity_id=None,version=2):
 if version==1:
  suffix=entity_id if phase=='target' else phase+'-'+entity_id if phase in ('intent','result') else phase
  return (key+':'+suffix).strip()
 if version!=2:raise ValueError('unsupported storage key version')
 raw=json.dumps([key,action,phase,entity_id],ensure_ascii=False,separators=(',',':')).encode()
 return 'sqlk-storage-v2:'+hashlib.sha256(raw).hexdigest()
