from pathlib import Path
import json,shutil
from sql_knowledge.engine import initialize,Project
ROOT=Path(__file__).resolve().parents[1]
def env(key,data=None):return dict(idempotency_key=key,actor='test',reason='Isolated test',source_ref='test:fixture',data=data or {})
def fixture(root,extension=False):
 initialize(str(root),'project','Fixture project')
 if extension:
  shutil.copytree(ROOT/'examples/demo/extensions',root/'extensions')
  with (root/'knowledge.toml').open('a') as f:f.write('[extensions.demo]\nversion = 1\npath = "extensions/demo/extension.toml"\n')
 p=Project(str(root));p.operation('entity',envelope=env('asset',dict(id='asset',kind='report',title='Gold proof',status='active')));return p
