import json,subprocess,sys,tempfile,unittest
from pathlib import Path
from sql_knowledge.engine import Project,initialize
from tests.helpers import fixture,env
class Projects(unittest.TestCase):
 def test_relative_missing_and_no_overwrite(self):
  with self.assertRaises(ValueError):initialize('.','project')
  with tempfile.TemporaryDirectory() as td:
   p=fixture(Path(td));before=p.database.read_bytes()
   with self.assertRaises(ValueError):initialize(td,'new')
   self.assertEqual(p.database.read_bytes(),before)
   r=subprocess.run([sys.executable,'-m','sql_knowledge','--project',td,'export'],capture_output=True,text=True);self.assertNotEqual(r.returncode,0);self.assertFalse(list(Path(td).glob('*.sql')))
 def test_lifecycle_readonly_search(self):
  with tempfile.TemporaryDirectory() as td:
   p=fixture(Path(td));p.operation('knowledge',envelope=env('gold',dict(id='fact',title='Gold finding',status='active',knowledge_type='finding',confidence='observed',body='Unique proof',evidence=[dict(ref='entity:asset')])))
   self.assertEqual(p.operation('search',dict(text='Unique',limit=1))['rows'][0]['id'],'fact');self.assertTrue(p.operation('check')['ok'])
   with self.assertRaises(Exception):p.operation('query',dict(sql='DELETE FROM entities'))
 def test_two_projects(self):
  with tempfile.TemporaryDirectory() as td:
   p=fixture(Path(td)/'one');q=fixture(Path(td)/'two');p.operation('entity',envelope=env('only',dict(id='only',kind='report',title='Only',status='active')))
   self.assertEqual(q.operation('query',dict(sql="SELECT count(*) AS n FROM entities WHERE id='only'"))['rows'][0]['n'],0)
if __name__=='__main__':unittest.main()
