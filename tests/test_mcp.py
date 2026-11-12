import asyncio,json,sys,tempfile,unittest
from pathlib import Path
from mcp import ClientSession,StdioServerParameters
from mcp.client.stdio import stdio_client
from tests.helpers import fixture,env
class MCP(unittest.IsolatedAsyncioTestCase):
 async def test_extension_roundtrip_and_no_export(self):
  from sql_knowledge.extensions import migrate
  with tempfile.TemporaryDirectory() as td:
   p=fixture(Path(td),True);migrate(p)
   with (p.root/'mcp-stderr.txt').open('w') as log:
    async with stdio_client(StdioServerParameters(command=sys.executable,args=['-B','-m','sql_knowledge.mcp','--project',td]),errlog=log) as streams:
     async with ClientSession(*streams) as c:
      await c.initialize();tools={t.name for t in (await c.list_tools()).tools};self.assertNotIn('project_export',tools);self.assertNotIn('buildings_get',tools);self.assertIn('extension_upsert',tools)
      async def call(name,args=None):
       r=await c.call_tool(name,args or {});self.assertFalse(r.isError,str(r));return r.structuredContent or json.loads(r.content[0].text)
      self.assertEqual((await call('project_context'))['project_id'],'project')
      args=dict(extension='demo',table='demo_items',row=dict(id='item',entity_id='asset',value='proof'),envelope={k:v for k,v in env('item').items() if k!='data'})
      self.assertFalse((await call('extension_upsert',args))['replayed']);self.assertTrue((await call('extension_upsert',args))['replayed'])
      self.assertEqual((await call('extension_query',dict(extension='demo',table='demo_items')))['rows'][0]['value'],'proof')
      job=await call('project_check')
      for _ in range(100):
       job=await call('project_check_step',dict(job_id=job['job_id'],batch_size=100))
       if job['status']!='running':break
       await asyncio.sleep(.02)
      self.assertTrue(job['ok'],str(job))
      over=await c.call_tool('project_check_report',dict(job_id=job['job_id'],limit=101))
      self.assertTrue(over.isError);self.assertIn('limit1..100',str(over))
      self.assertTrue((await c.call_tool('query_readonly',dict(sql='DELETE FROM entities'))).isError)
      self.assertTrue((await c.call_tool('extension_upsert',dict(args,row=dict(id='bad',entity_id='missing',value='x')))).isError)
      self.assertFalse(list(p.root.glob('*.sql')))
if __name__=='__main__':unittest.main()
