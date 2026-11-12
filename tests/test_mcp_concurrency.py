import asyncio,json,sys,tempfile,time,unittest
from pathlib import Path
from mcp import ClientSession,StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp import types
from tests.helpers import fixture
class SameAdapter(unittest.IsolatedAsyncioTestCase):
 async def test_same_stdio_progress_cancel_and_reader_release(self):
  with tempfile.TemporaryDirectory() as td:
   fixture(Path(td))
   with (Path(td)/'stderr.txt').open('w') as log:
    async with stdio_client(StdioServerParameters(command=sys.executable,args=['-B','-m','sql_knowledge.mcp','--project',td]),errlog=log) as io:
     async with ClientSession(*io) as client:
      await client.initialize()
      async def call(name,args=None):
       result=await client.call_tool(name,args or {})
       return result.structuredContent or json.loads(result.content[0].text)
      request_id=client._request_id
      slow=asyncio.create_task(call('query_readonly',dict(sql='WITH RECURSIVE x(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM x WHERE n<100000000) SELECT sum(n) FROM x')))
      await asyncio.sleep(.1)
      started=time.monotonic();context=await call('project_context')
      self.assertTrue(context['ok']);self.assertFalse(slow.done(),'short read must complete while the long query remains active')
      status=await call('project_writer_status');self.assertGreaterEqual(status['readers']['active'],1)
      await client.send_notification(types.ClientNotification(types.CancelledNotification(params=types.CancelledNotificationParams(requestId=request_id,reason='Explicit test cancellation'))))
      try:await asyncio.wait_for(slow,2)
      except Exception:pass
      for _ in range(100):
       status=await call('project_writer_status')
       if status['readers']['active']==0:break
       await asyncio.sleep(.03)
      self.assertEqual(status['readers']['active'],0,status)
      self.assertTrue((await call('project_context'))['ok'])
if __name__=='__main__':unittest.main()
