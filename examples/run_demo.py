"""Run against an explicit NEW demo root. Never replaces an existing project."""
import argparse,asyncio,json,shutil,sys
from pathlib import Path
from sql_knowledge.engine import initialize,Project
from sql_knowledge.extensions import migrate
from mcp import ClientSession,StdioServerParameters
from mcp.client.stdio import stdio_client
async def run(root):
 initialize(str(root),'demo-project','SQL Knowledge demo')
 shutil.copytree(Path(__file__).parent/'demo/extensions',root/'extensions')
 with (root/'knowledge.toml').open('a') as f:f.write('[extensions.demo]\nversion = 1\npath = "extensions/demo/extension.toml"\n')
 migrate(Project(str(root)))
 async with stdio_client(StdioServerParameters(command=sys.executable,args=['-m','sql_knowledge.mcp','--project',str(root)])) as io:
  async with ClientSession(*io) as c:
   await c.initialize()
   meta=dict(actor='demo',reason='Explicit example',source_ref='demo:operator')
   for name,args in [('entity_write',dict(envelope=dict(meta,idempotency_key='demo-asset',data=dict(id='asset',kind='report',title='Demo item',status='active')))),('extension_upsert',dict(extension='demo',table='demo_items',row=dict(id='one',entity_id='asset',value='Hello'),envelope=dict(meta,idempotency_key='demo-one'))),('extension_query',dict(extension='demo',table='demo_items')),('project_check',{})]:
    r=await c.call_tool(name,args)
    if r.isError:raise RuntimeError(str(r))
    value=r.structuredContent or json.loads(r.content[0].text)
    print(name,json.dumps(value,ensure_ascii=False))
    if name=='project_check':
     for _ in range(1000):
      r=await c.call_tool('project_check_step',dict(job_id=value['job_id'],batch_size=1000))
      if r.isError:raise RuntimeError(str(r))
      value=r.structuredContent or json.loads(r.content[0].text)
      if value['status']!='running':break
      if value['phase']=='physical':await asyncio.sleep(.02)
     if not value['ok']:raise RuntimeError('Full demo audit did not pass: '+json.dumps(value))
     print('project_check_complete',json.dumps(value,ensure_ascii=False))
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--project',required=True);asyncio.run(run(Path(p.parse_args().project)))
