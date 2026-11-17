"""Shared stdio MCP, bound to one explicit project. No export tool."""
import argparse,asyncio,datetime,functools,json,subprocess,sys,tempfile,uuid,threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal
from .engine import BASE,Project,module
from . import extensions

def create_server(root):
 project=Project(root);project.client_id=uuid.uuid4().hex;adapter=module('sql_knowledge._core.bound_server',BASE/'mcp_server.py');adapter.ROOT=project.root;adapter.memory=project.memory
 adapter.storage=module('sql_knowledge._core.bound_storage',BASE/'storage.py');adapter.storage.m=project.memory
 from .local_storage import configured
 local_storage=configured(project)
 original=adapter.Service;adapter.TIMEOUT=120
 class Service(original):
  def call(self,command,*argv,envelope=None):
   args={};i=0
   while i<len(argv):
    key=argv[i].removeprefix('--').replace('-','_')
    if key=='include_inactive':args[key]=True;i+=1
    else:args[key]=argv[i+1];i+=2
   if envelope is not None:envelope={k:v for k,v in envelope.items() if k not in ('expected_revision','expected_revisions') or v is not None}
   if envelope is not None and len(json.dumps(envelope,ensure_ascii=False).encode())>adapter.MAX_INPUT:adapter.fail('limit_exceeded','write envelope exceeds64KiB')
   try:
    if command=='check':
     from . import check_jobs
     return check_jobs.start(project)
    result=project.operation(command,args,envelope)
   except Exception as exc:adapter.fail(getattr(exc,'code','read_timeout' if 'interrupted' in str(exc) else 'operation_failed'),str(exc))
   if len(json.dumps(result,ensure_ascii=False).encode())>adapter.MAX_OUTPUT:adapter.fail('response_too_large','narrow the query; response exceeds512KiB; writes may have committed')
   return result
  def storage_call(self,action,*args,**kwargs):
   if len(json.dumps([args,kwargs],ensure_ascii=False).encode())>adapter.MAX_INPUT:adapter.fail('limit_exceeded','storage input exceeds64KiB')
   try:
    if local_storage is not None:
     result=local_storage.call(action,*args,**kwargs)
    elif action=='inventory':
     with project.connect() as db:result=adapter.storage.inventory(db,*args,**kwargs)
    else:
     from .coordinator import submit
     result=submit(project,'storage',dict(action=action,args=args,kwargs=kwargs),timeout=30)
    if len(json.dumps(result,ensure_ascii=False).encode())>adapter.MAX_OUTPUT:adapter.fail('response_too_large','narrow storage input; write may have committed')
    return result
   except Exception as exc:adapter.fail(getattr(exc,'code','storage_error'),str(exc))
 class GenericEntityData(adapter.EntityData):
  kind:Literal[tuple(sorted(project.memory.ARTIFACT_KINDS))]
 class RevisionEnvelope(adapter.Envelope):
  expected_revision:int|None=None
  expected_revisions:dict[str,int]|None=None
 class GenericEntityEnvelope(RevisionEnvelope):
  data:GenericEntityData
 class GenericRelationData(adapter.RelationData):
  predicate:Literal[tuple(sorted(project.memory.PREDICATES))]
 class GenericRelationEnvelope(RevisionEnvelope):
  data:GenericRelationData
 class KnowledgeEnvelope(RevisionEnvelope):
  data:adapter.KnowledgeData
 class DecisionEnvelope(RevisionEnvelope):
  data:adapter.DecisionData
 class StorageFileEnvelope(RevisionEnvelope):
  data:adapter.StorageFileData
 class StorageIDsEnvelope(RevisionEnvelope):
  data:adapter.StorageIDs
 class CleanupPlanEnvelope(RevisionEnvelope):
  data:adapter.CleanupPlanData
 class CleanupExecuteEnvelope(RevisionEnvelope):
  data:adapter.CleanupExecuteData
 adapter.KnowledgeEnvelope=KnowledgeEnvelope;adapter.DecisionEnvelope=DecisionEnvelope
 adapter.StorageFileEnvelope=StorageFileEnvelope;adapter.StorageIDsEnvelope=StorageIDsEnvelope
 adapter.CleanupPlanEnvelope=CleanupPlanEnvelope;adapter.CleanupExecuteEnvelope=CleanupExecuteEnvelope
 adapter.EntityEnvelope=GenericEntityEnvelope;adapter.RelationEnvelope=GenericRelationEnvelope
 from . import audit_cache
 @asynccontextmanager
 async def lifespan(server):
  stop=asyncio.Event();task=asyncio.create_task(audit_cache.periodic(project,stop))
  try:yield
  finally:
   stop.set()
   await task
 adapter.Service=Service;server=adapter.create_server(project.database,lifespan=lifespan)
 server.remove_tool('buildings_get');server.remove_tool('scenes_get')
 def checked(function,*args,**kwargs):
  try:
   result=function(project,*args,**kwargs)
   if len(json.dumps(result,ensure_ascii=False).encode())>adapter.MAX_OUTPUT:adapter.fail('response_too_large','response exceeds512KiB; request a smaller report page')
   return result
  except Exception as exc:adapter.fail(getattr(exc,'code','check_error'),str(exc))
 from . import check_jobs
 @server.tool(annotations=adapter.READ)
 def project_check_start(files:bool=True)->dict:
  """Create an explicit backup snapshot for a resumable full audit."""
  return checked(check_jobs.start,files=files)
 @server.tool(annotations=adapter.READ)
 def project_check_step(job_id:str,batch_size:int=1000)->dict:
  """Advance bounded audit work; incomplete is never a passing result."""
  return checked(check_jobs.step,job_id,batch_size)
 @server.tool(annotations=adapter.READ)
 def project_check_report(job_id:str,offset:int=0,limit:int=100)->dict:
  if type(limit) is not int or not 1<=limit<=100:adapter.fail('invalid_input','MCP report limit1..100 required')
  return checked(check_jobs.result,job_id,offset,limit)
 @server.tool(annotations=adapter.READ)
 def project_check_cancel(job_id:str)->dict:
  return checked(check_jobs.cancel,job_id)
 @server.tool(annotations=adapter.READ)
 def project_check_reports()->dict:
  """List retained audit summaries: last20 errors and last1 success per project."""
  return checked(audit_cache.reports)
 @server.tool(annotations=adapter.WRITE)
 def project_check_maintenance()->dict:
  """Retry automatic report retention and completed-cache release now.

  Also runs on MCP startup, every five minutes and after audit completion.
  Preserves incomplete, busy, keep and catalog-owned files. No model calls.
  """
  return checked(audit_cache.maintain)
 @server.tool(annotations=adapter.READ)
 def project_writer_status()->dict:
  from .coordinator import submit
  return checked(lambda p:submit(p,'status',{}))
 @server.tool(annotations=adapter.WRITE)
 def extension_upsert(extension:str,table:str,row:dict,envelope:dict)->dict:
  if len(json.dumps([extension,table,row,envelope],ensure_ascii=False).encode())>adapter.MAX_INPUT:adapter.fail('limit_exceeded','extension input exceeds64KiB')
  try:return extensions.upsert(project,extension,table,row,envelope)
  except Exception as exc:adapter.fail(getattr(exc,'code','extension_error'),str(exc))
 @server.tool(annotations=adapter.READ)
 def extension_query(extension:str,table:str,limit:int=100,after_id:str|None=None)->dict:
  try:result=extensions.query(project,extension,table,limit,after_id)
  except Exception as exc:adapter.fail(getattr(exc,'code','extension_error'),str(exc))
  if len(json.dumps(result,ensure_ascii=False).encode())>adapter.MAX_OUTPUT:adapter.fail('response_too_large','narrow extension page')
  return result
 # FastMCP executes plain synchronous functions on its event loop. Retain the
 # generated schemas/models but offload validated invocations to cancellable work.
 from .runtime import CANCEL
 def asynchronous(function):
  @functools.wraps(function)
  async def invoke(*args,**kwargs):
   cancelled=threading.Event();token=CANCEL.set(cancelled)
   try:return await asyncio.to_thread(function,*args,**kwargs)
   finally:cancelled.set();CANCEL.reset(token)
  return invoke
 for tool in server._tool_manager.list_tools():
  if not tool.is_async:tool.fn=asynchronous(tool.fn);tool.is_async=True
 return server

def main():
 p=argparse.ArgumentParser();p.add_argument('--project',required=True);args=p.parse_args();create_server(args.project).run(transport='stdio')
if __name__=='__main__':main()
