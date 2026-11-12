import argparse,json,sys,time
from .engine import Project,initialize

def main():
 p=argparse.ArgumentParser(description='SQL Knowledge: explicit local SQLite project, no SQL dumps');p.add_argument('--project',required=True);sub=p.add_subparsers(dest='command',required=True)
 q=sub.add_parser('init');q.add_argument('--project-id',required=True);q.add_argument('--title',default='SQL Knowledge project')
 for name in ['context','schema','migrate','writer-status','writer-stop','writer-start','checkpoint']:sub.add_parser(name)
 q=sub.add_parser('check');q.add_argument('--no-files',action='store_true')
 q=sub.add_parser('check-start');q.add_argument('--no-files',action='store_true')
 for name in ['check-step','check-result','check-cancel']:
  q=sub.add_parser(name);q.add_argument('--job-id',required=True)
  if name=='check-step':q.add_argument('--batch-size',type=int,default=1000)
  if name=='check-result':q.add_argument('--offset',type=int,default=0);q.add_argument('--limit',type=int,default=100)
 q=sub.add_parser('query');q.add_argument('--sql',required=True);q.add_argument('--params',default='[]');q.add_argument('--limit',type=int,default=100)
 q=sub.add_parser('search');q.add_argument('--text',required=True);q.add_argument('--limit',type=int,default=20);q.add_argument('--include-inactive',action='store_true')
 q=sub.add_parser('history');q.add_argument('--id',required=True);q.add_argument('--limit',type=int,default=100)
 for name in ['entity','knowledge','decision','link']:sub.add_parser(name).add_argument('--input',required=True)
 a=p.parse_args()
 try:
  if a.command=='init':result=initialize(a.project,a.project_id,a.title)
  elif a.command in ['writer-status','writer-stop','writer-start','checkpoint']:
   from .coordinator import submit
   result=submit(Project(a.project),{'writer-status':'status','writer-stop':'shutdown','writer-start':'resume','checkpoint':'checkpoint'}[a.command],{})
  elif a.command.startswith('check'):
   from . import check_jobs as jobs
   project=Project(a.project)
   if a.command in ['check','check-start']:
    result=jobs.start(project,files=not a.no_files)
    if a.command=='check':
     while result['status']=='running':
      result=jobs.step(project,result['job_id'],10000)
      if result['phase']=='physical':time.sleep(.1)
     result=jobs.result(project,result['job_id'])
   elif a.command=='check-step':result=jobs.step(project,a.job_id,a.batch_size)
   elif a.command=='check-result':result=jobs.result(project,a.job_id,a.offset,a.limit)
   else:result=jobs.cancel(project,a.job_id)
  elif a.command=='migrate':
   from .extensions import migrate
   result=migrate(Project(a.project))
  else:result=Project(a.project).operation(a.command,vars(a),json.loads(sys.stdin.read() if a.input=='-' else open(a.input).read()) if hasattr(a,'input') else None)
  print(json.dumps(result,ensure_ascii=False,indent=2));return 0 if result.get('ok') or result.get('status')=='running' else 1
 except Exception as e:print(json.dumps(dict(ok=False,code=getattr(e,'code','operation_failed'),error=str(e)),ensure_ascii=False),file=sys.stderr);return 1
if __name__=='__main__':sys.exit(main())
