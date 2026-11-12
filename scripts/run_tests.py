from pathlib import Path
import datetime,json,subprocess,sys
root=Path(__file__).resolve().parents[1];stamp=datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f');folder=root/'work'/('tests-'+stamp);folder.mkdir(parents=True)
commands=[[sys.executable,'-B','-m','unittest','discover','-s','tests','-t','.','-p','test_*.py','-v']]
results=[]
for command in commands:
 with (folder/'output.txt').open('w') as f:r=subprocess.run(command,cwd=root,stdout=f,stderr=subprocess.STDOUT,timeout=180)
 results.append(dict(command=command,returncode=r.returncode));print(folder/'output.txt')
(folder/'result.json').write_text(json.dumps(results,indent=2));sys.exit(any(r['returncode'] for r in results))
