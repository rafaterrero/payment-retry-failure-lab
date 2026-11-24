"""Re-run both harnesses in fresh instances of the immutable recorded Docker image."""
import hashlib,json,os,ssl,subprocess,sys,tempfile,time,urllib.request,uuid
from pathlib import Path
root=Path(__file__).resolve().parents[1]
lock=json.loads((root/'aws-emulator/image-lock.json').read_text())
report=root/'reports'/('pinned-repeat-'+uuid.uuid4().hex[:8]); report.mkdir()
def docker(*args): return subprocess.check_output(['docker',*args],text=True).strip()
info=json.loads(docker('image','inspect',lock['image_id']))[0]
if info['Id']!=lock['image_id'] or info['Architecture']!=lock['architecture']: raise RuntimeError('Wrong image')
manifest={'image_lock':lock,'aws_cli':subprocess.check_output(['aws','--version'],text=True).strip(),
          'python':sys.version.split()[0],'source_sha256':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root/'aws-emulator').glob('*')) if p.is_file()},'runs':[]}
(report/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
for label,command in [('P01',['bash','aws-emulator/p01_run.sh']),('P02-P05',[sys.executable,'aws-emulator/p02_p05_run.py'])]:
    with tempfile.TemporaryDirectory(prefix='synthetic-emulator-') as temp:
        docker('run','-d','--rm','--name','payment-lab-emulator','--memory=2g','--memory-swap=2g',
               '-p','127.0.0.1:9297:9297','-e','EMULATOR_TELEMETRY=off','-v',temp+':/home/emulator/.aws/emulator',lock['image_id'],'-p','9297')
        try:
            version=docker('exec','payment-lab-emulator','aws-emulator','--version')
            if ('Version: '+lock['version']) not in version or lock['commit'] not in version: raise RuntimeError('Wrong emulator version')
            ca=Path(temp)/'9297/ca.pem'
            for _ in range(30):
                try:
                    with urllib.request.urlopen('https://127.0.0.1:9297/__admin/health',context=ssl.create_default_context(cafile=str(ca)),timeout=2) as response:
                        if response.read()==b'ok': break
                except (OSError,ValueError): pass
                time.sleep(1)
            else: raise RuntimeError('Emulator not ready')
            env=os.environ.copy();env.update(EMULATOR_DIR=temp,REPORT_DIR=str(report/'p01'),PYTHONOPTIMIZE='1')
            before=set((root/'reports').glob('aws-emulator-p02-p05-*'))
            print('Starting '+label+' on verified '+lock['version']+' in a fresh container',flush=True)
            process=subprocess.Popen(command,cwd=root,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
            output=[]
            for line in process.stdout:
                print(line,end='',flush=True);output.append(line)
            code=process.wait()
            (report/(label+'.log')).write_text(''.join(output))
            entry={'scenario_group':label,'exit_code':code,'version':version,'python_optimization_enabled':True}
            if label=='P02-P05':
                created=set((root/'reports').glob('aws-emulator-p02-p05-*'))-before
                entry['evidence_directories']=[str(p.relative_to(root)) for p in sorted(created)]
            manifest['runs'].append(entry)
            (report/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
            if code: raise RuntimeError(label+' failed')
        finally:
            docker('stop','payment-lab-emulator')
print('Pinned repeat passed. Evidence: '+str(report.relative_to(root)),flush=True)
