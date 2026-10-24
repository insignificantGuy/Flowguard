#!/usr/bin/env python3
"""Run paired experiments against real Kafka; preserve logs, k6 summaries and samples.
Requires deploy/evidence/compose.yml running, local k6, and bin/evidence.
"""
import json,os,subprocess,sys,time,urllib.request
from datetime import datetime
from pathlib import Path
root=Path(__file__).resolve().parents[1]
os.chdir(root)
out=root/'docs/evidence';out.mkdir(exist_ok=True)
stamp=time.strftime('%Y%m%d%H%M%S',time.gmtime())
env=os.environ.copy();env['PATH']=str(Path.home()/'.local/bin')+':'+env['PATH']
def get(path):
    with urllib.request.urlopen('http://localhost:3330'+path,timeout=3) as r:return json.load(r)
def millis(raw):
    value=raw.removesuffix('Z')
    whole,_,fraction=value.partition('.')
    return datetime.fromisoformat(whole+'.'+fraction[:6].ljust(6,'0')+'+00:00').timestamp()*1000
all_results=[]
for scenario in ['backpressure','breaker']:
  for mode in ['enabled','disabled']:
    name=f'{scenario}-{mode}'
    topic=f'evidence-{stamp}-{name}'
    log=(out/f'{name}.log').open('w')
    app=subprocess.Popen(['bin/evidence','-topic',topic,'-scenario',scenario,'-mode',mode],stdout=log,stderr=log)
    load=None
    try:
      deadline=time.monotonic()+45
      while True:
        if app.poll() is not None:raise RuntimeError(f'{name} exited; see log')
        try:before=get('/report');break
        except Exception:
          if time.monotonic()>deadline:raise
          time.sleep(.2)
      start=time.monotonic();start_ms=int(time.time()*1000)
      loadenv=env.copy();loadenv['SUMMARY_PATH']=str(out/f'{name}-k6.json')
      with (out/f'{name}-k6.log').open('w') as loadlog:
        load=subprocess.Popen(['bash','scripts/load-test.sh','--url','http://localhost:3330','--duration','15s','--vus','200','--rate','200'],env=loadenv,stdout=loadlog,stderr=loadlog)
        samples=[]
        while load.poll() is None:
          sample=get('/report');samples.append(sample)
          time.sleep(.1)
      if load.returncode:raise RuntimeError(f'{name}: k6 failed ({load.returncode})')
      # Hold the application until every newly published record is accounted for.
      deadline=time.monotonic()+90
      while True:
        after=get('/report');samples.append(after)
        if after['metrics'].get('published',0)>0 and after['succeeded']+after['dead_lettered']==after['metrics']['published'] and after['outstanding']==0:break
        if time.monotonic()>deadline:raise RuntimeError(f'{name}: failed to drain')
        time.sleep(.2)
      summary=json.loads((out/f'{name}-k6.json').read_text())
      if summary['metrics'].get('dropped_iterations',{}).get('count',0):
        raise RuntimeError(f'{name}: generator dropped iterations; increase VUs for BOTH runs')
      # Capture the stored dashboard at an explicit time inside this run,
      # after load generation, so Chromium does not perturb the load generator.
      candidates=[s for s in samples if 5 <= (millis(s['time_utc'])-start_ms)/1000 <= 12]
      if mode=='enabled':
        active=[s for s in candidates if s.get('paused') or s.get('breaker_state')==1]
        if active: candidates=active
      capture_ms=int(millis(candidates[len(candidates)//2]['time_utc']))
      subprocess.run([sys.executable,'scripts/capture-dashboard.py',str(out/f'{name}.png'),'--from-ms',str(start_ms),'--to-ms',str(capture_ms),'--topic',topic],check=True)
      (out/f'{name}-samples.json').write_text(json.dumps(samples,indent=2)+'\n')
      result={'name':name,'settings':{'duration_seconds':15,'rate':200,'vus':200,'workers':4,'queue_capacity':512,'pause':32,'resume':8,'delay_ms':50 if scenario=='backpressure' else 5,'failure_window_seconds':[2,8] if scenario=='breaker' else None},'before':before,'after':after}
      all_results.append(result)
      (out/'results.json').write_text(json.dumps(all_results,indent=2)+'\n')
      print(json.dumps(result),flush=True)
    finally:
      if load is not None and load.poll() is None:load.terminate();load.wait(timeout=10)
      app.terminate()
      try:app.wait(timeout=15)
      except subprocess.TimeoutExpired:app.kill();app.wait();raise
      log.close()

subprocess.run([sys.executable,"scripts/publish-evidence.py"],check=True)
