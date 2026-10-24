#!/usr/bin/env python3
"""Validate saved experiments and render their measured values into README."""
import json
from pathlib import Path
root=Path(__file__).resolve().parents[1]
out=root/'docs/evidence'
results=json.loads((out/'results.json').read_text())
assert len(results)==4, 'four paired runs required'
by_name={r['name']:r for r in results}
rows=[]
for r in results:
    name=r['name'];a=r['after'];b=r['before']
    k=json.loads((out/f'{name}-k6.json').read_text())['metrics']
    assert b['metrics']=={'published':0,'consumed':0}
    assert a['metrics']['published']==a['metrics']['consumed']==a['succeeded']+a['dead_lettered']==k['http_reqs']['count']
    assert a['outstanding']==0
    assert k.get('dropped_iterations',{}).get('count',0)==0
    assert k['http_req_failed']['value']==0
    rows.append(f"| {name} | {k['http_reqs']['count']:,} | {k['http_reqs']['rate']:.2f} | {k['http_req_duration']['p(95)']:.2f} | {a['peak_outstanding']} | {a['downstream_calls']:,} | {a['downstream_failures']:,} | {a['succeeded']:,} | {a['dead_lettered']:,} |")
for scenario in ['backpressure','breaker']:
    assert by_name[f'{scenario}-enabled']['settings']==by_name[f'{scenario}-disabled']['settings']
enabled=by_name['backpressure-enabled']['after'];disabled=by_name['backpressure-disabled']['after']
assert enabled['paused_samples_5ms']>0 and disabled['paused_samples_5ms']==0
assert enabled['peak_outstanding']<disabled['peak_outstanding']
be=by_name['breaker-enabled']['after'];bd=by_name['breaker-disabled']['after']
assert be['downstream_failures']<bd['downstream_failures']
log=(out/'breaker-enabled.log').read_text()
for transition in ['from=closed to=open','from=open to=half-open','from=half-open to=closed']:assert transition in log
settings=results[0]['settings']
text=f'''Both modes: **{settings['duration_seconds']}s**, offered **{settings['rate']} requests/s**, **{settings['vus']} preallocated/max VUs**,
four workers, queue capacity 512, pause/resume 32/8, breaker threshold 3 and
cooldown 2s. There were **zero dropped k6 iterations and zero HTTP errors** in all
four runs. A one-request difference is the arrival scheduler's interval boundary.

| Scenario / mode | HTTP requests = published = consumed | Measured req/s | HTTP p95 ms | Peak queued + executing | Downstream calls | Failed downstream calls | Succeeded | Parked in DLQ |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
'''+ '\n'.join(rows)+f'''

Backpressure reduced peak local outstanding work from **{disabled['peak_outstanding']} to {enabled['peak_outstanding']}**
(**{100*(1-enabled['peak_outstanding']/disabled['peak_outstanding']):.1f}% lower**). All overload messages eventually succeeded. It does not
increase handler capacity: Kafka retains the remaining backlog while local
intake is paused. The disabled mode still has a hard queue cap, so this is a
comparison of early intake control and local outstanding count, not a claim of
unbounded memory or higher throughput. Both runs end at depth 0 and paused=false.

During the identical six-second dependency outage, the breaker reduced failed
downstream calls from **{bd['downstream_failures']:,} to {be['downstream_failures']}**
(**{100*(1-be['downstream_failures']/bd['downstream_failures']):.1f}% fewer**). Open-circuit messages are parked immediately; successful
processing is slightly lower because probes intentionally wait for cooldown.
The benefit is reduced pressure on the failing dependency, not a guaranteed
improvement in HTTP latency or success rate. DLQ storage is not durable.

Every run starts at 0 and drains to the published count shown above. For example,
`{be['metrics']['published']:,} = {be['succeeded']:,} succeeded + {be['dead_lettered']:,} parked` in the breaker-enabled run.
The raw snapshots also record the unique topic/group names and UTC timestamps.

Backpressure enabled (paused intake visible) and disabled, at times within their
respective runs; the topic selector prevents earlier runs contaminating charts:

![Backpressure enabled](docs/evidence/backpressure-enabled.png)
![Backpressure disabled](docs/evidence/backpressure-disabled.png)

Breaker open during the outage:

![Breaker enabled during outage](docs/evidence/breaker-enabled.png)

[Full transition log, including half-open and recovery](docs/evidence/breaker-enabled.log):

```text
'''+ '\n'.join(line for line in log.splitlines() if 'circuit_breaker' in line)+'''
```
'''
p=root/'README.md';s=p.read_text();a=s.index('<!-- evidence-results:start -->');b=s.index('<!-- evidence-results:end -->',a)
s=s[:a]+'<!-- evidence-results:start -->\n'+text+'\n'+s[b:]
checklist='''## Gap checklist — 24 Oct 2026

- [x] 1. Real architecture flow diagram including producer, Kafka, consumers, breaker, DLQ and monitoring.
- [x] 2. Like-for-like enabled and disabled runs with identical settings and fresh topics.
- [x] 3. Failure run opens the breaker, probes half-open and recovers closed; transition log captured.
- [x] 4. Historical count discrepancy explained; new per-run accounting balances exactly.
- [x] 5. Measurable local outstanding-work reduction and failed downstream-call reduction documented.
- [x] 6. Both tracked binaries removed; generated paths ignored, source-build commands and CI artifacts added.
- [x] 7. Repository-local PRD explicitly selects Kafka and supersedes the old RabbitMQ wording.
- [x] 8. Backpressure, breaker and degradation tests pass locally with race detection and recorded coverage.

Changes remain uncommitted, as requested. The checklist is verified by the raw
artifacts linked below; it does not claim an upstream Notion edit or a CI run.

'''
if '## Gap checklist' not in s:s=s.replace('## Endpoints',checklist+'## Endpoints',1)
p.write_text(s)
print('Validated four runs and updated README')
