#!/usr/bin/env python3
"""Capture the provisioned local Grafana dashboard (pip install playwright first)."""
import argparse
from pathlib import Path
from playwright.sync_api import sync_playwright
p=argparse.ArgumentParser()
p.add_argument('output'); p.add_argument('--from-ms',type=int,required=True)
p.add_argument('--url',default='http://localhost:3005')
p.add_argument('--to-ms',type=int)
p.add_argument('--topic',default='.*')
a=p.parse_args()
with sync_playwright() as pw:
    browser=pw.chromium.launch(headless=True)
    page=browser.new_page(viewport={'width':1800,'height':1500},device_scale_factor=1)
    page.goto(f'{a.url}/d/flowguard-observability?orgId=1&from={a.from_ms}&to={a.to_ms or "now"}&var-topic={a.topic}',wait_until='networkidle')
    page.get_by_text('Backpressure Queue Depth',exact=True).wait_for(timeout=30000)
    page.wait_for_timeout(2500)
    page.screenshot(path=str(Path(a.output).absolute()),full_page=True)
    browser.close()
