#!/usr/bin/env python3
import urllib.request, json

doc = json.dumps({"test":123,"symbol":"GOLD","event_time":"2026-07-11 13:32:00"}).encode()
req = urllib.request.Request(
    'http://elasticsearch:9200/finance_ohlc_1min/_doc/test123',
    data=doc,
    headers={'Content-Type':'application/json'},
    method='PUT')
try:
    resp = urllib.request.urlopen(req, timeout=10)
    print('OK:', resp.read()[:300])
except urllib.error.HTTPError as e:
    print('HTTP', e.code)
    print(e.read()[:300])
except Exception as e:
    print('FAIL:', e)
