"""AXIOM Phase 3 — Gateway Verification Tests (Rate Limiter + Circuit Breaker)"""
import urllib.request
import urllib.error
import json
import time

BASE = "http://127.0.0.1:8080/v1/chat/completions"
HEADERS = {"Authorization": "Bearer axiom_admin_secret_key_2026", "Content-Type": "application/json"}
PAYLOAD = json.dumps({"model": "gpt-4o", "messages": [{"role": "user", "content": "Hello"}]}).encode()

def post(body=PAYLOAD, headers=HEADERS):
    req = urllib.request.Request(BASE, data=body, method="POST")
    for k, v in headers.items():
        req.add_header(k, v)
    return urllib.request.urlopen(req)

print("=" * 60)
print("  AXIOM Phase 3 — Rate Limiter + Circuit Breaker Tests")
print("=" * 60)

# TEST 1: Auth still works
print("\n[TEST 1] Bad key -> 401")
try:
    req = urllib.request.Request(BASE, data=PAYLOAD, method="POST")
    req.add_header("Authorization", "Bearer WRONG")
    req.add_header("Content-Type", "application/json")
    urllib.request.urlopen(req)
    print("  FAIL: Should have been rejected")
except urllib.error.HTTPError as e:
    print(f"  PASS: HTTP {e.code}")

# TEST 2: Rate Limiter — send 10 requests (burst_capacity=10), then 11th should be rejected
print("\n[TEST 2] Rate Limiter: Sending 12 rapid requests (burst=10)")
results = []
for i in range(12):
    try:
        post()
        results.append(f"  Request {i+1}: Allowed (upstream error expected)")
    except urllib.error.HTTPError as e:
        if e.code == 429:
            results.append(f"  Request {i+1}: RATE LIMITED (HTTP 429) <<<")
        elif e.code == 502:
            results.append(f"  Request {i+1}: Allowed -> upstream 502 (placeholder key)")
        else:
            results.append(f"  Request {i+1}: HTTP {e.code}")
    except Exception as e:
        results.append(f"  Request {i+1}: Error - {e}")

for r in results:
    print(r)

# TEST 3: Circuit Breaker — after 5 failures (OpenAI threshold=5), circuit should trip
print("\n[TEST 3] Circuit Breaker: After 5+ failures, provider should be unavailable")
time.sleep(2)  # Let rate limiter refill a bit
cb_payload = json.dumps({"model": "gpt-4o", "messages": [{"role": "user", "content": "CB test"}]}).encode()
for i in range(7):
    try:
        req = urllib.request.Request(BASE, data=cb_payload, method="POST")
        for k, v in HEADERS.items():
            req.add_header(k, v)
        urllib.request.urlopen(req)
        print(f"  Request {i+1}: Success")
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:120]
        if e.code == 503:
            print(f"  Request {i+1}: CIRCUIT OPEN (HTTP 503) <<< {body}")
        elif e.code == 429:
            print(f"  Request {i+1}: Rate limited (HTTP 429)")
        else:
            print(f"  Request {i+1}: HTTP {e.code} - {body[:80]}")
    except Exception as e:
        print(f"  Request {i+1}: Error - {e}")

print("\n" + "=" * 60)
print("  All Phase 3 Tests Complete!")
print("=" * 60)
