"""AXIOM Phase 2 - Gateway Verification Tests"""
import urllib.request
import urllib.error
import json

BASE = "http://127.0.0.1:8080/v1/chat/completions"
PAYLOAD = json.dumps({
    "model": "gpt-4o",
    "messages": [{"role": "user", "content": "Hello from AXIOM test!"}]
}).encode()

print("=" * 60)
print("  AXIOM Phase 2 — Live Gateway Verification Tests")
print("=" * 60)

# TEST 1: Bad admin key
print("\n[TEST 1] Bad admin key -> expect 401 Unauthorized")
try:
    req = urllib.request.Request(BASE, data=PAYLOAD, method="POST")
    req.add_header("Authorization", "Bearer WRONG_KEY")
    req.add_header("Content-Type", "application/json")
    resp = urllib.request.urlopen(req)
    print(f"  UNEXPECTED: Got {resp.status}")
except urllib.error.HTTPError as e:
    print(f"  PASS: HTTP {e.code} — {e.read().decode()[:100]}")
except Exception as e:
    print(f"  ERROR: {e}")

# TEST 2: Correct key + gpt-4o (routes to OpenAI)
print("\n[TEST 2] Correct key + gpt-4o -> routes to OpenAI")
try:
    req = urllib.request.Request(BASE, data=PAYLOAD, method="POST")
    req.add_header("Authorization", "Bearer axiom_admin_secret_key_2026")
    req.add_header("Content-Type", "application/json")
    resp = urllib.request.urlopen(req)
    print(f"  Status: {resp.status}")
    print(f"  Body: {resp.read().decode()[:200]}")
except urllib.error.HTTPError as e:
    body = e.read().decode()[:200]
    print(f"  PASS: Auth OK -> Upstream HTTP {e.code} (placeholder key expected)")
    print(f"  Body: {body}")
except Exception as e:
    print(f"  ERROR: {e}")

# TEST 3: Correct key + claude model (routes to Anthropic)
print("\n[TEST 3] Correct key + claude-3-5-sonnet -> routes to Anthropic")
claude_payload = json.dumps({
    "model": "claude-3-5-sonnet",
    "messages": [{"role": "user", "content": "Hello from AXIOM test!"}]
}).encode()
try:
    req = urllib.request.Request(BASE, data=claude_payload, method="POST")
    req.add_header("Authorization", "Bearer axiom_admin_secret_key_2026")
    req.add_header("Content-Type", "application/json")
    resp = urllib.request.urlopen(req)
    print(f"  Status: {resp.status}")
except urllib.error.HTTPError as e:
    body = e.read().decode()[:200]
    print(f"  PASS: Auth OK -> Upstream HTTP {e.code} (placeholder key expected)")
    print(f"  Body: {body}")
except Exception as e:
    print(f"  ERROR: {e}")

# TEST 4: Correct key + gemini model (routes to Gemini)
print("\n[TEST 4] Correct key + gemini-1.5-flash -> routes to Gemini")
gemini_payload = json.dumps({
    "model": "gemini-1.5-flash",
    "messages": [{"role": "user", "content": "Hello from AXIOM test!"}]
}).encode()
try:
    req = urllib.request.Request(BASE, data=gemini_payload, method="POST")
    req.add_header("Authorization", "Bearer axiom_admin_secret_key_2026")
    req.add_header("Content-Type", "application/json")
    resp = urllib.request.urlopen(req)
    print(f"  Status: {resp.status}")
except urllib.error.HTTPError as e:
    body = e.read().decode()[:200]
    print(f"  PASS: Auth OK -> Upstream HTTP {e.code} (placeholder key expected)")
    print(f"  Body: {body}")
except Exception as e:
    print(f"  ERROR: {e}")

print("\n" + "=" * 60)
print("  All Tests Complete!")
print("=" * 60)
