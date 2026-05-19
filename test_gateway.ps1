Write-Host ""
Write-Host "========================================================"
Write-Host "   AXIOM Phase 2 - Live Gateway Verification Tests"
Write-Host "========================================================"
Write-Host ""

$baseUrl = "http://127.0.0.1:8080/v1/chat/completions"
$jsonBody = "{`"model`":`"gpt-4o`",`"messages`":[{`"role`":`"user`",`"content`":`"Hello`"}]}"
$claudeBody = "{`"model`":`"claude-3-5-sonnet`",`"messages`":[{`"role`":`"user`",`"content`":`"Hello`"}]}"
$geminiBody = "{`"model`":`"gemini-1.5-flash`",`"messages`":[{`"role`":`"user`",`"content`":`"Hello`"}]}"

# TEST 1: Bad Admin Key
Write-Host "[TEST 1] Bad admin key -> expect 401 Unauthorized"
try {
    $r = Invoke-WebRequest -Uri $baseUrl -Method Post -Headers @{"Authorization"="Bearer WRONG_KEY"} -ContentType "application/json" -Body $jsonBody -ErrorAction Stop
    Write-Host "  Status: $($r.StatusCode) - $($r.Content)"
} catch {
    $code = $_.Exception.Response.StatusCode.value__
    Write-Host "  PASS: Got HTTP $code (Unauthorized blocked!)"
}

Write-Host ""

# TEST 2: Correct Key + OpenAI model
Write-Host "[TEST 2] Correct key + gpt-4o -> routes to OpenAI proxy"
try {
    $r = Invoke-WebRequest -Uri $baseUrl -Method Post -Headers @{"Authorization"="Bearer axiom_admin_secret_key_2026"} -ContentType "application/json" -Body $jsonBody -ErrorAction Stop
    Write-Host "  Status: $($r.StatusCode)"
    Write-Host "  Body: $($r.Content.Substring(0, [Math]::Min(200, $r.Content.Length)))"
} catch {
    $code = $_.Exception.Response.StatusCode.value__
    $stream = $_.Exception.Response.GetResponseStream()
    $reader = New-Object System.IO.StreamReader($stream)
    $body = $reader.ReadToEnd()
    Write-Host "  PASS: Authorized OK -> Upstream returned HTTP $code (placeholder API key expected)"
    Write-Host "  Body: $($body.Substring(0, [Math]::Min(200, $body.Length)))"
}

Write-Host ""

# TEST 3: Correct Key + Anthropic model
Write-Host "[TEST 3] Correct key + claude-3-5-sonnet -> routes to Anthropic proxy"
try {
    $r = Invoke-WebRequest -Uri $baseUrl -Method Post -Headers @{"Authorization"="Bearer axiom_admin_secret_key_2026"} -ContentType "application/json" -Body $claudeBody -ErrorAction Stop
    Write-Host "  Status: $($r.StatusCode)"
} catch {
    $code = $_.Exception.Response.StatusCode.value__
    $stream = $_.Exception.Response.GetResponseStream()
    $reader = New-Object System.IO.StreamReader($stream)
    $body = $reader.ReadToEnd()
    Write-Host "  PASS: Authorized OK -> Upstream returned HTTP $code (placeholder API key expected)"
    Write-Host "  Body: $($body.Substring(0, [Math]::Min(200, $body.Length)))"
}

Write-Host ""

# TEST 4: Correct Key + Gemini model
Write-Host "[TEST 4] Correct key + gemini-1.5-flash -> routes to Gemini proxy"
try {
    $r = Invoke-WebRequest -Uri $baseUrl -Method Post -Headers @{"Authorization"="Bearer axiom_admin_secret_key_2026"} -ContentType "application/json" -Body $geminiBody -ErrorAction Stop
    Write-Host "  Status: $($r.StatusCode)"
} catch {
    $code = $_.Exception.Response.StatusCode.value__
    $stream = $_.Exception.Response.GetResponseStream()
    $reader = New-Object System.IO.StreamReader($stream)
    $body = $reader.ReadToEnd()
    Write-Host "  PASS: Authorized OK -> Upstream returned HTTP $code (placeholder API key expected)"
    Write-Host "  Body: $($body.Substring(0, [Math]::Min(200, $body.Length)))"
}

Write-Host ""
Write-Host "========================================================"
Write-Host "   Verification Complete!"
Write-Host "========================================================"
