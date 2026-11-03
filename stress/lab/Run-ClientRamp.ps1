# Ramp RM_CLIENT_USERS and save CLIENT_METRICS (Windows).
# Usage: .\stress\lab\Run-ClientRamp.ps1 -OutDir stress\results\lab_ramp
param(
    [string]$OutDir = "stress\results\lab_ramp",
    [int[]]$Users = @(1, 5, 10, 20, 40),
    [int]$RequestsPerUser = 20,
    [int]$MaxInFlight = 32,
    [int]$TimeoutMs = 8000,
    [int]$PauseSec = 2,
    [string]$ClientId = ""
)

$Root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $Root
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$env:RM_NODES = "1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
if (-not $ClientId) {
    $ClientId = "ramp-$env:COMPUTERNAME-$PID"
}
$env:RM_CLIENT_ID = $ClientId
$env:RM_CLIENT_REQUESTS_PER_USER = "$RequestsPerUser"
$env:RM_CLIENT_MAX_IN_FLIGHT = "$MaxInFlight"
$env:RM_CLIENT_TIMEOUT_MS = "$TimeoutMs"

@"
started=$(Get-Date -Format o)
host=$env:COMPUTERNAME
RM_NODES=$($env:RM_NODES)
RM_CLIENT_ID=$ClientId
"@ | Set-Content -Path (Join-Path $OutDir "meta.txt")

foreach ($u in $Users) {
    $env:RM_CLIENT_USERS = "$u"
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $raw = Join-Path $OutDir "u${u}_${stamp}.log"
    Write-Host "--- users=$u -> $raw ---"
    cargo run --release -- client 2>&1 | Tee-Object -FilePath $raw
    Select-String -Path $raw -Pattern "^CLIENT_METRICS," |
        ForEach-Object { $_.Line } |
        Set-Content -Path (Join-Path $OutDir "u${u}_${stamp}.metrics")
    Start-Sleep -Seconds $PauseSec
}

Write-Host "Done. Parse with:"
Write-Host "  python stress/lab/parse_client_metrics.py $OutDir --out $OutDir/report.csv"
