# Start a lab client on Windows PowerShell.
# Usage: .\stress\lab\Start-Client.ps1 -ClientId pc4 -Users 10 -RequestsPerUser 10
param(
    [string]$ClientId = "pc-win",
    [int]$Users = 1,
    [int]$RequestsPerUser = 1,
    [int]$MaxInFlight = 32,
    [int]$TimeoutMs = 5000
)

$Root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $Root

$env:RM_NODES = "1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
$env:RM_CLIENT_ID = $ClientId
$env:RM_CLIENT_USERS = "$Users"
$env:RM_CLIENT_REQUESTS_PER_USER = "$RequestsPerUser"
$env:RM_CLIENT_MAX_IN_FLIGHT = "$MaxInFlight"
$env:RM_CLIENT_TIMEOUT_MS = "$TimeoutMs"

Write-Host "Client id=$ClientId users=$Users req/user=$RequestsPerUser"
cargo run --release -- client
