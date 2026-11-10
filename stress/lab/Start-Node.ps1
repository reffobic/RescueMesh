# Start a cloud node on Windows PowerShell.
# Usage: .\stress\lab\Start-Node.ps1 -NodeId 1
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("1", "2", "3")]
    [string]$NodeId
)

$Root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $Root

$env:RM_NODES = "1@10.40.32.242:7001,2@10.40.2.29:7002,3@10.40.0.73:7003"
$env:RM_NODE_ID = $NodeId

Write-Host "Starting node id=$NodeId RM_NODES=$($env:RM_NODES)"
cargo run --release -- node
