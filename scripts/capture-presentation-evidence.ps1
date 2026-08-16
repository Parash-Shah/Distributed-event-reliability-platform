param(
    [string]$OutputDirectory = "evidence\presentation",
    [string]$BrowserPath = "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    [int]$AlertWaitSeconds = 40
)

$ErrorActionPreference = "Stop"
$composeArgs = @("compose", "--profile", "aws", "--profile", "monitoring")
$outputPath = (New-Item -ItemType Directory -Path $OutputDirectory -Force).FullName
$browserProfile = Join-Path $env:TEMP "event-platform-presentation-$PID"

function Invoke-DockerText {
    $dockerArguments = @($args)
    $output = & docker @composeArgs @dockerArguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: $($dockerArguments -join ' ')`n$($output -join "`n")"
    }
    return ($output -join "`n")
}

function Capture-Page {
    param([string]$Url, [string]$FileName, [int]$Width = 1920, [int]$Height = 1400)
    $destination = Join-Path $outputPath $FileName
    $browserError = Join-Path $env:TEMP "event-platform-edge-$PID.err"
    $arguments = @(
        "--headless=new", "--disable-gpu", "--hide-scrollbars",
        "--run-all-compositor-stages-before-draw", "--virtual-time-budget=12000",
        "--window-size=$Width,$Height", "--user-data-dir=`"$browserProfile`"",
        "--screenshot=`"$destination`"", $Url
    )
    Start-Process -FilePath $BrowserPath -ArgumentList $arguments -Wait -WindowStyle Hidden `
        -RedirectStandardError $browserError | Out-Null
    if (-not (Test-Path -LiteralPath $destination) -or (Get-Item -LiteralPath $destination).Length -lt 10000) {
        throw "Browser screenshot was not created correctly: $destination"
    }
}

function Write-EvidenceHtml {
    param([string]$Title, $Sections, [string]$FileName)
    $cards = foreach ($entry in $Sections.GetEnumerator()) {
        $heading = [Net.WebUtility]::HtmlEncode([string]$entry.Key)
        $content = [Net.WebUtility]::HtmlEncode([string]$entry.Value)
        "<section><h2>$heading</h2><pre>$content</pre></section>"
    }
    $html = @"
<!doctype html>
<html><head><meta charset="utf-8"><title>$Title</title><style>
body{margin:0;padding:28px;background:#0b0f14;color:#e6edf3;font:16px/1.45 Consolas,monospace}
h1{font:700 30px Segoe UI,sans-serif;margin:0 0 22px;color:#f0f6fc}
h2{font:600 19px Segoe UI,sans-serif;margin:0 0 12px;color:#58a6ff}
section{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:18px;margin:0 0 18px}
pre{margin:0;white-space:pre-wrap;overflow-wrap:anywhere;color:#c9d1d9}
.stamp{color:#8b949e;font:14px Segoe UI,sans-serif;margin-bottom:20px}
</style></head><body><h1>$Title</h1><div class="stamp">Captured $([DateTimeOffset]::Now.ToString("yyyy-MM-dd HH:mm:ss zzz")) from the live LocalStack stack</div>$($cards -join "`n")</body></html>
"@
    $html | Set-Content -LiteralPath (Join-Path $outputPath $FileName) -Encoding utf8
}

if (-not (Test-Path -LiteralPath $BrowserPath)) { throw "Browser not found: $BrowserPath" }
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    $dockerBin = "C:\Program Files\Docker\Docker\resources\bin"
    if (Test-Path -LiteralPath (Join-Path $dockerBin "docker.exe")) {
        $env:Path += ";$dockerBin"
    }
}
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "Docker is required." }

$queueUrl = "http://localhost:4566/queue/us-east-1/000000000000/event-platform-events"
$dlqUrl = "http://localhost:4566/queue/us-east-1/000000000000/event-platform-dlq"
$queueAttributes = Invoke-DockerText exec -T localstack awslocal sqs get-queue-attributes `
    --region us-east-1 --queue-url $queueUrl --attribute-names All
$dlqAttributes = Invoke-DockerText exec -T localstack awslocal sqs get-queue-attributes `
    --region us-east-1 --queue-url $dlqUrl --attribute-names All
$dlqMessages = Invoke-DockerText exec -T localstack awslocal sqs receive-message `
    --region us-east-1 --queue-url $dlqUrl --max-number-of-messages 10 `
    --visibility-timeout 0 --wait-time-seconds 1 --attribute-names All --message-attribute-names All

$queueAttributes | Set-Content -LiteralPath (Join-Path $outputPath "localstack-event-queue.json") -Encoding utf8
$dlqAttributes | Set-Content -LiteralPath (Join-Path $outputPath "localstack-dlq-attributes.json") -Encoding utf8
$dlqMessages | Set-Content -LiteralPath (Join-Path $outputPath "localstack-dlq-messages.json") -Encoding utf8

$workerLogs = Invoke-DockerText logs --no-color --since 20m event-worker-aws
$workerEvidence = @($workerLogs -split "`r?`n" | Where-Object {
    $_ -match "worker-recovery-|event_processing_interrupted|event_moved_to_dlq|event_silently_dropped" -or
    ($_ -match "monitoring-.*-transient" -and $_ -match "event_retry_scheduled|event_processed")
} | Select-Object -Last 24) -join "`n"
$reconciliationLogs = Invoke-DockerText logs --no-color --since 20m event-reconciliation-aws
$reconciliationEvidence = @($reconciliationLogs -split "`r?`n" | Where-Object {
    $_ -match "reconciliation_discrepancy|missing_count"
} | Select-Object -Last 3) -join "`n"
$workerEvidence | Set-Content -LiteralPath (Join-Path $outputPath "worker-recovery-and-failure.log") -Encoding utf8
$reconciliationEvidence | Set-Content -LiteralPath (Join-Path $outputPath "reconciliation-silent-loss.log") -Encoding utf8

$unprocessed = Invoke-RestMethod "http://127.0.0.1:8081/api/v1/events" -TimeoutSec 60 |
    Where-Object status -ne "PROCESSED"
$unprocessed | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath `
    (Join-Path $outputPath "unprocessed-event-records.json") -Encoding utf8

if ($AlertWaitSeconds -gt 0) { Start-Sleep -Seconds $AlertWaitSeconds }
$alertExpression = [Uri]::EscapeDataString('ALERTS{alertstate="firing"}')
$alerts = Invoke-RestMethod "http://127.0.0.1:9090/api/v1/query?query=$alertExpression" -TimeoutSec 30
$alerts | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $outputPath "prometheus-firing-alerts.json") -Encoding utf8

Write-EvidenceHtml "LocalStack Queue and DLQ Evidence" ([ordered]@{
    "Event queue attributes" = $queueAttributes
    "Dead-letter queue attributes" = $dlqAttributes
    "Dead-letter queue messages" = $dlqMessages
}) "localstack-evidence.html"
Write-EvidenceHtml "Worker Recovery and Reconciliation Evidence" ([ordered]@{
    "Worker termination, recovery, retries, and DLQ" = $workerEvidence
    "Reconciliation detecting silent loss" = $reconciliationEvidence
}) "reliability-evidence.html"

$grafanaUrl = "http://127.0.0.1:3000/d/event-platform-overview/distributed-event-platform-overview?kiosk&from=now-30m&to=now&refresh=5s"
Capture-Page $grafanaUrl "grafana-failure-dashboard.png" 1920 1400
Capture-Page "http://127.0.0.1:9090/alerts" "prometheus-alerts.png" 1920 1080
Capture-Page "http://127.0.0.1:9093" "alertmanager-alerts.png" 1920 1080
Capture-Page ([Uri]::new((Join-Path $outputPath "localstack-evidence.html")).AbsoluteUri) `
    "localstack-evidence.png" 1920 1600
Capture-Page ([Uri]::new((Join-Path $outputPath "reliability-evidence.html")).AbsoluteUri) `
    "reliability-evidence.png" 1920 1600

$locustRun = Get-ChildItem -LiteralPath "evidence\load-tests" -Directory |
    Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName "users-100.html") } |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $locustRun) { throw "No completed progressive Locust report directory was found." }
Capture-Page ([Uri]::new((Join-Path $locustRun.FullName "users-100.html")).AbsoluteUri) `
    "locust-100-users.png" 1920 1400
Capture-Page ([Uri]::new((Join-Path $locustRun.FullName "users-250.html")).AbsoluteUri) `
    "locust-250-users.png" 1920 1400

$manifest = @'
# Presentation evidence

Captured from the local AWS-compatible stack running through LocalStack. This is not evidence of an AWS deployment.

- `grafana-load-dashboard.png`: progressive benchmark dashboard captured before the failure campaign
- `grafana-failure-dashboard.png`: dashboard after transient, poison, silent-drop, recovery, and backlog scenarios
- `locust-100-users.png`: peak-throughput Locust report
- `locust-250-users.png`: saturation/tail-latency Locust report
- `prometheus-alerts.png` and `alertmanager-alerts.png`: active local alert evidence
- `localstack-evidence.png`: queue, DLQ depth, and DLQ message evidence
- `reliability-evidence.png`: worker recovery and reconciliation log evidence
- JSON and log files: raw machine-readable/readable sources for the screenshots
'@
$manifest | Set-Content -LiteralPath (Join-Path $outputPath "README.md") -Encoding utf8

Write-Host "Presentation evidence captured at $outputPath"
