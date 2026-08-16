param(
    [string]$ApiBaseUrl = "http://127.0.0.1:8081",
    [int]$BacklogSize = 50,
    [int]$TimeoutSeconds = 180,
    [switch]$SkipBuild,
    [switch]$PreserveState
)

$ErrorActionPreference = "Stop"
$composeArgs = @("compose", "--profile", "aws", "--profile", "monitoring")
$campaignStartedAt = [DateTimeOffset]::UtcNow

function Invoke-Compose {
    $dockerArguments = @($args)
    & docker @composeArgs @dockerArguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: $($dockerArguments -join ' ')"
    }
}

function Wait-Until {
    param([scriptblock]$Condition, [string]$Description)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            if (& $Condition) { return }
        } catch {
            # The target may be temporarily unavailable while containers restart.
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for $Description"
}

function Get-QueueDepth {
    $metric = Invoke-RestMethod "$ApiBaseUrl/actuator/metrics/events.queue.depth"
    return [int](($metric.measurements | Where-Object statistic -eq "VALUE").value)
}

function Submit-Event {
    param([string]$EventId, [string]$EventType, [hashtable]$Payload = @{})
    $body = @{
        event_id = $EventId
        event_type = $EventType
        payload = $Payload
    } | ConvertTo-Json -Depth 4
    Invoke-RestMethod -Method Post -Uri "$ApiBaseUrl/api/v1/events" `
        -ContentType "application/json" -Body $body | Out-Null
}

function Get-Event {
    param([string]$EventId)
    return Invoke-RestMethod "$ApiBaseUrl/api/v1/events/$EventId"
}

function Resolve-JavaHome {
    if ($env:JAVA_HOME -and (Test-Path -LiteralPath (Join-Path $env:JAVA_HOME "bin\java.exe"))) {
        return $env:JAVA_HOME
    }
    $installedJdk = Get-ChildItem -Path (Join-Path $env:USERPROFILE ".jdks") -Directory -ErrorAction SilentlyContinue |
        Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName "bin\java.exe") } |
        Sort-Object Name -Descending |
        Select-Object -First 1
    if ($installedJdk) {
        return $installedJdk.FullName
    }
    throw "JAVA_HOME is not set and no IntelliJ-managed JDK was found under $env:USERPROFILE\.jdks."
}

function Reset-LocalStackState {
    Write-Host "Resetting the campaign queues and DynamoDB table..."
    Invoke-Compose exec -T localstack awslocal sqs purge-queue --region us-east-1 `
        --queue-url http://localhost:4566/queue/us-east-1/000000000000/event-platform-events
    Invoke-Compose exec -T localstack awslocal sqs purge-queue --region us-east-1 `
        --queue-url http://localhost:4566/queue/us-east-1/000000000000/event-platform-dlq
    & docker @composeArgs exec -T localstack sh -c `
        "awslocal dynamodb describe-table --region us-east-1 --table-name event-platform-events >/dev/null 2>&1"
    if ($LASTEXITCODE -eq 0) {
        Invoke-Compose exec -T localstack awslocal dynamodb delete-table --region us-east-1 `
            --table-name event-platform-events
        Invoke-Compose exec -T localstack awslocal dynamodb wait table-not-exists --region us-east-1 `
            --table-name event-platform-events
    }
    Invoke-Compose exec -T localstack awslocal dynamodb create-table --region us-east-1 `
        --table-name event-platform-events `
        --attribute-definitions "AttributeName=event_id,AttributeType=S" `
        --key-schema "AttributeName=event_id,KeyType=HASH" `
        --billing-mode PAY_PER_REQUEST
    Invoke-Compose exec -T localstack awslocal dynamodb wait table-exists --region us-east-1 `
        --table-name event-platform-events
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker is required to run the LocalStack failure campaign."
}
if ($BacklogSize -lt 1) {
    throw "BacklogSize must be at least one."
}

$services = @(
    "localstack", "event-platform-aws", "event-worker-aws", "event-reconciliation-aws",
    "alertmanager", "prometheus", "grafana"
)
if ($SkipBuild) {
    Invoke-Compose up -d @services
} else {
    Invoke-Compose up -d --build @services
}
Wait-Until { (Invoke-RestMethod "$ApiBaseUrl/actuator/health").status -eq "UP" } "the API"

Invoke-Compose stop event-worker-aws event-reconciliation-aws
if (-not $PreserveState) {
    Reset-LocalStackState
}
Invoke-Compose up -d --no-deps event-reconciliation-aws

Write-Host "[1/4] Running LocalStack adapter failure tests with the external worker stopped..."
$env:JAVA_HOME = Resolve-JavaHome
$env:RUN_LOCALSTACK_TESTS = "true"
$env:AWS_ACCESS_KEY_ID = "test"
$env:AWS_SECRET_ACCESS_KEY = "test"
$env:AWS_REGION = "us-east-1"
& .\mvnw.cmd test "-Dtest=AwsLocalStackIntegrationTest"
if ($LASTEXITCODE -ne 0) {
    throw "The LocalStack adapter failure tests failed."
}
Invoke-Compose up -d --no-deps event-worker-aws

Write-Host "[2/4] Demonstrating worker-container termination and lease recovery..."
& .\scripts\verify-worker-recovery.ps1 -ApiBaseUrl $ApiBaseUrl -TimeoutSeconds $TimeoutSeconds -SkipBuild
if ($LASTEXITCODE -ne 0) {
    throw "The worker recovery demonstration failed."
}

Write-Host "[3/4] Building and draining a queue backlog..."
Invoke-Compose stop event-worker-aws
$backlogPrefix = "backlog-$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())"
for ($index = 1; $index -le $BacklogSize; $index++) {
    $eventId = "$backlogPrefix-$index"
    Submit-Event -EventId $eventId -EventType "ORDER_CREATED" `
        -Payload @{ campaign = $backlogPrefix; sequence = $index }
}

Wait-Until { (Get-QueueDepth) -ge $BacklogSize } "the queue backlog to become visible"
$maximumBacklog = Get-QueueDepth
$drainStartedAt = Get-Date
Invoke-Compose up -d --no-deps event-worker-aws
Wait-Until {
    $events = Invoke-RestMethod "$ApiBaseUrl/api/v1/events"
    @($events | Where-Object { $_.event_id -like "$backlogPrefix-*" -and $_.status -eq "PROCESSED" }).Count `
        -eq $BacklogSize
} "all backlog events to reach PROCESSED"
$drainSeconds = [math]::Round(((Get-Date) - $drainStartedAt).TotalSeconds, 3)

Write-Host "[4/4] Seeding fresh monitoring traffic and alert states..."
$monitoringPrefix = "monitoring-$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())"
$transientEventId = "$monitoringPrefix-transient"
$poisonEventId = "$monitoringPrefix-poison"
$silentEventId = "$monitoringPrefix-silent"
Submit-Event -EventId $transientEventId -EventType "TRANSIENT_FAILURE" `
    -Payload @{ failUntilAttempt = 2; campaign = $monitoringPrefix }
Submit-Event -EventId $poisonEventId -EventType "POISON" -Payload @{ campaign = $monitoringPrefix }
Submit-Event -EventId $silentEventId -EventType "SILENT_DROP" -Payload @{ campaign = $monitoringPrefix }
Wait-Until { (Get-Event $transientEventId).status -eq "PROCESSED" } "the monitoring transient event to recover"
Wait-Until { (Get-Event $poisonEventId).status -eq "FAILED" -and (Get-Event $poisonEventId).attempts -ge 4 } `
    "the monitoring poison event to exhaust its retries"
Wait-Until { (Get-Event $silentEventId).status -eq "PROCESSING" } "the monitoring silent-drop event to be acknowledged"

$evidenceDirectory = Join-Path (Get-Location) "evidence\localstack"
New-Item -ItemType Directory -Path $evidenceDirectory -Force | Out-Null
$evidencePath = Join-Path $evidenceDirectory "latest-failure-campaign.json"
$evidence = [ordered]@{
    started_at_utc = $campaignStartedAt.ToString("O")
    completed_at_utc = [DateTimeOffset]::UtcNow.ToString("O")
    localstack_adapter_tests = "passed"
    isolated_state_reset = -not $PreserveState
    worker_termination_recovery = "passed"
    backlog = [ordered]@{
        submitted_events = $BacklogSize
        maximum_queue_depth = $maximumBacklog
        processed_events = $BacklogSize
        drain_seconds = $drainSeconds
    }
    monitoring = [ordered]@{
        grafana = "http://127.0.0.1:3000/d/event-platform-overview/distributed-event-platform-overview"
        prometheus = "http://127.0.0.1:9090/query"
        alertmanager = "http://127.0.0.1:9093"
        seeded_event_ids = [ordered]@{
            transient = $transientEventId
            poison = $poisonEventId
            silent_drop = $silentEventId
        }
    }
}
$evidence | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $evidencePath -Encoding utf8

Write-Host "PASS: LocalStack failure campaign completed."
Write-Host "Backlog: $maximumBacklog events; drain time: $drainSeconds seconds."
Write-Host "Evidence: $evidencePath"
