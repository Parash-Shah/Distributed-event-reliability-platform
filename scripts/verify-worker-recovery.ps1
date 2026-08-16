param(
    [string]$ApiBaseUrl = "http://localhost:8081",
    [int]$TimeoutSeconds = 60,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$composeArgs = @("compose", "--profile", "aws")

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
            # The API or event may not be ready yet.
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    throw "Timed out waiting for $Description"
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker is required to run the LocalStack recovery demonstration."
}

if ($SkipBuild) {
    Invoke-Compose up -d localstack event-platform-aws
} else {
    Invoke-Compose up -d --build localstack event-platform-aws
}
Wait-Until { (Invoke-RestMethod "$ApiBaseUrl/actuator/health").status -eq "UP" } "the API"

if ($SkipBuild) {
    Invoke-Compose up -d event-worker-aws
} else {
    Invoke-Compose up -d --build event-worker-aws
}
$eventId = "worker-recovery-$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())"
$body = @{
    event_id = $eventId
    event_type = "SLOW_PROCESSING"
    payload = @{ processingDelayMs = 30000 }
} | ConvertTo-Json -Depth 3

Invoke-RestMethod -Method Post -Uri "$ApiBaseUrl/api/v1/events" -ContentType "application/json" -Body $body | Out-Null
Wait-Until {
    (Invoke-RestMethod "$ApiBaseUrl/api/v1/events/$eventId").status -eq "PROCESSING"
} "worker $eventId to acquire its processing lease"

Write-Host "Worker acquired the lease. Killing it before acknowledgement..."
Invoke-Compose kill event-worker-aws
Invoke-Compose up -d --no-deps event-worker-aws

$recovered = $null
Wait-Until {
    $script:recovered = Invoke-RestMethod "$ApiBaseUrl/api/v1/events/$eventId"
    $script:recovered.status -eq "PROCESSED"
} "SQS visibility and processing leases to expire"

if ($recovered.attempts -lt 2) {
    throw "Expected a redelivery attempt, but the stored attempt is $($recovered.attempts)."
}

$workerLogs = (& docker @composeArgs logs --no-color event-worker-aws) -join "`n"
$processedCount = ([regex]::Matches($workerLogs, "event_processed event_id=$([regex]::Escape($eventId))")).Count
if ($processedCount -ne 1) {
    throw "Expected exactly one completed side effect log, found $processedCount."
}

Write-Host "PASS: $eventId recovered on attempt $($recovered.attempts) with exactly one completed side effect."
Write-Host "Use 'docker compose --profile aws logs event-worker-aws' to inspect the recovery timeline."
