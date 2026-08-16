param(
    [string]$UserLevels = "20,50,100,250",
    [int]$StageSeconds = 30,
    [int]$RampSeconds = 5,
    [int]$WorkerReplicas = 4,
    [int]$DrainTimeoutSeconds = 900,
    [switch]$SkipBuild,
    [switch]$PreserveState
)

$ErrorActionPreference = "Stop"
$apiBaseUrl = "http://127.0.0.1:8081"
$prometheusBaseUrl = "http://127.0.0.1:9090"
$composeArgs = @("compose", "--profile", "aws", "--profile", "monitoring", "--profile", "load")
$benchmarkStartedAt = [DateTimeOffset]::UtcNow
$runId = $benchmarkStartedAt.ToString("yyyyMMdd-HHmmss")
$evidenceDirectory = Join-Path (Get-Location) "evidence\load-tests\$runId"
New-Item -ItemType Directory -Path $evidenceDirectory -Force | Out-Null

function Invoke-Compose {
    $dockerArguments = @($args)
    & docker @composeArgs @dockerArguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed: $($dockerArguments -join ' ')"
    }
}

function Reset-LocalStackState {
    Write-Host "Resetting the benchmark queues and DynamoDB table..."
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

function Wait-Until {
    param([scriptblock]$Condition, [string]$Description, [int]$TimeoutSeconds)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            if (& $Condition) { return $true }
        } catch {
            # Metrics may be briefly unavailable while containers or scrape targets settle.
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    Write-Warning "Timed out waiting for $Description"
    return $false
}

function Invoke-PrometheusQuery {
    param([string]$Expression)
    $encoded = [Uri]::EscapeDataString($Expression)
    return Invoke-RestMethod -Uri "$prometheusBaseUrl/api/v1/query?query=$encoded" -TimeoutSec 15
}

function Get-PrometheusScalar {
    param([string]$Expression)
    $response = Invoke-PrometheusQuery $Expression
    if (@($response.data.result).Count -eq 0) { return 0.0 }
    return [double]$response.data.result[0].value[1]
}

function Get-HistogramSnapshot {
    param([string]$Expression)
    $response = Invoke-PrometheusQuery $Expression
    $snapshot = @{}
    foreach ($series in @($response.data.result)) {
        $snapshot[[string]$series.metric.le] = [double]$series.value[1]
    }
    return $snapshot
}

function Get-HistogramQuantile {
    param([double]$Quantile, [hashtable]$Before, [hashtable]$After)
    $buckets = foreach ($key in $After.Keys) {
        $beforeValue = if ($Before.ContainsKey($key)) { [double]$Before[$key] } else { 0.0 }
        $upperBound = if ($key -eq "+Inf") { [double]::PositiveInfinity } else { [double]::Parse($key, [Globalization.CultureInfo]::InvariantCulture) }
        [pscustomobject]@{ Upper = $upperBound; Count = [math]::Max(0.0, ([double]$After[$key] - $beforeValue)) }
    }
    $buckets = @($buckets | Sort-Object Upper)
    if ($buckets.Count -eq 0) { return $null }
    $total = $buckets[-1].Count
    if ($total -le 0) { return $null }

    $rank = $Quantile * $total
    $previousUpper = 0.0
    $previousCount = 0.0
    foreach ($bucket in $buckets) {
        if ($bucket.Count -ge $rank) {
            if ([double]::IsPositiveInfinity($bucket.Upper)) { return $previousUpper }
            $bucketCount = $bucket.Count - $previousCount
            if ($bucketCount -le 0) { return $bucket.Upper }
            return $previousUpper + (($bucket.Upper - $previousUpper) * (($rank - $previousCount) / $bucketCount))
        }
        $previousUpper = $bucket.Upper
        $previousCount = $bucket.Count
    }
    return $null
}

function Convert-ToMilliseconds {
    param($Seconds)
    if ($null -eq $Seconds) { return $null }
    return [math]::Round(([double]$Seconds * 1000.0), 3)
}

function Get-NearestRankPercentile {
    param([double[]]$Values, [double]$Quantile)
    if ($Values.Count -eq 0) { return $null }
    $sorted = @($Values | Sort-Object)
    $index = [math]::Max(0, [math]::Ceiling($Quantile * $sorted.Count) - 1)
    return [math]::Round([double]$sorted[$index], 3)
}

function Get-StageProcessingLatencies {
    param([DateTimeOffset]$StartedAt, [DateTimeOffset]$EndedAt)
    $lowerBound = $StartedAt.AddSeconds(-1)
    $upperBound = $EndedAt.AddSeconds(1)
    $events = Invoke-RestMethod "$apiBaseUrl/api/v1/events" -TimeoutSec 180
    $latencies = @(
        $events | Where-Object {
            $_.status -eq "PROCESSED" -and $_.processed_at -and
            ([DateTimeOffset]::Parse($_.received_at) -ge $lowerBound) -and
            ([DateTimeOffset]::Parse($_.received_at) -le $upperBound)
        } | ForEach-Object {
            ([DateTimeOffset]::Parse($_.processed_at) - [DateTimeOffset]::Parse($_.received_at)).TotalMilliseconds
        }
    )
    return [pscustomobject]@{
        Count = $latencies.Count
        P50 = Get-NearestRankPercentile $latencies 0.50
        P95 = Get-NearestRankPercentile $latencies 0.95
        P99 = Get-NearestRankPercentile $latencies 0.99
    }
}

function Get-MaxQueueDepth {
    param([DateTimeOffset]$StartedAt)
    $windowSeconds = [math]::Max(10, [math]::Ceiling(([DateTimeOffset]::UtcNow - $StartedAt).TotalSeconds) + 5)
    return [int][math]::Round((Get-PrometheusScalar "max(max_over_time(events_queue_depth[${windowSeconds}s]))"))
}

function Get-QueueDepth {
    $metric = Invoke-RestMethod "$apiBaseUrl/actuator/metrics/events.queue.depth" -TimeoutSec 15
    return [int](($metric.measurements | Where-Object statistic -eq "VALUE").value)
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker is required to run the progressive benchmark."
}
$userCounts = @($UserLevels.Split(",") | ForEach-Object {
    $value = 0
    if (-not [int]::TryParse($_.Trim(), [ref]$value) -or $value -lt 1 -or $value -gt 10000) {
        throw "UserLevels must be a comma-separated list of integers from 1 through 10000."
    }
    $value
})
if ($userCounts.Count -eq 0) {
    throw "UserLevels must contain at least one user count."
}
if ($StageSeconds -lt 10 -or $RampSeconds -lt 1 -or $WorkerReplicas -lt 1) {
    throw "StageSeconds must be at least 10; RampSeconds and WorkerReplicas must be positive."
}

$services = @(
    "localstack", "event-platform-aws", "event-worker-aws", "event-reconciliation-aws",
    "alertmanager", "prometheus", "grafana"
)
if ($SkipBuild) {
    Invoke-Compose up -d --scale "event-worker-aws=$WorkerReplicas" @services
} else {
    Invoke-Compose up -d --build --scale "event-worker-aws=$WorkerReplicas" @services
}

if (-not $PreserveState) {
    Invoke-Compose stop event-worker-aws event-reconciliation-aws
    Reset-LocalStackState
    Invoke-Compose up -d --no-deps --scale "event-worker-aws=$WorkerReplicas" `
        event-worker-aws event-reconciliation-aws
}

if (-not (Wait-Until { (Invoke-RestMethod "$apiBaseUrl/actuator/health" -TimeoutSec 10).status -eq "UP" } `
        "the API to become healthy" 120)) {
    throw "The API did not become healthy."
}
if (-not (Wait-Until { (Get-PrometheusScalar 'sum(up{job="event-workers"})') -ge $WorkerReplicas } `
        "$WorkerReplicas Prometheus worker targets" 120)) {
    throw "Prometheus did not discover all worker replicas."
}

$results = @()
$latestEvidencePath = Join-Path (Get-Location) "evidence\load-tests\latest-progressive-benchmark.json"
$receivedExpression = "sum(events_received_total)"
$duplicateExpression = "sum(events_duplicates_total)"
$processedExpression = "sum(events_processed_total)"
foreach ($userCount in $userCounts) {
    Write-Host "Starting $userCount-user stage for $StageSeconds seconds..."
    $stageName = "users-$userCount"
    $csvPrefix = "/mnt/evidence/$runId/$stageName"
    $spawnRate = [math]::Max(1, [math]::Ceiling($userCount / $RampSeconds))
    $stageStartedAt = [DateTimeOffset]::UtcNow
    $receivedBefore = Get-PrometheusScalar $receivedExpression
    $duplicatesBefore = Get-PrometheusScalar $duplicateExpression
    $processedBefore = Get-PrometheusScalar $processedExpression

    Invoke-Compose run --rm --no-deps locust `
        -f /mnt/locust/locustfile.py `
        --host http://event-platform-aws:8080 `
        --headless --users $userCount --spawn-rate $spawnRate `
        --run-time "${StageSeconds}s" --stop-timeout 5 `
        --csv $csvPrefix --csv-full-history `
        --html "$csvPrefix.html" `
        --only-summary --exit-code-on-error 0

    $htmlPath = Join-Path $evidenceDirectory "${stageName}.html"
    $templateLine = Get-Content -LiteralPath $htmlPath | Where-Object { $_ -match 'window\.templateArgs = ' } | Select-Object -First 1
    if (-not $templateLine) { throw "Locust final statistics are missing from $htmlPath" }
    $templateJson = $templateLine.Substring($templateLine.IndexOf("{")).TrimEnd(";", " ")
    $template = $templateJson | ConvertFrom-Json
    $aggregate = $template.requests_statistics | Where-Object name -eq "Aggregated" | Select-Object -First 1
    $aggregatePercentiles = $template.response_time_statistics | Where-Object name -eq "Aggregated" | Select-Object -First 1
    if (-not $aggregate -or -not $aggregatePercentiles) { throw "Locust aggregate statistics are missing from $htmlPath" }

    $loadStartedAt = [DateTimeOffset]::Parse($template.start_time, [Globalization.CultureInfo]::InvariantCulture)
    $loadEndedAt = [DateTimeOffset]::Parse($template.end_time, [Globalization.CultureInfo]::InvariantCulture)
    $drained = Wait-Until { (Get-QueueDepth) -eq 0 } "$userCount-user backlog to drain" $DrainTimeoutSeconds
    $drainSeconds = [math]::Max(0.0, [math]::Round(([DateTimeOffset]::UtcNow - $loadEndedAt).TotalSeconds, 3))
    Start-Sleep -Seconds 7
    $receivedAfter = Get-PrometheusScalar $receivedExpression
    $duplicatesAfter = Get-PrometheusScalar $duplicateExpression
    $processedAfter = Get-PrometheusScalar $processedExpression
    $acceptedEvents = [int][math]::Round($receivedAfter - $receivedBefore)
    $duplicateEvents = [int][math]::Round($duplicatesAfter - $duplicatesBefore)
    $processedEvents = [int][math]::Round($processedAfter - $processedBefore)

    $processingLatencies = Get-StageProcessingLatencies $loadStartedAt $loadEndedAt
    $maxQueueDepth = Get-MaxQueueDepth $stageStartedAt

    $requestCount = [int]$aggregate.num_requests
    $failureCount = [int]$aggregate.num_failures
    $result = [ordered]@{
        users = $userCount
        worker_replicas = $WorkerReplicas
        configured_duration_seconds = $StageSeconds
        total_requests = $requestCount
        accepted_events = $acceptedEvents
        processed_events = $processedEvents
        all_accepted_processed = ($processedEvents -ge $acceptedEvents)
        accepted_events_per_minute = [math]::Round(($acceptedEvents * 60.0 / $StageSeconds), 2)
        duplicate_count = $duplicateEvents
        request_failures = $failureCount
        request_failure_rate_percent = if ($requestCount -eq 0) { 0.0 } else { [math]::Round(($failureCount * 100.0 / $requestCount), 4) }
        api_latency_ms = [ordered]@{
            p50 = [double]$aggregatePercentiles.'0.5'
            p95 = [double]$aggregatePercentiles.'0.95'
            p99 = [double]$aggregatePercentiles.'0.99'
        }
        processing_latency_ms = [ordered]@{
            p50 = $processingLatencies.P50
            p95 = $processingLatencies.P95
            p99 = $processingLatencies.P99
        }
        processing_latency_source = "persisted received_at-to-processed_at nearest-rank percentile"
        processing_latency_sample_count = $processingLatencies.Count
        all_accepted_have_processing_latency = ($processingLatencies.Count -eq $acceptedEvents)
        maximum_queue_backlog = $maxQueueDepth
        backlog_drained = $drained
        backlog_drain_seconds = $drainSeconds
        locust_html_report = "$stageName.html"
    }
    $results += [pscustomobject]$result

    $evidence = [ordered]@{
        started_at_utc = $benchmarkStartedAt.ToString("O")
        completed_at_utc = [DateTimeOffset]::UtcNow.ToString("O")
        host = "LocalStack on Docker Desktop"
        api = $apiBaseUrl
        locust_version = "2.40.4"
        stage_seconds = $StageSeconds
        ramp_seconds = $RampSeconds
        worker_replicas = $WorkerReplicas
        isolated_state_reset = -not $PreserveState
        results = $results
    }
    $evidence | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $latestEvidencePath -Encoding utf8
    $evidence | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $evidenceDirectory "benchmark.json") -Encoding utf8

    Write-Host ("PASS: {0} users; {1} accepted events/min; failures {2}%; max backlog {3}; drain {4}s" -f `
        $userCount, $result.accepted_events_per_minute, $result.request_failure_rate_percent, `
        $result.maximum_queue_backlog, $result.backlog_drain_seconds)
}

Write-Host "Progressive benchmark completed."
Write-Host "Evidence: $latestEvidencePath"
