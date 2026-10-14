param(
    [string]$BaseUrl = 'https://failureintel.onrender.com',
    [ValidateSet('TASK15_CAPACITY_BASELINE', 'TASK15_CAPACITY_CANDIDATE', 'TASK15_WORKER_OFF')]
    [string]$WorkloadLabel = 'TASK15_CAPACITY_BASELINE'
)

$ErrorActionPreference = 'Stop'
$endpoint = "$($BaseUrl.TrimEnd('/'))/api/v1/failure-events"
$runId = [guid]::NewGuid().ToString('N')
$labelSlug = $WorkloadLabel.ToLowerInvariant().Replace('_', '-')
$serviceName = "failureintel-task15-6-$labelSlug-$runId"
$results = [System.Collections.Generic.List[object]]::new()

for ($i = 1; $i -le 5; $i++) {
    $requestTimestamp = [DateTimeOffset]::UtcNow.ToString('o')
    $identifier = "task15-6-$labelSlug-$runId-$i"
    $body = @{
        occurredAt     = $requestTimestamp
        serviceName    = $serviceName
        serverName     = 'task15-6-capacity-canary'
        environment    = 'production'
        eventType      = $WorkloadLabel
        errorType      = 'SyntheticCapacityCanary'
        errorMessage   = "Dedicated $WorkloadLabel synthetic event"
        traceId        = $identifier
        idempotencyKey = $identifier
        severityHint   = 'LOW'
        rawPayload     = @{
            serviceName = $serviceName
            environment = 'production'
            eventType   = $WorkloadLabel
            errorType   = 'SyntheticCapacityCanary'
            message     = "Dedicated $WorkloadLabel synthetic event"
            traceId     = $identifier
        }
    } | ConvertTo-Json -Depth 5 -Compress

    try {
        $response = Invoke-WebRequest -Method Post -Uri $endpoint -ContentType 'application/json' `
            -Headers @{ 'Idempotency-Key' = $identifier } -Body $body
        $statusCode = [int]$response.StatusCode
        $responseBody = $response.Content
    }
    catch {
        $statusCode = $null
        $responseBody = $null
        if ($_.Exception.Response) {
            $statusCode = [int]$_.Exception.Response.StatusCode
            $reader = [System.IO.StreamReader]::new($_.Exception.Response.GetResponseStream())
            try { $responseBody = $reader.ReadToEnd() } finally { $reader.Dispose() }
        }
        $results.Add([pscustomobject]@{
            requestNumber = $i
            requestTimestampUtc = $requestTimestamp
            workloadLabel = $WorkloadLabel
            serviceName = $serviceName
            traceId = $identifier
            idempotencyKey = $identifier
            httpStatus = $statusCode
            eventId = $null
            responseBody = $responseBody
            error = $_.Exception.Message
        })
        $results | ConvertTo-Json -Depth 6
        throw "Stopped after request $i failed; no automatic retries were made."
    }

    $accepted = $null
    try { $accepted = $responseBody | ConvertFrom-Json } catch { }
    $eventId = if ($accepted) { $accepted.eventId } else { $null }
    $results.Add([pscustomobject]@{
        requestNumber = $i
        requestTimestampUtc = $requestTimestamp
        workloadLabel = $WorkloadLabel
        serviceName = $serviceName
        traceId = $identifier
        idempotencyKey = $identifier
        httpStatus = $statusCode
        eventId = $eventId
        responseBody = $responseBody
    })

    if ($statusCode -ne 202 -or [string]::IsNullOrWhiteSpace([string]$eventId)) {
        $results | ConvertTo-Json -Depth 6
        throw "Unexpected response on request $i (HTTP $statusCode or missing eventId); stopped without retry."
    }
}

$results | ConvertTo-Json -Depth 6
