# FailureIntel ingestion pipeline load test runbook

**Purpose:** Measure the traffic the ingestion and normalization pipeline handles on a local machine, identify the first observed bottleneck, and account for every accepted event. The rates below are test inputs, not capacity claims. A small production smoke check is not a load-test baseline.

## Frozen boundaries

- Run only against a dedicated local PostgreSQL database. Do not point this workload at Render, Neon production, or the normal integration-test database.
- Use the existing FailureIntel Java 17 Spring Boot ingestion API, Flyway V1–V14 schema, database-backed worker, and metrics. Use the official `grafana/k6:1.8.1` Docker image as the load generator. No new application components, schema changes, broker, or monitoring stack.
- Keep `FAILURE_EVENT_WORKER_ENABLED=true`, `FAILURE_EVENT_WORKER_BATCH_SIZE=1`, and `FAILURE_EVENT_WORKER_CONCURRENCY=1` for all comparable runs. Record other effective settings, including the worker's default two-second fixed delay and five-minute processing lease, and keep them constant.
- Keep the same application commit, database, machine, payload shape, and resource allocation across runs. Start each run only after the previous run has drained and been reconciled.

## Local environment setup

The isolated ingestion API and PostgreSQL database are defined in `load-tests/compose.yml`, using the repository Dockerfile, a dedicated Compose network, and the persistent `failureintel-loadtest-postgres-storage` volume. PostgreSQL is reachable by the API on the private Compose network and is not published to the host. The API is published only on `127.0.0.1:8082`. Keep the generated database credential in the ignored `load-tests/.env.failureintel-loadtest` file. On first setup, generate the credential once and retain the file and database volume for this environment:

```powershell
if (-not (Test-Path 'load-tests/.env.failureintel-loadtest')) {
  [byte[]]$secretBytes = New-Object byte[] 48
  $generator = [Security.Cryptography.RandomNumberGenerator]::Create()
  $generator.GetBytes($secretBytes)
  $generator.Dispose()
  $secret = [Convert]::ToBase64String($secretBytes)
  Set-Content -LiteralPath 'load-tests/.env.failureintel-loadtest' -Value "FAILUREINTEL_LOADTEST_DB_PASSWORD=$secret" -Encoding ascii -NoNewline
  $acl = New-Object System.Security.AccessControl.FileSecurity
  $acl.SetAccessRuleProtection($true, $false)
  $rule = New-Object System.Security.AccessControl.FileSystemAccessRule([Security.Principal.WindowsIdentity]::GetCurrent().User, 'FullControl', 'Allow')
  $acl.AddAccessRule($rule)
  Set-Acl -LiteralPath 'load-tests/.env.failureintel-loadtest' -AclObject $acl
  Remove-Variable secret, secretBytes
}
docker compose --env-file load-tests/.env.failureintel-loadtest -f load-tests/compose.yml config --quiet
docker compose --env-file load-tests/.env.failureintel-loadtest -f load-tests/compose.yml up -d --build --remove-orphans
docker compose --env-file load-tests/.env.failureintel-loadtest -f load-tests/compose.yml exec ingestion-api printenv SPRING_DATASOURCE_URL FAILURE_EVENT_WORKER_ENABLED FAILURE_EVENT_WORKER_BATCH_SIZE FAILURE_EVENT_WORKER_CONCURRENCY
docker compose --env-file load-tests/.env.failureintel-loadtest -f load-tests/compose.yml exec postgres-database psql -U failureintel_loadtest -d failureintel_loadtest -c "SELECT version, success FROM flyway_schema_history ORDER BY installed_rank DESC LIMIT 1;"
curl.exe -i http://127.0.0.1:8082/actuator/health
```

The Compose file reads the secret from `FAILUREINTEL_LOADTEST_DB_PASSWORD` during configuration and sets the database name, role, JDBC URL, and required single-worker settings explicitly. The API container runs unprivileged with a read-only root filesystem and bounded temporary/log storage. The root `docker-compose.yml` remains the separate development stack. `--remove-orphans` replaces old containers from earlier Compose service names while preserving the explicitly named database volume. To stop the test environment while retaining its database, run `docker compose --env-file load-tests/.env.failureintel-loadtest -f load-tests/compose.yml down`. Do not use `down --volumes` or remove the volume while retaining this test environment.

## Checkpoints

Each checkpoint has one output. Complete it once, then reuse that output in later checkpoints.

### 16.1 — Fix the local test boundary

Choose the dedicated local PostgreSQL database `failureintel_loadtest` and confirm the effective JDBC URL is `jdbc:postgresql://postgres-database:5432/failureintel_loadtest`. Confirm the running API reports `FAILURE_EVENT_WORKER_ENABLED=true`, `FAILURE_EVENT_WORKER_BATCH_SIZE=1`, and `FAILURE_EVENT_WORKER_CONCURRENCY=1`. Query `flyway_schema_history` and confirm version `14` has `success=true`; then request `/actuator/health` and confirm HTTP 200 with `UP`. The API defaults to port **8081**; the root Compose file publishes container port 8081 on `${SERVER_PORT:-8080}`. This dedicated setup binds the API to `127.0.0.1:8082` to avoid the active development service. Record the actual URL, application commit, Java/PostgreSQL/k6 versions, machine CPU/memory, and worker settings **once** as the test environment record.

**Exit:** The isolated target, schema, health, port, and frozen settings are verified. Later runs refer to this record; repeat setup only if the environment changes, in which case results form a new series.

### 16.2 — Build one reusable ingestion workload

Implement `load-tests/ingestion.js` with one `constant-arrival-rate` scenario and configurable `FAILUREINTEL_LOADTEST_RATE`, `FAILUREINTEL_LOADTEST_DURATION`, `FAILUREINTEL_LOADTEST_BASE_URL`, and `FAILUREINTEL_LOADTEST_RUN_ID` inputs. Start with 20 preallocated VUs and a 100 VU ceiling. Send one POST per iteration to `/api/v1/failure-events` with `Content-Type: application/json` and a unique `Idempotency-Key` such as `k6-${__ENV.FAILUREINTEL_LOADTEST_RUN_ID}-${__VU}-${__ITER}`. Use the current DTO fields: `occurredAt`, `serviceName`, `serverName`, `environment`, `eventType`, `errorType`, `errorMessage`, `severityHint`, and nonempty `rawPayload`. Vary a harmless value per event. Check for HTTP 202 plus an `eventId` and `status: RECEIVED`; count accepted responses, failures, latency, achieved rate, and dropped iterations.

**Exit:** One script matches the current API without changing it. The same script and payload shape serve every run.

### 16.3 — Smoke and prove the measurement path

Run **1 req/s for 30 seconds** with a unique `FAILUREINTEL_LOADTEST_RUN_ID`. Save the k6 summary. Apply the run procedure below, including the SQL reconciliation, and wait for the backlog to drain. Confirm the HTTP 202 count equals committed raw rows for this run, and every accepted valid event reaches one normalized row with the same event ID. Correct script or environment faults here before measuring higher rates.

**Exit:** The request, response, raw persistence, worker outcome, and measurement procedure agree. This is the frozen smoke run; no extra trial is needed.

### 16.4 — Measure the remaining rates

Run **5, 10, 20, then 40 req/s**, each for **2 minutes**, in that order. For each rate, change only `FAILUREINTEL_LOADTEST_RATE` and `FAILUREINTEL_LOADTEST_RUN_ID`, follow the same run procedure, and fill one results row. Stop escalation if k6 drops iterations, valid requests receive unexpected responses, database reconciliation fails, backlog cannot drain, or the ingestion API becomes unstable. Record the reason; leave later levels unrun.

**Exit:** Every safely reached level has a k6 summary, database reconciliation, backlog observation, and result. There is no separate verification pass over all runs.

### 16.5 — State the measured boundary

From the completed rows, report the highest demonstrated sustainable rate, or the highest tested rate if no boundary appeared. Identify the first bottleneck only from observed HTTP, worker, backlog, database, or resource evidence. Record limitations, including that this local result does not establish Render/Neon production capacity.

**Exit:** The results table and conclusion below are filled. This runbook ends without an optimization or architecture change unless a later task authorizes one.

## One procedure for every run

1. Select a fresh `FAILUREINTEL_LOADTEST_RUN_ID` for this invocation, including reruns. Record the run's start time and confirm the prior run has drained. Keep the environment record from 16.1 rather than re-collecting it.
2. Start k6 with the chosen rate and duration. Record k6 HTTP 202 count, achieved requests/s, p95, errors, and dropped iterations. Sample the run-specific status counts during the run to see whether backlog grows.
3. Immediately after k6, record the same status counts. Wait for drainage, then run the SQL below for that `FAILUREINTEL_LOADTEST_RUN_ID`. Save the terminal summary and counts with the results row.
4. Compare HTTP 202 count with committed raw-row count. Confirm `NORMALIZED` raw rows each have one matching normalized row and that other statuses have none. Investigate differences, `FAILED` rows, and claims still `PROCESSING` after the configured lease and recovery window. Mark the row, then decide whether the stop rule permits the next rate.

For a run, backlog is its raw rows in `RECEIVED`, `PROCESSING`, or `RETRYABLE`; `FAILED` is terminal, not backlog. A 202 proves durable raw capture, not immediate normalization. After drainage, all valid synthetic accepted events should be `NORMALIZED` with one matching normalized row.

## Run commands and observation

From PowerShell in the repository root, verify the pinned image before testing:

```powershell
docker pull grafana/k6:1.8.1@sha256:23f2279054c3e01535455c4d92914a625df12aeb631ea0e3ea7a43f96bcbc843
docker run --rm grafana/k6:1.8.1@sha256:23f2279054c3e01535455c4d92914a625df12aeb631ea0e3ea7a43f96bcbc843 version
curl.exe -i http://127.0.0.1:8082/actuator/health
```

After checkpoint 16.2, set a new workload run ID and the verified API URL for each run:

```powershell
docker run --rm `
  -v "${PWD}/load-tests/ingestion.js:/scripts/ingestion.js:ro" `
  -e FAILUREINTEL_LOADTEST_BASE_URL=http://host.docker.internal:8082 `
  -e FAILUREINTEL_LOADTEST_RUN_ID=ingestion-smoke-20261012-001 `
  -e FAILUREINTEL_LOADTEST_RATE=1 `
  -e FAILUREINTEL_LOADTEST_DURATION=30s `
  grafana/k6:1.8.1@sha256:23f2279054c3e01535455c4d92914a625df12aeb631ea0e3ea7a43f96bcbc843 run /scripts/ingestion.js
```

The SQL below uses the **stored** key prefix `key:k6-...`; the ingestion service adds `key:` to explicit header keys. Use the current `FAILUREINTEL_LOADTEST_RUN_ID` in the query prefix. Run these reads against the dedicated local database with `docker exec -it failureintel-db-loadtest psql -U failureintel_loadtest -d failureintel_loadtest`. Keep the run ID specific enough that its prefix cannot match an earlier run.

```sql
SELECT processing_status, COUNT(*)
FROM failure_event
WHERE idempotency_key LIKE 'key:k6-ingestion-smoke-20261012-001-%'
GROUP BY processing_status
ORDER BY processing_status;

SELECT COUNT(*) AS raw_rows,
       COUNT(n.event_id) AS normalized_rows,
       COUNT(*) FILTER (WHERE f.processing_status = 'NORMALIZED') AS raw_normalized,
       COUNT(*) FILTER (WHERE f.processing_status = 'FAILED') AS raw_failed
FROM failure_event f
LEFT JOIN normalized_failure_event n ON n.event_id = f.event_id
WHERE f.idempotency_key LIKE 'key:k6-ingestion-smoke-20261012-001-%';

SELECT f.event_id, f.processing_status, f.attempt_count,
       f.processing_started_at, f.next_attempt_at, f.failure_code,
       (n.event_id IS NOT NULL) AS has_normalized_row
FROM failure_event f
LEFT JOIN normalized_failure_event n ON n.event_id = f.event_id
WHERE f.idempotency_key LIKE 'key:k6-ingestion-smoke-20261012-001-%'
  AND (f.processing_status <> 'NORMALIZED' OR n.event_id IS NULL)
ORDER BY f.ingested_at, f.event_id;
```

Use count changes over recorded time intervals to calculate normalized events/s and backlog growth. The normalized table's primary key prevents duplicate event IDs; the join also checks whether each raw outcome has the expected normalized row.

## Results

| Run | Target RPS | Achieved RPS | HTTP 202 | p95 | Dropped | Normalized/s | Backlog trend | Retry/failed | Result |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| Smoke | 1 | — | — | — | — | — | — | — | Pending |
| Baseline | 5 | — | — | — | — | — | — | — | Pending |
| Moderate | 10 | — | — | — | — | — | — | — | Pending |
| Higher | 20 | — | — | — | — | — | — | — | Pending |
| Stress | 40 | — | — | — | — | — | — | — | Pending |

**Test environment record (captured once on 2026-10-12):**

- Application commit: `c732f4a5a90eaf019fb35c940434adfa03d017c9`; Compose-built image `failureintel-api:local-loadtest` (`sha256:5d4388caa115fbe5068c87f6addca278c9c2dafe3dcf2fa44e0fd2689e17ace1`).
- Effective JDBC URL: `jdbc:postgresql://postgres-database:5432/failureintel_loadtest` on the dedicated Compose network. PostgreSQL was `16.14` from the `postgres:16-alpine` tag, persisted in volume `failureintel-loadtest-postgres-storage` and not published to a host port. The tag is not digest-pinned; recheck and update the recorded PostgreSQL version if the image is pulled again. Database role: `failureintel_loadtest`.
- Runtime: Eclipse Temurin Java `17.0.20.1`; Flyway validated 14 migrations and confirmed schema version V14 (`success=true`) at `2026-10-12 22:17 UTC`. `GET /actuator/health` returned HTTP `200` with `status: UP` and `db: UP` at `2026-10-12 22:17 UTC` after restarting from the corrected Compose configuration.
- Ingestion API endpoint: `http://127.0.0.1:8082` (`127.0.0.1:8082` mapped to container port `8081`; the development service occupies host port 8081). The API container runs as UID `10001` with a read-only root filesystem, no Linux capabilities, `no-new-privileges`, bounded container logs, and ephemeral tmpfs mounts for `/tmp` and the configured Logback path.
- Load generator: official `grafana/k6:1.8.1`, k6 `v1.8.1` (`sha256:23f2279054c3e01535455c4d92914a625df12aeb631ea0e3ea7a43f96bcbc843`).
- Machine: Dell G3 3579; Intel Core i5-8300H 2.30 GHz, 4 cores / 8 logical processors; 15.86 GiB physical memory. Docker Desktop Linux engine allocation: 8 CPUs and 7.69 GiB memory.
- Worker: `FAILURE_EVENT_WORKER_ENABLED=true`, `FAILURE_EVENT_WORKER_BATCH_SIZE=1`, `FAILURE_EVENT_WORKER_CONCURRENCY=1`; effective fixed delay `2s`, recovery scan interval `30s`, processing lease `5m`, shutdown await `30s`, retry maximum `5`, delays `PT1M,PT5M,PT30M,PT2H`, jitter fraction `0.2`.

Compose project: `failureintel-loadtest`; services: `ingestion-api` and `postgres-database`; containers: `failureintel-api-loadtest` and `failureintel-db-loadtest`; network: `failureintel-loadtest-network`; persistent volume: `failureintel-loadtest-postgres-storage`. The API is published on `127.0.0.1:8082`; PostgreSQL has no host-published port. Keep this environment running for the series; if its commit, target, worker settings, or resource allocation changes, create a new environment record and series.

The Compose correction reused the existing database volume. Its existing `failureintel_loadtest` role password was aligned with the retained ignored env-file credential through local PostgreSQL access; the database was not reinitialized. The same environment record was updated in place after confirming the effective JDBC URL and worker variables, Flyway V14, PostgreSQL 16.14, and HTTP 200 `UP` health on the restarted service.

**Highest demonstrated sustainable rate:** Pending.

**First observed bottleneck and evidence:** Pending.

**Limitations:** Pending.

The load-test assessment is complete when the script runs through Docker, the smoke and baseline plus every safely reachable higher level are measured, each executed run is reconciled to database state, and the sustainable rate or tested limit and first observed bottleneck are documented. If a stop condition prevents a later level, record the reason and leave that level unrun.
