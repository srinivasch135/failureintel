# Durable Failure-Event Ingestion Architecture

Status: Final design for implementation
Decision date: 2026-08-29
Target checkpoint: Durable raw capture with asynchronous, retryable normalization

## 1. Purpose

FailureIntel must not lose an accepted failure event because parsing, normalization, or persistence of the normalized representation fails.

This design separates raw-event capture from processing. The ingestion API durably stores the raw event first. A database-backed worker then parses and normalizes it in a separate transaction with bounded retries and crash recovery.

The core guarantee is:

> If the ingestion API returns `202 Accepted`, the raw failure event has committed to the database and remains available even if all later processing attempts fail.

The normalized record and the raw event's `NORMALIZED` status remain atomic:

> A raw event may be marked `NORMALIZED` only when its `normalized_failure_event` row commits in the same transaction.

## 2. Scope

This checkpoint includes:

- synchronous durable raw-event capture;
- database-backed asynchronous processing;
- safe work claiming across multiple application instances;
- parsing and normalization outside the capture transaction;
- retry classification, bounded backoff, and jitter;
- recovery of abandoned processing work;
- idempotent ingestion and normalized persistence;
- operational metrics and alerts;
- a backward-compatible deployment sequence.

This checkpoint does not require Kafka, RabbitMQ, an external workflow engine, or an administrative replay UI. Those may be added later if scale or operational requirements justify them.

## 3. Architectural decision

Use the `failure_event` table as the durable work queue and system of record for received data.

```text
Client
  |
  | POST /api/v1/failure-events
  v
Raw capture transaction
  |-- persist failure_event(RECEIVED)
  `-- commit
  |
  `----> 202 Accepted + eventId

Database-backed worker
  |-- claim eligible rows as PROCESSING
  |-- reconstruct RawFailureEvent
  |-- parse
  |-- normalize
  |
  |-- success transaction
  |     |-- upsert normalized_failure_event
  |     `-- set failure_event=NORMALIZED
  |
  |-- permanent data failure
  |     `-- set failure_event=FAILED
  |
  `-- temporary technical failure
        `-- separate transaction sets failure_event=RETRYABLE
```

Plain `@Async`, in-memory application events, and `@TransactionalEventListener(AFTER_COMMIT)` are not durable work queues. A process crash after raw commit could discard the work notification. The persisted status is therefore the authoritative work signal, and the worker scans the database.

## 4. Processing state model

The target `ProcessingStatus` values are:

| Status | Meaning | Terminal |
|---|---|---:|
| `RECEIVED` | Raw event committed and waiting for processing | No |
| `PROCESSING` | Claimed by a worker under a time-limited processing lease | No |
| `NORMALIZED` | Normalized row committed successfully | Yes |
| `RETRYABLE` | Previous attempt failed temporarily and is scheduled for retry | No |
| `FAILED` | Permanently unusable or automatic retry limit reached | Yes for automatic processing |

Allowed transitions:

```text
RECEIVED   -> PROCESSING
RETRYABLE  -> PROCESSING
PROCESSING -> NORMALIZED
PROCESSING -> RETRYABLE
PROCESSING -> FAILED
```

Recovery may perform:

```text
stale PROCESSING -> RETRYABLE
```

Manual replay, when implemented, may perform:

```text
FAILED -> RETRYABLE
```

No other transition is valid. Transition methods on `FailureEventEntity` should enforce these rules rather than allowing arbitrary status assignment throughout the application.

Existing `QUEUED` and `PROCESSED` enum values may remain during compatibility rollout. New code must not produce them, and they may be removed only after existing data and all deployed application versions no longer depend on them.

## 5. Authoritative data ownership

### 5.1 `failure_event`

The raw table owns:

- stable event ID;
- values exactly as received at the API boundary;
- source system;
- raw payload and source metadata;
- trace/idempotency identifier;
- received and original occurrence timestamps;
- processing status;
- attempt and scheduling metadata;
- sanitized failure classification and reason.

Parser-derived or canonical values must not overwrite raw values.

### 5.2 `normalized_failure_event`

The normalized table owns:

- canonical service, environment, event type, error type, severity, and timestamp;
- sanitized normalized error message;
- normalized payload and normalization metadata;
- normalization status and normalized timestamp;
- the same `event_id` as the raw event.

The normalized row has a one-to-one relationship with the raw row and uses `event_id` as both primary key and foreign key.

## 6. Stable event identity

Generate the event ID once when creating `RawFailureEvent` and preserve it through persistence, processing, responses, logs, and retries.

The raw mapper must use:

```java
entity.setEventId(rawFailureEvent.getRawEventId());
```

It must not generate a second UUID.

## 7. Ingestion API contract

### 7.1 Accepted request

The API performs only validation required to store the request safely:

- request body is syntactically valid JSON;
- request does not exceed the configured size limit;
- values do not violate storage/security limits;
- the raw payload can be represented by the persistence model.

Business completeness is evaluated during processing, not raw capture. A syntactically valid but semantically incomplete failure event should normally be retained and later marked `FAILED`.

The current DTO and database `NOT NULL` constraints must be reviewed together. The API cannot promise retention of incomplete events while the raw table rejects their missing fields.

### 7.2 Successful response

After the raw-capture transaction commits:

```http
HTTP/1.1 202 Accepted
```

```json
{
  "eventId": "ea9f71fa-10b5-43dd-8b96-159b4c681bf7",
  "status": "RECEIVED",
  "message": "Failure event accepted for processing"
}
```

`202` means durable acceptance, not completed normalization.

### 7.3 Capture failure

If raw persistence or its commit fails, the endpoint must return a server error and must not return `202`.

### 7.4 Invalid transport request

Invalid JSON returns `400`. An oversized body returns `413`. Such requests are outside the durable-event guarantee because no safe event representation was accepted.

## 8. Idempotent ingestion

`existsByTraceId()` followed by an insert is not concurrency-safe. Enforce idempotency at the database boundary.

After auditing and resolving existing duplicate values, add a partial unique index:

```sql
CREATE UNIQUE INDEX uq_failure_event_trace_id
    ON failure_event(trace_id)
    WHERE trace_id IS NOT NULL
      AND trace_id <> '';
```

Target behavior:

- same trace ID and equivalent payload: return the existing event ID;
- same trace ID and materially different payload: return `409 Conflict`;
- no trace ID: accept with a generated event ID, but exactly-once client retry behavior is not guaranteed.

A deterministic payload hash may be stored to distinguish an idempotent retry from conflicting reuse of the same trace ID.

The database unique index is the final concurrency guard. If two inserts race, handle the unique-constraint failure by loading the existing row and applying the behavior above.

## 9. Transaction boundaries and component ownership

Transaction boundaries must live on separate Spring beans so that proxy-based `@Transactional` behavior is applied. Do not call a transactional method through `this` and expect a new transaction.

### 9.1 `RawFailureEventCaptureService`

Transaction A:

1. resolve idempotency;
2. construct `RawFailureEvent`;
3. map exact raw values to `FailureEventEntity`;
4. save with `RECEIVED` and zero attempts;
5. commit and return the stable event ID.

No parser or normalizer is invoked here.

### 9.2 `FailureEventClaimService`

Transaction B:

1. lock a small eligible batch with `FOR UPDATE SKIP LOCKED`;
2. transition rows to `PROCESSING`;
3. set `processing_started_at` and `last_attempt_at`;
4. increment `attempt_count`;
5. clear `next_attempt_at`;
6. commit immediately and release row locks.

The transaction must not include parsing or normalization.

### 9.3 `FailureEventNormalizationProcessor`

Transaction C for one event:

1. load the claimed raw row;
2. confirm its status is `PROCESSING`;
3. reconstruct `RawFailureEvent`;
4. select and invoke the parser;
5. invoke the normalizer;
6. idempotently insert or update `normalized_failure_event`;
7. transition the raw row to `NORMALIZED`;
8. clear failure and lease metadata;
9. commit the normalized row and raw status together.

If an exception escapes, Transaction C rolls back.

### 9.4 `PermanentFailureRecorder`

For an expected permanent data failure, use a short transaction to transition the claimed row to `FAILED`, save a stable failure code and sanitized reason, and clear lease/retry metadata.

This may be a separate service or an explicit permanent-result path in the processor. It must not accidentally catch and commit after a database transaction has become rollback-only.

### 9.5 `RetryableFailureRecorder`

Transaction D, invoked only after the failed processing transaction has exited:

1. load the raw row;
2. retain its already-incremented attempt count;
3. if attempts are exhausted, transition to `FAILED`;
4. otherwise transition to `RETRYABLE`;
5. record a stable failure code and sanitized reason;
6. calculate `next_attempt_at`;
7. clear `processing_started_at`;
8. commit.

Use a separate Spring bean and `REQUIRES_NEW` where the caller may still have transaction context.

### 9.6 `AbandonedWorkRecoveryService`

An independent transaction finds expired `PROCESSING` leases and moves them to `RETRYABLE` or `FAILED` when the attempt limit has been reached.

## 10. Work claiming and concurrency

Eligible work is:

```text
processing_status = RECEIVED
OR
processing_status = RETRYABLE AND next_attempt_at <= now()
```

Use PostgreSQL row locking:

```sql
SELECT event_id
FROM failure_event
WHERE processing_status = 'RECEIVED'
   OR (
        processing_status = 'RETRYABLE'
        AND next_attempt_at <= now()
      )
ORDER BY ingested_at, event_id
FOR UPDATE SKIP LOCKED
LIMIT :batch_size;
```

Parenthesize the eligibility predicate when adding other conditions. The claim implementation then updates selected rows to `PROCESSING` in the same short transaction.

`SKIP LOCKED` permits multiple workers and application instances without assigning the same row concurrently.

Increment `attempt_count` when claiming, not when recording failure. A worker crash is still a real attempt and must count toward the retry ceiling.

Process each claimed event in its own normalization transaction. One bad event must not roll back an entire claimed batch.

## 11. Idempotent normalized persistence

The normalized table primary key is the raw `event_id`. Processing the same event again must update or confirm that row rather than create duplicates.

Required behavior:

- if no normalized row exists, insert it;
- if one exists for the same event, update it deterministically or treat an already-`NORMALIZED` event as complete;
- never create a second normalized identity;
- never mark the raw row `NORMALIZED` before normalized persistence succeeds.

The processor should lock or version-check the raw row and verify expected status to prevent stale workers from overwriting a newer terminal result.

## 12. Failure classification

Use stable failure codes in the database. Exception messages alone are unsuitable for control flow and reporting.

### 12.1 Permanent failures

Examples:

- `UNSUPPORTED_PAYLOAD`;
- `MALFORMED_EVENT`;
- `INSUFFICIENT_FAILURE_DATA`;
- `PAYLOAD_POLICY_VIOLATION`;
- `RETRY_EXHAUSTED`.

These transition to `FAILED` without automatic retry.

### 12.2 Retryable failures

Examples:

- `DATABASE_UNAVAILABLE`;
- `DATABASE_TIMEOUT`;
- `DEADLOCK_OR_LOCK_TIMEOUT`;
- `TEMPORARY_INFRASTRUCTURE_FAILURE`;
- `UNEXPECTED_PROCESSING_FAILURE`.

Unknown unexpected exceptions should initially be retryable so that an event is not discarded because of a transient defect. The bounded retry limit prevents infinite processing.

### 12.3 Stored versus logged error details

Store only a bounded, sanitized reason. Log the full exception and stack trace with `eventId`, `traceId`, attempt number, and failure code. Never persist credentials, secrets, or unbounded stack traces in `failure_reason`.

## 13. Retry policy

Initial defaults:

| Failed attempt | Base delay |
|---:|---:|
| 1 | 1 minute |
| 2 | 5 minutes |
| 3 | 30 minutes |
| 4 | 2 hours |
| 5 | 12 hours |

After the configured maximum attempt count, transition to `FAILED` with failure code `RETRY_EXHAUSTED` while preserving the most recent sanitized cause.

Add bounded random jitter to each base delay to avoid synchronized retry spikes. Make the maximum attempts, delays, worker interval, batch size, and lease timeout external configuration.

## 14. Crash and lease recovery

An event is abandoned when:

```text
processing_status = PROCESSING
AND processing_started_at < now() - processing_lease_timeout
```

Recovery transitions it to `RETRYABLE`, schedules the next attempt, clears `processing_started_at`, and records `WORKER_LEASE_EXPIRED`. If the already-incremented attempt count has reached the maximum, recovery transitions it to `FAILED`.

The lease timeout must be longer than the expected maximum time for one parse-and-normalize attempt. Recovery must be idempotent and safe to run on every application instance using row locking.

## 15. Database changes

Use additive Flyway migrations before changing runtime behavior.

Required or recommended raw-table columns:

```sql
ALTER TABLE failure_event
    ADD COLUMN IF NOT EXISTS source_system VARCHAR(255),
    ADD COLUMN IF NOT EXISTS source_metadata JSONB,
    ADD COLUMN IF NOT EXISTS payload_hash VARCHAR(64),
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS processing_started_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS failure_code VARCHAR(80);
```

Queue index:

```sql
CREATE INDEX IF NOT EXISTS idx_failure_event_processing_queue
    ON failure_event (
        processing_status,
        next_attempt_at,
        ingested_at
    );
```

Also add the trace-ID unique index described in the idempotency section after auditing duplicates.

Before finalizing the migration, reconcile the existing `server_name` column with the target `source_system` name. Do not maintain two authoritative columns for the same value.

Review whether `occurred_at`, `service_name`, `environment`, and `event_type` should remain `NOT NULL`. If semantically incomplete but valid JSON requests must be retained, those raw-table constraints and the request validation rules must be relaxed together. Canonical normalized fields can still apply stricter rules.

## 16. Entity behavior

`FailureEventEntity` should provide intent-revealing operations such as:

```java
claimForProcessing(now);
markNormalized();
markRetryable(code, reason, nextAttemptAt);
markFailed(code, reason);
recoverExpiredLease(code, reason, nextAttemptAt);
```

These methods should:

- validate the current status;
- apply only legal transitions;
- clear fields that no longer apply;
- bound failure-reason length;
- prevent negative or externally assigned attempt counts.

Direct public status mutation should be restricted where practical.

## 17. Worker orchestration

The scheduler coordinates work but does not own a long transaction:

```java
@Scheduled(fixedDelayString = "${failureintel.processing.worker-delay:2000}")
public void processBatch() {
    List<UUID> claimed = claimService.claimNextBatch(configuredBatchSize);

    for (UUID eventId : claimed) {
        try {
            processor.process(eventId);
        } catch (PermanentProcessingException exception) {
            permanentFailureRecorder.record(eventId, exception);
        } catch (Exception exception) {
            retryableFailureRecorder.record(eventId, exception);
        }
    }
}
```

Implementation may use a bounded executor, but concurrency must be explicitly limited relative to the database connection pool. Unbounded parallel processing is prohibited.

## 18. Read behavior

`GET /api/v1/failure-events/{eventId}` must return raw events even when no normalized row exists.

The response should expose processing information:

- `processingStatus`;
- `attemptCount`;
- `lastAttemptAt`;
- `nextAttemptAt` when retryable;
- stable `failureCode`;
- sanitized `failureReason`;
- normalized fields when present.

Search endpoints based only on `normalized_failure_event` will intentionally omit pending and failed raw events. Operational endpoints or filters must query `failure_event` when users need backlog and failure visibility.

## 19. Observability

At minimum publish:

- raw events accepted, rejected, and capture failures;
- processing attempts by result and failure code;
- transitions to `NORMALIZED`, `RETRYABLE`, and `FAILED`;
- current `RECEIVED`, `PROCESSING`, and eligible `RETRYABLE` counts;
- age of the oldest eligible event;
- normalization duration;
- retries exhausted;
- expired leases recovered.

Recommended alerts:

- oldest eligible event exceeds the processing service-level objective;
- backlog grows continuously;
- retry or permanent-failure rate exceeds its threshold;
- normalization success rate drops sharply;
- repeated lease expirations occur;
- raw capture returns database errors.

Logs for all processing paths must include `eventId`, `traceId` when present, attempt number, old status, new status, and failure code.

## 20. Security and retention

- Enforce an HTTP body-size limit and database payload-size policy.
- Sanitize secrets before logging or storing failure reasons.
- Define whether the raw payload may contain personal or confidential data.
- Apply access control to raw-event reads.
- Define retention and archival policies separately for raw and normalized data.
- Preserve enough raw information for replay without requiring the original producer.

## 21. Deployment plan

### Checkpoint 1: compatibility release

1. Audit duplicate nonblank trace IDs.
2. Add schema columns and indexes using additive migrations.
3. Add new enum values to every application instance.
4. Map and preserve source-system and raw metadata fields.
5. Add processing metrics and configuration.
6. Retain current synchronous behavior temporarily.
7. Deploy everywhere and verify schema/application compatibility.

### Checkpoint 2: durable processing release

1. Deploy raw-only capture and the database worker behind feature flags.
2. Initially disable worker claiming while all instances are updated.
3. Enable a single worker instance.
4. Verify raw acceptance, claiming, normalization, retries, and recovery.
5. Gradually enable additional worker instances.
6. Monitor backlog age, failure rate, database locks, and pool utilization.

Rollback must never delete queued rows. If worker processing is disabled or the application is rolled back, `RECEIVED` and `RETRYABLE` events remain durable for later processing.

## 22. Required integration tests

The checkpoint is not complete until automated tests prove:

1. `202` is returned only after raw persistence commits.
2. Raw-capture commit failure returns an error and does not claim acceptance.
3. Parsing is not invoked inside the raw-capture transaction.
4. Parser or permanent normalization failure retains raw data and ends in `FAILED`.
5. Normalized persistence failure leaves the raw row durable and schedules `RETRYABLE`.
6. Normalized persistence and the raw `NORMALIZED` transition commit or roll back together.
7. A later retry creates exactly one normalized row.
8. Two worker instances cannot claim the same event concurrently.
9. One failed event does not roll back other events in a claimed batch.
10. An expired `PROCESSING` lease becomes `RETRYABLE` or `FAILED` at the attempt limit.
11. Retry delays and maximum-attempt handling are deterministic under a fixed clock.
12. Concurrent identical trace IDs produce one raw event.
13. Conflicting reuse of a trace ID returns `409`.
14. Restarting the application does not lose `RECEIVED` or `RETRYABLE` work.
15. Read-by-ID works before normalization and after permanent failure.

The existing test that expects the raw row to roll back when normalized persistence fails must be replaced with the new durability expectation.

## 23. Implementation sequence

Build in small, independently testable changes:

1. schema and enum compatibility;
2. raw entity/mapping corrections and stable event ID;
3. raw capture service and updated API response;
4. legal state-transition methods;
5. database claim query and claim service;
6. single-event transactional normalization processor;
7. failure classifier and permanent/retryable recorders;
8. scheduled worker with bounded concurrency;
9. expired-lease recovery;
10. database-enforced idempotency;
11. read-model processing fields;
12. metrics, alerts, and deployment feature flags;
13. end-to-end and concurrency verification.

## 24. Definition of done

The architecture is successfully implemented when:

- every `202` response corresponds to a committed raw row;
- normalization failures cannot erase an accepted raw event;
- normalized data and `NORMALIZED` status are atomic;
- processing survives application restarts and worker crashes;
- duplicate work is safe and does not create duplicate normalized rows;
- retries are bounded, observable, and operationally controllable;
- pending and failed events remain queryable and replayable;
- rolling deployment and rollback preserve queued data.
