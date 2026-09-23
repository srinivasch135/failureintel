# Read Path Inventory

## Scope

This document records the repository read methods currently available for failure events and where they are used. It covers:

- `FailureEventRepository`
- `NormalizedFailureEventRepository`
- production callers under `src/main`
- integration-test reads under `src/test`

The first read endpoints are now available:

- `GET /api/v1/failure-events/{eventId}`
- `GET /api/v1/failure-events/by-trace-id/{traceId}`
- `GET /api/v1/failure-events/search`

This inventory remains the baseline for deciding where future read operations belong.

## Repository ownership

`FailureEventRepository` owns ingestion data: the received values, raw payload, trace ID, ingestion time, and processing state.

`NormalizedFailureEventRepository` owns the canonical values produced by normalization, including normalized service, environment, event type, error type, severity, and normalization status.

A read that needs data from both records should be coordinated by an application service. Repository selection does not belong in the controller.

## Production usage

Repository reads currently support idempotency checks during ingestion, direct lookup by event ID, lookup by trace ID, processing claims, and normalized search.

| Caller | Method | Use | Status |
| --- | --- | --- | --- |
| `FailureEventIngestionService` | `FailureEventRepository.findByIdempotencyKey(...)` | Resolve explicit idempotency retries and legacy trace-based retries | Correct repository |
| `FailureEventQueryService` | `FailureEventRepository.findById(...)` | Load the ingestion record for an event | Correct repository |
| `FailureEventQueryService` | `FailureEventRepository.findFirstByTraceIdOrderByIngestedAtDescEventIdDesc(...)` | Load the latest ingestion record for a trace | Correct repository |
| `FailureEventQueryService` | `NormalizedFailureEventRepository.findById(...)` | Load normalized details when available | Correct repository |
| `FailureEventQueryService` | `NormalizedFailureEventRepository.searchByFailureDetails(...)` | Search by normalized service, environment, and severity | Correct repository |
| `FailureEventClaimService` | `FailureEventRepository.lockNextEligibleForProcessing(...)` | Lock a batch of received or due retryable events for processing | Correct repository |

Methods marked with `None` in the repository tables do not have production callers yet.

## FailureEventRepository

| Method | Use | Caller | Status |
| --- | --- | --- | --- |
| `findById(...)` | Load an ingested event by ID | `FailureEventQueryService`, integration tests | Keep |
| `findFirstByTraceIdOrderByIngestedAtDescEventIdDesc(...)` | Load the latest ingested event by trace ID | `FailureEventQueryService` | Keep |
| `findByIdempotencyKey(...)` | Resolve explicit idempotency retries and legacy trace-based retries | `FailureEventIngestionService` | Keep |
| `lockNextEligibleForProcessing(...)` | Lock received or due retryable events for a processing attempt | `FailureEventClaimService` | Keep |
| `existsByTraceId(...)` | Check whether any event has a trace ID | None | Review; it is not used for ingestion deduplication |
| `findByProcessingStatus(...)` | Find events in a processing state | Tests only | Review |
| `fetchNextBatchForProcessing(...)` | Load events in one processing state | None | Review; claims use `lockNextEligibleForProcessing(...)` |
| `findRecentSimilarErrors(...)` | Match service, environment, and error type | None | Review |
| `findByOccurredAtBetween(...)` | Find ingested events in a time range | None | Review |

`updateProcessingStatus(...)` is not included because it is a write operation.

### `findRecentSimilarErrors(...)`

This method is still on the raw repository but filters on service, environment, and error type. Its final location depends on the intended meaning of “similar”:

- exact values received during ingestion: keep it on `FailureEventRepository`;
- canonical values produced by normalization: move it to `NormalizedFailureEventRepository`.

For user-facing search or analysis, canonical matching is the expected behavior. The method has no caller, so it can remain unchanged until that use case is implemented.

## NormalizedFailureEventRepository

| Method | Use | Caller | Status |
| --- | --- | --- | --- |
| `findById(...)` | Load the normalized record by event ID | `FailureEventQueryService`, integration tests | Keep |
| `findByNormalizationStatusOrderByNormalizedAtDesc(...)` | Filter by normalization status | None | Keep |
| `findByServiceNameAndEnvironmentWithinOccurredAtRange(...)` | Filter by normalized service, environment, and occurrence time | None | Keep |
| `findByEventTypeWithinOccurredAtRange(...)` | Filter by normalized event type and occurrence time | None | Keep |
| `searchByFailureDetails(...)` | Search by normalized service, environment, and severity | `FailureEventQueryService` | Keep |

## Test usage

The ingestion integration tests load both records by event ID. They verify that ingestion writes the raw event and, when normalization succeeds, the corresponding normalized event. These are persistence checks and should continue to use the repositories directly.

## Findings

- Legacy trace-based compatibility lookup uses the failure-event repository; new
  events are not deduplicated solely by trace ID. Equivalent retries can resolve
  to an existing legacy row, while distinct events sharing a trace ID are kept.
- Explicit idempotency keys are checked through `findByIdempotencyKey(...)`;
  matching content resolves to the existing event and conflicting content is
  rejected.
- `existsByTraceId(...)` remains in the repository interface but has no
  production caller.
- The normalized search methods are on the correct repository.
- The first GET endpoint reads through `FailureEventQueryService`; the controller does not select repositories.
- `findRecentSimilarErrors(...)` is the only read method that needs an ownership decision.
- Pagination and sorting refinements remain future work.

## Keeping this document current

Use the following searches when repository methods or callers change:

```powershell
rg -n --glob '*.java' "FailureEventRepository|NormalizedFailureEventRepository" src
rg -n --glob '*.java' "failureEventRepository\.|normalizedFailureEventRepository\." src
```

Update the relevant row when a read method is added, removed, or starts being used.

## Current read flow

`GET /api/v1/failure-events/{eventId}` loads the ingestion record by event ID. `GET /api/v1/failure-events/by-trace-id/{traceId}` loads the ingestion record by trace ID.

After the ingestion record is found, the query service also attempts to load the normalized record with the same event ID. The response mapper prefers a normalized value when one is available and falls back to the received value when it is not.

`GET /api/v1/failure-events/search` searches the normalized record first. It supports optional `serviceName`, `environment`, and `severity` query parameters. Matching events are mapped through the same response mapper used by the direct lookup routes.

If the ingestion record does not exist, the endpoint returns `404 Not Found` and does not query the normalized repository.

## Next step

The `eventId`, `traceId`, and normalized search routes are covered by unit tests and a database-backed controller integration test.

Pagination and sorting can be refined later as separate use cases.
