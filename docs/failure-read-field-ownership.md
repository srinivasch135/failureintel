# Failure Read Field Ownership

This map defines which record should supply each field when failure reads are added. It is intentionally limited to fields that already exist in the current entities.

## Default rule

- Use `failure_event` for ingestion and processing details.
- Use `normalized_failure_event` for searchable failure details.
- Use `event_id` to connect the two records.

## First read contract

| API field | Source | Repository | Notes |
| --- | --- | --- | --- |
| `eventId` | `failure_event.event_id` | `FailureEventRepository` | Shared identifier for both records |
| `traceId` | Normalized value, then received value | Both repositories | Prefer the normalized trace ID; fall back to the received trace ID |
| `ingestedAt` | `failure_event.ingested_at` | `FailureEventRepository` | Set during ingestion |
| `processingStatus` | `failure_event.processing_status` | `FailureEventRepository` | State of the ingestion pipeline |
| `serviceName` | Normalized value, then received value | Both repositories | Prefer the canonical value; fall back to the value received during ingestion |
| `environment` | Normalized value, then received value | Both repositories | Prefer the canonical value; fall back to the value received during ingestion |
| `eventType` | Normalized value, then received value | Both repositories | Prefer the canonical value; fall back to the value received during ingestion |
| `errorType` | Normalized value, then received value | Both repositories | Prefer the canonical value; fall back to the value received during ingestion |
| `message` | Normalized value, then received value | Both repositories | Prefer the normalized message; fall back to the received message |
| `dependencyTarget` | Normalized value, then received value | Both repositories | Prefer the canonical dependency; fall back to the received dependency |
| `severity` | Normalized value, then severity hint | Both repositories | Prefer normalized severity; fall back to the received severity hint |
| `occurredAt` | Normalized value, then received value | Both repositories | Prefer normalized occurrence time; fall back to the received occurrence time |

## Filter ownership

| Filter | Query through |
| --- | --- |
| `eventId` | `FailureEventRepository` for the raw record, then load normalized data by the same ID when needed |
| `traceId` | `FailureEventRepository` |
| `processingStatus` | `FailureEventRepository` |
| `ingestedFrom`, `ingestedTo` | `FailureEventRepository` |
| `serviceName` | `NormalizedFailureEventRepository` |
| `environment` | `NormalizedFailureEventRepository` |
| `eventType` | `NormalizedFailureEventRepository` |
| `errorType` | `NormalizedFailureEventRepository` |
| `severity` | `NormalizedFailureEventRepository` |
| `normalizationStatus` | `NormalizedFailureEventRepository` |
| `occurredFrom`, `occurredTo` | `NormalizedFailureEventRepository` |

## Naming decisions

The public fields `serviceName`, `environment`, `eventType`, `errorType`, `message`, `dependencyTarget`, `severity`, and `occurredAt` prefer normalized values. When a normalized record or field is unavailable, the response uses the corresponding received value.

If received values are exposed later, name them explicitly: `rawServiceName`, `rawEnvironment`, `rawEventType`, `rawErrorType`, `rawMessage`, `severityHint`, and `rawOccurredAt`. This avoids returning two meanings under the same field name.

The response prefers the normalized trace ID when one is available. Lookup and duplicate detection still use the trace ID stored on the ingestion record.

## Missing normalized record

An ingested event may not have a normalized record when parsing fails or normalization has not completed. In that case:

- the event can still be returned by `eventId`;
- ingestion fields remain available;
- failure details fall back to the values received during ingestion;
- the event is not returned by normalized-only filters.

## Processing and normalization status contract

The two statuses answer different questions:

- `failure_event.processing_status` describes whether pipeline processing produced a usable normalized record.
- `normalized_failure_event.normalization_status` describes the quality of that normalized record.

| Outcome | Processing status | Normalized row | Normalization status |
| --- | --- | --- | --- |
| All required values are canonical | `NORMALIZED` | Present | `FULLY_NORMALIZED` |
| A usable record needs defaults or contains an unknown controlled-taxonomy value | `NORMALIZED` | Present | `PARTIALLY_NORMALIZED` |
| The payload is unsupported, empty, unparseable, or contains no minimum useful failure data | `FAILED` | Absent | Not applicable |

A failed event must preserve the raw payload and populate `failure_reason`. It must not be published as a normalized event.

Unknown environment, event-type, and severity values make a record partially normalized. An unknown but nonblank error type is preserved and does not by itself make the record partial because error types are an open taxonomy.

## Initial scope

The current implementation supports:

- direct lookup by `eventId`;
- direct lookup by `traceId`;
- normalized search by `serviceName`, `environment`, and `severity`.

The next read use cases can be added independently:

- pagination and sorting by `occurredAt`.

Add the remaining filters only when their use cases are introduced.
