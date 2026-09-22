# Phase 1 Normalization Correctness Matrix

This document is the executable correctness contract for the synchronous ingestion pipeline before Kafka is introduced.

## Status contract

| Classification | `failure_event.processing_status` | Normalized row | `normalization_status` |
| --- | --- | --- | --- |
| Fully canonical | `NORMALIZED` | Exactly one | `FULLY_NORMALIZED` |
| Usable with a fallback or unknown controlled value | `NORMALIZED` | Exactly one | `PARTIALLY_NORMALIZED` |
| Unsupported, unparseable, or without minimum useful data | `FAILED` | None | Not applicable |

## Event matrix

| Area | Cases covered | Required result |
| --- | --- | --- |
| Parser support | Null, null payload, empty payload | Unsupported; direct parse returns a documented malformed result |
| Payload aliases | Nested and case-insensitive aliases for every parsed field | All fields extracted without modifying the payload |
| Field precedence | Direct value and payload alias both present | Direct value wins |
| Timestamp parsing | Valid and invalid ISO-8601 values | Valid value extracted; invalid value becomes absent and is handled by normalization |
| Environment | Production, development, staging, QA aliases | Canonical `prod`, `dev`, `staging`, or `qa` |
| Event type | Exception, timeout, dependency, validation aliases | Canonical event type |
| Error type | PostgreSQL and timeout aliases | Canonical exception type |
| Open error taxonomy | Unknown nonblank exception type | Original value preserved; does not make the record partial |
| Severity | Critical, high, medium, and low aliases | Canonical severity |
| Missing required canonical value | Service, environment, error type, error message, severity, or occurrence time | Documented fallback, metadata flag, and partial status |
| Unknown controlled value | Environment, event type, or severity | Documented value, metadata flag, and partial status |
| Sensitive message | `password`, `token`, `apiKey`, and `secret` | Value replaced with `****`; sanitization metadata recorded |
| Fully normalized persistence | Complete usable request | One raw row and one normalized row sharing the event ID |
| Partially normalized persistence | Unknown environment/event type | One raw row and one normalized row with partial status and metadata |
| Empty payload | Empty JSON object | One failed raw row with reason; no normalized row |
| No useful failure data | Non-empty payload without minimum useful fields | One failed raw row with reason; no normalized row |
| Parser failure | Parser throws | One failed raw row with reason; no normalized write attempted |
| Trace correlation | Distinct events share a nonblank trace ID | Both raw events are preserved; trace ID is not an idempotency key |
| Legacy trace retry | Equivalent no-key retry matches an existing `trace:<traceId>` row | Existing legacy event is returned; conflicting content is persisted as a new event |
| HTTP validation | Missing/null/blank required fields | HTTP 400 and no database write |
| Persistence-safe envelope | Bounded fields exceed raw-table limits | HTTP 400 and no database write |
| Request size | Request exceeds configured transport limit | HTTP 413 and no database write |
| Transaction rollback | Normalized repository write fails after raw save | Exception propagates and raw insert is rolled back |

## Exit gate

Phase 1 is complete when every matrix test passes, the entire Maven suite passes repeatedly, and no test leaves duplicate, orphaned, or partially committed rows.

## Verification result

Verified on 2026-08-25 with two consecutive complete Maven test runs.

| Tests | Failures | Errors | Skipped | Result |
| ---: | ---: | ---: | ---: | --- |
| 98 | 0 | 0 | 0 | Passed twice |

The database-backed tests use isolated PostgreSQL Testcontainers, so verification does not read from or modify the developer database.
