# FailureIntel — Load Test Results

## 1. Test Environment

| Component | Configuration |
|---|---|
| Application | Spring Boot / Java 17 |
| Database | PostgreSQL 16 |
| Infrastructure | Docker Compose |
| Load generator | Grafana k6 |
| Worker concurrency | 1 |
| Worker batch size | 1 |
| Test environment | Dedicated local environment |

## 2. Performance Measurements

| Target rate | Duration | HTTP 202 | Achieved req/s | HTTP p95 | Errors | Dropped |
|---|---|---|---|---|---|---|
| 1 req/s | 30s | 31 | 1.031 | 135.21 ms | 0 | 0 |
| 5 req/s | 120s | 601 | 5.007 | 74.84 ms | 0 | 0 |
| 10 req/s | 120s | 1201 | 10.008 | 36.69 ms | 0 | 0 |

## 3. Data Integrity Verification

**Run ID:** `smoke-163-002`

| Verification | Observed |
|---|---:|
| Committed raw events | 31 |
| NORMALIZED events | 31 |
| Normalized rows | 31 |
| Unique normalized event IDs | 31 |
| Pending events | 0 |
| Failed events | 0 |

**Result: PASS**

All 31 HTTP-accepted events were durably persisted and normalized. Each has exactly one corresponding normalized row.

## 4. Measurement Scope

- HTTP p95 represents ingestion request-response latency.
- Database reconciliation verifies eventual normalization correctness.
- Final run-specific backlog reached zero.
- During-run backlog progression was not recorded.
- Results represent local Docker performance, not production capacity.

## 5. Evidence

- k6 output: `smoke-163-002.txt`
- PostgreSQL reconciliation: run ID `smoke-163-002`

## 6. Load Test — 5 req/s

**Run ID:** `rate5-001`  
**Execution:** 120 seconds at 5 iterations/s; 601 completed, 0 interrupted.  
**HTTP checks:** 1,803/1,803 passed; 601/601 HTTP 202 accepted.  
**HTTP latency:** average 40.93 ms; median 35.28 ms; p90 64.26 ms; p95 74.84 ms; maximum 376.09 ms.  
**Dropped iterations:** 0; **HTTP failures:** 0.

| Reconciliation metric | Observed |
|---|---:|
| Committed raw events | 601 |
| NORMALIZED events | 601 |
| Normalized rows | 601 |
| Pending events | 0 |
| Failed events | 0 |

**Result: Ingestion and observed reconciliation checks PASS.** All 601 accepted events were persisted and marked NORMALIZED, with 601 normalized rows and no pending or failed events.

**Evidence:** k6 console summary and run-specific SQL reconciliation for `rate5-001`.

## 7. Load Test — 10 req/s

**Run ID:** `rate10-001`  
**Execution:** 120 seconds at 10 iterations/s; 1,201 completed, 0 interrupted.  
**HTTP checks:** 3,603/3,603 passed; 1,201/1,201 HTTP 202 accepted.  
**HTTP latency:** average 20.04 ms; median 17.24 ms; p90 28.02 ms; p95 36.69 ms; maximum 310.28 ms.  
**Dropped iterations:** 0; **HTTP failures:** 0.

| Reconciliation metric | Observed |
|---|---:|
| Committed raw events | 1,201 |
| NORMALIZED events | 1,201 (completion reported) |
| Normalized rows | 936 at last shared SQL sample |
| Unique normalized event IDs | 936 at last shared SQL sample |
| Pending events | 0 (completion reported) |
| Failed events | 0 at last shared SQL sample |

**Result: Ingestion PASS; backlog drainage reported complete.** The k6 run accepted all 1,201 requests without HTTP errors or dropped iterations. Run-specific SQL observations showed normalized events increasing from 731 to 936 while pending events fell from 470 to 265, with zero failed events in the samples. Full drainage was subsequently reported; a final all-1,201-row SQL reconciliation output was not provided.

**Evidence:** k6 console summary and run-specific PostgreSQL backlog observations for `rate10-001`.
