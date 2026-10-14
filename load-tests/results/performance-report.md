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

## 6. Checkpoint 16.4 — 5 req/s

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
| Unique normalized event IDs | Not confirmed (pasted output truncated) |

**Assessment: Ingestion PASS; final run verification pending.** The accepted HTTP count matches persisted raw events, and all 601 raw events are NORMALIZED with 601 normalized rows. The backlog is drained after the run. Confirm the distinct normalized event-ID count and document any during-run backlog samples before marking the full run PASS. During-run samples were not provided. The 601 requests versus the nominal 600 target are not treated as an error.

**Evidence:** k6 console summary and run-specific SQL reconciliation supplied for `rate5-001`; during-run backlog observation not supplied.
