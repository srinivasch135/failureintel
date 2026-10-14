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
**Duration:** 120 seconds  
**Target rate:** 10 req/s  
**Worker concurrency:** 1  
**Worker batch size:** 1

### Performance results

| Metric | Observed |
|---|---:|
| Total HTTP requests | 1,201 |
| HTTP 202 responses | 1,201 |
| Achieved throughput | 10.008 req/s |
| HTTP average latency | 20.04 ms |
| HTTP median latency | 17.24 ms |
| HTTP p90 latency | 28.02 ms |
| HTTP p95 latency | 36.69 ms |
| HTTP maximum latency | 310.28 ms |
| Iteration p95 | 37.12 ms |
| Peak active VUs | 2 |
| Preallocated VUs | 20 |
| HTTP failures | 0 |
| Dropped iterations | 0 |
| Checks passed | 3,603 / 3,603 |

### Database reconciliation

| Metric | Final result |
|---|---:|
| Raw events | 1,201 |
| NORMALIZED events | 1,201 |
| Normalized rows | 1,201 |
| Pending events | 0 |
| Failed events | 0 |

**Result: PASS**

All 1,201 requests returned HTTP 202. The configured k6 thresholds passed with no dropped iterations or HTTP failures. Final database reconciliation confirmed 1,201 normalized events and zero pending or failed events.

**Evidence:** `results/raw-logs/rate10-001.txt`; PostgreSQL reconciliation for `rate10-001`.



## 8. Load Test — 20 req/s

**Run ID:** `rate20-001`  
**Duration:** 120 seconds  
**Target rate:** 20 req/s

### k6 Results

| Metric | Observed |
|---|---:|
| Total requests | 2,401 |
| Achieved rate | 20.003 req/s |
| HTTP 202 checks passed | 1 |
| Failed HTTP checks | 2,400 |
| Ingestion success rate | 0.04% |
| HTTP p95 | 12.06 ms |
| Dropped iterations | 0 |

### Database Verification

| Metric | Observed |
|---|---:|
| Raw events | 2,444 |
| Normalized events | 2,444 |
| Pending events | 0 |
| Failed events | 0 |
| Normalized rows | 2,444 |
| Unique normalized events | 2,444 |

**Result:** Database normalization PASS; HTTP acceptance FAIL in the recorded k6 execution. Overall result INCONCLUSIVE because the k6 request count does not reconcile with the run-specific database count.

**Evidence:** k6 console summary and PostgreSQL reconciliation for `rate20-001`.