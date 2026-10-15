# FailureIntel — Load Test Results

## Test Environment

| Component | Configuration |
|---|---|
| Application | Spring Boot / Java 17 |
| Database | PostgreSQL 16 |
| Infrastructure | Dedicated local Docker Compose environment |
| Load generator | Grafana k6 |
| Worker configuration | Specified separately for each test |

**Worker configuration notation:** **C** = worker concurrency (number of worker execution slots); **B** = worker batch size (maximum events claimed per batch). For example, **C=4, B=6** means concurrency 4 and batch size 6. These are worker settings, not k6 virtual users.

## Results Summary

| Test | C | B | Duration | HTTP 202 | Achieved req/s | HTTP p95 | HTTP failures | Dropped | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Smoke — 1 req/s | 1 | 1 | 30s | 31 | 1.031 | 135.21 ms | 0 | 0 | PASS |
| 5 req/s | 1 | 1 | 120s | 601 | 5.007 | 74.84 ms | 0 | 0 | PASS |
| 10 req/s | 1 | 1 | 120s | 1,201 | 10.008 | 36.69 ms | 0 | 0 | PASS |
| 20 req/s | 4 | 6 | 120s | 2,400 | 19.9986 | 44.36 ms | 0 | 0 | PASS |
| 40 req/s | 4 | 6 | 120s | 4,801 | 40.0006 | 65.93 ms | 0 | 0 | PASS |

## Measurement Scope

- **HTTP 202 and latency** measure the synchronous ingestion request-response path.
- **Final database reconciliation** verifies eventual normalization and checks for missing or duplicate normalized rows.
- **PASS** means ingestion checks and final reconciliation passed. It does **not** imply the normalization worker sustained the offered request rate throughout the test.
- Results reflect the dedicated local Docker environment, not production capacity.
- Concurrency and batch size were changed together for the 20 and 40 req/s runs; their individual effects cannot be isolated.

## Smoke Test — 1 req/s

**Run ID:** `smoke-163-002`  
**Configuration:** 1 req/s for 30 seconds; **C=1, B=1**.

### k6 Results

| Metric | Observed |
|---|---:|
| HTTP requests / HTTP 202 | 31 / 31 |
| Achieved throughput | 1.031 req/s |
| HTTP p95 | 135.21 ms |
| HTTP failures | 0 |
| Dropped iterations | 0 |

### Final Database Reconciliation

| Metric | Observed |
|---|---:|
| Raw events | 31 |
| NORMALIZED events | 31 |
| Normalized rows | 31 |
| Unique normalized events | 31 |
| Pending events | 0 |
| Failed events | 0 |

**Assessment: PASS.** All accepted events were persisted and normalized once.

**Evidence:** `smoke-163-002.txt`; run-specific PostgreSQL reconciliation.

## Load Test — 5 req/s

**Run ID:** `rate5-001`  
**Configuration:** 5 req/s for 120 seconds; **C=1, B=1**.

### k6 Results

| Metric | Observed |
|---|---:|
| HTTP requests / HTTP 202 | 601 / 601 |
| Achieved throughput | 5.007 req/s |
| Ingestion success rate | 100% |
| HTTP average | 40.93 ms |
| HTTP median | 35.28 ms |
| HTTP p90 | 64.26 ms |
| HTTP p95 | 74.84 ms |
| HTTP maximum | 376.09 ms |
| Checks passed | 1,803 / 1,803 |
| HTTP failures | 0 |
| Dropped iterations | 0 |

### Final Database Reconciliation

| Metric | Observed |
|---|---:|
| Raw events | 601 |
| NORMALIZED events | 601 |
| Normalized rows | 601 |
| Pending events | 0 |
| Failed events | 0 |

**Assessment: PASS.** All 601 accepted events were persisted and normalized, with zero pending or failed events.

**Evidence:** k6 console summary and run-specific PostgreSQL reconciliation for `rate5-001`.

## Load Test — 10 req/s

**Run ID:** `rate10-001`  
**Configuration:** 10 req/s for 120 seconds; **C=1, B=1**.

### k6 Results

| Metric | Observed |
|---|---:|
| HTTP requests / HTTP 202 | 1,201 / 1,201 |
| Achieved throughput | 10.008 req/s |
| Ingestion success rate | 100% |
| HTTP average | 20.04 ms |
| HTTP median | 17.24 ms |
| HTTP p90 | 28.02 ms |
| HTTP p95 | 36.69 ms |
| HTTP maximum | 310.28 ms |
| Iteration p95 | 37.12 ms |
| Peak active VUs | 2 |
| Preallocated VUs | 20 |
| Checks passed | 3,603 / 3,603 |
| HTTP failures | 0 |
| Dropped iterations | 0 |

### Final Database Reconciliation

| Metric | Observed |
|---|---:|
| Raw events | 1,201 |
| NORMALIZED events | 1,201 |
| Normalized rows | 1,201 |
| Pending events | 0 |
| Failed events | 0 |

**Assessment: PASS.** All accepted events were normalized, with no pending or failed events at final reconciliation.

**Evidence:** `results/raw-logs/rate10-001.txt`; run-specific PostgreSQL reconciliation.

## Load Test — 20 req/s

**Run ID:** `rate20-c4-b6-001`  
**Configuration:** 20 req/s for 120 seconds; worker enabled; **C=4, B=6**.

### k6 Results

| Metric | Observed |
|---|---:|
| HTTP requests / HTTP 202 | 2,400 / 2,400 |
| Achieved throughput | 19.9986 req/s |
| Ingestion success rate | 100% |
| HTTP average | 24.72 ms |
| HTTP median | 19.54 ms |
| HTTP p90 | 31.83 ms |
| HTTP p95 | 44.36 ms |
| HTTP maximum | 706.84 ms |
| Iteration p95 | 45.33 ms |
| Checks passed | 7,200 / 7,200 |
| HTTP failures | 0 |
| Dropped iterations | 0 |

### Final Database Reconciliation

| Metric | Observed |
|---|---:|
| Raw events | 2,400 |
| NORMALIZED events | 2,400 |
| Normalized rows | 2,400 |
| Unique normalized events | 2,400 |
| Pending events | 0 |
| Failed events | 0 |

**Assessment: PASS.** All accepted events were eventually normalized exactly once. Final backlog was zero; sustained worker throughput at 20 events/sec was not established.

**Evidence:** k6 output for `rate20-c4-b6-001`; final run-specific PostgreSQL reconciliation.

## Load Test — 40 req/s

**Run ID:** `rate40-c4-b6-002-k6`  
**Configuration:** 40 req/s for 120 seconds; worker enabled; **C=4, B=6**.

### k6 Results

| Metric | Observed |
|---|---:|
| HTTP requests / HTTP 202 | 4,801 / 4,801 |
| Achieved throughput | 40.0006 req/s |
| Ingestion success rate | 100% |
| HTTP average | 23.72 ms |
| HTTP median | 17.53 ms |
| HTTP p90 | 42.18 ms |
| HTTP p95 | 65.93 ms |
| HTTP maximum | 332.29 ms |
| Iteration p95 | 66.41 ms |
| Checks passed | 14,403 / 14,403 |
| HTTP failures | 0 |
| Dropped iterations | 0 |

### Final Database Reconciliation

| Metric | Observed |
|---|---:|
| Raw events | 4,801 |
| NORMALIZED events | 4,801 |
| Normalized rows | 4,801 |
| Unique normalized events | 4,801 |
| Pending events | 0 |
| Failed events | 0 |

**Assessment: PASS.** All 4,801 requests were accepted and eventually normalized exactly once. The worker accumulated backlog during ingestion, so sustained normalization throughput at 40 events/sec was not demonstrated.

**Evidence:** k6 output for `rate40-c4-b6-002-k6` in `results/raw-logs/`; final run-specific PostgreSQL reconciliation.
