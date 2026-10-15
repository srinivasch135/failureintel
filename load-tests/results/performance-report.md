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
| 5 req/s | 3 | 5 | 120s | 601 | 5.008 | 69.29 ms | 0 | 0 | PASS |
| 10 req/s | 1 | 1 | 120s | 1,201 | 10.008 | 36.69 ms | 0 | 0 | PASS |
| 20 req/s | 4 | 6 | 120s | 2,401 | 20.00624 | 40.17 ms | 0 | 0 | PASS |
| 40 req/s | 4 | 6 | 120s | 4,801 | 40.0006 | 65.93 ms | 0 | 0 | PASS |

## Measurement Scope

- **HTTP 202 and latency** measure the synchronous ingestion request-response path.
- **Final database reconciliation** verifies eventual normalization and checks for missing or duplicate normalized rows.
- **PASS** means ingestion checks and final reconciliation passed. It does **not** imply the normalization worker sustained the offered request rate throughout the test.
- Results reflect the dedicated local Docker environment, not production capacity.
- Worker configurations differed across test rates (C=1/B=1 at smoke and 10 req/s; C=3/B=5 at 5 req/s; C=4/B=6 at 20 and 40 req/s). The individual effects of worker concurrency and batch size cannot be isolated.

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

**Run ID:** `rate5-c3-b5-001`  
**Configuration:** 5 req/s for 120 seconds; worker enabled; **C=3, B=5**.

### k6 Results

| Metric | Observed |
|---|---:|
| HTTP requests / HTTP 202 | 601 / 601 |
| Achieved throughput | 5.008029 req/s |
| Ingestion success rate | 100% |
| HTTP average | 43.92 ms |
| HTTP median | 42.29 ms |
| HTTP p90 | 59.59 ms |
| HTTP p95 | 69.29 ms |
| HTTP maximum | 377.21 ms |
| Iteration p95 | 69.55 ms |
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

**Assessment: PASS for ingestion and final raw-event processing status.**  All 601 ingestion requests passed k6 checks, with zero HTTP failures or dropped iterations. Run-specific PostgreSQL reconciliation confirmed all 601 events were eventually normalized exactly once, with no pending or failed events.

**Evidence:** `results/raw_logs/rate5-c3-b5-001-k6.txt`; run-specific PostgreSQL processing-status query.

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

**Evidence:** `results/raw_logs/rate10-001.txt`; run-specific PostgreSQL reconciliation.

## Load Test — 20 req/s

**Run ID:** `rate20-c4-b6-004`  
**Configuration:** 20 req/s for 120 seconds; worker enabled; **C=4, B=6**.

### k6 Results

| Metric | Observed |
|---|---:|
| HTTP requests / HTTP 202 | 2,401 / 2,401 |
| Achieved throughput | 20.00624 req/s |
| Ingestion success rate | 100% |
| HTTP average | 22.94 ms |
| HTTP median | 19.84 ms |
| HTTP p90 | 31.65 ms |
| HTTP p95 | 40.17 ms |
| HTTP maximum | 251.58 ms |
| Iteration p95 | 40.46 ms |
| Checks passed | 7,203 / 7,203 |
| HTTP failures | 0 |
| Dropped iterations | 0 |

### Final Database Reconciliation

| Metric | Observed |
|---|---:|
| Raw events | 2,401 |
| NORMALIZED events | 2,401 |
| Normalized rows | 2,401 |
| Unique normalized events | 2,401 |
| Pending events | 0 |
| Failed events | 0 |

**Assessment: PASS.** All 2,401 ingestion requests passed k6 checks, with zero HTTP failures or dropped iterations. Run-specific PostgreSQL reconciliation confirmed all 2,401 events were eventually normalized exactly once, with no pending or failed events.

**Evidence:** `results/raw_logs/rate20-c4-b6-004-k6.txt`; confirmed run-specific PostgreSQL reconciliation.

## Load Test — 40 req/s

**Run ID:** `rate40-c4-b6-001-k6`  
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

**Evidence:** k6 output for `rate40-c4-b6-001-k6` in `results/raw_logs/`; final run-specific PostgreSQL reconciliation.

## Measured Boundary:

The highest successfully tested ingestion rate was **40 req/s for 120 seconds**, with worker concurrency **C=4** and batch size **B=6**. The k6 run recorded 4,801 accepted HTTP 202 responses, 100% ingestion success, zero HTTP failures, zero dropped iterations, and HTTP p95 latency of 65.93 ms. Run-specific PostgreSQL reconciliation recorded 4,801 raw events, 4,801 NORMALIZED events, and 4,801 unique normalized records, with no pending or failed events at final observation.

**Observed processing limitation:** During the 40 req/s run, the normalization backlog accumulated before eventually draining. This shows that asynchronous normalization did not keep pace with ingestion throughout the measurement interval. It does not establish a definitive CPU, database, or other resource-level root cause. No ingestion failure boundary was observed at the highest tested rate; **40 req/s is the highest tested successful ingestion rate, not a proven maximum or sustained normalization capacity**.

**Limitations:** Tests used a dedicated local Docker Compose environment with Spring Boot/Java 17, PostgreSQL 16, and Grafana k6. Worker configurations differed across rates (C=1/B=1 for smoke and 10 req/s, C=3/B=5 for 5 req/s, and C=4/B=6 for 20 and 40 req/s), so their independent effects cannot be isolated. The 120-second load levels do not establish long-duration stability. Results depend on local hardware and resource allocation and cannot be directly extrapolated to Render/Neon capacity.

