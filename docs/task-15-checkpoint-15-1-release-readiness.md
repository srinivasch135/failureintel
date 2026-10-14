# Task 15 — Checkpoint 15.1 release-readiness gate

**Assessment date:** 2026-10-11
**Decision:** **TASK 15.1 COMPLETE. GO to prepare controlled live worker verification (Task 15.2); production execution still requires explicit authorization.**

This is a release-boundary assessment, not a production deployment. Repository evidence is separated from facts that require access to the production database and deployment environment.

## Release identity

| Release | Commit/artifact | Purpose | Decision |
|---|---|---|---|
| A | `f2c41582809bf43f11e08734204458994246f938` (same source candidate as B; image not yet built/pinned) | First-version startup with worker disabled | As no application has previously been deployed to production, a separate compatibility-only binary is not applicable if the target has no pre-existing application version. This phase can use the same candidate image with the worker disabled. |
| B | `f2c41582809bf43f11e08734204458994246f938` (current `development` HEAD; release candidate only; image not yet built/pinned) | Enable asynchronous worker processing | Confirm the target and build an immutable image from this commit; enable the startup feature flag deliberately for this phase. |

The source commit candidate is known, but its releasable image is not. Build and record one immutable image digest for both phases if the same-image rollout is selected. The worker flag is startup configuration, so moving from A to B requires recreating/restarting the application with the flag enabled. If the eventual target already has an application version or non-empty schema/data, revisit the compatibility decision before rollout.

## Task 14 verification

The repository contains focused coverage for durable capture, HTTP acceptance, atomic normalized persistence, rollback, retry, claiming, concurrency, lease recovery, worker isolation, restart, and pipeline behavior. The external PowerShell environment completed `clean verify` on 2026-10-08 against the current `development` HEAD using Maven 3.9.14 and Java 17.0.20.1. The fresh Surefire reports provide the test counts and runtime metadata below.

```text
.\mvnw.cmd '-Dtest=FailureEventDurableCaptureIntegrationTest,FailureEventControllerIntegrationTest,FailureEventNormalizationProcessorIntegrationTest,FailureEventNormalizationProcessorRollbackIntegrationTest,FailureEventClaimServiceIntegrationTest,FailureEventProcessingWorkerTest,FailureEventProcessingWorkerRestartIntegrationTest,FailureEventRecoverySchedulingIntegrationTest,FailureEventPipelineIntegrationTest' test
.\mvnw.cmd test
```

Results: `clean verify` produced **31 Surefire XML reports, 272 tests, 0 failures, 0 errors, and 0 skipped**. All reports identify `java.version=17.0.20.1` and `java.home=C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot`. The nine Task 14 integration suites named by the focused selector are all represented in those reports and total **89 tests, 0 failures, 0 errors, and 0 skipped**. The user confirmed the external Maven command completed successfully; the report files were last modified between 16:23:43 and 16:24:17 on 2026-10-08.

A separate focused-suite execution is not required for this verification: `clean verify` includes Maven's test phase, and the fresh full-suite reports include every class in the focused selector. The 89-test focused count is calculated from those nine reports; it is not a separate execution result for this run.

If either command fails, or PostgreSQL/Testcontainers is unavailable, Task 14 is not green and this gate remains **NO-GO**. Tests must not be disabled or weakened.

## Repository facts

- Evidence paths: `src/main/resources/db/migration/V11__enforce_unique_failure_event_trace_id.sql`, `src/main/resources/db/migration/V12__*.sql`, `V13__*.sql`, `V14__*.sql`; `src/main/java/**/ProcessingStatus.java`; `application.yml`; `docker-compose.yml`; `Dockerfile`; and the Task 14 integration-test classes named below.
- Flyway migrations V1–V14 are present. V10 is non-additive: it drops normalized columns from the raw table, so compatibility depends on the production baseline and any still-running binary no longer requiring those columns. V11 adds a unique trace-ID index; V12 adds processing/retry/lease columns and a queue index; V13 backfills legacy idempotency keys, drops the old trace-ID index, and creates a replacement unique index; V14 adds the optimistic-lock version column. V11, V12, and V13 create indexes without `CONCURRENTLY`. Applied migrations must not be edited.
- `ProcessingStatus` is persisted as `VARCHAR` (`EnumType.STRING`). The active lifecycle is `RECEIVED`, `PROCESSING`, `NORMALIZED`, `RETRYABLE`, and `FAILED`; `QUEUED` and `PROCESSED` remain compatibility values.
- The ingestion path persists and flushes the raw row before returning `202`; parsing and normalization are worker work. The worker is startup-gated by `FAILURE_EVENT_WORKER_ENABLED` (default `false`) and its bean, scheduler, and executor are absent when disabled.
- Claiming, per-event processing, retry recording, and lease recovery use separate proxied transactions. PostgreSQL `FOR UPDATE SKIP LOCKED`, attempt ownership, and optimistic versioning protect concurrent/stale workers.
- Repository configuration uses Flyway validation, `ddl-auto=validate`, HikariCP (maximum pool size 10), bounded worker batch/concurrency settings, retry/lease settings, and Actuator health/info/metrics exposure. `FailureEventMetrics` registers backlog count/age, raw acceptance/capture failure, processing duration, normalized, retry, failed, lease recovery, and normalization-success-ratio meters. These are repository defaults/instrumentation, not verified production overrides or access controls.
- Docker/Compose files are local packaging/development evidence. No repository file proves the production platform, replica topology, migration owner, rollback mechanism, backup/PITR procedure, or secured production metrics/log access.

## Local Docker observations (2026-10-08)

The user confirms no application has been deployed to production; Docker is currently used for containerization. The repository's local Compose stack is running, but this does not establish that Compose is the intended production deployment mechanism.

- `failureintel-app` is running and `GET http://localhost:8081/actuator/health` returned `UP`; PostgreSQL 16 is healthy.
- The local database has successful Flyway entries V1–V14, zero failed entries in the recorded history, zero `failure_event` rows, zero duplicate trimmed nonblank trace IDs, and a `failure_event` relation size of 73,728 bytes. V13's idempotency-key index and V12's processing-queue index are present.
- The PostgreSQL Compose volume exists and is persistent. That alone does not prove backup, restore, or PITR capability.
- The running local app image ID is `sha256:82601de4551d23402905c81e2be2e24428a44933e5052b3d1c1564061b88320a`, created 2026-10-06 14:11:54 UTC. It predates current HEAD (`f2c4158…`, committed 2026-10-06 22:30 EDT); the image has no source-commit label, so it cannot be attributed to the current release candidate. The nearest earlier Git commit is `87c92fd` (Task 13), but that is not proof of the image's source.
- External PowerShell `clean verify` completed under Java 17.0.20.1 and Maven 3.9.14. Its fresh Surefire reports show all 31 suites ran under Java 17.0.20.1. The local image is not proven to be built from current HEAD and remains unsuitable as the pinned release artifact.

These are local-development observations only. The empty local table gives no representative production index-lock estimate.

## Production checks (no production application deployed yet)

| Check | Required evidence | Gate status |
|---|---|---|
| Task 14 result | External `clean verify` and 31 Java 17 Surefire reports recorded below | **PASS** |
| Flyway baseline | Local Compose DB is V14; production baseline does not apply yet. Verify the actual target DB before first deployment. | **N/A YET** |
| Migration-chain compatibility | Confirm target baseline and deployed binaries are compatible with the non-additive V10 column drops and V13 index replacement. | **N/A YET** |
| V11/V13 precondition | Local DB has no raw rows and V11/V13 completed; audit the actual target if it is not a new empty database or its baseline differs. | **N/A YET** |
| Index risk | Local DB has zero raw rows / 73,728-byte table; measure the actual target before applicable non-concurrent indexes. | **N/A YET** |
| Deployment topology/owner | Local Compose exists; intended deployment method, replicas, and migration owner have not been selected/verified. | **UNRESOLVED** |
| Rollback | No target-specific application/config rollback procedure has been recorded. | **UNRESOLVED** |
| Backup/PITR | Local persistent volume exists; backup/restore/PITR procedure is not verified. | **UNRESOLVED** |
| Configuration source | Local defaults and Compose settings are repository-only; production overrides are not yet defined. | **REPO-ONLY** |
| Metrics/log access | Metrics are registered and Actuator exposes health/info/metrics in app config; deployed access/security is not established. | **N/A YET** |

Do not infer production behavior from local Docker, `.env`, or `application.yml` defaults. Since no application has been deployed to production, production Flyway/data checks are deferred until a concrete target is selected; they must be completed before that target's first migration/deployment. Any duplicate audit or size measurement must be read-only.

## Compatibility boundary and rollout gate

For a first deployment with no pre-existing application version, Checkpoint A need not be a separate compatibility-only binary: use the same candidate image with `FAILURE_EVENT_WORKER_ENABLED=false`, then move to Checkpoint B by restarting/recreating that image with processing deliberately enabled. This preserves the worker-off verification window while the release uses one code version. Before doing so, confirm the target database baseline and migration owner. If an older application version or existing data/schema is present, explicitly verify mixed-version enum/schema compatibility; string-persisted enums can fail when an older binary reads an unknown newer state.

Checkpoint B is the immutable durable-capture/worker artifact. Start with the smallest supported worker configuration, observe backlog age/count, retries, processing duration, lease recovery, database lock symptoms, and Hikari utilization, then increase concurrency or replicas only while those signals remain healthy. If problems appear, disable the worker and preserve the persisted backlog; do not delete or rewrite events. Flyway rollback is not a casual reverse operation; use forward-compatible fix-forward procedures.

The production durability acceptance is demonstrated primarily by the deterministic Task 14 PostgreSQL tests: an accepted raw row remains present after a controlled normalization/write failure and no false `NORMALIZED` completion is committed. Production should provide passive evidence (accepted rows, retries, and no disappearing raw records), not an intentionally destructive failure injection.

## Documentation review

The durable-ingestion architecture document describes the asynchronous raw-capture/worker lifecycle. Its deployment section still contained an outdated instruction to retain synchronous behavior temporarily; this checkpoint marks that rollout sequence as historical and points to the current Task 15 release boundary. The Phase 1 correctness matrix’s old rollback statement is explicitly labeled historical/superseded. Current behavior is raw capture committed independently, followed by asynchronous processing whose later failure preserves the raw row.

## Production evidence update (2026-10-11)

- Render's active deploy is commit `37cc60d8014d1b5ae8e934f698d462d5fd1e1d69` (`fix(api): return 404 for unmapped routes`), deploy `dep-db573qqvcj2c73ed70g0`. The service is configured for one instance.
- The Render application startup log identifies instance `srv-db4fp8t9fdbs73bqorp0-4fwl8`. That instance connected to the production Neon database, successfully validated 14 migrations, found schema version 14 current, and logged that no migration was necessary. This proves that this instance validated the migration chain at startup; it does not establish which actor originally applied the migrations. Neon `flyway_schema_history` independently records successful V1–V14 entries.
- The Render Environment screenshot confirms the exact full variable name `FAILURE_EVENT_WORKER_ENABLED` and effective value `false` for the current service. This confirms worker-disabled capture-only operation; no configuration was changed in this assessment.
- The V13 production idempotency-key unique index was inspected read-only in Neon and found with the expected trimmed-key partial unique definition. Production ingestion was confirmed as HTTP 202 with a committed `RECEIVED` event. Render application logs are queryable and include health checks.
- Neon PITR evidence: the historical recovery branch `pitr-drill-20261010` was successfully created and verified in the earlier drill. It was temporary and its automatic deletion after approximately six hours is expected. Branch creation was verified; restoring production data from it was not tested.

### Release-specific rollback and recovery procedure

1. Disable `FAILURE_EVENT_WORKER_ENABLED` and restart the single Render application instance. Keep the service and Neon production database available for raw capture if the API remains healthy. Do not delete, rewrite, or purge `RECEIVED`, `RETRYABLE`, or in-flight event rows; they remain durable for later processing.
2. Restore the previous known-good Render application release through Render's deploy history. Confirm application health and leave the worker disabled until the release decision is reassessed. The database remains at Flyway V14; do not attempt to reverse Flyway migrations as part of application rollback.
3. Use Neon historical recovery only if database state itself requires recovery after a data-impacting failure. The prior drill establishes successful historical branch creation, not a tested production restore. If needed, create a new point-in-time recovery branch from the appropriate pre-incident timestamp, verify it, and follow Neon’s controlled recovery/cutover process. Do not overwrite production or claim restore success based solely on the earlier branch-creation drill.

## Task 15.1 completion and Task 15.2 preparation

**TASK 15.1 COMPLETE.** Task 14 tests, deployed release identity and health, single-instance topology, successful Flyway V1–V14 history and live startup validation, V13 idempotency index, live `202 Accepted` raw capture, usable application logs, worker-disabled configuration, and the release-specific application rollback/PITR evidence are satisfied. Startup logs show validation of the already-current schema; they do not identify the original migration actor. The Task 15.1 acceptance does not require attributing prior migration execution beyond the verified successful Flyway history and startup validation.

**Task 15.2 execution plan (not run; production changes require explicit authorization):**

1. Capture the current Render deploy/commit, single instance, healthy status, worker value `false`, and Neon production raw/normalized counts and latest event IDs. Record the starting backlog so existing queued rows remain distinguishable.
2. In Render, change only `FAILURE_EVENT_WORKER_ENABLED` to `true` and restart/redeploy the same application release on the single instance. Confirm healthy startup and worker-enabled configuration in logs before submitting test traffic.
3. Submit one valid failure event with a fresh, unique `Idempotency-Key`; record response, correlation ID, and returned event ID. Expect HTTP `202` for committed raw capture.
4. In Neon, poll only that event ID and its normalized row. Verify the raw lifecycle reaches `NORMALIZED` and the normalized row uses the same event ID. `PROCESSING` may be brief; capture it from application logs if polling does not observe it in the database. Correlate claim/processing logs by event ID and attempt.
5. Verify retry using an event that produces a supported retryable technical failure through the agreed non-destructive test path; inspect `RETRYABLE`, attempt count, `next_retry_at`, then verify a subsequent successful attempt reaches `NORMALIZED`. Verify lease recovery with a controlled worker-stop/crash only if the existing approved test scenario can safely leave a claim to expire; inspect expiry, recovery transition, and later processing without deleting or editing rows. Do not simulate database-write failure against production. If a safe retry/recovery trigger is unavailable, report that specific scenario as unverified rather than injecting destructive failure.
6. Restore `FAILURE_EVENT_WORKER_ENABLED=false` after the scoped run if requested by the release operator, confirm the single instance is healthy, and verify any remaining backlog is still durable. Preserve test event rows and evidence.

Repository defaults for the plan are batch size 8, worker concurrency 2, claim delay 2 seconds, lease timeout 5 minutes, recovery scan 30 seconds, and retry maximum 5 with configured delays/jitter; apply production overrides only if already approved. The plan intentionally does not enable the worker, submit events, or alter production data/configuration.

## Gate summary

```text
Task 14:                 PASS (272 full; 31 reports; Java 17.0.20.1; zero failures/errors/skips)
Active release:          Render commit 37cc60d8014d1b5ae8e934f698d462d5fd1e1d69; deploy dep-db573qqvcj2c73ed70g0
Render instances:        1 configured
Flyway startup:          PASS; instance validated 14; current version 14, no migration run
Original migration actor: UNVERIFIED; startup evidence is validation only
V13 production index:    PASS; expected partial unique index present
Worker flag:             PASS; screenshot confirms FAILURE_EVENT_WORKER_ENABLED=false
Render application logs: PASS; logs queryable
Rollback procedure:      DOCUMENTED; application rollback preserves database queue
PITR evidence:           PASS for historical branch creation only; restore/cutover not tested
Task 15.1:               COMPLETE
Task 15.2:               PREPARED; execution not authorized/performed
Controlled worker check: GO to begin only after explicit production-change authorization
Documentation:           CORRECTED (historical baseline explicitly marked; release docs added)
Release decision:        Task 15.1 PASS; Task 15.2 awaits explicit authorization
```

External PowerShell reported `clean verify` completed successfully on 2026-10-08 with Maven 3.9.14 and Java 17.0.20.1. The 31 fresh Surefire XML reports independently confirm Java 17.0.20.1 for all 272 tests and include all nine focused Task 14 integration classes (89 tests). This establishes repository test status only; it does not verify production state or release artifacts.

## Files changed in this checkpoint

- `docs/task-15-checkpoint-15-1-release-readiness.md` — release gate, evidence classification, rollout boundary, and blockers.
- `docs/phase-1-correctness-matrix.md` — explicit historical/superseded notice for the old synchronous rollback statement.
- `docs/durable-failure-event-ingestion-architecture.md` — labels the obsolete synchronous rollout sequence as historical and identifies the current release plan.

No production code, Flyway migration, production data, deployment infrastructure, or runtime configuration was changed. The only repository edit was this release-evidence documentation update.

## Task 15.6 — final production capacity checkpoint

**Decision: CHECKPOINT 15.6 PASS; TASK 15 COMPLETE.** This records the bounded capacity and worker-off/on evidence gathered on 2026-10-12. It does not repeat Task 14 or Checkpoints 15.1–15.5.

### Capacity measurements

| Measure | Concurrency 1 baseline | Concurrency 2 candidate |
|---|---|---|
| Configuration / topology | Worker enabled, concurrency 1, batch size 1; one Render instance (operator-confirmed settings) | Candidate deployment reported Live/healthy after the concurrency change; one configured Render instance |
| Workload | Five unique synthetic events; five HTTP 202 responses | Five unique synthetic events; five HTTP 202 responses. The request label was accidentally `TASK15_CAPACITY_BASELINE`, not `TASK15_CAPACITY_CANDIDATE`; workload size and event payload class were otherwise equivalent. |
| Database arrival span | `ingested_at` 17:31:55.422–17:32:00.826 UTC (5.404 s) | `ingested_at` 17:38:57.945–17:39:02.146 UTC (4.201 s) |
| Processing result | Five `NORMALIZED`, five attempts total, one attempt each, zero failed, zero backlog, five normalized rows | Five `NORMALIZED`, one attempt each, zero failure codes, one normalized row per raw event |
| Completion timing | All five normalized by the read-only Neon check at 17:32:26 UTC; exact drain time was not captured | All five normalized by the subsequent Neon check; exact drain time was not captured |
| Resource / database pool signals | Render CPU samples 0.116, 0.150, 0.042 CPU and memory samples 185 MB, 306 MB, 330 MB during the 17:31–17:33 UTC window. Neon connection-pool utilization unavailable. | Render log and metrics reads returned unauthorized, so candidate-specific instance/resource measurements are unavailable. Neon pool utilization unavailable. |

No useful processing-capacity improvement was established for concurrency 2: exact worker drain timing and candidate resource signals were unavailable, and the candidate workload label was wrong. The small difference in database arrival spans is not evidence of faster processing. Concurrency 2 was not selected; the final setting is concurrency 1.

### Worker-off/on durability and recovery

- Worker disabled: the operator confirmed the worker-off deployment was Live and healthy. Five synthetic requests returned HTTP 202. Neon showed all five committed raw rows as `RECEIVED`, `attempt_count=0`, with no normalized rows.
- Worker re-enabled: the operator confirmed the worker-enabled deployment was Live and healthy. Read-only Neon verification of those same five event IDs found all five `NORMALIZED`, `attempt_count=1`, no `failure_code`, `failure_reason`, or `next_attempt_at`, and exactly one normalized row per raw event using the same event ID.
- All five raw rows remain present and retain their original non-empty synthetic payload. No production rows were deleted or reset. The exercise was performed once.

### Final configuration and gate

```text
Worker enabled:          true (operator-confirmed)
Worker concurrency:      1 (operator-confirmed; concurrency 2 not selected)
Worker batch size:       1 (operator-confirmed)
Render instances:        1 configured
Worker-off durability:   PASS; five committed RECEIVED rows, zero attempts, no normalized rows
Worker-on recovery:      PASS; same five rows NORMALIZED at attempt 1, one normalized row each
Raw payload durability:  PASS; all five raw payloads remain present
Candidate capacity gain: NOT DEMONSTRATED; candidate-specific Render metrics unavailable
Checkpoint 15.6:         PASS
Task 15:                 COMPLETE
```

Optional candidate resource and Neon connection-pool signals were unavailable and are not represented as measured. No further capacity workload or worker toggle was performed after the successful recovery verification.
