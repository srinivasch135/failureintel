# Task 15 — Checkpoint 15.1 release-readiness gate

**Assessment date:** 2026-10-08
**Decision:** **NO-GO for first deployment** until the release boundary and target deployment/recovery plan are established.

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

## Release decision

**NO-GO for first deployment.** No production migration defect has been observed: no production application is deployed, and the local Compose database is healthy at V14. A distinct compatibility-only binary is not needed if the first target has no existing app version; use one pinned image for A (worker off) and B (worker on). The target deployment/migration owner and recovery procedure are not established, and the running local image predates current HEAD, so it cannot serve as the pinned release image. Select the target, confirm its DB baseline, produce and identify the exact candidate image, and establish rollback/backup checks before deployment.

## Gate summary

```text
Task 14:                 PASS (272 full; 31 reports; Java 17.0.20.1; zero failures/errors/skips)
Artifact A:              f2c41582809bf43f11e08734204458994246f938 (same-image candidate; digest unresolved)
Artifact B:              f2c41582809bf43f11e08734204458994246f938 (same-image candidate; digest unresolved)
Compatibility A:         NOT APPLICABLE if first target has no previous app version; confirm before rollout
Flyway baseline:         Local V14 verified; production N/A until target selected
V11/V13 preconditions:   Local PASS (empty table); target N/A until selected
Index risk:              Local footprint measured; target N/A until selected
Deployment topology:     REPO-ONLY (Compose is local; target not selected)
Migration owner:         UNRESOLVED
Rollback:                UNRESOLVED
Backup/PITR:             UNRESOLVED
Config source:           REPO-ONLY
Metrics/log access:      REPO-ONLY; deployed access N/A until target exists
Documentation:           CORRECTED (historical baseline explicitly marked; release docs added)
Release decision:        NO-GO for first deployment
```

External PowerShell reported `clean verify` completed successfully on 2026-10-08 with Maven 3.9.14 and Java 17.0.20.1. The 31 fresh Surefire XML reports independently confirm Java 17.0.20.1 for all 272 tests and include all nine focused Task 14 integration classes (89 tests). This establishes repository test status only; it does not verify production state or release artifacts.

## Files changed in this checkpoint

- `docs/task-15-checkpoint-15-1-release-readiness.md` — release gate, evidence classification, rollout boundary, and blockers.
- `docs/phase-1-correctness-matrix.md` — explicit historical/superseded notice for the old synchronous rollback statement.
- `docs/durable-failure-event-ingestion-architecture.md` — labels the obsolete synchronous rollout sequence as historical and identifies the current release plan.

No production code, Flyway migration, production data, deployment infrastructure, or runtime configuration was changed.
