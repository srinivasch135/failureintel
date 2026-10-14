# AGENTS.md

## Project Context

This repository contains the **Failure Intelligence & Automated Recovery Platform** backend.

The project is currently being developed as a **modular monolith using Java and Spring Boot**.

The current focus is building a reliable failure-event processing pipeline before introducing additional distributed-system complexity.

Implemented flow:

API

→ Failure event ingestion

→ Persist raw failure event

→ Processing worker

→ Parse event

→ Normalize event

→ Persist normalized failure event

Kafka, grouping similar failures, and incident processing are planned downstream stages; they are not part of the implemented pipeline.

Do not assume future components already exist.

Use `docs/durable-failure-event-ingestion-architecture.md` for architectural context and the read ownership documents in `docs/` for field/query ownership. Verify historical names, examples, and checkpoint descriptions against current code. If code and documented intent conflict, report the conflict before changing the contract.

---

# Core Working Principle

Work with the **existing architecture first**.

Before making changes:

1. Inspect the relevant code.
2. Understand the existing execution path.
3. Identify the smallest required change.
4. Modify only what is necessary.
5. Run relevant tests.
6. Review the resulting diff.

Do not generate large amounts of code before understanding the current implementation.

---

# Inspect Before Editing

For any non-trivial task, first inspect the relevant path.

Trace the implementation when applicable:

Controller

→ Application Service

→ Domain logic

→ Repository

→ Entity

→ Database

Before editing, determine:

* Which files are involved.
* What the current behavior is.
* Where the requested change belongs.
* Which existing components can be reused.
* Which files should remain untouched.

Do not invent missing requirements.

If something is unclear, explicitly state the assumption instead of silently designing around it.

---

# Architecture Boundaries

Respect the existing separation of responsibilities.

## Raw Failure Events

Raw ingestion data belongs to:

* `FailureEventEntity`
* `FailureEventRepository`

The raw failure-event table represents the originally ingested failure event and its processing state.

Do not place normalized fields back into the raw failure-event model.

---

## Normalized Failure Events

Normalized data belongs to:

* `NormalizedFailureEventEntity`
* `NormalizedFailureEventRepository`

Queries concerning normalized fields should use the normalized persistence path.

Do not read normalized information from `FailureEventEntity`.

This ownership rule does not prohibit the existing API's normalized-first/raw-fallback mapping: use canonical values from the normalized row when present, and original raw values as fallback. Normalization-only metadata must come from the normalized row.

Preserve normalized-only search, deterministic latest-event lookup by trace ID, and the coherent raw/normalized snapshot used by direct reads. Do not weaken the current read transaction isolation without a requirement and concurrency validation.

---

## Parsing

Parsers are responsible for:

source-specific/raw representation

→ structured parsed representation

Do not put canonical normalization rules inside parsers unless the current architecture explicitly requires it.

---

## Normalization

Normalization is responsible for:

parsed representation

→ canonical normalized representation

Normalization rules belong in normalization-related domain/application components.

Avoid persistence, HTTP, Kafka, or infrastructure concerns inside normalization logic.

---

## Processing Worker

The worker is responsible for asynchronously processing already-persisted raw failure events.

Raw persistence protects the original event before background processing begins.

The worker orchestrates claim and per-event processing services; the database remains the durability layer. Keep parsing, normalization, and persistence behavior in their existing components.

Do not introduce additional queues or brokers unless explicitly requested.

---

## Kafka

Kafka is planned for later stages of the system, primarily around downstream asynchronous processing such as grouping related normalized failures.

Do not introduce Kafka:

* for hypothetical scalability,
* for raw-event durability already provided by the database,
* or simply because asynchronous processing is involved.

Only modify or introduce Kafka-related components when explicitly requested.

---

# Implementation Rules

Prefer this order:

1. Reuse an existing component.
2. Extend an existing component.
3. Add a small focused method.
4. Create a new component only when the existing design cannot cleanly support the requirement. Separate Spring beans required for transaction boundaries are a concrete correctness requirement; reuse must not bypass transactional proxies.

Do not create new:

* services,
* repositories,
* interfaces,
* DTOs,
* factories,
* strategies,
* managers,
* coordinators,
* wrappers,
* helper classes,

unless they solve a concrete requirement.

Avoid abstraction for hypothetical future needs.

---

# Minimum Change Principle

Make the smallest coherent change that satisfies the task.

A small task should produce a small diff.

Do not:

* refactor unrelated code,
* rename unrelated classes,
* reorganize packages,
* reformat entire files,
* update dependencies unnecessarily,
* change public APIs without requirement,
* alter database schema unless necessary for the authorized task and implemented through Flyway,
* introduce architectural patterns merely for cleanliness.

If unrelated problems are discovered, report them separately instead of fixing them automatically.

**Discovery is not authorization.**

Finding an issue, cleanup opportunity, TODO, architectural weakness, possible abstraction, optimization opportunity, or unrelated failing test does not authorize changing it.

Only expand implementation scope when the discovered work is necessary to satisfy the current task.

---

# Service Layer Responsibility

Application/service-layer code should coordinate use cases.

It may decide which repository or domain operation is required based on the use case.

Avoid putting repository-routing intelligence into controllers when it belongs to application logic.

Controllers should primarily handle:

HTTP request

→ validation/input mapping

→ application service

→ HTTP response

---

# Repository Responsibility

Repositories should represent persistence/query operations for their corresponding model.

Use:

`FailureEventRepository`

for raw ingestion/event data.

Use:

`NormalizedFailureEventRepository`

for normalized data.

Do not create repository methods solely because they might be useful later.

Add queries only when there is a current use case.

---

# Database Changes

Database schema changes use Flyway migrations.

For any migration:

1. Inspect existing migration versions.
2. Create the next valid migration.
3. Avoid modifying already-applied migrations unless explicitly instructed.
4. Preserve existing data when possible.
5. Consider indexes only for known query patterns.
6. Keep entity mappings synchronized with schema changes.

Do not generate indexes speculatively.

---

# Persistence and Transaction Invariants

* `202 Accepted` means the raw failure event has committed. Never acknowledge acceptance when raw persistence or commit fails. Acceptance does not mean normalization has completed.
* Keep parsing and normalization outside the raw-capture transaction. Preserve the original raw data when later processing fails.
* The normalized row and raw `NORMALIZED` transition must commit or roll back together in one per-event transaction.
* Preserve the same event ID across raw and normalized persistence and the existing `@MapsId` relationship. Retry processing must reuse the normalized row rather than create duplicates.
* Claiming, per-event normalization, retry-state recording, and expired-claim recovery have separate Spring-proxied `REQUIRES_NEW` boundaries. Do not rely on transactional self-invocation or wrap a whole batch in one transaction.
* Claim transactions finish before parsing or normalization starts. Retry-state recording happens only after the failed processing transaction exits, including failures during transaction completion.
* Preserve the repository-specific transaction behavior: raw `saveAndFlush()` and idempotency lookup use independent transactions. Use transaction-participating `save()` inside processing/retry transactions; do not substitute raw `saveAndFlush()` and split an atomic outcome.
* Do not catch persistence errors inside a rollback-only transaction and assume subsequent state changes will commit. Preserve flush/commit failure visibility and rollback coverage.

---

# Claims, Concurrency, and Recovery

* Claim a bounded, deterministically ordered batch using `FOR UPDATE SKIP LOCKED`. Preserve eligibility for `RECEIVED` and due `RETRYABLE` events, including handling retries already at the attempt limit.
* Increment `attempt_count` when claiming, not when recording failure or recovering a lease. A crashed worker's claim counts as an attempt.
* Preserve `ClaimedFailureEvent(eventId, attemptNumber)`, matching `PROCESSING` status/attempt ownership, and the raw entity's optimistic `@Version`. An event ID alone does not authorize a late worker to update an event.
* Use the entity transition methods to maintain lifecycle, lease, retry, and failure metadata together. Do not bypass these guards with ad hoc status updates in the processing path.
* Recovery is a separate short, bounded transaction. It must not parse or normalize, and its commit must not depend on a later normal-claim transaction succeeding.
* Preserve database-clock claim timestamps and recovery cutoffs, inclusive expiry (`<=`), and the existing fallback for older rows missing lease timestamps. Retry scheduling continues to use the existing retry policy.
* Recovery clears the lease and schedules retry or records terminal failure; it does not increment attempts or replace `last_attempt_at`.
* Keep recovery independently schedulable while normalization tasks are blocked or saturated. Preserve bounded executor concurrency/queueing, worker feature gating, and shutdown behavior.
* Lease duration must account for processing time and time already-claimed events may wait in the executor queue. Unfinished claims remain durable and recoverable after shutdown or restart.

---

# Event Identity and Idempotency

* `eventId` identifies a failure occurrence. `traceId` is nonunique correlation data; multiple events may share it. Do not restore a trace-ID uniqueness constraint or deduplicate all events by trace ID.
* Preserve explicit idempotency-key semantics: equivalent content returns the existing event ID; conflicting reuse returns `409 Conflict`; different keys may share a trace ID.
* A nonblank `Idempotency-Key` header takes precedence over the request-body key. Preserve trimmed keys, explicit/legacy namespaces, and validation that accounts for the stored prefix.
* The database unique index is the final guard against concurrent inserts. Only resolve the expected idempotency-key violation as a duplicate race; let the losing transaction exit, then reload and compare the winner in a fresh transaction.
* No-key requests normally create new events. Preserve the compatibility lookup for equivalent migrated `trace:<traceId>` rows without rejecting distinct events sharing that trace.
* Reuse the existing deterministic fingerprint. Preserve its canonicalization and exclusions of generated identity, ingestion time, processing state, normalized fields, and the top-level trace ID. Do not casually change fingerprint semantics for existing keys.
* Resolving an existing retry must not mutate its raw content or restart processing.

---

# Processing Failure Policy

* Expected unsupported, malformed, or insufficient-data outcomes become terminal `FAILED` events with their specific failure code and no scheduled retry.
* Keep persisted-payload decoding failures narrowly classified. Do not turn unrelated parser, normalizer, database, or unexpected exceptions into malformed-data outcomes.
* Technical and unexpected processing exceptions use the existing bounded retry policy, including normalized-write failures. Preserve configurable attempt limits, backoff, and jitter.
* Exhausted retries use processing status `FAILED` with failure code `RETRY_EXHAUSTED`; this is not another processing state. Preserve a controlled last-cause explanation and clear lease/retry scheduling metadata.
* Stale attempts must not overwrite a newer attempt, recovery, or a terminal outcome. Preserve ownership checks in both normalization and failure recording.
* Keep exception handling that isolates individual events and allows the worker/recovery schedule to continue. Report failures without breaking this intentional runtime isolation.

---

# API and Security Constraints

* Keep HTTP validation aligned with persistence limits, including idempotency-key prefixes. Preserve the existing distinction between transport validation and background processing outcomes.
* Public processing failure metadata uses allowlisted codes and controlled explanations. Never return arbitrary stored `failureReason`, stack traces, SQL details, secrets, or raw payloads as processing diagnostics.
* Preserve safe database/unexpected-error responses through the existing exception handler. Do not expose internal exception messages when adding failure paths.
* Enforce the configured JSON ingestion body limit before binding, using actual bounded byte reads even when content length is unknown. Preserve exact-limit acceptance and oversized rejection without persistence side effects.
* Filter route matching must agree with Spring MVC's decoded path semantics. Preserve encoded-route coverage and ingestion path-parameter rejection; raw URI equality is insufficient.

---

# Observability

* Reuse the existing logging, correlation filter, Micrometer component, and Actuator configuration. Do not introduce another monitoring layer without a concrete requirement.
* Preserve `X-Correlation-ID` response propagation and MDC cleanup. Use event ID, attempt number, and failure code to identify background processing failures; do not assume request MDC automatically reaches executor threads.
* Keep metric dimensions bounded. Do not use event IDs, trace IDs, idempotency keys, payloads, or arbitrary exception text as metric labels.
* Metrics must not invalidate durable acceptance or change processing transaction boundaries. Outcome counters must describe committed outcomes, not rolled-back writes or equivalent idempotent retries.
* Verify production instrumentation call sites before claiming a metric is operational; a registered meter or unit test alone does not prove pipeline integration.

---

# Testing Workflow

After implementation, run the smallest relevant test first.

Examples:

Normalizer change

→ normalizer unit tests

Parser change

→ parser tests

Parser + normalizer boundary change

→ parser-normalizer integration test

Repository/entity/database change

→ repository/Testcontainers integration test

Pipeline change

→ pipeline integration test

Then run the broader test suite when appropriate.

Typical sequence:

targeted test

→ related integration test

→ `./mvnw.cmd test` on Windows (or `./mvnw test` on Unix)

Do not modify tests merely to make a failing implementation pass.

Tests should represent intended behavior.

If an existing test conflicts with the requested architectural change, explain why before changing it.

For transaction, concurrency, retry, or recovery changes, use the existing PostgreSQL/Testcontainers integration tests to verify committed state, rollback, locking, and stale-attempt behavior. Mocks alone cannot prove these properties.

For HTTP boundary changes, use real HTTP integration coverage for routing/encoding, byte limits, status codes, and database side effects. Extend existing relevant coverage rather than duplicate test infrastructure.

Documentation-only changes require content/diff verification; application tests are not required unless executable behavior or commands were changed in a way that needs execution validation.

## Build and Verification Commands

Use the Maven wrapper with Java 17. PostgreSQL integration tests require accessible Docker; they run in the normal `test` phase. Report unavailable prerequisites rather than disabling tests.

PowerShell examples (replace test selectors with those relevant to the change):

```powershell
.\mvnw.cmd '-Dtest=FailureEventNormalizerTest,FailureEventNormalizationRulesTest' test
.\mvnw.cmd '-Dtest=FailureEventClaimServiceIntegrationTest,FailureEventNormalizationProcessorRollbackIntegrationTest' test
.\mvnw.cmd test
.\mvnw.cmd verify
git diff --check
git status --short
```

Quote comma-separated Maven selectors in PowerShell. Run focused tests first, then related integration tests and broader verification appropriate to the change. A Docker image build that skips tests is not test verification.

---

# Failure Handling

Do not hide failures.

If:

* compilation fails,
* a test fails,
* Flyway fails,
* an entity does not match the schema,
* or an assumption proves incorrect,

report the exact issue.

Fix the root cause rather than suppressing the failure.

Do not:

* skip tests,
* weaken assertions,
* catch exceptions without reason,
* return dummy values,
* disable validations,

just to make the build green.

---

# Code Quality

Maintain high code quality and follow sound coding principles within the existing architecture. Avoid AI-generated codebase slop: speculative abstractions, duplicate logic/components, unnecessary wrappers, verbose boilerplate, misleading comments, and broad rewrites unsupported by the task.

Prefer code that is:

* explicit,
* readable,
* easy to trace,
* easy to test,
* consistent with the surrounding repository.

Avoid cleverness.

Favor clear control flow over unnecessary abstraction.

Do not introduce design patterns unless they provide an actual benefit for the current requirement.

Do not introduce deprecated or unsupported packages, APIs, functions, or methods. Use supported replacements compatible with this project's Java/Spring baseline and verify support status when uncertain. Do not migrate unrelated working code or update dependencies solely to adopt something newer.

Prefer cohesive components, explicit contracts, clear names/control flow, and focused methods. Remove unnecessary duplication without creating abstractions for hypothetical use cases. Comments should explain a non-obvious invariant or reason, not narrate the code. Keep correctness, architecture boundaries, and meaningful tests ahead of code volume or cleverness.

---

# Domain Terminology

Use existing project terminology consistently.

Important concepts include:

* Raw Failure Event
* Parsed Failure Event
* Normalized Failure Event
* Processing Status
* Normalization Status
* Failure Category
* Severity
* Incident

Do not create alternate terminology for concepts that already exist.

---

# Processing Status

The active ingestion/normalization pipeline uses:

* RECEIVED
* PROCESSING
* NORMALIZED
* RETRYABLE
* FAILED

`QUEUED` and `PROCESSED` remain compatibility enum values; do not introduce new transitions to them in this pipeline or remove them without checking persisted data and deployed-version compatibility.

Treat processing status as lifecycle state for event processing.

Do not conflate processing status with normalization quality/status.

---

# Normalization Status

Normalization status describes the quality/result of normalization.

The current normalization statuses are:

* FULLY_NORMALIZED
* PARTIALLY_NORMALIZED
* MALFORMED

Do not use processing lifecycle status to represent normalization quality.

---

# Performance Work

When evaluating normalization throughput or pipeline performance:

1. Establish correctness first.
2. Measure the existing database-backed worker and parser/normalizer path before proposing new infrastructure.
3. Use realistic batches of persisted/raw events.
4. Measure throughput, latency, failure rate, and resource behavior.
5. Preserve the current asynchronous worker; propose additional infrastructure only for an identified bottleneck and within explicitly authorized scope.

Do not optimize based only on assumptions.

---

# Codex Task Workflow

For every meaningful task, follow this sequence.

## 1. Understand

Read the request and identify the exact desired behavior.

Identify:

* the requested outcome,
* explicit requirements,
* explicit constraints,
* behavior that must remain unchanged,
* and anything that is outside the requested scope.

The original task objective remains authoritative throughout implementation unless the user changes it.

---

## 2. Inspect

Inspect relevant existing code before editing.

Do not infer architecture from filenames or assumptions when the implementation can be inspected directly.

---

## 3. Trace

Understand the current execution/data flow.

Identify where the requested behavior belongs and which existing components participate in that path.

---

## 4. Plan

State the minimum required changes as explicit implementation steps.

The plan must be concrete enough that progress can be checked against it.

Keep the plan active throughout implementation.

The plan is not a one-time explanation before coding. It is the current execution contract for the task.

Do not silently abandon incomplete plan items.

The plan may change when repository evidence invalidates an assumption, but changes to the plan must be explicit.

---

## 5. Implement

Modify only the required files.

Execute one planned step at a time.

Before beginning a new major implementation step, reconcile the intended action with:

* the original user request,
* the current plan,
* the existing architecture,
* and the current task scope.

Do not silently switch from the planned task to newly discovered work.

If new work is discovered during implementation, classify it as:

### Blocking

The work is necessary to complete the requested behavior and prevents the current plan from proceeding.

Blocking work may interrupt the current planned step.

If it materially changes the implementation approach, update the plan first and state why the change is required.

### Required but Non-Blocking

The work is related to the requested behavior but does not prevent the current planned step from continuing.

Do not interrupt the current step unnecessarily.

Add it to the remaining plan if it is required for task completion.

### Unrelated

The work is outside the requested behavior.

Do not implement it.

Report it separately if it is important.

Do not change the task objective merely because the implementation path changes.

The goal is stable unless the user changes it.

---

## 6. Reconcile

After each meaningful implementation step:

1. Mark the completed plan item as complete.
2. Identify the next incomplete plan item.
3. Check whether new repository evidence invalidates any remaining step.
4. Update the plan explicitly if necessary.
5. Continue from the current plan rather than generating a new direction from scratch.

Use this execution loop:

Plan

→ Execute current step

→ Inspect result

→ Validate assumptions

→ Reconcile with plan

→ Update plan if necessary

→ Execute next step

Do not use this execution pattern:

Plan

→ Begin implementation

→ Discover something interesting

→ Follow the new direction

→ Expand scope

→ Forget remaining planned work

---

## 7. Validate

Compile and run relevant tests.

Validation does not replace plan completion.

Passing tests do not mean the task is complete if required planned behavior remains unfinished.

---

## 8. Review

Inspect the git diff.

Compare the resulting implementation with both:

* the original request,
* and the final task plan.

Remove changes that are not necessary for either.

---

## 9. Final Reconciliation

Before considering the task complete, explicitly reconcile:

* the original user request,
* the current/final plan,
* every required plan item,
* the implemented behavior,
* architecture boundaries,
* tests and validation,
* and the final git diff.

Every planned item must end in one of these states:

* **Completed** — implemented and validated.
* **Removed** — repository evidence showed it was unnecessary; explain why.
* **Blocked** — cannot be completed; report the exact blocker.
* **Deferred** — outside the current task scope; report it separately if relevant.

Do not silently leave planned work unfinished.

Do not declare the task complete while required plan items remain unresolved.

---

## 10. Report

Summarize:

* files changed,
* why they changed,
* behavior added or modified,
* behavior intentionally preserved,
* tests executed,
* plan changes made during implementation,
* deferred discoveries,
* failures or unresolved assumptions.

Keep the report tied to the original requested task.

---

# Task Drift Control

During implementation, new information will be discovered.

New information may change **how** the task should be implemented.

It does not automatically change **what** task is being implemented.

Maintain these invariants:

**The user objective is stable.**

**The plan is mutable but explicit.**

**Execution follows the current plan.**

**Discovery is not authorization.**

**Scope expansion requires task necessity.**

When repository evidence requires a different implementation approach:

1. Stop before expanding the implementation.
2. Identify which plan assumption was invalid.
3. Determine whether the discovered work is required for the original task.
4. Update the plan explicitly.
5. Preserve the original task objective and architecture constraints.
6. Resume execution from the revised plan.

Do not create a replacement objective from implementation discoveries.

Do not allow local cleanup or architectural opportunities to become the new task.

---

# Git Diff Review

Before considering a task complete, inspect the diff for:

* unexpected files,
* unrelated formatting,
* duplicate logic,
* unnecessary classes,
* new dependencies,
* accidental API changes,
* architecture boundary violations,
* raw/normalized data mixing,
* unnecessary database changes,
* weakened tests,
* changes not represented by the task plan,
* planned behavior that is missing from the implementation.

Remove unnecessary changes before finishing.

---

# When Asked to Refactor

Refactoring does not automatically mean redesigning.

First determine:

* what problem the refactor solves,
* what behavior must remain unchanged,
* which existing boundaries should remain intact.

Prefer localized refactoring.

Do not turn a focused refactor into a repository-wide architecture rewrite.

---

# When Asked to Add a Feature

Before implementing:

1. Find the closest existing use case.
2. Find the existing extension point.
3. Reuse current domain terminology and architecture.
4. Define the smallest behavioral change.
5. Add tests covering that behavior.

Do not build infrastructure for possible future versions of the feature.

---

# When Asked to Fix a Bug

First reproduce or identify the failing path.

Use:

symptom

→ execution path

→ root cause

→ smallest fix

→ regression test

Do not perform broad cleanup unless it directly contributes to the fix.

---

# When Asked for Architecture Advice

Do not immediately modify code.

First provide:

* current architecture,
* observed limitation,
* possible options,
* trade-offs,
* recommended option,
* files/components likely affected.

Architecture decisions should remain explicit.

Do not silently implement a new architecture while solving another task.

---

# What to Avoid

Avoid AI-generated codebase slop.

Specifically avoid:

* unnecessary abstractions,
* speculative scalability layers,
* duplicate services,
* duplicate DTOs,
* generic utility classes,
* premature Kafka usage,
* premature microservices,
* excessive interfaces,
* excessive configuration,
* rewriting working code for style reasons,
* broad refactors unrelated to the requested task,
* architecture invented from incomplete context,
* silently abandoning the original task plan,
* replacing the task with newly discovered work,
* treating cleanup opportunities as requirements,
* declaring completion with unresolved required plan items.

---

# Completion Standard

A task is complete when:

* the requested behavior is implemented,
* every required plan item is completed or explicitly accounted for,
* the final implementation still matches the original request,
* architecture boundaries remain intact,
* the code compiles,
* relevant tests pass,
* the final diff contains no unnecessary changes,
* no discovered unrelated work was silently added to scope,
* assumptions and limitations are clearly reported.

Correctness and architectural consistency are more important than generating more code.

Plan completion and task completion are not separate concepts.

A task is not complete merely because the code compiles or tests pass.

The implementation must reconcile with the original objective, the final plan, and the resulting diff.
