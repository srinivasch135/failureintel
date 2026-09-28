# AGENTS.md

## Project Context

This repository contains the **Failure Intelligence & Automated Recovery Platform** backend.

The project is currently being developed as a **modular monolith using Java and Spring Boot**.

The current focus is building a reliable failure-event processing pipeline before introducing additional distributed-system complexity.

High-level flow:

API

→ Failure event ingestion

→ Persist raw failure event

→ Processing worker

→ Parse event

→ Normalize event

→ Persist normalized failure event

→ Kafka later

→ Group similar failures

→ Incident processing

Do not assume future components already exist.

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

Do not treat the worker as another persistence layer.

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
4. Create a new component only when the existing design cannot cleanly support the requirement.

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
* alter database schema outside the requested migration,
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

→ `mvn test`

Do not modify tests merely to make a failing implementation pass.

Tests should represent intended behavior.

If an existing test conflicts with the requested architectural change, explain why before changing it.

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

Prefer code that is:

* explicit,
* readable,
* easy to trace,
* easy to test,
* consistent with the surrounding repository.

Avoid cleverness.

Favor clear control flow over unnecessary abstraction.

Do not introduce design patterns unless they provide an actual benefit for the current requirement.

Do not use deprecated or unsupported packages, APIs, functions, or methods in new or changed code. Use the currently supported replacement that is widely adopted as the industry standard for the relevant use case, and verify its support status when uncertain. Deprecated or unsupported components may be removed in future releases and can cause build, runtime, or production failures.

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

Current processing states may include concepts such as:

* RECEIVED
* NORMALIZED
* QUEUED
* PROCESSED
* FAILED

Treat processing status as lifecycle state for event processing.

Do not conflate processing status with normalization quality/status.

---

# Normalization Status

Normalization status describes the quality/result of normalization.

Examples may include:

* FULLY_NORMALIZED
* PARTIALLY_NORMALIZED
* MALFORMED

Do not use processing lifecycle status to represent normalization quality.

---

# Performance Work

When evaluating normalization throughput or pipeline performance:

1. Establish correctness first.
2. Measure the synchronous/local pipeline before Kafka.
3. Use realistic batches of persisted/raw events.
4. Measure throughput, latency, failure rate, and resource behavior.
5. Introduce asynchronous infrastructure only after identifying the actual bottleneck.

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
