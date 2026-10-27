package com.failureintel.infrastructure.persistence.incidentgrouping;

import com.failureintel.infrastructure.persistence.incidentgrouping.entity.GroupingWorkEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.entity.IncidentEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.entity.IncidentMembershipEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentMembershipRepository;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentRepository;
import com.failureintel.ingestion.application.service.ClaimedGroupingWork;
import com.failureintel.ingestion.application.service.GroupingCoordinationService;
import com.failureintel.ingestion.application.service.GroupingWorkCompletionProcessor;
import com.failureintel.test.support.PostgresTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = "spring.jpa.hibernate.ddl-auto=validate")
class IncidentGroupingPersistenceIntegrationTest {
    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        PostgresTestContainer.registerDatabase(registry, "failureintel_normalization_processor_test");
    }

    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private GroupingWorkRepository groupingWorkRepository;
    @Autowired private IncidentRepository incidentRepository;
    @Autowired private IncidentMembershipRepository membershipRepository;
    @Autowired private GroupingCoordinationService coordinationService;
    @Autowired private GroupingWorkCompletionProcessor completionProcessor;
    @Autowired private PlatformTransactionManager transactionManager;

    private final List<UUID> eventIds = new ArrayList<>();
    private final List<UUID> incidentIds = new ArrayList<>();

    @BeforeEach
    void cleanInterruptedFixtures() {
        cleanTestFixtures();
    }

    @AfterEach
    void cleanUp() {
        cleanTestFixtures();
        eventIds.clear();
        incidentIds.clear();
    }

    private void cleanTestFixtures() {
        new TransactionTemplate(transactionManager).executeWithoutResult(transaction -> {
            jdbcTemplate.update("""
                    UPDATE grouping_work SET status = 'FAILED', processing_started_at = NULL,
                        next_attempt_at = NULL, terminal_outcome = NULL, unassigned_reason = NULL
                    WHERE event_id IN (
                        SELECT event_id FROM failure_event WHERE service_name = 'grouping-test'
                    )
                    """);
            jdbcTemplate.update("""
                    DELETE FROM incident_membership
                    WHERE event_id IN (
                        SELECT event_id FROM failure_event WHERE service_name = 'grouping-test'
                    )
                    """);
        });
        jdbcTemplate.update("""
                DELETE FROM grouping_work
                WHERE event_id IN (
                    SELECT event_id FROM failure_event WHERE service_name = 'grouping-test'
                )
                """);
        jdbcTemplate.update("DELETE FROM incident WHERE service_name = 'grouping-test'");
        jdbcTemplate.update("""
                DELETE FROM normalized_failure_event
                WHERE event_id IN (
                    SELECT event_id FROM failure_event WHERE service_name = 'grouping-test'
                )
                """);
        jdbcTemplate.update("DELETE FROM failure_event WHERE service_name = 'grouping-test'");
    }

    @Test
    void persistsGroupingRefs() {
        UUID eventId = newNormalizedEvent();
        UUID incidentId = newIncident("ACTIVE", null);
        LocalDateTime now = LocalDateTime.now();

        new TransactionTemplate(transactionManager).executeWithoutResult(transaction -> {
            groupingWorkRepository.saveAndFlush(new GroupingWorkEntity(
                    eventId, "PROCESSING", 1, now, now, now, null,
                    null, null, null, null));
            membershipRepository.saveAndFlush(new IncidentMembershipEntity(eventId, incidentId));
            jdbcTemplate.update("""
                    UPDATE grouping_work SET status = 'COMPLETED', processing_started_at = NULL,
                        terminal_outcome = 'ASSOCIATED' WHERE event_id = ?
                    """, eventId);
        });

        assertEquals("ASSOCIATED", groupingWorkRepository.findById(eventId).orElseThrow().getTerminalOutcome());
        assertEquals(incidentId, membershipRepository.findById(eventId).orElseThrow().getIncidentId());
        assertEquals(1, membershipRepository.findByIncidentId(incidentId).size());

        UUID invalidEvent = UUID.randomUUID();
        assertThrows(RuntimeException.class, () -> groupingWorkRepository.saveAndFlush(new GroupingWorkEntity(
                invalidEvent, "PENDING", 0, now, null, null, null, null, null, null, null)));
        assertThrows(RuntimeException.class, () -> membershipRepository.saveAndFlush(
                new IncidentMembershipEntity(UUID.randomUUID(), incidentId)));
        UUID anotherEvent = newNormalizedEvent();
        groupingWorkRepository.saveAndFlush(new GroupingWorkEntity(
                anotherEvent, "PENDING", 0, now, null, null, null, null, null, null, null));
        assertThrows(RuntimeException.class, () -> membershipRepository.saveAndFlush(
                new IncidentMembershipEntity(anotherEvent, UUID.randomUUID())));
    }

    @Test
    void checksUnassignedAndHistory() {
        UUID eventId = newNormalizedEvent();
        LocalDateTime now = LocalDateTime.now();
        groupingWorkRepository.saveAndFlush(new GroupingWorkEntity(
                eventId, "COMPLETED", 0, now, null, null, null,
                null, null, "UNASSIGNED", "MISSING_ATTRIBUTES"));

        UUID first = newIncident("CLOSED", now);
        UUID second = newIncident("ACTIVE", null);
        List<IncidentEntity> history = incidentRepository.findHistoryBySignature(
                "grouping-test", "test", "ExampleError", null);
        assertEquals(List.of(second, first), history.stream().map(IncidentEntity::getIncidentId).toList());
        assertEquals("CLOSED", incidentRepository.findById(first).orElseThrow().getStatus());
        assertEquals("ACTIVE", incidentRepository.findById(second).orElseThrow().getStatus());

        UUID invalidEvent = newNormalizedEvent();
        assertThrows(RuntimeException.class, () -> groupingWorkRepository.saveAndFlush(new GroupingWorkEntity(
                invalidEvent, "COMPLETED", 0, now, null, null, null,
                null, null, "UNASSIGNED", null)));
    }

    @Test
    void coordinatesBySignature() throws Exception {
        UUID firstEvent = newClaimedNormalizedEvent("grouping-test", "ExampleError", 1);
        UUID sameSignatureEvent = newClaimedNormalizedEvent("grouping-test", "ExampleError", 1);
        UUID otherSignatureEvent = newClaimedNormalizedEvent("grouping-test", "OtherError", 1);
        CountDownLatch firstHasLock = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch sameEntered = new CountDownLatch(1);
        CountDownLatch differentEntered = new CountDownLatch(1);
        AtomicBoolean secondSawCommittedCandidate = new AtomicBoolean();

        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            var first = executor.submit(() -> coordinationService.withCoordination(
                    new ClaimedGroupingWork(firstEvent, 1), snapshot -> {
                        firstHasLock.countDown();
                        await(releaseFirst);
                        incidentRepository.saveAndFlush(new IncidentEntity(UUID.randomUUID(),
                                snapshot.signature().orElseThrow().serviceName(),
                                snapshot.signature().orElseThrow().environment(),
                                snapshot.signature().orElseThrow().errorType(),
                                snapshot.signature().orElseThrow().dependencyTarget().orElse(null),
                                "ACTIVE", LocalDateTime.now().minusMinutes(1), LocalDateTime.now(),
                                LocalDateTime.now().plusMinutes(10), null, 1));
                    }));
            assertTrue(firstHasLock.await(5, TimeUnit.SECONDS));

            var same = executor.submit(() -> coordinationService.withCoordination(
                    new ClaimedGroupingWork(sameSignatureEvent, 1), snapshot -> {
                        sameEntered.countDown();
                        secondSawCommittedCandidate.set(!snapshot.candidates().isEmpty());
                    }));
            var different = executor.submit(() -> coordinationService.withCoordination(
                    new ClaimedGroupingWork(otherSignatureEvent, 1), snapshot -> differentEntered.countDown()));

            assertTrue(differentEntered.await(5, TimeUnit.SECONDS));
            assertFalse(sameEntered.await(300, TimeUnit.MILLISECONDS));
            releaseFirst.countDown();
            assertTrue(sameEntered.await(5, TimeUnit.SECONDS));
            assertTrue(first.get(5, TimeUnit.SECONDS));
            assertTrue(same.get(5, TimeUnit.SECONDS));
            assertTrue(different.get(5, TimeUnit.SECONDS));
            assertTrue(secondSawCommittedCandidate.get());
        } finally {
            releaseFirst.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void readsExactCandidateNoWrite() {
        UUID eventId = newClaimedEvent("ExampleError");
        LocalDateTime first = LocalDateTime.now().minusHours(3).truncatedTo(ChronoUnit.MICROS);
        LocalDateTime last = first.plusMinutes(5);
        LocalDateTime closedAt = last.plusMinutes(2);
        UUID activeId = UUID.randomUUID();
        UUID closedId = UUID.randomUUID();
        UUID otherDependencyId = UUID.randomUUID();
        UUID otherErrorId = UUID.randomUUID();
        UUID otherServiceId = UUID.randomUUID();
        UUID otherEnvironmentId = UUID.randomUUID();
        incidentIds.addAll(List.of(activeId, closedId, otherDependencyId, otherErrorId,
                otherServiceId, otherEnvironmentId));
        incidentRepository.saveAndFlush(new IncidentEntity(activeId, "grouping-test", "test", "ExampleError",
                null, "ACTIVE", first, last, last.plusMinutes(30), null, 4));
        incidentRepository.saveAndFlush(new IncidentEntity(closedId, "grouping-test", "test", "ExampleError",
                null, "CLOSED", first.minusHours(2), last.minusHours(2), last.plusMinutes(30), closedAt, 7));
        incidentRepository.saveAndFlush(new IncidentEntity(otherDependencyId, "grouping-test", "test",
                "ExampleError", "database", "ACTIVE", first, last, last.plusMinutes(30), null, 1));
        incidentRepository.saveAndFlush(new IncidentEntity(otherErrorId, "grouping-test", "test", "OtherError",
                null, "ACTIVE", first, last, last.plusMinutes(30), null, 1));
        incidentRepository.saveAndFlush(new IncidentEntity(otherServiceId, "another-service", "test", "ExampleError",
                null, "ACTIVE", first, last, last.plusMinutes(30), null, 1));
        incidentRepository.saveAndFlush(new IncidentEntity(otherEnvironmentId, "grouping-test", "production",
                "ExampleError", null, "ACTIVE", first, last, last.plusMinutes(30), null, 1));

        long membershipsBefore = jdbcTemplate.queryForObject("SELECT count(*) FROM incident_membership", Long.class);
        AtomicReference<GroupingCoordinationService.CoordinationSnapshot> observed = new AtomicReference<>();
        assertTrue(coordinationService.withCoordination(claimFor(eventId), observed::set));

        var snapshot = observed.get();
        assertEquals(List.of(activeId, closedId), snapshot.candidates().stream()
                .map(IncidentEntity::getIncidentId).toList());
        IncidentEntity active = snapshot.candidates().get(0);
        assertEquals("ACTIVE", active.getStatus());
        assertEquals(first, active.getFirstOccurredAt());
        assertEquals(last, active.getLastOccurredAt());
        IncidentEntity closed = snapshot.candidates().get(1);
        assertEquals("CLOSED", closed.getStatus());
        assertEquals(first.minusHours(2), closed.getFirstOccurredAt());
        assertEquals(last.minusHours(2), closed.getLastOccurredAt());
        assertEquals(closedAt, closed.getClosedAt());
        assertEquals("PROCESSING", groupingWorkRepository.findById(eventId).orElseThrow().getStatus());
        assertEquals(membershipsBefore, jdbcTemplate.queryForObject(
                "SELECT count(*) FROM incident_membership", Long.class));
        assertEquals(6L, jdbcTemplate.queryForObject("SELECT count(*) FROM incident WHERE service_name IN "
                + "('grouping-test', 'another-service')",
                Long.class));
    }

    @Test
    void checksExpiryAndCorrection() {
        UUID expiryEvent = newClaimedEvent("ExampleError");
        UUID activeId = newIncident("ACTIVE", null);
        IncidentEntity active = incidentRepository.findById(activeId).orElseThrow();
        LocalDateTime exactExpiry = active.getLastOccurredAt().plusMinutes(30);
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = ? WHERE event_id = ?",
                exactExpiry, expiryEvent);

        completeClaim(expiryEvent);

        UUID newIncidentId = membershipRepository.findById(expiryEvent).orElseThrow().getIncidentId();
        assertFalse(newIncidentId.equals(activeId));
        assertEquals("ACTIVE", incidentRepository.findById(activeId).orElseThrow().getStatus());

        UUID correctionEvent = newClaimedEvent("ExampleError");
        LocalDateTime closedAt = LocalDateTime.now().minusHours(25).truncatedTo(ChronoUnit.MICROS);
        UUID closedId = newIncident("CLOSED", closedAt);
        IncidentEntity closed = incidentRepository.findById(closedId).orElseThrow();
        LocalDateTime historicalLast = closedAt.minusHours(1);
        LocalDateTime historicalFirst = historicalLast.minusMinutes(1);
        jdbcTemplate.update("UPDATE incident SET first_occurred_at = ?, last_occurred_at = ?, "
                        + "inactivity_expires_at = ? WHERE incident_id = ?",
                historicalFirst, historicalLast, historicalLast.plusMinutes(30), closedId);
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = ? WHERE event_id = ?",
                historicalLast, correctionEvent);

        completeClaim(correctionEvent);

        assertUnassignedWork(correctionEvent, "HISTORICAL_CORRECTION_EXPIRED");
        IncidentEntity closedAfter = incidentRepository.findById(closedId).orElseThrow();
        assertEquals("CLOSED", closedAfter.getStatus());
        assertEquals(historicalFirst, closedAfter.getFirstOccurredAt());
        assertEquals(historicalLast, closedAfter.getLastOccurredAt());
        assertEquals(closed.getClosedAt(), closedAfter.getClosedAt());
    }

    @Test
    void appliesOccurrenceRules() {
        UUID incidentId = newTimedIncident("ACTIVE", LocalDateTime.of(2026, 1, 1, 12, 0), null);
        UUID forwardEvent = newClaimedEvent("ExampleError");
        LocalDateTime first = LocalDateTime.of(2026, 1, 1, 12, 0);
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = ? WHERE event_id = ?",
                first.plusMinutes(20), forwardEvent);

        completeClaim(forwardEvent);

        IncidentEntity extended = incidentRepository.findById(incidentId).orElseThrow();
        assertEquals(first, extended.getFirstOccurredAt());
        assertEquals(first.plusMinutes(20), extended.getLastOccurredAt());

        UUID backwardEvent = newClaimedEvent("ExampleError");
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = ? WHERE event_id = ?",
                first.minusMinutes(30), backwardEvent);

        completeClaim(backwardEvent);

        IncidentEntity extendedBothWays = incidentRepository.findById(incidentId).orElseThrow();
        assertEquals(first.minusMinutes(30), extendedBothWays.getFirstOccurredAt());
        assertEquals(first.plusMinutes(20), extendedBothWays.getLastOccurredAt());
        assertEquals(incidentId, membershipRepository.findById(backwardEvent).orElseThrow().getIncidentId());
    }

    @Test
    void selectsByOccurrenceTime() {
        LocalDateTime first = LocalDateTime.of(2026, 1, 1, 12, 0);
        UUID incidentId = newTimedIncident("ACTIVE", first, null);
        jdbcTemplate.update("UPDATE incident SET last_occurred_at = ?, inactivity_expires_at = ? WHERE incident_id = ?",
                first.plusMinutes(20), first.plusMinutes(50), incidentId);
        UUID eventId = newClaimedEvent("ExampleError");
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = ?, normalized_at = ? "
                        + "WHERE event_id = ?",
                first.plusMinutes(10), LocalDateTime.of(2026, 10, 26, 12, 0), eventId);

        completeClaim(eventId);

        assertEquals(incidentId, membershipRepository.findById(eventId).orElseThrow().getIncidentId());
        assertEquals(first, incidentRepository.findById(incidentId).orElseThrow().getFirstOccurredAt());
        assertEquals(first.plusMinutes(20), incidentRepository.findById(incidentId).orElseThrow().getLastOccurredAt());
    }

    @Test
    void rejectsOverlappingHistory() {
        LocalDateTime first = LocalDateTime.of(2026, 1, 1, 12, 0);
        UUID activeId = newTimedIncident("ACTIVE", first, null);
        jdbcTemplate.update("UPDATE incident SET last_occurred_at = ?, inactivity_expires_at = ? WHERE incident_id = ?",
                first.plusMinutes(1), first.plusMinutes(31), activeId);
        UUID closedId = UUID.randomUUID();
        incidentIds.add(closedId);
        incidentRepository.saveAndFlush(new IncidentEntity(closedId, "grouping-test", "test", "ExampleError",
                null, "CLOSED", first.minusMinutes(5), first.plusMinutes(5), first.plusMinutes(35),
                first.plusDays(1), 1));
        UUID eventId = newClaimedEvent("ExampleError");
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = ? WHERE event_id = ?",
                first.minusMinutes(10), eventId);

        completeClaim(eventId);

        assertUnassignedWork(eventId, "TEMPORAL_OVERLAP_CONFLICT");
        assertEquals(first, incidentRepository.findById(activeId).orElseThrow().getFirstOccurredAt());
        assertEquals(first.plusMinutes(1), incidentRepository.findById(activeId).orElseThrow().getLastOccurredAt());
        assertEquals("CLOSED", incidentRepository.findById(closedId).orElseThrow().getStatus());
    }

    @Test
    void rejectsAmbiguousHistory() {
        LocalDateTime first = LocalDateTime.of(2026, 1, 1, 12, 0);
        UUID earlier = newTimedIncident("ACTIVE", first, null);
        UUID later = UUID.randomUUID();
        incidentIds.add(later);
        incidentRepository.saveAndFlush(new IncidentEntity(later, "grouping-test", "test", "ExampleError",
                null, "ACTIVE", first.plusMinutes(10), first.plusMinutes(11), first.plusMinutes(41), null, 1));
        UUID eventId = newClaimedEvent("ExampleError");
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = ? WHERE event_id = ?",
                first.plusMinutes(5), eventId);

        completeClaim(eventId);

        assertUnassignedWork(eventId, "AMBIGUOUS_INCIDENT_MATCH");
        assertEquals(1, incidentRepository.findById(earlier).orElseThrow().getAssociatedEventCount());
        assertEquals(1, incidentRepository.findById(later).orElseThrow().getAssociatedEventCount());
    }

    @Test
    void rejectsExpiredClosedHistory() {
        LocalDateTime closedAt = LocalDateTime.of(2026, 1, 1, 12, 0);
        LocalDateTime first = closedAt.minusHours(2);
        UUID closedId = newTimedIncident("CLOSED", first, closedAt);
        jdbcTemplate.update("UPDATE incident SET last_occurred_at = ?, inactivity_expires_at = ? WHERE incident_id = ?",
                first.plusMinutes(1), first.plusMinutes(31), closedId);
        UUID eventId = newClaimedEvent("ExampleError");
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = ? WHERE event_id = ?",
                first.plusSeconds(30), eventId);

        completeClaim(eventId);

        assertUnassignedWork(eventId, "HISTORICAL_CORRECTION_EXPIRED");
        IncidentEntity closed = incidentRepository.findById(closedId).orElseThrow();
        assertEquals("CLOSED", closed.getStatus());
        assertEquals(first, closed.getFirstOccurredAt());
        assertEquals(first.plusMinutes(1), closed.getLastOccurredAt());
        assertEquals(closedAt, closed.getClosedAt());
    }

    @Test
    void fencesStaleClaimsAndLock() {
        UUID eventId = newClaimedNormalizedEvent("grouping-test", "ExampleError", 2);
        AtomicBoolean called = new AtomicBoolean();
        assertFalse(coordinationService.withCoordination(new ClaimedGroupingWork(eventId, 1), snapshot ->
                called.set(true)));
        assertFalse(called.get());
        completionProcessor.process(new ClaimedGroupingWork(eventId, 1));
        assertEquals("PROCESSING", groupingWorkRepository.findById(eventId).orElseThrow().getStatus());

        assertTrue(coordinationService.withCoordination(new ClaimedGroupingWork(eventId, 2), snapshot -> {}));
        assertThrows(RuntimeException.class, () -> coordinationService.withCoordination(
                new ClaimedGroupingWork(eventId, 2), snapshot -> {
                    throw new IllegalStateException("rollback fixture");
                }));
        assertTrue(coordinationService.withCoordination(new ClaimedGroupingWork(eventId, 2), snapshot -> {}));
    }

    @Test
    void completesAssociationOnce() {
        UUID eventId = newClaimedEvent("ExampleError");

        completeClaim(eventId);
        completeClaim(eventId);

        assertCompletedAssociation(eventId, 1);
    }

    @Test
    void retriesCommittedCompletion() {
        UUID eventId = newClaimedEvent("ExampleError");
        ClaimedGroupingWork claim = claimFor(eventId);

        // The caller can lose the response after the proxied transaction commits.
        assertThrows(IllegalStateException.class, () -> {
            completionProcessor.process(claim);
            throw new IllegalStateException("simulated lost completion response");
        });
        assertEquals("COMPLETED", jdbcTemplate.queryForObject(
                "SELECT status FROM grouping_work WHERE event_id = ?", String.class, eventId));

        completionProcessor.process(claim);

        assertCompletedAssociation(eventId, 1);
    }

    @Test
    void keepsMembershipOnReplay() {
        UUID eventId = newClaimedEvent("ExampleError");
        ClaimedGroupingWork claim = claimFor(eventId);
        completionProcessor.process(claim);
        UUID originalIncidentId = membershipRepository.findById(eventId).orElseThrow().getIncidentId();
        long originalAggregate = incidentRepository.findById(originalIncidentId).orElseThrow()
                .getAssociatedEventCount();

        completionProcessor.process(claim);

        assertEquals(originalIncidentId, jdbcTemplate.queryForObject(
                "SELECT incident_id FROM incident_membership WHERE event_id = ?", UUID.class, eventId));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT count(*) FROM incident_membership WHERE event_id = ?", Long.class, eventId));
        assertEquals(originalAggregate, jdbcTemplate.queryForObject(
                "SELECT associated_event_count FROM incident WHERE incident_id = ?", Long.class, originalIncidentId));
        assertEquals("ASSOCIATED", jdbcTemplate.queryForObject(
                "SELECT terminal_outcome FROM grouping_work WHERE event_id = ?", String.class, eventId));
    }

    @Test
    void checksAggregateAndBounds() {
        UUID firstEvent = newClaimedEvent("ExampleError");
        completeClaim(firstEvent);
        UUID incidentId = jdbcTemplate.queryForObject(
                "SELECT incident_id FROM incident_membership WHERE event_id = ?", UUID.class, firstEvent);
        LocalDateTime boundary = jdbcTemplate.queryForObject(
                "SELECT last_occurred_at FROM incident WHERE incident_id = ?", LocalDateTime.class, incidentId);
        UUID secondEvent = newClaimedEvent("ExampleError");
        LocalDateTime laterOccurrence = boundary.plusSeconds(1);
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = ? WHERE event_id = ?",
                laterOccurrence, secondEvent);

        completeClaim(secondEvent);

        assertEquals(2, jdbcTemplate.queryForObject(
                "SELECT associated_event_count FROM incident WHERE incident_id = ?", Long.class, incidentId));
        assertEquals(2, jdbcTemplate.queryForObject(
                "SELECT count(*) FROM incident_membership WHERE incident_id = ?", Long.class, incidentId));
        assertEquals(jdbcTemplate.queryForObject("""
                SELECT MIN(n.normalized_occurred_at) FROM incident_membership m
                JOIN normalized_failure_event n ON n.event_id = m.event_id WHERE m.incident_id = ?
                """, LocalDateTime.class, incidentId), jdbcTemplate.queryForObject(
                "SELECT first_occurred_at FROM incident WHERE incident_id = ?", LocalDateTime.class, incidentId));
        assertEquals(jdbcTemplate.queryForObject("""
                SELECT MAX(n.normalized_occurred_at) FROM incident_membership m
                JOIN normalized_failure_event n ON n.event_id = m.event_id WHERE m.incident_id = ?
                """, LocalDateTime.class, incidentId), jdbcTemplate.queryForObject(
                "SELECT last_occurred_at FROM incident WHERE incident_id = ?", LocalDateTime.class, incidentId));
        assertEquals(laterOccurrence, jdbcTemplate.queryForObject(
                "SELECT last_occurred_at FROM incident WHERE incident_id = ?", LocalDateTime.class, incidentId));
    }

    @Test
    void associatesExistingIncident() {
        UUID associatedEvent = newClaimedEvent("ExampleError");
        UUID incidentId = newIncident("ACTIVE", null);

        completeClaim(associatedEvent);

        assertEquals(incidentId, membershipRepository.findById(associatedEvent).orElseThrow().getIncidentId());
        assertEquals(2, incidentRepository.findById(incidentId).orElseThrow().getAssociatedEventCount());
        assertCompletedWork(associatedEvent, "ASSOCIATED");
    }

    @Test
    void unassignsNoOccurrence() {
        UUID unassignedEvent = newClaimedEvent("ExampleError");
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = NULL WHERE event_id = ?",
                unassignedEvent);

        completeClaim(unassignedEvent);

        assertUnassignedWork(unassignedEvent, "MISSING_OCCURRED_AT");
    }

    @Test
    void unassignsNoSignature() {
        UUID missingSignatureEvent = newClaimedEvent("ExampleError");
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_service_name = NULL WHERE event_id = ?",
                missingSignatureEvent);

        completeClaim(missingSignatureEvent);

        assertUnassignedWork(missingSignatureEvent, "MISSING_GROUPING_ATTRIBUTES");
    }

    @Test
    void rollsBackOnInsertFailure() {
        UUID eventId = newClaimedEvent("ExampleError");
        UUID incidentId = newIncident("ACTIVE", null);
        IncidentEntity incidentBefore = incidentRepository.findById(incidentId).orElseThrow();

        installMembershipFailure();
        try {
            assertThrows(RuntimeException.class, () -> completeClaim(eventId));
        } finally {
            removeMembershipFailure();
        }

        assertGroupingRollback(eventId, incidentId, incidentBefore);
    }

    @Test
    void acceptsInBoundsCorrection() {
        UUID eventId = newClaimedNormalizedEvent("grouping-test", "ExampleError", 1);
        UUID incidentId = newIncident("CLOSED", LocalDateTime.now());
        IncidentEntity closed = incidentRepository.findById(incidentId).orElseThrow();
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = ? WHERE event_id = ?",
                closed.getLastOccurredAt(), eventId);

        completionProcessor.process(new ClaimedGroupingWork(eventId, 1));

        assertEquals(incidentId, membershipRepository.findById(eventId).orElseThrow().getIncidentId());
        IncidentEntity after = incidentRepository.findById(incidentId).orElseThrow();
        assertEquals("CLOSED", after.getStatus());
        assertEquals(1, after.getAssociatedEventCount());
        assertEquals(closed.getFirstOccurredAt(), after.getFirstOccurredAt());
        assertEquals(closed.getLastOccurredAt(), after.getLastOccurredAt());
    }

    @Test
    void groupsConcurrentEventsOnce() throws Exception {
        UUID firstEvent = newClaimedNormalizedEvent("grouping-test", "ExampleError", 1);
        UUID secondEvent = newClaimedNormalizedEvent("grouping-test", "ExampleError", 1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> completionProcessor.process(new ClaimedGroupingWork(firstEvent, 1)));
            var second = executor.submit(() -> completionProcessor.process(new ClaimedGroupingWork(secondEvent, 1)));
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        List<IncidentEntity> incidents = incidentRepository.findHistoryBySignature(
                "grouping-test", "test", "ExampleError", null);
        assertEquals(1, incidents.size());
        assertEquals(2, incidents.get(0).getAssociatedEventCount());
        assertEquals(2, membershipRepository.findByIncidentId(incidents.get(0).getIncidentId()).size());
    }

    private UUID newNormalizedEvent() {
        UUID eventId = UUID.randomUUID();
        eventIds.add(eventId);
        jdbcTemplate.update("""
                INSERT INTO failure_event (
                    event_id, occurred_at, ingested_at, service_name, environment,
                    event_type, processing_status, version
                ) VALUES (?, now(), now(), 'grouping-test', 'test', 'exception', 'NORMALIZED', 0)
                """, eventId);
        jdbcTemplate.update("""
                INSERT INTO normalized_failure_event(event_id, normalization_status)
                VALUES (?, 'FULLY_NORMALIZED')
                """, eventId);
        jdbcTemplate.update("""
                UPDATE normalized_failure_event SET normalized_service_name = 'grouping-test',
                    normalized_environment = 'test', normalized_error_type = 'ExampleError',
                    normalized_occurred_at = now(), normalization_metadata = '{}'::jsonb
                WHERE event_id = ?
                """, eventId);
        return eventId;
    }

    private UUID newClaimedNormalizedEvent(String serviceName, String errorType, int attempt) {
        UUID eventId = newNormalizedEvent();
        jdbcTemplate.update("""
                UPDATE normalized_failure_event
                SET normalized_service_name = ?, normalized_environment = 'test', normalized_error_type = ?,
                    normalization_status = 'FULLY_NORMALIZED', normalization_metadata = '{}'::jsonb
                WHERE event_id = ?
                """, serviceName, errorType, eventId);
        LocalDateTime now = LocalDateTime.now();
        groupingWorkRepository.saveAndFlush(new GroupingWorkEntity(
                eventId, "PROCESSING", attempt, now, now, now, null, null, null, null, null));
        return eventId;
    }

    private UUID newClaimedEvent(String errorType) {
        return newClaimedNormalizedEvent("grouping-test", errorType, 1);
    }

    private ClaimedGroupingWork claimFor(UUID eventId) {
        return new ClaimedGroupingWork(eventId, 1);
    }

    private void completeClaim(UUID eventId) {
        completionProcessor.process(claimFor(eventId));
    }

    private void assertCompletedAssociation(UUID eventId, long expectedAggregateCount) {
        IncidentMembershipEntity membership = membershipRepository.findById(eventId).orElseThrow();
        IncidentEntity incident = incidentRepository.findById(membership.getIncidentId()).orElseThrow();

        assertCompletedWork(eventId, "ASSOCIATED");
        assertEquals(expectedAggregateCount, incident.getAssociatedEventCount());
        assertEquals(expectedAggregateCount,
                membershipRepository.findByIncidentId(incident.getIncidentId()).size());
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT count(*) FROM incident_membership WHERE event_id = ?", Long.class, eventId));
        assertEquals(1, jdbcTemplate.queryForObject("SELECT count(*) FROM grouping_work WHERE event_id = ? "
                + "AND status = 'COMPLETED' AND terminal_outcome = 'ASSOCIATED'", Long.class, eventId));
    }

    private void assertCompletedWork(UUID eventId, String outcome) {
        GroupingWorkEntity work = groupingWorkRepository.findById(eventId).orElseThrow();
        assertEquals("COMPLETED", work.getStatus());
        assertEquals(outcome, work.getTerminalOutcome());
        assertEquals(1, jdbcTemplate.queryForObject("SELECT count(*) FROM grouping_work WHERE event_id = ? "
                + "AND status = 'COMPLETED' AND terminal_outcome = ?", Long.class, eventId, outcome));
    }

    private void assertUnassignedWork(UUID eventId, String reason) {
        GroupingWorkEntity work = groupingWorkRepository.findById(eventId).orElseThrow();
        assertCompletedWork(eventId, "UNASSIGNED");
        assertEquals(reason, work.getUnassignedReason());
        assertFalse(membershipRepository.existsById(eventId));
    }

    private void installMembershipFailure() {
        jdbcTemplate.execute("CREATE FUNCTION fail_grouping_membership_insert() RETURNS trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN RAISE EXCEPTION 'injected membership failure'; END $$");
        jdbcTemplate.execute("CREATE TRIGGER trg_fail_grouping_membership_insert BEFORE INSERT "
                + "ON incident_membership FOR EACH ROW EXECUTE FUNCTION fail_grouping_membership_insert()");
    }

    private void removeMembershipFailure() {
        jdbcTemplate.execute("DROP TRIGGER IF EXISTS trg_fail_grouping_membership_insert ON incident_membership");
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS fail_grouping_membership_insert()");
    }

    private void assertGroupingRollback(
            UUID eventId, UUID incidentId, IncidentEntity incidentBefore) {
        assertEquals("PROCESSING", groupingWorkRepository.findById(eventId).orElseThrow().getStatus());
        assertFalse(membershipRepository.existsById(eventId));
        assertEquals(List.of(incidentId), incidentRepository.findHistoryBySignature(
                "grouping-test", "test", "ExampleError", null).stream()
                .map(IncidentEntity::getIncidentId).toList());
        assertEquals("NORMALIZED", jdbcTemplate.queryForObject(
                "SELECT processing_status FROM failure_event WHERE event_id = ?", String.class, eventId));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT count(*) FROM normalized_failure_event WHERE event_id = ?", Long.class, eventId));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM grouping_work WHERE event_id = ?", Integer.class, eventId));
        assertEquals(0, jdbcTemplate.queryForObject(
                "SELECT count(*) FROM grouping_work WHERE event_id = ? AND status = 'COMPLETED'", Long.class, eventId));

        IncidentEntity incidentAfter = incidentRepository.findById(incidentId).orElseThrow();
        assertEquals(incidentBefore.getAssociatedEventCount(), incidentAfter.getAssociatedEventCount());
        assertEquals(incidentBefore.getFirstOccurredAt(), incidentAfter.getFirstOccurredAt());
        assertEquals(incidentBefore.getLastOccurredAt(), incidentAfter.getLastOccurredAt());
        assertEquals(0, membershipRepository.findByIncidentId(incidentId).size());
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for test coordination");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for test coordination", exception);
        }
    }

    private UUID newIncident(String status, LocalDateTime closedAt) {
        UUID incidentId = UUID.randomUUID();
        incidentIds.add(incidentId);
        LocalDateTime now = LocalDateTime.now();
        incidentRepository.saveAndFlush(new IncidentEntity(incidentId, "grouping-test", "test",
                "ExampleError", null, status, now.minusMinutes(1), now, now.plusMinutes(10), closedAt, 1));
        return incidentId;
    }

    private UUID newTimedIncident(String status, LocalDateTime first, LocalDateTime closedAt) {
        UUID incidentId = UUID.randomUUID();
        incidentIds.add(incidentId);
        incidentRepository.saveAndFlush(new IncidentEntity(incidentId, "grouping-test", "test",
                "ExampleError", null, status, first, first, first.plusMinutes(30), closedAt, 1));
        return incidentId;
    }
}
