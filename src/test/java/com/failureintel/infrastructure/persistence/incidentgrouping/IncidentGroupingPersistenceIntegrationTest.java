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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    void removeFixturesLeftByInterruptedRuns() {
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
    void persistsWorkMembershipAndTerminalOutcomeAndRejectsInvalidReferences() {
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
    void enforcesUnassignedTerminalRepresentationAndFindsIncidentHistoryBySignature() {
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
    void sameSignatureWaiterSeesPreviousCommitAndDifferentSignatureProceedsIndependently() throws Exception {
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
    void staleClaimDoesNotRunAndBothCommitAndRollbackReleaseSignatureLock() {
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
    void processorCreatesIncidentAndCompletesAssociatedOutcomeExactlyOnce() {
        UUID eventId = newClaimedNormalizedEvent("grouping-test", "ExampleError", 1);

        completionProcessor.process(new ClaimedGroupingWork(eventId, 1));
        completionProcessor.process(new ClaimedGroupingWork(eventId, 1));

        GroupingWorkEntity work = groupingWorkRepository.findById(eventId).orElseThrow();
        IncidentMembershipEntity membership = membershipRepository.findById(eventId).orElseThrow();
        IncidentEntity incident = incidentRepository.findById(membership.getIncidentId()).orElseThrow();
        assertEquals("COMPLETED", work.getStatus());
        assertEquals("ASSOCIATED", work.getTerminalOutcome());
        assertEquals(1, incident.getAssociatedEventCount());
        assertEquals(1, membershipRepository.findByIncidentId(incident.getIncidentId()).size());
    }

    @Test
    void processorAssociatesExistingIncidentAndPersistsExplicitUnassignedReason() {
        UUID associatedEvent = newClaimedNormalizedEvent("grouping-test", "ExampleError", 1);
        UUID incidentId = newIncident("ACTIVE", null);
        completionProcessor.process(new ClaimedGroupingWork(associatedEvent, 1));
        assertEquals(incidentId, membershipRepository.findById(associatedEvent).orElseThrow().getIncidentId());
        assertEquals(2, incidentRepository.findById(incidentId).orElseThrow().getAssociatedEventCount());

        UUID unassignedEvent = newClaimedNormalizedEvent("grouping-test", "ExampleError", 1);
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = NULL WHERE event_id = ?",
                unassignedEvent);
        completionProcessor.process(new ClaimedGroupingWork(unassignedEvent, 1));
        GroupingWorkEntity work = groupingWorkRepository.findById(unassignedEvent).orElseThrow();
        assertEquals("COMPLETED", work.getStatus());
        assertEquals("UNASSIGNED", work.getTerminalOutcome());
        assertEquals("MISSING_OCCURRED_AT", work.getUnassignedReason());
        assertFalse(membershipRepository.existsById(unassignedEvent));

        UUID missingSignatureEvent = newClaimedNormalizedEvent("grouping-test", "ExampleError", 1);
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_service_name = NULL WHERE event_id = ?",
                missingSignatureEvent);
        completionProcessor.process(new ClaimedGroupingWork(missingSignatureEvent, 1));
        assertEquals("MISSING_GROUPING_ATTRIBUTES", groupingWorkRepository.findById(missingSignatureEvent)
                .orElseThrow().getUnassignedReason());
        assertFalse(membershipRepository.existsById(missingSignatureEvent));
    }

    @Test
    void membershipPersistenceFailureRollsBackIncidentAndCompletion() {
        UUID eventId = newClaimedNormalizedEvent("grouping-test", "ExampleError", 1);
        jdbcTemplate.execute("CREATE FUNCTION fail_grouping_membership_insert() RETURNS trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN RAISE EXCEPTION 'injected membership failure'; END $$");
        jdbcTemplate.execute("CREATE TRIGGER trg_fail_grouping_membership_insert BEFORE INSERT "
                + "ON incident_membership FOR EACH ROW EXECUTE FUNCTION fail_grouping_membership_insert()");
        try {
            assertThrows(RuntimeException.class,
                    () -> completionProcessor.process(new ClaimedGroupingWork(eventId, 1)));
        } finally {
            jdbcTemplate.execute("DROP TRIGGER IF EXISTS trg_fail_grouping_membership_insert ON incident_membership");
            jdbcTemplate.execute("DROP FUNCTION IF EXISTS fail_grouping_membership_insert()");
        }
        assertEquals("PROCESSING", groupingWorkRepository.findById(eventId).orElseThrow().getStatus());
        assertFalse(membershipRepository.existsById(eventId));
        assertTrue(incidentRepository.findHistoryBySignature("grouping-test", "test", "ExampleError", null)
                .isEmpty());
    }

    @Test
    void closedIncidentAcceptsOnlyAnInBoundsHistoricalCorrectionWithoutChangingAggregates() {
        UUID eventId = newClaimedNormalizedEvent("grouping-test", "ExampleError", 1);
        UUID incidentId = newIncident("CLOSED", LocalDateTime.now());
        IncidentEntity closed = incidentRepository.findById(incidentId).orElseThrow();
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_occurred_at = ? WHERE event_id = ?",
                java.sql.Timestamp.from(closed.getLastOccurredAt().toInstant(java.time.ZoneOffset.UTC)), eventId);

        completionProcessor.process(new ClaimedGroupingWork(eventId, 1));

        assertEquals(incidentId, membershipRepository.findById(eventId).orElseThrow().getIncidentId());
        IncidentEntity after = incidentRepository.findById(incidentId).orElseThrow();
        assertEquals("CLOSED", after.getStatus());
        assertEquals(1, after.getAssociatedEventCount());
        assertEquals(closed.getFirstOccurredAt(), after.getFirstOccurredAt());
        assertEquals(closed.getLastOccurredAt(), after.getLastOccurredAt());
    }

    @Test
    void concurrentSameSignatureCompletionCreatesOneIncidentAndConsistentMemberships() throws Exception {
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
}
