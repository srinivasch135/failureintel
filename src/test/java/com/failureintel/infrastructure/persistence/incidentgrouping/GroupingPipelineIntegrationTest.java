package com.failureintel.infrastructure.persistence.incidentgrouping;

import com.failureintel.ingestion.api.dto.FailureEventIngestionRequest;
import com.failureintel.ingestion.application.service.ClaimedFailureEvent;
import com.failureintel.ingestion.application.service.ClaimedGroupingWork;
import com.failureintel.ingestion.application.service.FailureEventClaimService;
import com.failureintel.ingestion.application.service.FailureEventIngestionService;
import com.failureintel.ingestion.application.service.FailureEventNormalizationProcessor;
import com.failureintel.ingestion.application.service.GroupingWorkClaimService;
import com.failureintel.ingestion.application.service.GroupingWorkRecoveryService;
import com.failureintel.ingestion.application.service.GroupingWorkProcessingWorker;
import com.failureintel.ingestion.application.service.GroupingWorkRunner;
import com.failureintel.ingestion.application.service.IncidentClosureProcessor;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import com.failureintel.test.support.FailureEventTestFixtures;
import com.failureintel.test.support.PostgresTestContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest(properties = {
        "spring.jpa.hibernate.ddl-auto=validate",
        "failure-event.processing.worker.enabled=false",
        "failure-event.grouping.worker.enabled=true",
        "failure-event.grouping.worker.fixed-delay=PT1H",
        "failure-event.grouping.worker.processing-lease-timeout=PT1M"
})
class GroupingPipelineIntegrationTest {

    private static final String TRACE_PREFIX = "checkpoint121-";

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        PostgresTestContainer.registerDatabase(registry, "failureintel_grouping_pipeline_test");
    }

    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private FailureEventIngestionService ingestionService;
    @Autowired private FailureEventClaimService eventClaimService;
    @Autowired private FailureEventNormalizationProcessor normalizationProcessor;
    @Autowired private FailureEventRepository failureEventRepository;
    @Autowired private NormalizedFailureEventRepository normalizedRepository;
    @Autowired private GroupingWorkClaimService workClaimService;
    @MockitoSpyBean private GroupingWorkRunner workRunner;
    @Autowired private GroupingWorkProcessingWorker groupingWorker;
    @Autowired private GroupingWorkRecoveryService workRecoveryService;
    @Autowired private GroupingWorkRepository workRepository;
    @Autowired private IncidentClosureProcessor closureProcessor;
    @Autowired private PlatformTransactionManager transactionManager;

    @AfterEach
    void cleanUp() {
        jdbcTemplate.execute("DROP TRIGGER IF EXISTS trg_fail_grouping_insert ON grouping_work");
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS fail_grouping_insert()");
        jdbcTemplate.execute("DROP TRIGGER IF EXISTS trg_fail_membership_insert ON incident_membership");
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS fail_membership_insert()");
        new TransactionTemplate(transactionManager).executeWithoutResult(transaction -> {
            jdbcTemplate.update("UPDATE grouping_work SET status = 'FAILED', processing_started_at = NULL, "
                    + "next_attempt_at = NULL, terminal_outcome = NULL, unassigned_reason = NULL WHERE event_id IN "
                    + "(SELECT event_id FROM failure_event WHERE trace_id LIKE ?)", TRACE_PREFIX + "%");
            jdbcTemplate.update("DELETE FROM incident_membership WHERE event_id IN "
                    + "(SELECT event_id FROM failure_event WHERE trace_id LIKE ?)", TRACE_PREFIX + "%");
        });
        jdbcTemplate.update("DELETE FROM grouping_work WHERE event_id IN "
                + "(SELECT event_id FROM failure_event WHERE trace_id LIKE ?)", TRACE_PREFIX + "%");
        jdbcTemplate.update("DELETE FROM incident WHERE incident_id IN (SELECT incident_id "
                + "FROM incident_membership WHERE event_id IN "
                + "(SELECT event_id FROM failure_event WHERE trace_id LIKE ?))", TRACE_PREFIX + "%");
        jdbcTemplate.update("DELETE FROM normalized_failure_event WHERE event_id IN "
                + "(SELECT event_id FROM failure_event WHERE trace_id LIKE ?)", TRACE_PREFIX + "%");
        jdbcTemplate.update("DELETE FROM incident WHERE service_name = 'payment-service' AND incident_id NOT IN "
                + "(SELECT incident_id FROM incident_membership)");
        jdbcTemplate.update("DELETE FROM failure_event WHERE trace_id LIKE ?", TRACE_PREFIX + "%");
    }

    @Test
    void normalizedCreatesWork() {
        UUID eventId = normalize("normal-one", Instant.parse("2026-08-03T20:00:00Z"));
        assertEquals(ProcessingStatus.NORMALIZED,
                failureEventRepository.findById(eventId).orElseThrow().getProcessingStatus());
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT count(*) FROM normalized_failure_event WHERE event_id = ?", Integer.class, eventId));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT count(*) FROM grouping_work WHERE event_id = ?", Integer.class, eventId));
        assertEquals("PENDING", workStatus(eventId));

        UUID rollbackId = ingest("normal-rollback", Instant.parse("2026-08-03T20:01:00Z"));
        jdbcTemplate.execute("CREATE FUNCTION fail_grouping_insert() RETURNS trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN RAISE EXCEPTION 'injected grouping insert failure'; END $$");
        jdbcTemplate.execute("CREATE TRIGGER trg_fail_grouping_insert BEFORE INSERT ON grouping_work "
                + "FOR EACH ROW EXECUTE FUNCTION fail_grouping_insert()");
        assertThrows(RuntimeException.class, () -> normalizeIngested(rollbackId));
        jdbcTemplate.execute("DROP TRIGGER trg_fail_grouping_insert ON grouping_work");
        jdbcTemplate.execute("DROP FUNCTION fail_grouping_insert()");

        assertEquals(ProcessingStatus.PROCESSING,
                failureEventRepository.findById(rollbackId).orElseThrow().getProcessingStatus());
        assertFalse(normalizedRepository.existsById(rollbackId));
        assertEquals(0, jdbcTemplate.queryForObject(
                "SELECT count(*) FROM grouping_work WHERE event_id = ?", Integer.class, rollbackId));
    }

    @Test
    void groupingHasOneOutcome() {
        UUID associated = normalize("outcome-associated", Instant.parse("2026-08-03T20:00:00Z"));
        finish(associated);
        assertEquals("COMPLETED", workStatus(associated));
        assertEquals("ASSOCIATED", terminalOutcome(associated));
        assertEquals(1, membershipCount(associated));
        assertExclusiveOutcome(associated);

        UUID unassigned = normalize("outcome-unassigned", Instant.parse("2026-08-03T20:00:00Z"));
        jdbcTemplate.update("UPDATE normalized_failure_event SET normalized_service_name = NULL WHERE event_id = ?",
                unassigned);
        finish(unassigned);
        assertEquals("COMPLETED", workStatus(unassigned));
        assertEquals("UNASSIGNED", terminalOutcome(unassigned));
        assertEquals("MISSING_GROUPING_ATTRIBUTES", jdbcTemplate.queryForObject(
                "SELECT unassigned_reason FROM grouping_work WHERE event_id = ?", String.class, unassigned));
        assertEquals(0, membershipCount(unassigned));
        assertExclusiveOutcome(unassigned);
    }

    @Test
    void matchesWithinWindow() {
        UUID first = normalize("window-first", Instant.parse("2026-08-03T20:00:00Z"));
        finish(first);
        UUID second = normalize("window-second", Instant.parse("2026-08-03T20:20:00Z"));
        finish(second);

        UUID incidentId = incidentFor(first);
        assertEquals(incidentId, incidentFor(second));
        assertEquals(2, jdbcTemplate.queryForObject(
                "SELECT associated_event_count FROM incident WHERE incident_id = ?", Integer.class, incidentId));
        assertEquals(2, jdbcTemplate.queryForObject(
                "SELECT count(*) FROM incident_membership WHERE incident_id = ?", Integer.class, incidentId));
        assertEquals(1, membershipCount(first));
        assertEquals(1, membershipCount(second));
        assertEquals(2, workCount("COMPLETED", incidentId));
    }

    @Test
    void honorsTemporalBounds() {
        Instant base = Instant.parse("2026-08-03T20:00:00Z");
        UUID first = normalize("bounds-first", base);
        finish(first);
        UUID expiry = normalize("bounds-expiry", base.plusSeconds(1800));
        finish(expiry);
        UUID backward = normalize("bounds-backward", base.minusSeconds(1800));
        finish(backward);

        UUID firstIncident = incidentFor(first);
        UUID secondIncident = incidentFor(expiry);
        assertNotEquals(firstIncident, secondIncident);
        assertEquals(firstIncident, incidentFor(backward));
        assertEquals(2, jdbcTemplate.queryForObject(
                "SELECT associated_event_count FROM incident WHERE incident_id = ?", Integer.class, firstIncident));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT associated_event_count FROM incident WHERE incident_id = ?", Integer.class, secondIncident));
        assertEquals(LocalDateTime.ofInstant(base.minusSeconds(1800), ZoneOffset.UTC), jdbcTemplate.queryForObject(
                "SELECT first_occurred_at FROM incident WHERE incident_id = ?", LocalDateTime.class, firstIncident));
        assertEquals(LocalDateTime.ofInstant(base.plusSeconds(1800), ZoneOffset.UTC), jdbcTemplate.queryForObject(
                "SELECT first_occurred_at FROM incident WHERE incident_id = ?", LocalDateTime.class, secondIncident));
        assertEquals(2, jdbcTemplate.queryForObject(
                "SELECT count(*) FROM incident WHERE incident_id IN (?, ?)", Integer.class,
                firstIncident, secondIncident));
    }

    @Test
    void correctsClosedIncident() {
        Instant occurredAt = databaseNow().minusSeconds(60);
        UUID original = normalize("correction-original", occurredAt);
        finish(original);
        UUID incidentId = incidentFor(original);
        expireIncident(incidentId);
        assertTrue(closureProcessor.closeIfExpired(incidentId));
        LocalDateTime closedAt = incidentTime(incidentId, "closed_at");
        LocalDateTime firstAt = incidentTime(incidentId, "first_occurred_at");
        LocalDateTime lastAt = incidentTime(incidentId, "last_occurred_at");

        UUID correction = normalize("correction-valid", occurredAt);
        finish(correction);
        assertEquals(incidentId, incidentFor(correction));
        assertEquals("ASSOCIATED", terminalOutcome(correction));
        assertEquals(2, jdbcTemplate.queryForObject(
                "SELECT associated_event_count FROM incident WHERE incident_id = ?", Integer.class, incidentId));
        assertEquals("CLOSED", incidentStatus(incidentId));
        assertEquals(closedAt, incidentTime(incidentId, "closed_at"));
        assertEquals(firstAt, incidentTime(incidentId, "first_occurred_at"));
        assertEquals(lastAt, incidentTime(incidentId, "last_occurred_at"));

        UUID expiredOriginal = normalize("correction-expired-original", occurredAt.plusSeconds(60));
        finish(expiredOriginal);
        UUID expiredIncident = incidentFor(expiredOriginal);
        expireIncident(expiredIncident);
        assertTrue(closureProcessor.closeIfExpired(expiredIncident));
        jdbcTemplate.update("UPDATE incident SET closed_at = (clock_timestamp() AT TIME ZONE 'UTC') "
                + "- INTERVAL '25 hours' WHERE incident_id = ?", expiredIncident);
        LocalDateTime expiredClosedAt = incidentTime(expiredIncident, "closed_at");
        LocalDateTime expiredFirst = incidentTime(expiredIncident, "first_occurred_at");
        LocalDateTime expiredLast = incidentTime(expiredIncident, "last_occurred_at");
        UUID expiredCorrection = normalize("correction-expired", occurredAt.plusSeconds(60));
        finish(expiredCorrection);
        assertEquals("COMPLETED", workStatus(expiredCorrection));
        assertEquals("UNASSIGNED", terminalOutcome(expiredCorrection));
        assertEquals("HISTORICAL_CORRECTION_EXPIRED", jdbcTemplate.queryForObject(
                "SELECT unassigned_reason FROM grouping_work WHERE event_id = ?", String.class, expiredCorrection));
        assertEquals(0, membershipCount(expiredCorrection));
        assertEquals("CLOSED", incidentStatus(expiredIncident));
        assertEquals(expiredClosedAt, incidentTime(expiredIncident, "closed_at"));
        assertEquals(expiredFirst, incidentTime(expiredIncident, "first_occurred_at"));
        assertEquals(expiredLast, incidentTime(expiredIncident, "last_occurred_at"));
    }

    @Test
    void recoversInterruptedWork() {
        UUID eventId = normalize("recovery-replay", Instant.parse("2026-08-03T20:00:00Z"));
        ClaimedGroupingWork firstClaim = claim(eventId);
        jdbcTemplate.execute("CREATE FUNCTION fail_membership_insert() RETURNS trigger LANGUAGE plpgsql "
                + "AS $$ BEGIN RAISE EXCEPTION 'injected membership failure' USING ERRCODE = '40001'; END $$");
        jdbcTemplate.execute("CREATE TRIGGER trg_fail_membership_insert BEFORE INSERT ON incident_membership "
                + "FOR EACH ROW EXECUTE FUNCTION fail_membership_insert()");
        workRunner.process(firstClaim);
        assertEquals("RETRYABLE", workStatus(eventId));
        assertEquals(ProcessingStatus.NORMALIZED,
                failureEventRepository.findById(eventId).orElseThrow().getProcessingStatus());
        assertTrue(normalizedRepository.existsById(eventId));
        assertEquals(0, membershipCount(eventId));

        jdbcTemplate.execute("DROP TRIGGER trg_fail_membership_insert ON incident_membership");
        jdbcTemplate.execute("DROP FUNCTION fail_membership_insert()");
        makeDue(eventId);
        ClaimedGroupingWork secondClaim = claim(eventId);
        expireWork(eventId);
        assertEquals(1, workRecoveryService.recoverExpiredClaims(10));
        assertEquals("RETRYABLE", workStatus(eventId));
        makeDue(eventId);
        ClaimedGroupingWork currentClaim = claim(eventId);
        assertTrue(currentClaim.attemptNumber() > secondClaim.attemptNumber());

        workRunner.process(secondClaim);
        assertEquals("PROCESSING", workStatus(eventId));
        workRunner.process(currentClaim);
        workRunner.process(currentClaim);

        assertEquals("COMPLETED", workStatus(eventId));
        assertEquals("ASSOCIATED", terminalOutcome(eventId));
        assertEquals(1, membershipCount(eventId));
        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT associated_event_count FROM incident WHERE incident_id = ?", Integer.class,
                incidentFor(eventId)));
        assertEquals(1, workCount("COMPLETED", incidentFor(eventId)));
        assertEquals(ProcessingStatus.NORMALIZED,
                failureEventRepository.findById(eventId).orElseThrow().getProcessingStatus());
    }

    private UUID normalize(String suffix, Instant occurredAt) {
        UUID eventId = ingest(suffix, occurredAt);
        normalizeIngested(eventId);
        return eventId;
    }

    private UUID ingest(String suffix, Instant occurredAt) {
        FailureEventIngestionRequest request = FailureEventTestFixtures.validRequest(TRACE_PREFIX + suffix);
        request.setOccurredAt(occurredAt);
        return UUID.fromString(ingestionService.ingestFailureEvent(request));
    }

    private void normalizeIngested(UUID eventId) {
        ClaimedFailureEvent claim = eventClaimService.claimNextEligibleForProcessing(10).stream()
                .filter(candidate -> candidate.eventId().equals(eventId))
                .findFirst().orElseThrow(() -> new AssertionError("Expected normalization claim for " + eventId));
        normalizationProcessor.process(claim);
    }

    private void finish(UUID eventId) {
        CountDownLatch completed = new CountDownLatch(1);
        doAnswer(invocation -> {
            try {
                invocation.callRealMethod();
            } finally {
                completed.countDown();
            }
            return null;
        }).when(workRunner).process(any());
        groupingWorker.processNextBatch();
        assertTrue(await(completed), "grouping worker did not finish event " + eventId);
        assertEquals("COMPLETED", workStatus(eventId));
    }

    private ClaimedGroupingWork claim(UUID eventId) {
        return workClaimService.claimNextEligibleForProcessing(10).stream()
                .filter(candidate -> candidate.eventId().equals(eventId))
                .findFirst().orElseThrow(() -> new AssertionError("Expected grouping claim for " + eventId));
    }

    private void makeDue(UUID eventId) {
        jdbcTemplate.update("UPDATE grouping_work SET next_attempt_at = ? WHERE event_id = ?",
                workRepository.currentDatabaseTime().minusSeconds(1), eventId);
    }

    private void expireWork(UUID eventId) {
        jdbcTemplate.update("UPDATE grouping_work SET processing_started_at = ? WHERE event_id = ?",
                workRepository.currentDatabaseTime().minusMinutes(2), eventId);
    }

    private void expireIncident(UUID incidentId) {
        jdbcTemplate.update("UPDATE incident SET inactivity_expires_at = "
                + "(clock_timestamp() AT TIME ZONE 'UTC') - INTERVAL '1 second' WHERE incident_id = ?", incidentId);
    }

    private String workStatus(UUID eventId) {
        return jdbcTemplate.queryForObject("SELECT status FROM grouping_work WHERE event_id = ?", String.class, eventId);
    }

    private String terminalOutcome(UUID eventId) {
        return jdbcTemplate.queryForObject("SELECT terminal_outcome FROM grouping_work WHERE event_id = ?",
                String.class, eventId);
    }

    private int membershipCount(UUID eventId) {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM incident_membership WHERE event_id = ?",
                Integer.class, eventId);
    }

    private void assertExclusiveOutcome(UUID eventId) {
        assertEquals(1, jdbcTemplate.queryForObject("SELECT count(*) FROM grouping_work WHERE event_id = ? "
                + "AND status = 'COMPLETED' AND ((terminal_outcome = 'ASSOCIATED' "
                + "AND unassigned_reason IS NULL) OR (terminal_outcome = 'UNASSIGNED' "
                + "AND unassigned_reason IS NOT NULL))", Integer.class, eventId));
    }

    private UUID incidentFor(UUID eventId) {
        return jdbcTemplate.queryForObject("SELECT incident_id FROM incident_membership WHERE event_id = ?",
                UUID.class, eventId);
    }

    private int workCount(String status, UUID incidentId) {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM grouping_work w "
                + "JOIN incident_membership m ON m.event_id = w.event_id "
                + "WHERE w.status = ? AND m.incident_id = ?", Integer.class, status, incidentId);
    }

    private String incidentStatus(UUID incidentId) {
        return jdbcTemplate.queryForObject("SELECT status FROM incident WHERE incident_id = ?",
                String.class, incidentId);
    }

    private LocalDateTime incidentTime(UUID incidentId, String column) {
        return jdbcTemplate.queryForObject("SELECT " + column + " FROM incident WHERE incident_id = ?",
                LocalDateTime.class, incidentId);
    }

    private Instant databaseNow() {
        return jdbcTemplate.queryForObject("SELECT clock_timestamp() AT TIME ZONE 'UTC'", LocalDateTime.class)
                .toInstant(ZoneOffset.UTC);
    }

    private static boolean await(CountDownLatch latch) {
        try {
            return latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while awaiting grouping worker", exception);
        }
    }
}
