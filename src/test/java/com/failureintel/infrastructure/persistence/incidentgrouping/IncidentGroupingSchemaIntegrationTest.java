package com.failureintel.infrastructure.persistence.incidentgrouping;

import com.failureintel.test.support.PostgresTestContainer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class IncidentGroupingSchemaIntegrationTest {

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        PostgresTestContainer.registerDatabase(registry, "failureintel_normalization_processor_test");
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private Flyway flyway;

    private final List<UUID> eventIds = new ArrayList<>();
    private final List<UUID> incidentIds = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
            for (UUID eventId : eventIds) {
                jdbcTemplate.update("""
                        UPDATE grouping_work
                        SET status = 'FAILED', processing_started_at = NULL, next_attempt_at = NULL,
                            terminal_outcome = NULL, unassigned_reason = NULL
                        WHERE event_id = ?
                        """, eventId);
                jdbcTemplate.update("DELETE FROM incident_membership WHERE event_id = ?", eventId);
            }
        });
        for (UUID eventId : eventIds) {
            jdbcTemplate.update("DELETE FROM grouping_work WHERE event_id = ?", eventId);
        }
        for (UUID incidentId : incidentIds) {
            jdbcTemplate.update("DELETE FROM incident WHERE incident_id = ?", incidentId);
        }
        for (UUID eventId : eventIds) {
            jdbcTemplate.update("DELETE FROM normalized_failure_event WHERE event_id = ?", eventId);
            jdbcTemplate.update("DELETE FROM failure_event WHERE event_id = ?", eventId);
        }
        eventIds.clear();
        incidentIds.clear();
    }

    @Test
    void flywayAppliesVersion15AndValidOutcomesWorkInEitherStatementOrder() {
        assertTrue(flyway.info().applied().length > 0);
        assertEquals("15", jdbcTemplate.queryForObject(
                "SELECT version FROM flyway_schema_history WHERE version = '15' AND success",
                String.class));

        UUID eventOne = newEvent();
        UUID incidentOne = newIncident();
        inTransaction(() -> {
            insertProcessingWork(eventOne);
            insertMembership(eventOne, incidentOne);
            completeAssociated(eventOne);
        });

        UUID eventTwo = newEvent();
        UUID incidentTwo = newIncident();
        inTransaction(() -> {
            insertProcessingWork(eventTwo);
            completeAssociated(eventTwo);
            insertMembership(eventTwo, incidentTwo);
        });

        UUID unassignedEvent = newEvent();
        inTransaction(() -> {
            insertProcessingWork(unassignedEvent);
            jdbcTemplate.update("""
                    UPDATE grouping_work
                    SET status = 'COMPLETED', processing_started_at = NULL,
                        terminal_outcome = 'UNASSIGNED', unassigned_reason = 'MISSING_ATTRIBUTES'
                    WHERE event_id = ?
                    """, unassignedEvent);
        });
    }

    @Test
    void primaryKeysAndRowChecksRejectDuplicateAndInvalidRows() {
        UUID eventId = newEvent();
        UUID incidentId = newIncident();
        insertProcessingWork(eventId);

        assertThrows(RuntimeException.class, () -> insertProcessingWork(eventId));

        // Membership insertion and completion are validated together at commit.
        inTransaction(() -> {
            insertMembership(eventId, incidentId);
            completeAssociated(eventId);
        });
        assertThrows(RuntimeException.class, () -> insertMembership(eventId, incidentId));
        assertThrows(RuntimeException.class, () -> jdbcTemplate.update("""
                UPDATE grouping_work SET status = 'NOT_A_STATE' WHERE event_id = ?
                """, eventId));
        UUID missingOutcomeEvent = newEvent();
        insertProcessingWork(missingOutcomeEvent);
        assertThrows(RuntimeException.class, () -> jdbcTemplate.update("""
                UPDATE grouping_work
                SET status = 'COMPLETED', processing_started_at = NULL
                WHERE event_id = ?
                """, missingOutcomeEvent));
        UUID missingReasonEvent = newEvent();
        insertProcessingWork(missingReasonEvent);
        assertThrows(RuntimeException.class, () -> jdbcTemplate.update("""
                UPDATE grouping_work
                SET status = 'COMPLETED', processing_started_at = NULL, terminal_outcome = 'UNASSIGNED'
                WHERE event_id = ?
                """, missingReasonEvent));
        assertThrows(RuntimeException.class, () -> jdbcTemplate.update("""
                INSERT INTO incident (
                    incident_id, service_name, environment, error_type, status,
                    first_occurred_at, last_occurred_at, inactivity_expires_at
                ) VALUES (?, 'svc', 'prod', 'Error', 'CLOSED', now(), now(), now())
                """, UUID.randomUUID()));
    }

    @Test
    void deferredValidationRejectsMissingContradictoryAndNoncompletedMemberships() {
        UUID missingEvent = newEvent();
        assertThrows(RuntimeException.class, () -> inTransaction(() -> {
            insertProcessingWork(missingEvent);
            completeAssociated(missingEvent);
        }));

        UUID contradictoryEvent = newEvent();
        UUID incidentId = newIncident();
        assertThrows(RuntimeException.class, () -> inTransaction(() -> {
            insertProcessingWork(contradictoryEvent);
            jdbcTemplate.update("""
                    UPDATE grouping_work
                    SET status = 'COMPLETED', processing_started_at = NULL,
                        terminal_outcome = 'UNASSIGNED', unassigned_reason = 'MISSING_ATTRIBUTES'
                    WHERE event_id = ?
                    """, contradictoryEvent);
            insertMembership(contradictoryEvent, incidentId);
        }));

        UUID noncompletedEvent = newEvent();
        UUID noncompletedIncident = newIncident();
        assertThrows(RuntimeException.class, () -> inTransaction(() -> {
            insertProcessingWork(noncompletedEvent);
            insertMembership(noncompletedEvent, noncompletedIncident);
        }));
    }

    @Test
    void membershipDeletionAndEventReassignmentCannotInvalidateCompletedOutcomes() {
        UUID associatedEvent = newEvent();
        UUID incidentId = newIncident();
        completeAssociatedInNewTransaction(associatedEvent, incidentId);

        assertThrows(RuntimeException.class, () -> inTransaction(() ->
                jdbcTemplate.update("DELETE FROM incident_membership WHERE event_id = ?", associatedEvent)));

        UUID pendingEvent = newEvent();
        insertProcessingWork(pendingEvent);
        assertThrows(RuntimeException.class, () -> inTransaction(() ->
                jdbcTemplate.update("UPDATE incident_membership SET event_id = ? WHERE event_id = ?",
                        pendingEvent, associatedEvent)));

        assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM incident_membership WHERE event_id = ?", Integer.class, associatedEvent));
    }

    @Test
    void membershipLockSerializesConcurrentOutcomeChanges() throws Exception {
        UUID eventId = newEvent();
        UUID incidentId = newIncident();
        insertProcessingWork(eventId);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection associationConnection = dataSource.getConnection()) {
            associationConnection.setAutoCommit(false);
            try (PreparedStatement insert = associationConnection.prepareStatement(
                    "INSERT INTO incident_membership(event_id, incident_id) VALUES (?, ?)")) {
                insert.setObject(1, eventId);
                insert.setObject(2, incidentId);
                insert.executeUpdate();
            }
            try (PreparedStatement update = associationConnection.prepareStatement("""
                    UPDATE grouping_work
                    SET status = 'COMPLETED', processing_started_at = NULL,
                        terminal_outcome = 'ASSOCIATED', unassigned_reason = NULL
                    WHERE event_id = ?
                    """)) {
                update.setObject(1, eventId);
                update.executeUpdate();
            }

            Future<Boolean> contradictoryCommit = executor.submit(() -> {
                try (Connection competingConnection = dataSource.getConnection()) {
                    competingConnection.setAutoCommit(false);
                    try (PreparedStatement update = competingConnection.prepareStatement("""
                            UPDATE grouping_work
                            SET status = 'COMPLETED', processing_started_at = NULL,
                                terminal_outcome = 'UNASSIGNED', unassigned_reason = 'MISSING_ATTRIBUTES'
                            WHERE event_id = ?
                            """)) {
                        update.setObject(1, eventId);
                        update.executeUpdate();
                    }
                    try {
                        competingConnection.commit();
                        return false;
                    } catch (SQLException expectedConstraintFailure) {
                        competingConnection.rollback();
                        return true;
                    }
                }
            });

            assertTrue(awaitLockWait());
            associationConnection.commit();
            assertTrue(contradictoryCommit.get(10, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    private boolean awaitLockWait() throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            Integer waiting = jdbcTemplate.queryForObject("""
                    SELECT COUNT(*) FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND wait_event_type = 'Lock'
                      AND query LIKE 'UPDATE grouping_work%'
                    """, Integer.class);
            if (waiting != null && waiting > 0) {
                return true;
            }
            Thread.sleep(25);
        }
        return false;
    }

    private UUID newEvent() {
        UUID eventId = UUID.randomUUID();
        eventIds.add(eventId);
        jdbcTemplate.update("""
                INSERT INTO failure_event (
                    event_id, occurred_at, ingested_at, service_name, environment,
                    event_type, processing_status, version
                ) VALUES (?, now(), now(), 'schema-test', 'test', 'exception', 'NORMALIZED', 0)
                """, eventId);
        jdbcTemplate.update("""
                INSERT INTO normalized_failure_event(event_id, normalization_status)
                VALUES (?, 'FULLY_NORMALIZED')
                """, eventId);
        return eventId;
    }

    private UUID newIncident() {
        UUID incidentId = UUID.randomUUID();
        incidentIds.add(incidentId);
        jdbcTemplate.update("""
                INSERT INTO incident (
                    incident_id, service_name, environment, error_type, dependency_target,
                    first_occurred_at, last_occurred_at, inactivity_expires_at
                ) VALUES (?, 'schema-test', 'test', 'ExampleError', NULL, now(), now(), now())
                """, incidentId);
        return incidentId;
    }

    private void insertProcessingWork(UUID eventId) {
        jdbcTemplate.update("""
                INSERT INTO grouping_work (
                    event_id, status, attempt_count, last_attempt_at, processing_started_at
                ) VALUES (?, 'PROCESSING', 1, now(), now())
                """, eventId);
    }

    private void insertMembership(UUID eventId, UUID incidentId) {
        jdbcTemplate.update("""
                INSERT INTO incident_membership(event_id, incident_id) VALUES (?, ?)
                """, eventId, incidentId);
    }

    private void completeAssociated(UUID eventId) {
        jdbcTemplate.update("""
                UPDATE grouping_work
                SET status = 'COMPLETED', processing_started_at = NULL,
                    terminal_outcome = 'ASSOCIATED', unassigned_reason = NULL
                WHERE event_id = ?
                """, eventId);
    }

    private void completeAssociatedInNewTransaction(UUID eventId, UUID incidentId) {
        insertProcessingWork(eventId);
        inTransaction(() -> {
            insertMembership(eventId, incidentId);
            completeAssociated(eventId);
        });
    }

    private void inTransaction(Runnable work) {
        new TransactionTemplate(transactionManager).executeWithoutResult(status -> work.run());
    }
}
