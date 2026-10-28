package com.failureintel.infrastructure.persistence.incidentgrouping;

import com.failureintel.ingestion.application.service.ClaimedGroupingWork;
import com.failureintel.ingestion.application.service.GroupingWorkCompletionProcessor;
import com.failureintel.ingestion.application.service.IncidentClosureProcessor;
import com.failureintel.infrastructure.persistence.incidentgrouping.entity.IncidentEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentMembershipRepository;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentRepository;
import com.failureintel.test.support.PostgresTestContainer;
import org.junit.jupiter.api.AfterEach;
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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
        "spring.jpa.hibernate.ddl-auto=validate",
        "spring.datasource.hikari.connection-init-sql=SET TIME ZONE 'Pacific/Kiritimati'"
})
class IncidentGroupingTimezoneIntegrationTest {
    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        PostgresTestContainer.registerDatabase(registry, "failureintel_normalization_processor_test");
    }

    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private IncidentRepository incidentRepository;
    @Autowired private GroupingWorkRepository groupingWorkRepository;
    @Autowired private IncidentMembershipRepository membershipRepository;
    @Autowired private IncidentClosureProcessor closureProcessor;
    @Autowired private GroupingWorkCompletionProcessor completionProcessor;
    @Autowired private PlatformTransactionManager transactionManager;

    @AfterEach
    void cleanFixtures() {
        new TransactionTemplate(transactionManager).executeWithoutResult(transaction -> {
            jdbcTemplate.update("""
                    UPDATE grouping_work SET status = 'FAILED', processing_started_at = NULL,
                        next_attempt_at = NULL, terminal_outcome = NULL, unassigned_reason = NULL
                    WHERE event_id IN (SELECT event_id FROM failure_event WHERE service_name = 'timezone-test')
                    """);
            jdbcTemplate.update("""
                    DELETE FROM incident_membership
                    WHERE event_id IN (SELECT event_id FROM failure_event WHERE service_name = 'timezone-test')
                    """);
        });
        jdbcTemplate.update("""
                DELETE FROM grouping_work
                WHERE event_id IN (SELECT event_id FROM failure_event WHERE service_name = 'timezone-test')
                """);
        jdbcTemplate.update("DELETE FROM incident WHERE service_name = 'timezone-test'");
        jdbcTemplate.update("""
                DELETE FROM normalized_failure_event
                WHERE event_id IN (SELECT event_id FROM failure_event WHERE service_name = 'timezone-test')
                """);
        jdbcTemplate.update("DELETE FROM failure_event WHERE service_name = 'timezone-test'");
    }

    @Test
    void closesInUtcSession() {
        assertEquals("Pacific/Kiritimati", jdbcTemplate.queryForObject("SHOW TIME ZONE", String.class));
        LocalDateTime now = utcNow();
        UUID incidentId = newIncident("ACTIVE", now.minusHours(2), now.minusHours(1), null);
        LocalDateTime before = utcNow();

        assertTrue(closureProcessor.closeIfExpired(incidentId));

        LocalDateTime closedAt = incidentRepository.findById(incidentId).orElseThrow().getClosedAt();
        LocalDateTime after = utcNow();
        assertEquals("CLOSED", incidentRepository.findById(incidentId).orElseThrow().getStatus());
        assertFalse(closedAt.isBefore(before.minusSeconds(1)));
        assertFalse(closedAt.isAfter(after.plusSeconds(1)));
    }

    @Test
    void associatesBeforeDeadline() {
        assertEquals("Pacific/Kiritimati", jdbcTemplate.queryForObject("SHOW TIME ZONE", String.class));
        LocalDateTime closedAt = utcNow().minusHours(23).truncatedTo(ChronoUnit.MICROS);
        LocalDateTime first = closedAt.minusHours(2);
        LocalDateTime last = closedAt.minusHours(1);
        UUID incidentId = newIncident("CLOSED", first, last, closedAt);
        IncidentEntity before = incidentRepository.findById(incidentId).orElseThrow();
        UUID eventId = newClaimedEvent(last);

        completionProcessor.process(new ClaimedGroupingWork(eventId, 1));

        IncidentEntity after = incidentRepository.findById(incidentId).orElseThrow();
        assertEquals("COMPLETED", groupingWorkRepository.findById(eventId).orElseThrow().getStatus());
        assertEquals("ASSOCIATED", groupingWorkRepository.findById(eventId).orElseThrow().getTerminalOutcome());
        assertEquals(incidentId, membershipRepository.findById(eventId).orElseThrow().getIncidentId());
        assertEquals(before.getAssociatedEventCount() + 1, after.getAssociatedEventCount());
        assertEquals("CLOSED", after.getStatus());
        assertEquals(before.getFirstOccurredAt(), after.getFirstOccurredAt());
        assertEquals(before.getLastOccurredAt(), after.getLastOccurredAt());
        assertEquals(before.getInactivityExpiresAt(), after.getInactivityExpiresAt());
        assertEquals(closedAt, after.getClosedAt());
    }

    private UUID newIncident(String status, LocalDateTime first, LocalDateTime last, LocalDateTime closedAt) {
        UUID incidentId = UUID.randomUUID();
        incidentRepository.saveAndFlush(new IncidentEntity(incidentId, "timezone-test", "test", "ExampleError",
                null, status, first, last, last.plusMinutes(30), closedAt, 1));
        return incidentId;
    }

    private UUID newClaimedEvent(LocalDateTime occurredAt) {
        UUID eventId = UUID.randomUUID();
        LocalDateTime now = utcNow();
        jdbcTemplate.update("""
                INSERT INTO failure_event (
                    event_id, occurred_at, ingested_at, service_name, environment,
                    event_type, processing_status, version
                ) VALUES (?, ?, ?, 'timezone-test', 'test', 'exception', 'NORMALIZED', 0)
                """, eventId, occurredAt, now);
        jdbcTemplate.update("""
                INSERT INTO normalized_failure_event (
                    event_id, normalized_service_name, normalized_environment,
                    normalized_error_type, normalized_occurred_at, normalization_status
                ) VALUES (?, 'timezone-test', 'test', 'ExampleError', ?, 'FULLY_NORMALIZED')
                """, eventId, occurredAt);
        jdbcTemplate.update("""
                INSERT INTO grouping_work (
                    event_id, status, attempt_count, created_at, last_attempt_at, processing_started_at
                ) VALUES (?, 'PROCESSING', 1, ?, ?, ?)
                """, eventId, now, now, now);
        return eventId;
    }

    private LocalDateTime utcNow() {
        return jdbcTemplate.queryForObject("SELECT clock_timestamp() AT TIME ZONE 'UTC'", LocalDateTime.class);
    }
}
