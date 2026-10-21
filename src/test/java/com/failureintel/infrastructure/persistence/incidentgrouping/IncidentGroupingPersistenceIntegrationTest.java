package com.failureintel.infrastructure.persistence.incidentgrouping;

import com.failureintel.infrastructure.persistence.incidentgrouping.entity.GroupingWorkEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.entity.IncidentEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.entity.IncidentMembershipEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.GroupingWorkRepository;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentMembershipRepository;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentRepository;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        return eventId;
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
