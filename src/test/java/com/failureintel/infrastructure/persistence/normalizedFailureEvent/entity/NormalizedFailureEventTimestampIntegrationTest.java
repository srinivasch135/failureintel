package com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity;

import com.failureintel.ingestion.domain.normalization.NormalizationStatus;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import com.failureintel.test.support.PostgresTestContainer;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@TestPropertySource(properties = "spring.jpa.hibernate.ddl-auto=validate")
class NormalizedFailureEventTimestampIntegrationTest {

    private static final Instant NORMALIZED_AT = Instant.parse("2026-06-15T12:00:00Z");

    @DynamicPropertySource
    static void postgresProperties(DynamicPropertyRegistry registry) {
        PostgresTestContainer.registerDatabase(registry, "failureintel_normalization_processor_test");
    }

    @Autowired
    private FailureEventRepository failureEventRepository;

    @Autowired
    private NormalizedFailureEventRepository normalizedFailureEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private EntityManager entityManager;

    private final List<UUID> eventIds = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        for (UUID eventId : eventIds) {
            normalizedFailureEventRepository.deleteById(eventId);
            failureEventRepository.deleteById(eventId);
        }
    }

    @Test
    void persistsAndReloadsInstantAcrossJvmDefaultTimeZones() {
        TimeZone originalTimeZone = TimeZone.getDefault();
        Instant expected = Instant.parse("2026-06-15T12:34:56.123456Z");
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
            UUID eventId = persistNormalizedEvent(expected);
            entityManager.clear();

            TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Auckland"));
            entityManager.clear();
            NormalizedFailureEventEntity reloaded = normalizedFailureEventRepository.findById(eventId)
                    .orElseThrow();

            assertEquals(expected, reloaded.getNormalizedOccurredAt());
        } finally {
            TimeZone.setDefault(originalTimeZone);
        }
    }

    @Test
    void persistsPostgresMicrosecondPrecisionAndRoundsSubMicrosecondValues() {
        TimeZone originalTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            UUID downEventId = persistNormalizedEvent(Instant.parse("2026-06-15T12:34:56.123456499Z"));
            UUID upEventId = persistNormalizedEvent(Instant.parse("2026-06-15T12:34:56.123456500Z"));
            entityManager.clear();

            assertEquals(Instant.parse("2026-06-15T12:34:56.123456Z"), normalizedFailureEventRepository
                    .findById(downEventId).orElseThrow().getNormalizedOccurredAt());
            assertEquals(Instant.parse("2026-06-15T12:34:56.123457Z"), normalizedFailureEventRepository
                    .findById(upEventId).orElseThrow().getNormalizedOccurredAt());
            assertEquals("timestamp without time zone", jdbcTemplate.queryForObject(
                    "SELECT pg_typeof(normalized_occurred_at)::text FROM normalized_failure_event WHERE event_id = ?",
                    String.class,
                    upEventId));
        } finally {
            TimeZone.setDefault(originalTimeZone);
        }
    }

    @Test
    void appliesStrictForwardAndBackwardInactivityWindowBoundariesAtMicrosecondPrecision() {
        TimeZone originalTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            UUID eventId = persistNormalizedEvent(NORMALIZED_AT);
            entityManager.clear();

            assertTrue(isForwardExtensionEligible(eventId, NORMALIZED_AT.plusSeconds(30 * 60L).minusNanos(1_000)));
            assertFalse(isForwardExtensionEligible(eventId, NORMALIZED_AT.plusSeconds(30 * 60L)));
            assertFalse(isForwardExtensionEligible(eventId, NORMALIZED_AT.plusSeconds(30 * 60L).plusNanos(1_000)));

            assertTrue(isBackwardExtensionEligible(eventId, NORMALIZED_AT.minusSeconds(30 * 60L).plusNanos(1_000)));
            assertFalse(isBackwardExtensionEligible(eventId, NORMALIZED_AT.minusSeconds(30 * 60L)));
            assertFalse(isBackwardExtensionEligible(eventId, NORMALIZED_AT.minusSeconds(30 * 60L).minusNanos(1_000)));
        } finally {
            TimeZone.setDefault(originalTimeZone);
        }
    }

    private UUID persistNormalizedEvent(Instant occurredAt) {
        UUID eventId = UUID.randomUUID();
        FailureEventEntity rawEvent = new FailureEventEntity();
        rawEvent.setEventId(eventId);
        rawEvent.setOccurredAt(occurredAt);
        rawEvent.setIngestedAt(NORMALIZED_AT);
        rawEvent.setServiceName("timestamp-test");
        rawEvent.setEnvironment("test");
        rawEvent.setEventType("exception");
        rawEvent.setRawPayload("{}");
        failureEventRepository.saveAndFlush(rawEvent);
        eventIds.add(eventId);

        NormalizedFailureEventEntity normalizedEvent = new NormalizedFailureEventEntity();
        normalizedEvent.setFailureEvent(rawEvent);
        normalizedEvent.setNormalizedOccurredAt(occurredAt);
        normalizedEvent.setNormalizedAt(NORMALIZED_AT);
        normalizedEvent.setNormalizationStatus(NormalizationStatus.FULLY_NORMALIZED);
        normalizedEvent.setNormalizedPayload(Map.of());
        normalizedEvent.setNormalizationMetadata(Map.of());
        normalizedFailureEventRepository.saveAndFlush(normalizedEvent);
        return eventId;
    }

    private boolean isForwardExtensionEligible(UUID eventId, Instant candidate) {
        return windowComparison(eventId, candidate,
                "?::timestamp < normalized_occurred_at + interval '30 minutes'");
    }

    private boolean isBackwardExtensionEligible(UUID eventId, Instant candidate) {
        return windowComparison(eventId, candidate,
                "?::timestamp > normalized_occurred_at - interval '30 minutes'");
    }

    private boolean windowComparison(UUID eventId, Instant candidate, String predicate) {
        Boolean eligible = jdbcTemplate.queryForObject(
                "SELECT " + predicate + " FROM normalized_failure_event WHERE event_id = ?",
                Boolean.class,
                Timestamp.from(candidate),
                eventId);
        return Boolean.TRUE.equals(eligible);
    }

}
