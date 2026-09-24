package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.exception.StaleFailureEventClaimException;
import com.failureintel.ingestion.domain.model.ParsedFailureEvent;
import com.failureintel.ingestion.domain.model.RawFailureEvent;
import com.failureintel.ingestion.domain.parser.FailureEventParser;
import com.failureintel.ingestion.domain.parser.GenericJsonFailureEventParser;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.failureintel.test.support.FailureEventTestFixtures.validRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@SpringBootTest
@Testcontainers
@TestPropertySource(properties = {
        "spring.jpa.hibernate.ddl-auto=validate",
        "failure-event.processing.retry.max-attempts=5"
})
class FailureEventNormalizationProcessorIntegrationTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(FailureEventNormalizationProcessorIntegrationTest.class);

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("failureintel_normalization_processor_test")
            .withUsername("testuser")
            .withPassword("testpassword")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));

    @Autowired
    private FailureEventIngestionService ingestionService;

    @Autowired
    private FailureEventClaimService claimService;

    @Autowired
    private FailureEventNormalizationProcessor processor;

    @Autowired
    private FailureEventProcessingService processingService;

    @Autowired
    private FailureEventRepository failureEventRepository;

    @Autowired
    private NormalizedFailureEventRepository normalizedFailureEventRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @MockitoBean
    private FailureEventParser failureEventParser;

    @BeforeEach
    void configureParserDelegate() {
        reset(failureEventParser);
        GenericJsonFailureEventParser delegate = new GenericJsonFailureEventParser();
        when(failureEventParser.supports(any(RawFailureEvent.class)))
                .thenAnswer(invocation -> delegate.supports(invocation.getArgument(0)));
        when(failureEventParser.parse(any(RawFailureEvent.class)))
                .thenAnswer(invocation -> delegate.parse(invocation.getArgument(0)));
    }

    @AfterEach
    void cleanUp() {
        normalizedFailureEventRepository.deleteAll();
        failureEventRepository.deleteAll();
    }

    @Test
    void shouldPersistNormalizedEventAndMarkRawEventNormalized() {
        ClaimedFailureEvent claim = ingestAndClaim("trace-normalization-001");
        UUID eventId = claim.eventId();

        processor.process(claim);

        FailureEventEntity rawEvent = failureEventRepository.findById(eventId).orElseThrow();
        NormalizedFailureEventEntity normalizedEvent = normalizedFailureEventRepository
                .findById(eventId)
                .orElseThrow();

        assertEquals(ProcessingStatus.NORMALIZED, rawEvent.getProcessingStatus());
        assertNull(rawEvent.getProcessingStartedAt());
        assertNull(rawEvent.getNextAttemptAt());
        assertNull(rawEvent.getFailureCode());
        assertNull(rawEvent.getFailureReason());
        assertEquals(eventId, normalizedEvent.getEventId());
        assertEquals("payment-service", normalizedEvent.getNormalizedServiceName());
        assertEquals("prod", normalizedEvent.getNormalizedEnvironment());
        assertEquals("exception", normalizedEvent.getNormalizedEventType());
        assertEquals(1, normalizedFailureEventRepository.count());
    }

    @Test
    void shouldUpdateExistingNormalizedRowWithoutCreatingDuplicate() {
        ClaimedFailureEvent initialClaim = ingestAndClaim("trace-normalization-idempotent-001");
        UUID eventId = initialClaim.eventId();
        processor.process(initialClaim);

        FailureEventEntity rawEvent = failureEventRepository.findById(eventId).orElseThrow();
        rawEvent.setProcessingStatus(ProcessingStatus.PROCESSING);
        rawEvent.markRetryable(
                "TEST_RETRY",
                "test retry before repeated processing",
                Instant.now().minusSeconds(1));
        failureEventRepository.saveAndFlush(rawEvent);

        processor.process(findClaim(eventId));

        assertEquals(ProcessingStatus.NORMALIZED,
                failureEventRepository.findById(eventId).orElseThrow().getProcessingStatus());
        assertEquals(1, normalizedFailureEventRepository.count());
    }

    @Test
    void shouldPreserveRawEventAndMarkUnsupportedPayloadAsFailed() {
        var request = validRequest("trace-normalization-unsupported-001");
        request.setRawPayload(java.util.Map.of());
        UUID eventId = UUID.fromString(ingestionService.ingestFailureEvent(request));

        processor.process(findClaim(eventId));

        FailureEventEntity rawEvent = failureEventRepository.findById(eventId).orElseThrow();
        assertEquals(ProcessingStatus.FAILED, rawEvent.getProcessingStatus());
        assertEquals("UNSUPPORTED_PAYLOAD", rawEvent.getFailureCode());
        assertFalse(normalizedFailureEventRepository.existsById(eventId));
        assertEventIsNoLongerClaimable(eventId);
    }

    @Test
    void shouldPreservePermanentFailureCodeOnTheFinalAllowedAttempt() {
        var request = validRequest("trace-normalization-permanent-last-attempt-001");
        request.setRawPayload(java.util.Map.of());
        UUID eventId = UUID.fromString(ingestionService.ingestFailureEvent(request));

        ClaimedFailureEvent firstClaim = findClaim(eventId);
        FailureEventEntity event = failureEventRepository.findById(eventId).orElseThrow();
        event.markRetryable(
                "DATABASE_TIMEOUT",
                "database operation timed out",
                Instant.now().minusSeconds(1));
        failureEventRepository.saveAndFlush(event);
        jdbcTemplate.update(
                "UPDATE failure_event SET attempt_count = 4, version = version + 1 WHERE event_id = ?",
                eventId);

        ClaimedFailureEvent finalClaim = findClaim(eventId);
        assertEquals(1, firstClaim.attemptNumber());
        assertEquals(5, finalClaim.attemptNumber());
        processingService.process(finalClaim);

        FailureEventEntity persisted = failureEventRepository.findById(eventId).orElseThrow();
        assertEquals(ProcessingStatus.FAILED, persisted.getProcessingStatus());
        assertEquals(5, persisted.getAttemptCount());
        assertEquals("UNSUPPORTED_PAYLOAD", persisted.getFailureCode());
        assertEquals("No parser supports the persisted failure event", persisted.getFailureReason());
    }

    @Test
    void shouldPreserveRawEventAndMarkExplicitlyMalformedParsedEventAsFailed() {
        var request = validRequest("trace-normalization-malformed-001");
        UUID eventId = UUID.fromString(ingestionService.ingestFailureEvent(request));
        doAnswer(invocation -> ParsedFailureEvent.malformed(
                invocation.getArgument(0),
                "untrusted parser detail that must not be persisted"))
                .when(failureEventParser).parse(any(RawFailureEvent.class));

        processor.process(findClaim(eventId));

        FailureEventEntity rawEvent = failureEventRepository.findById(eventId).orElseThrow();
        assertEquals(ProcessingStatus.FAILED, rawEvent.getProcessingStatus());
        assertEquals("MALFORMED_EVENT", rawEvent.getFailureCode());
        assertEquals("Persisted failure event is malformed", rawEvent.getFailureReason());
        assertFalse(normalizedFailureEventRepository.existsById(eventId));
        assertEventIsNoLongerClaimable(eventId);
    }

    @Test
    void shouldPreserveRawEventAndMarkEventWithInsufficientUsefulDataAsFailed() {
        var request = validRequest(null);
        request.setServiceName(" ");
        request.setEnvironment(" ");
        request.setEventType(" ");
        request.setErrorType(null);
        request.setErrorMessage(" ");
        request.setSeverityHint(null);
        request.setRawPayload(java.util.Map.of("host", "unknown-host"));
        UUID eventId = UUID.fromString(ingestionService.ingestFailureEvent(request));

        processor.process(findClaim(eventId));

        FailureEventEntity rawEvent = failureEventRepository.findById(eventId).orElseThrow();
        assertEquals(ProcessingStatus.FAILED, rawEvent.getProcessingStatus());
        assertEquals("INSUFFICIENT_FAILURE_DATA", rawEvent.getFailureCode());
        assertEquals("Event does not contain minimum useful failure data", rawEvent.getFailureReason());
        assertFalse(normalizedFailureEventRepository.existsById(eventId));
        assertEventIsNoLongerClaimable(eventId);
    }

    @Test
    void shouldMarkUndecodablePersistedPayloadAsMalformedWithoutRetrying() {
        UUID eventId = UUID.fromString(
                ingestionService.ingestFailureEvent(validRequest("trace-normalization-invalid-json-001")));
        FailureEventEntity rawEvent = failureEventRepository.findById(eventId).orElseThrow();
        rawEvent.setRawPayload("{invalid-json");
        failureEventRepository.saveAndFlush(rawEvent);

        processingService.process(findClaim(eventId));

        FailureEventEntity failedEvent = failureEventRepository.findById(eventId).orElseThrow();
        assertEquals(ProcessingStatus.FAILED, failedEvent.getProcessingStatus());
        assertEquals("MALFORMED_EVENT", failedEvent.getFailureCode());
        assertEquals("Persisted raw payload could not be decoded", failedEvent.getFailureReason());
        assertEquals(1, failedEvent.getAttemptCount());
        assertNull(failedEvent.getNextAttemptAt());
        assertFalse(normalizedFailureEventRepository.existsById(eventId));
        assertEventIsNoLongerClaimable(eventId);
    }

    @Test
    void shouldPropagateUnexpectedParserFailureWithoutMarkingEventPermanentlyFailed() {
        UUID eventId = UUID.fromString(
                ingestionService.ingestFailureEvent(validRequest("trace-normalization-parser-error-001")));
        doAnswer(invocation -> {
            throw new IllegalStateException("unexpected parser failure");
        }).when(failureEventParser).parse(any(RawFailureEvent.class));

        assertThrows(
                IllegalStateException.class,
                () -> processor.process(findClaim(eventId)));

        FailureEventEntity rawEvent = failureEventRepository.findById(eventId).orElseThrow();
        assertEquals(ProcessingStatus.PROCESSING, rawEvent.getProcessingStatus());
        assertNull(rawEvent.getFailureCode());
        assertFalse(normalizedFailureEventRepository.existsById(eventId));
    }

    @Test
    void shouldRejectAClaimThatBelongsToAnOlderAttempt() {
        ClaimedFailureEvent firstClaim = ingestAndClaim("trace-normalization-stale-001");
        UUID eventId = firstClaim.eventId();

        FailureEventEntity claimedEvent = failureEventRepository.findById(eventId).orElseThrow();
        claimedEvent.markRetryable(
                "TEST_RETRY",
                "test lease recovery",
                Instant.now().minusSeconds(1));
        failureEventRepository.saveAndFlush(claimedEvent);

        ClaimedFailureEvent currentClaim = findClaim(eventId);
        assertEquals(firstClaim.attemptNumber() + 1, currentClaim.attemptNumber());

        assertThrows(
                StaleFailureEventClaimException.class,
                () -> processor.process(firstClaim));

        FailureEventEntity currentEvent = failureEventRepository.findById(eventId).orElseThrow();
        assertEquals(ProcessingStatus.PROCESSING, currentEvent.getProcessingStatus());
        assertEquals(currentClaim.attemptNumber(), currentEvent.getAttemptCount());
        assertFalse(normalizedFailureEventRepository.existsById(eventId));
    }

    private ClaimedFailureEvent ingestAndClaim(String traceId) {
        UUID eventId = UUID.fromString(ingestionService.ingestFailureEvent(validRequest(traceId)));
        return findClaim(eventId);
    }

    private ClaimedFailureEvent findClaim(UUID eventId) {
        List<ClaimedFailureEvent> claims = claimService.claimNextEligibleForProcessing(1);
        return claims.stream()
                .filter(claim -> claim.eventId().equals(eventId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected claim for event " + eventId));
    }

    private void assertEventIsNoLongerClaimable(UUID eventId) {
        assertTrue(claimService.claimNextEligibleForProcessing(1).stream()
                .noneMatch(claim -> claim.eventId().equals(eventId)));
    }
}
