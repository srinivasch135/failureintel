package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.entity.NormalizedFailureEventEntity;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.UUID;

import static com.failureintel.test.support.FailureEventTestFixtures.validRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@SpringBootTest
@Testcontainers
@TestPropertySource(properties = "spring.jpa.hibernate.ddl-auto=validate")
class FailureEventNormalizationProcessorRollbackIntegrationTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(FailureEventNormalizationProcessorRollbackIntegrationTest.class);

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("failureintel_normalization_rollback_test")
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
    private FailureEventRepository failureEventRepository;

    @MockitoBean
    private NormalizedFailureEventRepository normalizedFailureEventRepository;

    @AfterEach
    void cleanUp() {
        failureEventRepository.deleteAll();
    }

    @Test
    void shouldLeaveClaimedRawEventAvailableWhenNormalizedPersistenceFails() {
        when(normalizedFailureEventRepository.findById(any(UUID.class)))
                .thenReturn(java.util.Optional.empty());
        when(normalizedFailureEventRepository.save(any(NormalizedFailureEventEntity.class)))
                .thenThrow(new DataIntegrityViolationException("simulated normalized write failure"));

        UUID eventId = UUID.fromString(
                ingestionService.ingestFailureEvent(validRequest("trace-normalization-rollback-001")));
        ClaimedFailureEvent claim = claimService
                .claimNextEligibleForProcessing(1)
                .get(0);

        assertEquals(eventId, claim.eventId());
        assertThrows(DataIntegrityViolationException.class, () -> processor.process(claim));

        assertEquals(
                ProcessingStatus.PROCESSING,
                failureEventRepository.findById(eventId).orElseThrow().getProcessingStatus());
    }
}
