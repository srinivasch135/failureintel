package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
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
import static org.mockito.Mockito.verifyNoInteractions;

@SpringBootTest
@Testcontainers
@TestPropertySource(properties = "spring.jpa.hibernate.ddl-auto=validate")
class FailureEventDurableCaptureIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureEventDurableCaptureIntegrationTest.class);

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("failureintel_durable_capture_test")
            .withUsername("testuser")
            .withPassword("testpassword")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER));

    @Autowired
    private FailureEventIngestionService ingestionService;

    @Autowired
    private FailureEventRepository failureEventRepository;

    @MockitoBean
    private NormalizedFailureEventRepository normalizedFailureEventRepository;

    @AfterEach
    void cleanUp() {
        failureEventRepository.deleteAll();
    }

    @Test
    void shouldCommitRawRowWithoutCallingNormalizedRepository() {
        String eventId = ingestionService.ingestFailureEvent(validRequest("trace-durable-capture-001"));

        var persisted = failureEventRepository.findById(UUID.fromString(eventId)).orElseThrow();
        assertEquals(ProcessingStatus.RECEIVED, persisted.getProcessingStatus());
        assertEquals(1, failureEventRepository.count());
        verifyNoInteractions(normalizedFailureEventRepository);
    }
}
