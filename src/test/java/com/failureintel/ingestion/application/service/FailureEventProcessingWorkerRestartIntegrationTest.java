package com.failureintel.ingestion.application.service;

import com.failureintel.FailureintelApplication;
import com.failureintel.infrastructure.persistence.failureevent.entity.FailureEventEntity;
import com.failureintel.infrastructure.persistence.failureevent.entity.ProcessingStatus;
import com.failureintel.infrastructure.persistence.failureevent.repository.FailureEventRepository;
import com.failureintel.infrastructure.persistence.normalizedFailureEvent.repository.NormalizedFailureEventRepository;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.UUID;

import static com.failureintel.test.support.FailureEventTestFixtures.validRequest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class FailureEventProcessingWorkerRestartIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("failureintel_worker_restart_test")
            .withUsername("testuser")
            .withPassword("testpassword");

    @Test
    void shouldProcessPersistedReceivedEventAfterApplicationContextRestart() {
        UUID eventId;
        try (ConfigurableApplicationContext initialContext = startApplicationContext(false)) {
            FailureEventIngestionService ingestionService = initialContext.getBean(
                    FailureEventIngestionService.class);
            eventId = UUID.fromString(ingestionService.ingestFailureEvent(
                    validRequest("worker-restart-" + UUID.randomUUID())));

            FailureEventEntity persisted = initialContext.getBean(FailureEventRepository.class)
                    .findById(eventId)
                    .orElseThrow();
            assertEquals(ProcessingStatus.RECEIVED, persisted.getProcessingStatus());
        }

        try (ConfigurableApplicationContext restartedContext = startApplicationContext(true)) {
            FailureEventRepository failureEventRepository = restartedContext.getBean(FailureEventRepository.class);
            NormalizedFailureEventRepository normalizedRepository = restartedContext.getBean(
                    NormalizedFailureEventRepository.class);

            assertTrue(failureEventRepository.existsById(eventId),
                    "The raw event should remain stored when the first application context closes");

            Awaitility.await()
                    .atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> {
                        assertEquals(ProcessingStatus.NORMALIZED,
                                failureEventRepository.findById(eventId).orElseThrow().getProcessingStatus());
                        assertTrue(normalizedRepository.existsById(eventId));
                    });
        }
    }

    private ConfigurableApplicationContext startApplicationContext(boolean workerEnabled) {
        return new SpringApplicationBuilder(FailureintelApplication.class)
                .web(WebApplicationType.NONE)
                .registerShutdownHook(false)
                .run(
                        "--spring.datasource.url=" + postgres.getJdbcUrl(),
                        "--spring.datasource.username=" + postgres.getUsername(),
                        "--spring.datasource.password=" + postgres.getPassword(),
                        "--spring.jpa.hibernate.ddl-auto=validate",
                        "--failure-event.processing.worker.enabled=" + workerEnabled,
                        "--failure-event.processing.worker.fixed-delay=1h",
                        "--failure-event.processing.worker.shutdown-await=5s",
                        "--spring.main.banner-mode=off");
    }
}
