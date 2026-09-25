package com.failureintel.ingestion.application.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FailureEventWorkerPropertiesTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withUserConfiguration(WorkerPropertiesConfiguration.class);

    @Test
    void shouldBindWorkerSettings() {
        contextRunner
                .withPropertyValues(
                        "failure-event.processing.worker.enabled=true",
                        "failure-event.processing.worker.fixed-delay=1500ms",
                        "failure-event.processing.worker.batch-size=12",
                        "failure-event.processing.worker.concurrency=3",
                        "failure-event.processing.worker.shutdown-await=0s")
                .run(context -> {
                    assertTrue(context.isRunning());
                    FailureEventWorkerProperties properties = context.getBean(FailureEventWorkerProperties.class);
                    assertTrue(properties.enabled());
                    assertEquals(Duration.ofMillis(1500), properties.fixedDelay());
                    assertEquals(12, properties.batchSize());
                    assertEquals(3, properties.concurrency());
                    assertEquals(Duration.ZERO, properties.shutdownAwait());
                });
    }

    @Test
    void shouldBindSafeWorkerDefaults() {
        contextRunner.run(context -> {
            assertTrue(context.isRunning());
            FailureEventWorkerProperties properties = context.getBean(FailureEventWorkerProperties.class);
            assertFalse(properties.enabled());
            assertEquals(Duration.ofSeconds(2), properties.fixedDelay());
            assertEquals(8, properties.batchSize());
            assertEquals(2, properties.concurrency());
            assertEquals(Duration.ofSeconds(30), properties.shutdownAwait());
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "failure-event.processing.worker.fixed-delay=0s",
            "failure-event.processing.worker.fixed-delay=-1s",
            "failure-event.processing.worker.batch-size=0",
            "failure-event.processing.worker.batch-size=-1",
            "failure-event.processing.worker.concurrency=0",
            "failure-event.processing.worker.concurrency=-1",
            "failure-event.processing.worker.shutdown-await=-1s"
    })
    void shouldRejectInvalidWorkerSettings(String property) {
        contextRunner
                .withPropertyValues(property)
                .run(context -> assertNotNull(context.getStartupFailure()));
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(FailureEventWorkerProperties.class)
    static class WorkerPropertiesConfiguration {
    }
}
