package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.config.FailureEventWorkerProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FailureEventProcessingWorkerTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withUserConfiguration(WorkerConfiguration.class);

    @Test
    void shouldNotRegisterOrScheduleWorkerWhenDisabled() {
        contextRunner.run(context -> {
            assertTrue(context.isRunning());
            assertFalse(context.containsBean("failureEventProcessingWorker"));
            assertTrue(context.getBean(ScheduledAnnotationBeanPostProcessor.class)
                    .getScheduledTasks()
                    .isEmpty());
        });
    }

    @Test
    void shouldRegisterAndScheduleWorkerWhenEnabled() {
        contextRunner
                .withPropertyValues(
                        "failure-event.processing.worker.enabled=true",
                        "failure-event.processing.worker.fixed-delay=1h")
                .run(context -> {
                    assertTrue(context.isRunning());
                    assertEquals("true", context.getEnvironment()
                            .getProperty("failure-event.processing.worker.enabled"));
                    assertTrue(context.containsBean("failureEventProcessingWorker"));
                    assertEquals(1, context.getBean(ScheduledAnnotationBeanPostProcessor.class)
                            .getScheduledTasks()
                            .size());
                });
    }

    @Test
    void shouldProcessClaimsIndividuallyAndContinueAfterAnAttemptThrows() {
        FailureEventClaimService claimService = mock(FailureEventClaimService.class);
        FailureEventProcessingService processingService = mock(FailureEventProcessingService.class);
        FailureEventWorkerProperties properties = new FailureEventWorkerProperties(
                true,
                Duration.ofSeconds(2),
                2,
                1,
                Duration.ofSeconds(30));
        ClaimedFailureEvent failedClaim = new ClaimedFailureEvent(UUID.randomUUID(), 1);
        ClaimedFailureEvent followingClaim = new ClaimedFailureEvent(UUID.randomUUID(), 1);
        when(claimService.claimNextEligibleForProcessing(2))
                .thenReturn(List.of(failedClaim, followingClaim));
        doThrow(new IllegalStateException("simulated processing-service failure"))
                .when(processingService)
                .process(failedClaim);

        new FailureEventProcessingWorker(claimService, processingService, properties).processNextBatch();

        verify(claimService).claimNextEligibleForProcessing(2);
        verify(processingService).process(failedClaim);
        verify(processingService).process(followingClaim);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableScheduling
    @EnableConfigurationProperties(FailureEventWorkerProperties.class)
    @ComponentScan(
            basePackageClasses = FailureEventProcessingWorker.class,
            useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(
                    type = FilterType.ASSIGNABLE_TYPE,
                    classes = FailureEventProcessingWorker.class))
    static class WorkerConfiguration {

        @Bean
        FailureEventClaimService failureEventClaimService() {
            FailureEventClaimService claimService = mock(FailureEventClaimService.class);
            when(claimService.claimNextEligibleForProcessing(anyInt())).thenReturn(List.of());
            return claimService;
        }

        @Bean
        FailureEventProcessingService failureEventProcessingService() {
            return mock(FailureEventProcessingService.class);
        }
    }
}
