package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.config.FailureEventWorkerExecutorConfiguration;
import com.failureintel.ingestion.application.config.FailureEventWorkerProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
            assertFalse(context.containsBean("failureEventNormalizationExecutor"));
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
                        "failure-event.processing.worker.fixed-delay=1h",
                        "failure-event.processing.worker.batch-size=5",
                        "failure-event.processing.worker.concurrency=3",
                        "failure-event.processing.worker.shutdown-await=250ms")
                .run(context -> {
                    assertTrue(context.isRunning());
                    assertEquals("true", context.getEnvironment()
                            .getProperty("failure-event.processing.worker.enabled"));
                    assertTrue(context.containsBean("failureEventProcessingWorker"));
                    assertEquals(1, context.getBean(ScheduledAnnotationBeanPostProcessor.class)
                            .getScheduledTasks()
                            .size());

                    ThreadPoolTaskExecutor executor = context.getBean(
                            "failureEventNormalizationExecutor",
                            ThreadPoolTaskExecutor.class);
                    assertEquals(3, executor.getCorePoolSize());
                    assertEquals(3, executor.getMaxPoolSize());
                    assertEquals(5, executor.getQueueCapacity());
                    assertEquals(5, executor.getThreadPoolExecutor().getQueue().remainingCapacity());
                    assertTrue(executor.getThreadNamePrefix().startsWith("failure-event-normalization-"));

                    CountDownLatch threadNameCaptured = new CountDownLatch(1);
                    AtomicReference<String> workerThreadName = new AtomicReference<>();
                    executor.execute(() -> {
                        workerThreadName.set(Thread.currentThread().getName());
                        threadNameCaptured.countDown();
                    });
                    try {
                        assertTrue(threadNameCaptured.await(1, TimeUnit.SECONDS));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("Interrupted while observing the executor thread", interrupted);
                    }
                    assertTrue(workerThreadName.get().startsWith("failure-event-normalization-"));
                });
    }

    @Test
    void shouldWaitForInFlightExecutorWorkDuringShutdown() throws InterruptedException {
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch allowTaskToFinish = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);
        AtomicBoolean taskWasInterrupted = new AtomicBoolean();

        contextRunner
                .withPropertyValues(
                        "failure-event.processing.worker.enabled=true",
                        "failure-event.processing.worker.fixed-delay=1h",
                        "failure-event.processing.worker.shutdown-await=500ms")
                .run(context -> {
                    ThreadPoolTaskExecutor executor = context.getBean(
                            "failureEventNormalizationExecutor",
                            ThreadPoolTaskExecutor.class);
                    executor.execute(() -> {
                        taskStarted.countDown();
                        try {
                            allowTaskToFinish.await();
                        } catch (InterruptedException interrupted) {
                            taskWasInterrupted.set(true);
                            Thread.currentThread().interrupt();
                        } finally {
                            taskFinished.countDown();
                        }
                    });

                    assertTrue(taskStarted.await(1, TimeUnit.SECONDS));
                    Thread taskReleaser = new Thread(() -> {
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                        while (!executor.getThreadPoolExecutor().isShutdown()
                                && System.nanoTime() < deadline) {
                            Thread.yield();
                        }
                        try {
                            Thread.sleep(700);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        } finally {
                            allowTaskToFinish.countDown();
                        }
                    });
                    taskReleaser.start();

                    long shutdownStarted = System.nanoTime();
                    context.close();
                    long shutdownWaitMillis = TimeUnit.NANOSECONDS.toMillis(
                            System.nanoTime() - shutdownStarted);

                    try {
                        taskReleaser.join(1_500);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("Interrupted while waiting for the shutdown test task", interrupted);
                    }
                    assertTrue(shutdownWaitMillis >= 300,
                            "Executor shutdown should wait for in-flight work up to its configured bound");
                    assertTrue(shutdownWaitMillis < 1_500,
                            "Executor shutdown should not wait indefinitely");
                    assertTrue(taskFinished.await(1, TimeUnit.SECONDS));
                    assertFalse(taskWasInterrupted.get());
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
    @Import(FailureEventWorkerExecutorConfiguration.class)
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
