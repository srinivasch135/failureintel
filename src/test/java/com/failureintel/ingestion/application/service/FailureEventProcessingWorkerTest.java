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
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class FailureEventProcessingWorkerTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withUserConfiguration(WorkerConfiguration.class);
    private final ApplicationContextRunner executionContextRunner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withUserConfiguration(WorkerExecutionConfiguration.class);

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

        executionContextRunner
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
    void shouldReturnQuietlyWhenNoClaimsAreAvailable() throws NoSuchMethodException {
        executionContextRunner
                .withPropertyValues(
                        "failure-event.processing.worker.enabled=true",
                        "failure-event.processing.worker.fixed-delay=1h",
                        "failure-event.processing.worker.batch-size=4")
                .run(context -> {
                    FailureEventProcessingWorker worker = context.getBean(FailureEventProcessingWorker.class);
                    FailureEventClaimService claimService = context.getBean(FailureEventClaimService.class);
                    FailureEventProcessingService processingService = context.getBean(
                            FailureEventProcessingService.class);

                    worker.processNextBatch();

                    verify(claimService, times(1)).claimNextEligibleForProcessing(4);
                    verifyNoInteractions(processingService);
                    assertEquals(0, context.getBean(
                            "failureEventNormalizationExecutor",
                            ThreadPoolTaskExecutor.class).getThreadPoolExecutor().getTaskCount());
                    assertFalse(FailureEventProcessingWorker.class.getMethod("processNextBatch")
                            .isAnnotationPresent(Transactional.class));
                });
    }

    @Test
    void shouldSubmitTheWholeBatchAndAwaitEveryAttemptWhenOneFails() throws Exception {
        List<ClaimedFailureEvent> claims = List.of(
                new ClaimedFailureEvent(UUID.randomUUID(), 1),
                new ClaimedFailureEvent(UUID.randomUUID(), 1),
                new ClaimedFailureEvent(UUID.randomUUID(), 1));
        ClaimedFailureEvent failedClaim = claims.get(1);
        CountDownLatch allTasksStarted = new CountDownLatch(claims.size());
        CountDownLatch allowFailedTaskToFinish = new CountDownLatch(1);
        CountDownLatch failedTaskFinished = new CountDownLatch(1);
        CountDownLatch allowSuccessfulTasksToFinish = new CountDownLatch(1);
        CountDownLatch successfulTasksFinished = new CountDownLatch(claims.size() - 1);
        Set<String> processingThreadNames = ConcurrentHashMap.newKeySet();
        AtomicReference<String> scheduledThreadName = new AtomicReference<>();

        executionContextRunner
                .withPropertyValues(
                        "failure-event.processing.worker.enabled=true",
                        "failure-event.processing.worker.fixed-delay=1h",
                        "failure-event.processing.worker.batch-size=3",
                        "failure-event.processing.worker.concurrency=3")
                .run(context -> {
                    FailureEventProcessingWorker worker = context.getBean(FailureEventProcessingWorker.class);
                    FailureEventClaimService claimService = context.getBean(FailureEventClaimService.class);
                    FailureEventProcessingService processingService = context.getBean(
                            FailureEventProcessingService.class);
                    when(claimService.claimNextEligibleForProcessing(3)).thenReturn(claims);
                    doAnswer(invocation -> {
                        ClaimedFailureEvent claim = invocation.getArgument(0);
                        processingThreadNames.add(Thread.currentThread().getName());
                        allTasksStarted.countDown();
                        if (claim.equals(failedClaim)) {
                            awaitLatch(allowFailedTaskToFinish);
                            failedTaskFinished.countDown();
                            throw new IllegalStateException("simulated processing failure");
                        }
                        awaitLatch(allowSuccessfulTasksToFinish);
                        successfulTasksFinished.countDown();
                        return null;
                    }).when(processingService).process(any(ClaimedFailureEvent.class));

                    ExecutorService scheduledThread = Executors.newSingleThreadExecutor();
                    try {
                        Future<?> cycle = scheduledThread.submit(() -> {
                            scheduledThreadName.set(Thread.currentThread().getName());
                            worker.processNextBatch();
                        });

                        assertTrue(allTasksStarted.await(3, TimeUnit.SECONDS),
                                "Every claimed event should be submitted before the worker waits");
                        verify(claimService, times(1)).claimNextEligibleForProcessing(3);
                        for (ClaimedFailureEvent claim : claims) {
                            verify(processingService, times(1)).process(claim);
                        }
                        assertEquals(claims.size(), processingThreadNames.size());
                        assertTrue(processingThreadNames.stream()
                                .allMatch(name -> name.startsWith("failure-event-normalization-")));
                        assertFalse(processingThreadNames.contains(scheduledThreadName.get()));

                        allowFailedTaskToFinish.countDown();
                        assertTrue(failedTaskFinished.await(2, TimeUnit.SECONDS));
                        assertFalse(cycle.isDone(),
                                "The cycle must continue waiting for the other attempts after one fails");

                        allowSuccessfulTasksToFinish.countDown();
                        assertTrue(successfulTasksFinished.await(2, TimeUnit.SECONDS));
                        cycle.get(2, TimeUnit.SECONDS);
                    } finally {
                        allowSuccessfulTasksToFinish.countDown();
                        allowFailedTaskToFinish.countDown();
                        scheduledThread.shutdownNow();
                    }
                });
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during worker test", interrupted);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableScheduling
    @EnableConfigurationProperties(FailureEventWorkerProperties.class)
    @Import({FailureEventWorkerExecutorConfiguration.class, WorkerMocksConfiguration.class})
    @ComponentScan(
            basePackageClasses = FailureEventProcessingWorker.class,
            useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(
                    type = FilterType.ASSIGNABLE_TYPE,
                    classes = FailureEventProcessingWorker.class))
    static class WorkerConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(FailureEventWorkerProperties.class)
    @Import({
        FailureEventWorkerExecutorConfiguration.class,
        FailureEventProcessingWorker.class,
        WorkerMocksConfiguration.class
    })
    static class WorkerExecutionConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    static class WorkerMocksConfiguration {
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
