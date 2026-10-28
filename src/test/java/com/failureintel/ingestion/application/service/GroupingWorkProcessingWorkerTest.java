package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.application.config.GroupingWorkerProperties;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GroupingWorkProcessingWorkerTest {
    private ThreadPoolTaskExecutor executor;

    @AfterEach
    void shutDown() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Test
    void claimsNoMoreThanBatchOrAvailableConcurrencyAndPropagatesFullOwnership() throws Exception {
        GroupingWorkClaimService claims = mock(GroupingWorkClaimService.class);
        GroupingWorkProcessor processor = mock(GroupingWorkProcessor.class);
        List<ClaimedGroupingWork> batch = List.of(claim(1), claim(2), claim(3), claim(4));
        when(claims.claimNextEligibleForProcessing(anyInt())).thenReturn(batch);
        CountDownLatch processed = new CountDownLatch(2);
        org.mockito.Mockito.doAnswer(invocation -> {
            ClaimedGroupingWork claim = invocation.getArgument(0);
            assertTrue(batch.contains(claim));
            assertTrue(!TransactionSynchronizationManager.isActualTransactionActive(),
                    "processor must run outside the claim transaction");
            processed.countDown();
            return null;
        }).when(processor).process(org.mockito.ArgumentMatchers.any());

        GroupingWorkProcessingWorker worker = worker(claims, processor, 3, 2);
        worker.processNextBatch();

        verify(claims).claimNextEligibleForProcessing(2);
        assertTrue(processed.await(2, TimeUnit.SECONDS));
        verify(processor, org.mockito.Mockito.times(2)).process(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void claimRollbackDoesNotDispatch() {
        GroupingWorkClaimService claims = mock(GroupingWorkClaimService.class);
        GroupingWorkProcessor processor = mock(GroupingWorkProcessor.class);
        when(claims.claimNextEligibleForProcessing(anyInt())).thenThrow(new IllegalStateException("rollback"));

        worker(claims, processor, 4, 2).processNextBatch();

        verify(processor, never()).process(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void isolatesProcessorFailureAndContinuesOtherClaims() throws Exception {
        GroupingWorkClaimService claims = mock(GroupingWorkClaimService.class);
        GroupingWorkProcessor processor = mock(GroupingWorkProcessor.class);
        ClaimedGroupingWork failed = claim(1);
        ClaimedGroupingWork succeeds = claim(2);
        when(claims.claimNextEligibleForProcessing(2)).thenReturn(List.of(failed, succeeds));
        CountDownLatch processed = new CountDownLatch(2);
        org.mockito.Mockito.doAnswer(invocation -> {
            ClaimedGroupingWork current = invocation.getArgument(0);
            processed.countDown();
            if (current.equals(failed)) throw new IllegalStateException("expected test failure");
            return null;
        }).when(processor).process(org.mockito.ArgumentMatchers.any());

        worker(claims, processor, 2, 2).processNextBatch();

        assertTrue(processed.await(2, TimeUnit.SECONDS));
        verify(processor, org.mockito.Mockito.times(2)).process(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void dispatchRejectionLeavesCommittedClaimsUndispatchedAndDoesNotClaimAgain() {
        GroupingWorkClaimService claims = mock(GroupingWorkClaimService.class);
        GroupingWorkProcessor processor = mock(GroupingWorkProcessor.class);
        when(claims.claimNextEligibleForProcessing(1)).thenReturn(List.of(claim(5)));
        GroupingWorkProcessingWorker worker = worker(claims, processor, 1, 1);
        executor.shutdown();

        worker.processNextBatch();

        verify(processor, never()).process(org.mockito.ArgumentMatchers.any());
        verify(claims).claimNextEligibleForProcessing(1);
    }

    @Test
    void shutdownWaitsForAlreadyDispatchedWorkWithoutMarkingItComplete() throws Exception {
        GroupingWorkClaimService claims = mock(GroupingWorkClaimService.class);
        GroupingWorkProcessor processor = mock(GroupingWorkProcessor.class);
        ClaimedGroupingWork claim = claim(6);
        when(claims.claimNextEligibleForProcessing(1)).thenReturn(List.of(claim));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        org.mockito.Mockito.doAnswer(invocation -> {
            started.countDown();
            assertTrue(release.await(2, TimeUnit.SECONDS));
            return null;
        }).when(processor).process(org.mockito.ArgumentMatchers.any());
        worker(claims, processor, 1, 1).processNextBatch();

        assertTrue(started.await(2, TimeUnit.SECONDS));
        Thread shutdown = new Thread(executor::shutdown);
        shutdown.start();
        Thread.sleep(50);
        assertTrue(shutdown.isAlive(), "shutdown should wait for the running task");
        release.countDown();
        shutdown.join(2000);
        assertTrue(!shutdown.isAlive());
        verify(processor).process(claim);
    }

    @Test
    void configurationValidatesIndependentSettings() {
        GroupingWorkerProperties properties = new GroupingWorkerProperties(
                false, Duration.ofSeconds(3), Duration.ofMinutes(5), 7, 4, Duration.ZERO);
        assertEquals(7, properties.batchSize());
        assertEquals(4, properties.concurrency());
    }

    @Test
    void groupingEnablementIsIndependentOfNormalizationWorkerSetting() {
        new ApplicationContextRunner()
                .withUserConfiguration(GroupingConfiguration.class)
                .withPropertyValues(
                        "failure-event.grouping.worker.enabled=true",
                        "failure-event.grouping.worker.fixed-delay=3s",
                        "failure-event.grouping.worker.processing-lease-timeout=5m",
                        "failure-event.grouping.worker.batch-size=7",
                        "failure-event.grouping.worker.concurrency=4",
                        "failure-event.grouping.worker.shutdown-await=0s")
                .run(context -> {
                    assertTrue(context.containsBean("groupingWorkProcessingWorker"));
                    assertTrue(!context.getEnvironment().containsProperty("failure-event.processing.worker.enabled"));
                });

        new ApplicationContextRunner()
                .withUserConfiguration(GroupingConfiguration.class)
                .withPropertyValues(
                        "failure-event.grouping.worker.enabled=false",
                        "failure-event.grouping.worker.fixed-delay=3s",
                        "failure-event.grouping.worker.processing-lease-timeout=5m",
                        "failure-event.grouping.worker.batch-size=7",
                        "failure-event.grouping.worker.concurrency=4",
                        "failure-event.grouping.worker.shutdown-await=0s")
                .run(context -> {
                    assertTrue(!context.containsBean("groupingWorkProcessingWorker"));
                    assertTrue(!context.containsBean("groupingWorkExecutor"));
                });
    }

    private GroupingWorkProcessingWorker worker(
            GroupingWorkClaimService claims, GroupingWorkProcessor processor, int batchSize, int concurrency) {
        executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(concurrency);
        executor.setMaxPoolSize(concurrency);
        executor.setQueueCapacity(concurrency);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationMillis(1000);
        executor.initialize();
        return new GroupingWorkProcessingWorker(claims, mock(GroupingWorkRecoveryService.class), processor,
                new GroupingWorkerProperties(true, Duration.ofSeconds(1), Duration.ofMinutes(5),
                        batchSize, concurrency, Duration.ofSeconds(1)),
                executor);
    }

    private static ClaimedGroupingWork claim(int attempt) {
        return new ClaimedGroupingWork(UUID.randomUUID(), attempt);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(GroupingWorkerProperties.class)
    @Import(com.failureintel.ingestion.application.config.GroupingWorkerExecutorConfiguration.class)
    static class GroupingConfiguration {
        @Bean
        GroupingWorkClaimService groupingWorkClaimService() {
            return mock(GroupingWorkClaimService.class);
        }

        @Bean
        GroupingWorkRecoveryService groupingWorkRecoveryService() {
            return mock(GroupingWorkRecoveryService.class);
        }

        @Bean
        GroupingWorkProcessor groupingWorkProcessor() {
            return mock(GroupingWorkProcessor.class);
        }

        @Bean
        IncidentRepository incidentRepository() {
            return mock(IncidentRepository.class);
        }

        @Bean
        IncidentClosureProcessor closureProcessor() {
            return mock(IncidentClosureProcessor.class);
        }

        @Bean
        JdbcTemplate jdbcTemplate() {
            return mock(JdbcTemplate.class);
        }
    }
}
