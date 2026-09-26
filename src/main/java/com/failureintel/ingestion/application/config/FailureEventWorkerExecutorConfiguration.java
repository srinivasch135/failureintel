package com.failureintel.ingestion.application.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
        prefix = "failure-event.processing.worker",
        name = "enabled",
        havingValue = "true")
public class FailureEventWorkerExecutorConfiguration {

    @Bean
    public ThreadPoolTaskExecutor failureEventNormalizationExecutor(
            FailureEventWorkerProperties workerProperties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(workerProperties.concurrency());
        executor.setMaxPoolSize(workerProperties.concurrency());
        executor.setQueueCapacity(workerProperties.batchSize());
        executor.setThreadNamePrefix("failure-event-normalization-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationMillis(workerProperties.shutdownAwait().toMillis());
        return executor;
    }
}
