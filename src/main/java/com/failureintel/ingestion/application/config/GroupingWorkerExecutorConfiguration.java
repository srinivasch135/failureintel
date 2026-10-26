package com.failureintel.ingestion.application.config;

import com.failureintel.ingestion.application.service.GroupingWorkClaimService;
import com.failureintel.ingestion.application.service.GroupingWorkProcessingWorker;
import com.failureintel.ingestion.application.service.GroupingWorkProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "failure-event.grouping.worker", name = "enabled", havingValue = "true")
public class GroupingWorkerExecutorConfiguration {

    @Bean
    public ThreadPoolTaskExecutor groupingWorkExecutor(GroupingWorkerProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(properties.concurrency());
        executor.setMaxPoolSize(properties.concurrency());
        executor.setQueueCapacity(properties.concurrency());
        executor.setThreadNamePrefix("failure-event-grouping-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationMillis(properties.shutdownAwait().toMillis());
        return executor;
    }

    @Bean
    public GroupingWorkProcessingWorker groupingWorkProcessingWorker(
            GroupingWorkClaimService claimService,
            GroupingWorkProcessor processor,
            GroupingWorkerProperties properties,
            ThreadPoolTaskExecutor groupingWorkExecutor) {
        return new GroupingWorkProcessingWorker(claimService, processor, properties, groupingWorkExecutor);
    }
}
