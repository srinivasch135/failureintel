package com.failureintel.ingestion.application.config;

import com.failureintel.ingestion.application.service.GroupingWorkClaimService;
import com.failureintel.ingestion.application.service.GroupingWorkProcessingWorker;
import com.failureintel.ingestion.application.service.GroupingWorkProcessor;
import com.failureintel.ingestion.application.service.GroupingWorkRecoveryService;
import com.failureintel.ingestion.application.service.IncidentClosureProcessor;
import com.failureintel.ingestion.application.service.IncidentClosureWorker;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
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
            GroupingWorkRecoveryService recoveryService,
            GroupingWorkProcessor processor,
            GroupingWorkerProperties properties,
            ThreadPoolTaskExecutor groupingWorkExecutor) {
        return new GroupingWorkProcessingWorker(
                claimService, recoveryService, processor, properties, groupingWorkExecutor);
    }

    @Bean
    public IncidentClosureWorker incidentClosureWorker(
            IncidentRepository incidentRepository,
            IncidentClosureProcessor closureProcessor,
            JdbcTemplate jdbcTemplate,
            GroupingWorkerProperties properties) {
        return new IncidentClosureWorker(incidentRepository, closureProcessor, jdbcTemplate, properties);
    }
}
