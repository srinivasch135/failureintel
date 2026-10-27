package com.failureintel.ingestion.application.service;

import com.failureintel.infrastructure.persistence.incidentgrouping.entity.IncidentEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentRepository;
import com.failureintel.ingestion.application.config.GroupingWorkerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDateTime;
import java.util.List;

/** Periodically discovers a bounded set of incidents for transactional closure. */
public class IncidentClosureWorker {
    private static final Logger LOGGER = LoggerFactory.getLogger(IncidentClosureWorker.class);

    private final IncidentRepository incidentRepository;
    private final IncidentClosureProcessor closureProcessor;
    private final JdbcTemplate jdbcTemplate;
    private final int batchSize;

    public IncidentClosureWorker(
            IncidentRepository incidentRepository,
            IncidentClosureProcessor closureProcessor,
            JdbcTemplate jdbcTemplate,
            GroupingWorkerProperties properties) {
        this.incidentRepository = incidentRepository;
        this.closureProcessor = closureProcessor;
        this.jdbcTemplate = jdbcTemplate;
        this.batchSize = properties.batchSize();
    }

    @Scheduled(fixedDelayString = "${failure-event.grouping.worker.fixed-delay}")
    public void closeExpiredIncidents() {
        LocalDateTime discoveryTime = jdbcTemplate.queryForObject("SELECT LOCALTIMESTAMP", LocalDateTime.class);
        List<IncidentEntity> candidates = incidentRepository.findExpiredActive(
                discoveryTime, PageRequest.of(0, batchSize));
        for (IncidentEntity candidate : candidates) {
            try {
                closureProcessor.closeIfExpired(candidate.getIncidentId());
            } catch (RuntimeException failure) {
                LOGGER.error("Incident closure failed: incidentId={}", candidate.getIncidentId(), failure);
            }
        }
    }
}
