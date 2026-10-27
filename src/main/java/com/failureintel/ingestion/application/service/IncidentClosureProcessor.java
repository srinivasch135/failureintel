package com.failureintel.ingestion.application.service;

import com.failureintel.ingestion.domain.grouping.CanonicalGroupingSignature;
import com.failureintel.infrastructure.persistence.incidentgrouping.entity.IncidentEntity;
import com.failureintel.infrastructure.persistence.incidentgrouping.repository.IncidentRepository;
import jakarta.persistence.EntityManager;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.UUID;

/** Revalidates and closes one incident under its grouping signature lock. */
@Service
public class IncidentClosureProcessor {
    private final IncidentRepository incidentRepository;
    private final JdbcTemplate jdbcTemplate;
    private final EntityManager entityManager;

    public IncidentClosureProcessor(
            IncidentRepository incidentRepository, JdbcTemplate jdbcTemplate, EntityManager entityManager) {
        this.incidentRepository = incidentRepository;
        this.jdbcTemplate = jdbcTemplate;
        this.entityManager = entityManager;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public boolean closeIfExpired(UUID incidentId) {
        IncidentEntity candidate = incidentRepository.findById(incidentId).orElse(null);
        if (candidate == null) {
            return false;
        }
        CanonicalGroupingSignature signature = CanonicalGroupingSignature.fromFields(
                candidate.getServiceName(), candidate.getEnvironment(), candidate.getErrorType(),
                candidate.getDependencyTarget()).orElse(null);
        if (signature == null) {
            return false;
        }

        jdbcTemplate.execute((ConnectionCallback<Void>) connection -> {
            try (PreparedStatement statement = connection.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
                statement.setLong(1, signature.advisoryLockKey());
                statement.execute();
            }
            return null;
        });

        entityManager.clear();
        IncidentEntity current = incidentRepository.findById(incidentId).orElse(null);
        if (current == null || !"ACTIVE".equals(current.getStatus())) {
            return false;
        }

        LocalDateTime closureTime = jdbcTemplate.queryForObject("SELECT LOCALTIMESTAMP", LocalDateTime.class);
        if (current.getInactivityExpiresAt().isAfter(closureTime)) {
            return false;
        }
        current.close(closureTime);
        return true;
    }
}
