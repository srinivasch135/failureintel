package com.failureintel.infrastructure.monitoring.health;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;


@Component
public class DatabasePerformanceHealthIndicator implements HealthIndicator {

    private final JdbcTemplate jdbcTemplate;
    private static final long LATENCY_THRESHOLD_MS = 500;

    public DatabasePerformanceHealthIndicator(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public Health health() {
        long startTime = System.currentTimeMillis();
        try {
            jdbcTemplate.execute("SELECT 1");
            long latency = System.currentTimeMillis() - startTime;

            if (latency < LATENCY_THRESHOLD_MS) {
                return Health.up().withDetail("latency_ms", latency).build();
            } else {
                return Health.down()
                        .withDetail("latency_ms", latency)
                        .withDetail("reason", "Latency exceeds threshold")
                        .build();
            }
        } catch (Exception e) {
            return Health.down(e).build();
        }
    }
}
