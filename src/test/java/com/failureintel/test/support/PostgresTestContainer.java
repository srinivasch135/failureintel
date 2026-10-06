package com.failureintel.test.support;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/** One PostgreSQL server per test JVM, with a separate database for each integration-test class. */
public final class PostgresTestContainer {

    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("postgres:16-alpine"))
            .withDatabaseName("failureintel_test")
            .withUsername("testuser")
            .withPassword("testpassword");

    private static final List<String> DATABASES = List.of(
            "failureintel_context_test",
            "failureintel_claim_test",
            "failureintel_controller_test",
            "failureintel_durable_capture_test",
            "failureintel_ingestion_test",
            "failureintel_normalization_processor_test",
            "failureintel_normalization_rollback_test",
            "failureintel_pipeline_test",
            "failureintel_recovery_schedule_test",
            "failureintel_worker_restart_test");

    static {
        POSTGRES.start();
        createDatabases();
    }

    private PostgresTestContainer() {
    }

    public static void registerDatabase(DynamicPropertyRegistry registry, String database) {
        registry.add("spring.datasource.url", () -> jdbcUrl(database));
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    public static String jdbcUrl(String database) {
        if (!DATABASES.contains(database)) {
            throw new IllegalArgumentException("Unknown test database: " + database);
        }
        return POSTGRES.getJdbcUrl().replace("/failureintel_test", "/" + database);
    }

    public static String username() {
        return POSTGRES.getUsername();
    }

    public static String password() {
        return POSTGRES.getPassword();
    }

    private static void createDatabases() {
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                Statement statement = connection.createStatement()) {
            for (String database : DATABASES) {
                statement.execute("CREATE DATABASE " + database);
            }
        } catch (SQLException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }
}
