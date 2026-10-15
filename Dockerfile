
# Stage 1: Build
FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /app

COPY pom.xml ./
COPY .mvn ./.mvn
COPY mvnw mvnw
COPY mvnw.cmd mvnw.cmd
COPY src ./src

RUN mvn -DskipTests package


# Stage 2: Runtime
FROM eclipse-temurin:17-jre

WORKDIR /app

# Install curl for application healthchecks
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build \
    /app/target/failureintel-0.0.1-SNAPSHOT.jar \
    /app/app.jar

EXPOSE 8081

# Verify Spring Boot Actuator health
HEALTHCHECK --interval=5s \
            --timeout=3s \
            --start-period=30s \
            --retries=10 \
    CMD curl --fail --silent --output /dev/null \
        http://127.0.0.1:8081/actuator/health || exit 1

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
