# Multi-stage Dockerfile for Streaming Job
# Stage 1: Build
FROM gradle:8.5-jdk17 AS builder

WORKDIR /workspace

# Copy Gradle files
COPY build.gradle settings.gradle ./
COPY gradle/ ./gradle/

# Copy source code
COPY src/jobs/ ./src/jobs/

# Build the application
RUN gradle :streaming:shadowJar --no-daemon

# Stage 2: Runtime
FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

# Copy JAR from builder stage
COPY --from=builder /workspace/src/jobs/streaming/build/libs/*-all.jar /app/streaming-job.jar

# Run as non-root user
RUN useradd -m -u 1000 benchmark
USER benchmark

ENTRYPOINT ["java", "-jar", "/app/streaming-job.jar"]
