# syntax=docker/dockerfile:1.7
FROM maven:3.9-eclipse-temurin-21-alpine AS builder
WORKDIR /src
COPY pom.xml .
COPY common/pom.xml common/
COPY order-service/pom.xml order-service/
COPY inventory-service/pom.xml inventory-service/
COPY payment-service/pom.xml payment-service/
COPY e2e-tests/pom.xml e2e-tests/
COPY common/src common/src
COPY order-service/src order-service/src
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -pl order-service -am package -DskipTests -Dspotless.check.skip=true

FROM eclipse-temurin:21-jre-alpine
RUN addgroup -S app && adduser -S app -G app
USER app
WORKDIR /app
COPY --from=builder /src/order-service/target/order-service-*-exec.jar app.jar
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70 -XX:+UseSerialGC"
EXPOSE 8081
HEALTHCHECK --interval=5s --timeout=3s --start-period=40s --retries=12 \
  CMD wget -qO- http://127.0.0.1:8081/actuator/health/readiness | grep -q UP || exit 1
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]
