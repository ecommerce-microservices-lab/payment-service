FROM maven:3.8.4-eclipse-temurin-11 AS build
WORKDIR /app

COPY pom.xml ./
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:11-jre

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

ARG PROJECT_VERSION=0.1.0
ARG ENVIRONMENT=dev
ARG USER_ID=1001
ARG GROUP_ID=1001

ENV SPRING_PROFILES_ACTIVE=${ENVIRONMENT}
ENV JAVA_OPTS="-Xmx512m -Xms256m -XX:+UseG1GC -XX:+UseContainerSupport"
ENV SERVER_PORT=8400

RUN groupadd -g ${GROUP_ID} appuser && \
    useradd -r -u ${USER_ID} -g appuser appuser

RUN mkdir -p /home/app && \
    chown -R appuser:appuser /home/app

WORKDIR /home/app
USER appuser

COPY --from=build --chown=appuser:appuser /app/target/payment-service-v${PROJECT_VERSION}.jar payment-service.jar

EXPOSE ${SERVER_PORT}

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:${SERVER_PORT}/actuator/health || exit 1

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -Dspring.profiles.active=$SPRING_PROFILES_ACTIVE -Dserver.port=$SERVER_PORT -Dmanagement.server.port=$SERVER_PORT -jar payment-service.jar"]
