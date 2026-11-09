# Build context: repository root.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY workers/pom.xml workers/pom.xml
COPY workers/java workers/java
COPY workers/spring-boot workers/spring-boot
RUN mvn -q -B -f workers/pom.xml -pl spring-boot -am -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/workers/spring-boot/target/orders-worker-spring-1.0.0.jar /app/worker.jar
EXPOSE 9464
ENTRYPOINT ["java", "-jar", "/app/worker.jar"]
