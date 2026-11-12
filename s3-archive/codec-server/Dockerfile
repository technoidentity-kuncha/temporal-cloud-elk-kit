FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre
COPY --from=build /src/target/codec-server.jar /app/codec-server.jar
EXPOSE 8888
ENTRYPOINT ["java", "-jar", "/app/codec-server.jar"]
