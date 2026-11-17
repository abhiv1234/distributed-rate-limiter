# ---- build ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

# ---- run ----
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/distributed-rate-limiter.jar app.jar
EXPOSE 8080
# Small heap is plenty: state lives in Redis; local mode state is evicted when idle.
ENTRYPOINT ["java", "-XX:+UseZGC", "-Xmx256m", "-jar", "app.jar"]
