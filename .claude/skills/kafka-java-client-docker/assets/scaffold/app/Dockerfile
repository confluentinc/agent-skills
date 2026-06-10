# syntax=docker/dockerfile:1

# --- Build stage: compile and package inside a Maven image -------------------
# Nothing is built on the host, so the developer needs no local JDK or Maven.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app

# Resolve dependencies first so Docker layer caching skips re-downloading them
# when only source files change.
COPY pom.xml .
RUN mvn -q -e -B dependency:go-offline

COPY src ./src
RUN mvn -q -e -B clean package

# --- Runtime stage: small JRE image that just runs the fat jar ---------------
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/app.jar app.jar

# MAIN_CLASS selects which app this container runs (producer or consumer), so a
# single image backs both docker-compose services. Override it per service.
ENV MAIN_CLASS=io.confluent.examples.ProducerApp
ENTRYPOINT ["sh", "-c", "exec java -cp app.jar \"$MAIN_CLASS\""]
