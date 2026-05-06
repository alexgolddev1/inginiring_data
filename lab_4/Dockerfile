FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY src ./src
RUN mvn -B -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/lab4-kafka-streams-1.0.0-jar-with-dependencies.jar app.jar
CMD ["java", "-jar", "/app/app.jar"]
