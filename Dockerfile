FROM maven:3.8-openjdk-11 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src src
RUN mvn -q package -DskipTests

FROM openjdk:11-jre-slim
COPY --from=build /src/target/dagrunner-1.0.0.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
