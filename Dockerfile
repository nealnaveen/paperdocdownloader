# Use an official Maven image to build the project
FROM maven:3.8.4-openjdk-17 AS build

# Set the working directory in the container
WORKDIR /app

# Copy the pom.xml file to download dependencies
# Copy the pom.xml file to download dependencies
COPY pom.xml .

# Copy the JAR file that is referenced in the pom.xml from the local path
COPY src/main/resources/BulkDownloader-0.0.2-SNAPSHOT.jar src/main/resources/BulkDownloader-0.0.2-SNAPSHOT.jar

# Download all dependencies (this step is cached unless pom.xml changes)
RUN mvn dependency:go-offline -B

# Copy the entire project to the working directory
COPY . .
COPY src/* .
# Package the application (compile + package)
RUN mvn package -DskipTests

# Use an official OpenJDK runtime as a parent image
FROM openjdk:17-jdk-slim

# Set the working directory in the container
WORKDIR /app

# Copy the jar file from the build stage to the runtime stage
COPY --from=build /app/target/*-jar-with-dependencies.jar /app/app.jar

# Command to run the application
CMD ["java", "-jar", "/app/app.jar"]
