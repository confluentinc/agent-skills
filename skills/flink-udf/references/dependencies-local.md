# Project Dependencies - Local Docker

Maven and Gradle build configuration for Flink UDFs (scalar UDFs, UDTFs, and PTFs) targeting a local Flink environment running in Docker.

Unlike Confluent Cloud, the local Flink runtime does not provide all needed Flink modules on its classpath, so dependencies are **not** marked as `provided`. They are bundled into the JAR by the shade/shadow plugin.

## Maven Dependencies

```xml
<properties>
    <flink.version>2.2.0</flink.version>
    <java.version>17</java.version>
</properties>

<dependencies>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-api-java</artifactId>
        <version>${flink.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-planner_2.12</artifactId>
        <version>${flink.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients</artifactId>
        <version>${flink.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-base</artifactId>
        <version>${flink.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka</artifactId>
        <version>4.0.1-2.0</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-avro-confluent-registry</artifactId>
        <version>${flink.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-avro</artifactId>
        <version>${flink.version}</version>
    </dependency>
</dependencies>

<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>3.5.0</version>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals>
                        <goal>shade</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

Build:
```bash
mvn clean package
```

Output: `target/<artifact-id>-<version>.jar`

## Gradle Configuration

```gradle
plugins {
    id 'java'
    id 'com.github.johnrengelman.shadow' version '8.1.1'
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

dependencies {
    implementation 'org.apache.flink:flink-table-api-java:2.2.0'
    implementation 'org.apache.flink:flink-table-planner_2.12:2.2.0'
    implementation 'org.apache.flink:flink-clients:2.2.0'
    implementation 'org.apache.flink:flink-connector-base:2.2.0'
    implementation 'org.apache.flink:flink-connector-kafka:4.0.1-2.0'
    implementation 'org.apache.flink:flink-avro-confluent-registry:2.2.0'
    implementation 'org.apache.flink:flink-avro:2.2.0'
}
```

Build:
```bash
./gradlew shadowJar
```

Output: `build/libs/<project-name>-all.jar`

## Adding Third-Party Dependencies

For additional libraries the UDF needs at runtime (e.g., Jackson, Guava), declare them with normal `compile` / `implementation` scope so they are bundled into the artifact:

Maven:
```xml
<dependency>
    <groupId>com.google.guava</groupId>
    <artifactId>guava</artifactId>
    <version>32.1.3-jre</version>
</dependency>
```

Gradle:
```gradle
implementation 'com.google.guava:guava:32.1.3-jre'
```
