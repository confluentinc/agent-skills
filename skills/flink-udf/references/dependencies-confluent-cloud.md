# Project Dependencies - Confluent Cloud

Maven and Gradle build configuration for Flink UDFs (scalar UDFs, UDTFs, and PTFs) targeting Confluent Cloud for Apache Flink.

Flink dependencies are marked as `provided` (Maven) / `compileOnly` (Gradle) since Confluent Cloud provides them at runtime. The `maven-shade-plugin` (or Gradle `shadow` plugin) excludes them from the JAR.

The `confluent-flink-table-api-java-plugin` is **required for PTFs**. It is harmless to include for scalar UDFs and UDTFs.

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
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-common</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>io.confluent.flink</groupId>
        <artifactId>confluent-flink-table-api-java-plugin</artifactId>
        <version>2.2-24</version>
        <scope>provided</scope>
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
                    <configuration>
                        <artifactSet>
                            <excludes>
                                <exclude>org.apache.flink:*</exclude>
                            </excludes>
                        </artifactSet>
                    </configuration>
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
    compileOnly 'org.apache.flink:flink-table-api-java:2.2.0'
    compileOnly 'org.apache.flink:flink-table-common:2.2.0'
    compileOnly 'io.confluent.flink:confluent-flink-table-api-java-plugin:2.2-24'

    // Add any third-party dependencies here (they will be bundled into the shaded JAR)
}

shadowJar {
    dependencies {
        exclude(dependency('org.apache.flink:.*'))
    }
}
```

Build:
```bash
./gradlew shadowJar
```

Output: `build/libs/<project-name>-all.jar`

## Adding Third-Party Dependencies

For libraries the UDF needs at runtime (e.g., Jackson, Guava), declare them with normal `compile` / `implementation` scope so the shade/shadow plugin bundles them into the artifact:

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
