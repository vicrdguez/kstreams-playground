/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    // Apply the java Plugin to add support for Java.
    java
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven{
        url = uri("https://packages.confluent.io/maven/")
    }
    // This one is needed to pull com.github.everit-org.json-schema:org.everit.json.schema:1.12.2
    // for io.confluent:kafka-json-schema-serializer:7.0.1 secured producer dependency
    // see: https://github.com/everit-org/json-schema/#maven-installation
    maven {
        url = uri("https://jitpack.io")
    }
}

var cfltVersion = "7.3.1"
var kafkaVersion = "3.3.1"
var slf4jVersion = "2.0.6"

dependencies {

    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
//    implementation("io.confluent:kafka-streams-avro-serde:$cfltVersion")
//    implementation("io.confluent:kafka-json-serializer:$cfltVersion")
    implementation("com.google.guava:guava:31.1-jre")

    implementation("org.slf4j:slf4j-api:$slf4jVersion")
    implementation("org.slf4j:slf4j-reload4j:$slf4jVersion")
    // Use JUnit Jupiter for testing.
    testImplementation("org.junit.jupiter:junit-jupiter:5.8.1")
}

tasks.named<Test>("test") {
    // Use JUnit Platform for unit tests.
    useJUnitPlatform()
}
