/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("java-shared")
}

dependencies {
//    api(project(":list"))
    implementation("io.confluent:kafka-json-schema-serializer:7.0.1")
    implementation("io.confluent:kafka-streams-avro-serde:7.3.1")
}
