plugins {
    kotlin("jvm") version "2.1.21"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Kotlin Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.8.0") // For Dispatchers.IO etc.

    // Apache Kafka Client
    implementation("org.apache.kafka:kafka-clients:3.9.1") // Or your preferred Kafka client version

    // JSON Serialization/Deserialization (Jackson with Kotlin module)
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0")

    // For SLF4J (logging for Kafka client)
    // IMPORTANT: For production, use a proper logging backend like logback or log4j2
    implementation("org.slf4j:slf4j-simple:2.0.12")

    // For Redis client (Lettuce) - simplified for example purposes
    // In a real application, you'd configure this properly.
    implementation("io.lettuce:lettuce-core:6.3.2.RELEASE")

    // Test dependencies (optional)
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}