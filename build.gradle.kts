
plugins {
    kotlin("jvm") version "1.9.22"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val http4kVersion = "5.14.0.0"
val kafkaApiVersion = "3.7.0"


dependencies {
    implementation("org.apache.kafka:kafka-clients:$kafkaApiVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1-Beta")
    implementation("org.http4k:http4k-server-jetty:4.11.0")
    implementation("org.http4k:http4k-core")
    implementation(platform("org.http4k:http4k-bom:${http4kVersion}"))
    implementation("org.postgresql:postgresql:42.7.3")
    implementation("com.h2database:h2:2.2.224")
    implementation("io.github.oshai:kotlin-logging-jvm:5.1.0")
    implementation("org.slf4j:slf4j-api:1.7.32") // SLF4J API
    implementation("ch.qos.logback:logback-classic:1.5.3")


    testImplementation("org.jetbrains.kotlin:kotlin-test")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(21)
}