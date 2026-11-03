import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.withType
import xyz.mon0mon.gradle.libsCatalog

plugins {
    kotlin("jvm")
}

group = "xyz.mon0mon"
version = "1.0-SNAPSHOT"

kotlin {
    jvmToolchain(25)
}

val kafkaClients = libsCatalog.findLibrary("kafka-clients").get()
val kafkaVersion = libsCatalog.findVersion("kafka").get().requiredVersion

dependencies {
    implementation(kafkaClients)
    implementation(libsCatalog.findLibrary("avro").get())
    implementation(libsCatalog.findLibrary("kafka-avro-serializer").get())
    implementation(libsCatalog.findLibrary("kotlin-logging").get())
    implementation(libsCatalog.findLibrary("slf4j-api").get())
    runtimeOnly(libsCatalog.findLibrary("logback-classic").get())
    testImplementation(kotlin("test"))
    constraints {
        // Confluent 8.4의 kafka-schema-registry-client가 kafka-clients:8.4.0-ccs를 요구한다.
        // 그 좌표는 Maven Central에 없다. 이 리포는 Apache kafka-clients 4.4.0을 쓴다.
        implementation(kafkaClients) {
            version { strictly(kafkaVersion) }
        }
    }
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}
