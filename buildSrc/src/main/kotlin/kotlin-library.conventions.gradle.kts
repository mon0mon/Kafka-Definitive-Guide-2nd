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

dependencies {
    implementation(libsCatalog.findLibrary("kafka-clients").get())
    implementation(libsCatalog.findLibrary("kotlin-logging").get())
    implementation(libsCatalog.findLibrary("slf4j-api").get())
    runtimeOnly(libsCatalog.findLibrary("logback-classic").get())
    testImplementation(kotlin("test"))
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}
