import org.gradle.api.tasks.JavaExec
import org.gradle.kotlin.dsl.named
import org.gradle.process.CommandLineArgumentProvider
import xyz.mon0mon.gradle.ChapterExtension
import xyz.mon0mon.gradle.KafkaCluster

plugins {
    id("kotlin-library.conventions")
    application
}

val chapter = extensions.create("chapter", ChapterExtension::class.java)
chapter.kafkaCluster.convention(KafkaCluster.SINGLE)

tasks.named<JavaExec>("run") {
    val cluster = chapter.kafkaCluster
    dependsOn(cluster.map { it.composeTaskPath })
    jvmArgumentProviders.add(
        CommandLineArgumentProvider {
            listOf("-Dkafka.bootstrap.servers=${cluster.get().bootstrapServers}")
        },
    )
}
