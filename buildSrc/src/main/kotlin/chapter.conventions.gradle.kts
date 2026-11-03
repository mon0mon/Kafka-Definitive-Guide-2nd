import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.the
import org.gradle.process.CommandLineArgumentProvider
import xyz.mon0mon.gradle.ChapterExtension
import xyz.mon0mon.gradle.KafkaCluster

plugins {
    id("kotlin-library.conventions")
    application
}

val chapter = extensions.create("chapter", ChapterExtension::class.java)
chapter.kafkaCluster.convention(KafkaCluster.SINGLE)

fun JavaExec.attachChapterKafka() {
    val cluster = chapter.kafkaCluster
    dependsOn(cluster.map { it.composeTaskPath })
    jvmArgumentProviders.add(
        CommandLineArgumentProvider {
            listOf(
                "-Dkafka.bootstrap.servers=${cluster.get().bootstrapServers}",
                "-Dkafka.schema.registry.url=${cluster.get().schemaRegistryUrl}",
            )
        },
    )
}

tasks.named<JavaExec>("run") {
    attachChapterKafka()
}

afterEvaluate {
    val mainRuntimeClasspath = the<SourceSetContainer>().named("main").map { it.runtimeClasspath }
    chapter.examples.get().forEach { (name, main) ->
        tasks.register<JavaExec>("run$name") {
            group = "application"
            description = "Run chapter example $name"
            mainClass.set(main)
            classpath = mainRuntimeClasspath.get()
            attachChapterKafka()
        }
    }
}
