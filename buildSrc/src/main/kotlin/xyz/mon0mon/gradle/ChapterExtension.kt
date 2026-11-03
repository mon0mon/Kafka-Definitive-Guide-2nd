package xyz.mon0mon.gradle

import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property

enum class KafkaCluster(
    val bootstrapServers: String,
    val schemaRegistryUrl: String,
    val composeTaskPath: String,
) {
    SINGLE(
        bootstrapServers = "localhost:9092",
        schemaRegistryUrl = "http://localhost:8081",
        composeTaskPath = ":composeUp",
    ),
    CLUSTER(
        bootstrapServers = "localhost:19092,localhost:19093,localhost:19094",
        schemaRegistryUrl = "http://localhost:18081",
        composeTaskPath = ":clusterComposeUp",
    ),
}

abstract class ChapterExtension {
    abstract val kafkaCluster: Property<KafkaCluster>

    /** task `run{Name}` → fully qualified main class (`object` with `@JvmStatic fun main`) */
    abstract val examples: MapProperty<String, String>
}
