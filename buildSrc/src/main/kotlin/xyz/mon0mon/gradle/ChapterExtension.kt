package xyz.mon0mon.gradle

import org.gradle.api.provider.Property

enum class KafkaCluster(
    val bootstrapServers: String,
    val composeTaskPath: String,
) {
    SINGLE(
        bootstrapServers = "localhost:9092",
        composeTaskPath = ":composeUp",
    ),
    CLUSTER(
        bootstrapServers = "localhost:19092,localhost:19093,localhost:19094",
        composeTaskPath = ":clusterComposeUp",
    ),
}

abstract class ChapterExtension {
    abstract val kafkaCluster: Property<KafkaCluster>
}
