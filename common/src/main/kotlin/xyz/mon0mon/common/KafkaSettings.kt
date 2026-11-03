package xyz.mon0mon.common

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

object KafkaSettings {
    const val BOOTSTRAP_SERVERS_PROPERTY = "kafka.bootstrap.servers"
    const val BOOTSTRAP_SERVERS = "localhost:9092"
    const val CLUSTER_BOOTSTRAP_SERVERS = "localhost:19092,localhost:19093,localhost:19094"
    const val SCHEMA_REGISTRY_URL_PROPERTY = "kafka.schema.registry.url"
    const val SCHEMA_REGISTRY_URL = "http://localhost:8081"
    const val CLUSTER_SCHEMA_REGISTRY_URL = "http://localhost:18081"

    fun bootstrapServers(): String =
        System.getProperty(BOOTSTRAP_SERVERS_PROPERTY, BOOTSTRAP_SERVERS)

    fun schemaRegistryUrl(): String =
        System.getProperty(SCHEMA_REGISTRY_URL_PROPERTY, SCHEMA_REGISTRY_URL)

    fun stringProducerProperties(
        clientId: String,
        bootstrapServers: String = bootstrapServers(),
    ): Properties = Properties().apply {
        put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.ACKS_CONFIG, "all")
        put(ProducerConfig.CLIENT_ID_CONFIG, clientId)
    }
}
