package xyz.mon0mon.common

import org.apache.kafka.clients.producer.ProducerConfig
import kotlin.test.Test
import kotlin.test.assertEquals

class KafkaSettingsTest {
    @Test
    fun `string producer properties include bootstrap servers and client id`() {
        val props = KafkaSettings.stringProducerProperties(clientId = "ch03-producer")

        assertEquals(KafkaSettings.BOOTSTRAP_SERVERS, props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
        assertEquals("ch03-producer", props.get(ProducerConfig.CLIENT_ID_CONFIG))
        assertEquals("all", props.get(ProducerConfig.ACKS_CONFIG))
    }

    @Test
    fun `bootstrap servers fall back to single cluster`() {
        System.clearProperty(KafkaSettings.BOOTSTRAP_SERVERS_PROPERTY)

        assertEquals(KafkaSettings.BOOTSTRAP_SERVERS, KafkaSettings.bootstrapServers())
    }

    @Test
    fun `schema registry url falls back to single cluster`() {
        System.clearProperty(KafkaSettings.SCHEMA_REGISTRY_URL_PROPERTY)

        assertEquals(KafkaSettings.SCHEMA_REGISTRY_URL, KafkaSettings.schemaRegistryUrl())
    }

    @Test
    fun `schema registry url honors system property`() {
        System.setProperty(
            KafkaSettings.SCHEMA_REGISTRY_URL_PROPERTY,
            KafkaSettings.CLUSTER_SCHEMA_REGISTRY_URL,
        )
        try {
            assertEquals(KafkaSettings.CLUSTER_SCHEMA_REGISTRY_URL, KafkaSettings.schemaRegistryUrl())
        } finally {
            System.clearProperty(KafkaSettings.SCHEMA_REGISTRY_URL_PROPERTY)
        }
    }

    @Test
    fun `bootstrap servers honor system property`() {
        System.setProperty(
            KafkaSettings.BOOTSTRAP_SERVERS_PROPERTY,
            KafkaSettings.CLUSTER_BOOTSTRAP_SERVERS,
        )
        try {
            assertEquals(KafkaSettings.CLUSTER_BOOTSTRAP_SERVERS, KafkaSettings.bootstrapServers())
        } finally {
            System.clearProperty(KafkaSettings.BOOTSTRAP_SERVERS_PROPERTY)
        }
    }
}
