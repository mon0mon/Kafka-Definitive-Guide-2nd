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
