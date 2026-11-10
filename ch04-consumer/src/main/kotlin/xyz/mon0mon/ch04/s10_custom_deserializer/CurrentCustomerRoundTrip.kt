package xyz.mon0mon.ch04.s10_custom_deserializer

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.ch03.s05_serializer.BookCustomSerializer
import xyz.mon0mon.ch03.s05_serializer.Customer as SerializedCustomer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.10.1 Custom Deserializer
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  FQCN으로 serializer/deserializer 클래스를 지정한다 ([BookDeserializerSample]).
 * - 4.4  `StringSerializer`와 3장 [BookCustomSerializer], [BookDeserializer] 인스턴스를 생성자에 넘긴다.
 *        `use`로 close를 보장한다.
 *
 * Replaces: [BookDeserializerSample]
 * Still valid: `subscribe`, `poll(Duration)`, `group.id`
 */
object CurrentCustomerRoundTrip {
    private const val TOPIC = "ch04Customer"
    private const val GROUP = "ch04-s101-current"

    @JvmStatic
    fun main(args: Array<String>) {
        ensureTopic(TOPIC)

        val producerConfigs = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.CLIENT_ID_CONFIG to "ch04-s101-current",
        )
        val consumerConfigs = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG to GROUP,
            ConsumerConfig.CLIENT_ID_CONFIG to "ch04-s101-current",
        )

        // region 4.10.1 커스텀 디시리얼라이저
        KafkaProducer(producerConfigs, StringSerializer(), BookCustomSerializer()).use { producer ->
            KafkaConsumer(consumerConfigs, StringDeserializer(), BookDeserializer()).use { consumer ->
                consumer.subscribe(listOf(TOPIC))
                awaitAssignment(consumer)
                if (consumer.assignment().isEmpty()) {
                    return
                }

                val samples = listOf(
                    SerializedCustomer(1, "Precision Products"),
                    SerializedCustomer(2, "Biomedical Materials"),
                )
                for (customer in samples) {
                    try {
                        val metadata = producer.send(ProducerRecord(TOPIC, customer.id.toString(), customer)).get()
                        logger.info {
                            "sent topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()} id=${customer.id} name=${customer.name}"
                        }
                    } catch (e: Exception) {
                        logger.error(e) { "send failed id=${customer.id}" }
                    }
                }

                val deadline = System.currentTimeMillis() + 15_000
                var seen = 0
                while (seen < samples.size && System.currentTimeMillis() < deadline) {
                    val records = consumer.poll(Duration.ofMillis(1000))
                    for (record in records) {
                        val customer = record.value()
                        logger.info {
                            "offset=${record.offset()} key=${record.key()} id=${customer?.id} name=${customer?.name}"
                        }
                        seen++
                    }
                }
                if (seen < samples.size) {
                    logger.error { "poll timed out seen=$seen expected=${samples.size}" }
                }
            }
        }
        // endregion
    }

    private fun ensureTopic(topic: String) {
        val configs = mapOf(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
        )
        AdminClient.create(configs).use { admin ->
            val names = admin.listTopics().names().get()
            if (names.contains(topic)) {
                logger.info { "topic exists name=$topic" }
                return
            }
            admin.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()
            logger.info { "topic created name=$topic partitions=1" }
        }
    }

    private fun awaitAssignment(consumer: KafkaConsumer<String, Customer>) {
        val deadline = System.currentTimeMillis() + 15_000
        while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200))
        }
        if (consumer.assignment().isEmpty()) {
            logger.error { "assignment timed out group=$GROUP topic=$TOPIC" }
        }
    }
}
