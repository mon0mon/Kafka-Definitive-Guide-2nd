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
import xyz.mon0mon.ch03.s05_serializer.BookCustomSerializer
import xyz.mon0mon.ch03.s05_serializer.Customer as SerializedCustomer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Properties

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.10.1 Custom Deserializer
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  값은 FQCN serializer/deserializer로 지정한다 (책 3장의 `CustomerSerializer`, 4.10.1의 `CustomerDeserializer`).
 * - 3.0  `poll(long)` 삭제. `poll(Duration)`만 남았다.
 * - 4.4  클래스 이름 설정은 그대로 동작한다. 인스턴스를 생성자에 넘기는 관용은 [CurrentCustomerRoundTrip].
 *
 * Still valid: `value.serializer`, `value.deserializer`, `poll(Duration)`
 */
object BookDeserializerSample {
    private const val TOPIC = "ch04Customer"
    private const val GROUP = "ch04-s101-book"

    @JvmStatic
    fun main(args: Array<String>) {
        // 4.4: 책은 토픽이 이미 있다고 가정한다. 지금은 이 절만의 토픽을 만든다.
        ensureTopic(TOPIC)

        // region 4.10.1 커스텀 디시리얼라이저
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        // 3장 BookCustomSerializer를 그대로 쓴다. 레코드 타입은 ch03의 Customer다.
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = BookCustomSerializer::class.java.name
        val producer = KafkaProducer<String, SerializedCustomer>(producerProps)

        val kafkaProps = Properties()
        kafkaProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ConsumerConfig.GROUP_ID_CONFIG] = GROUP
        kafkaProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        kafkaProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = BookDeserializer::class.java.name
        val consumer = KafkaConsumer<String, Customer>(kafkaProps)

        try {
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
        } finally {
            producer.close()
            consumer.close()
        }
        // endregion
    }

    private fun ensureTopic(topic: String) {
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        AdminClient.create(props).use { admin ->
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
