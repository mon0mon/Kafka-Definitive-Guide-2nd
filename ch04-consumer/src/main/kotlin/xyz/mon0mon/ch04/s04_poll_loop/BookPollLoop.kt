package xyz.mon0mon.ch04.s04_poll_loop

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Properties

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.4 The Poll Loop
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 1.x  `poll(long)`.
 * - 2.x  `poll(Duration)`, 국가별 건수 맵 (책 2판, 이 파일).
 * - 4.4  `poll(long)`은 3.0에서 삭제됐다. `poll(Duration)`만 남았다 → [CurrentPollLoop].
 *
 * Removed: `KafkaConsumer.poll(long)`
 * Still valid: `poll(Duration)`, `ConsumerRecords`, `close()`
 */
object BookPollLoop {
    private const val TOPIC = "customerCountries"
    private const val GROUP = "CountryCounter"

    @JvmStatic
    fun main(args: Array<String>) {
        val kafkaProps = Properties()
        kafkaProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ConsumerConfig.GROUP_ID_CONFIG] = GROUP
        kafkaProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        kafkaProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        val consumer = KafkaConsumer<String, String>(kafkaProps)

        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        val producer = KafkaProducer<String, String>(producerProps)

        try {
            consumer.subscribe(listOf(TOPIC))

            // region 4.4 폴링 루프
            // 4.4: 책은 while (true)로 이미 있는 레코드를 읽는다.
            // 지금은 그룹 가입으로 파티션을 받은 뒤 세 건을 보내고, 그 레코드를 로그한 다음 끝낸다.
            awaitAssignment(consumer)

            val samples = listOf(
                "Precision Products" to "France",
                "Biomedical Materials" to "USA",
                "Precision Products" to "France",
            )
            for ((customer, country) in samples) {
                try {
                    producer.send(ProducerRecord(TOPIC, customer, country)).get()
                } catch (e: Exception) {
                    logger.error(e) { "send failed" }
                }
            }

            val custCountryMap = HashMap<String, Int>()
            val deadline = System.currentTimeMillis() + 10_000
            var seen = 0
            while (seen < samples.size && System.currentTimeMillis() < deadline) {
                val records = consumer.poll(Duration.ofMillis(100))
                for (record in records) {
                    logger.info {
                        "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} customer=${record.key()} country=${record.value()}"
                    }
                    val country = record.value()
                    if (country != null) {
                        var updatedCount = 1
                        if (custCountryMap.containsKey(country)) {
                            updatedCount = custCountryMap.getValue(country) + 1
                        }
                        custCountryMap[country] = updatedCount
                        // 4.4: 책이 JSONObject(custCountryMap).toString(4)를 println. 지금은 KotlinLogging.
                        logger.info { "counts=$custCountryMap" }
                    }
                    seen++
                }
            }
            if (seen < samples.size) {
                logger.error { "poll timed out seen=$seen expected=${samples.size}" }
            }
            // endregion
        } finally {
            producer.close()
            consumer.close()
        }
    }

    private fun awaitAssignment(consumer: KafkaConsumer<String, String>) {
        val deadline = System.currentTimeMillis() + 15_000
        while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200))
        }
        if (consumer.assignment().isEmpty()) {
            logger.error { "assignment timed out group=$GROUP topic=$TOPIC" }
        }
    }
}
