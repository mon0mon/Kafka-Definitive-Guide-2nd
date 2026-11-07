package xyz.mon0mon.ch04.s02_create_consumer

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
 * Book: §4.2 Creating a Kafka Consumer
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 1.x  문자열 키 `"bootstrap.servers"`, `"group.id"`, `"key.deserializer"`. `poll(long)`.
 * - 2.x  `ConsumerConfig` 상수, FQCN deserializer, `poll(Duration)` (책 2판, 이 파일).
 *         4.3에서 `customerCountries`를 구독하고, 4.4에서 국가별 건수를 센다.
 * - 4.4  상수는 그대로 유효하다. deserializer는 클래스 객체를 생성자에 넘기는 편이 안전하다
 *         → [CurrentCreateConsumer].
 *
 * Removed: `KafkaConsumer.poll(long)`. 3.0에서 삭제. `poll(Duration)`만 남았다.
 * Still valid: `Properties` + `ConsumerConfig` + `subscribe` + `poll(Duration)` + `close()`
 */
object BookCreateConsumer {
    private const val TOPIC = "customerCountries"
    private const val GROUP = "CountryCounter"

    @JvmStatic
    fun main(args: Array<String>) {
        // region 4.2 카프카 컨슈머 생성하기
        val kafkaProps = Properties()

        // 1.x
        // kafkaProps["bootstrap.servers"] = "broker1:9092,broker2:9092"
        // kafkaProps["group.id"] = "CountryCounter"
        // kafkaProps["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        // kafkaProps["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"

        kafkaProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ConsumerConfig.GROUP_ID_CONFIG] = GROUP
        kafkaProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        kafkaProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"

        val consumer = KafkaConsumer<String, String>(kafkaProps)
        // endregion

        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        val producer = KafkaProducer<String, String>(producerProps)

        try {
            // region 4.3 토픽 구독하기
            consumer.subscribe(listOf(TOPIC))
            // endregion

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
