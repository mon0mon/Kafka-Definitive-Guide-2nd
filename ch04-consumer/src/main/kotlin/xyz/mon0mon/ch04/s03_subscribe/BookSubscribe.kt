package xyz.mon0mon.ch04.s03_subscribe

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Properties
import java.util.regex.Pattern

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.3 Subscribing to Topics
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `subscribe(Collection)`, `subscribe(Pattern)` (책 2판, 이 파일).
 *         구독 목록은 `poll`로 그룹에 조인한 뒤에 적용된다.
 * - 4.4  시그니처는 같다. 정규식 구독도 그대로 유효하다 → [CurrentSubscribe].
 *
 * Still valid: `subscribe(Collection)`, `subscribe(Pattern)`, `subscription()`
 */
object BookSubscribe {
    private const val TOPIC = "customerCountries"

    @JvmStatic
    fun main(args: Array<String>) {
        val producer = KafkaProducer<String, String>(producerProps())
        try {
            // region 4.3 토픽 구독하기
            val consumer = KafkaConsumer<String, String>(consumerProps("CountryCounter"))
            try {
                consumer.subscribe(listOf(TOPIC))
                awaitAssignment(consumer, "CountryCounter")
                logger.info { "subscription=${consumer.subscription()} assignment=${consumer.assignment()}" }

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
                pollAndCount(consumer, samples.size)
            } finally {
                consumer.close()
            }

            // 4.4: 책은 Pattern.compile("test.*")를 별도 예제로 보여 준다.
            // subscribe는 이전 구독을 바꾸므로, 지금은 customer.* 로 다른 컨슈머에서 실행한다.
            val patternConsumer = KafkaConsumer<String, String>(consumerProps("CountryCounterPattern"))
            try {
                patternConsumer.subscribe(Pattern.compile("customer.*"))
                awaitAssignment(patternConsumer, "CountryCounterPattern")
                logger.info {
                    "subscription=${patternConsumer.subscription()} assignment=${patternConsumer.assignment()}"
                }
                try {
                    producer.send(ProducerRecord(TOPIC, "Acme Freight", "Canada")).get()
                } catch (e: Exception) {
                    logger.error(e) { "send failed" }
                }
                pollAndCount(patternConsumer, 1)
            } finally {
                patternConsumer.close()
            }
            // endregion
        } finally {
            producer.close()
        }
    }

    private fun consumerProps(group: String): Properties {
        val kafkaProps = Properties()
        kafkaProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ConsumerConfig.GROUP_ID_CONFIG] = group
        kafkaProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        kafkaProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        return kafkaProps
    }

    private fun producerProps(): Properties {
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        return producerProps
    }

    private fun awaitAssignment(consumer: KafkaConsumer<String, String>, group: String) {
        val deadline = System.currentTimeMillis() + 15_000
        while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200))
        }
        if (consumer.assignment().isEmpty()) {
            logger.error { "assignment timed out group=$group topic=$TOPIC" }
        }
    }

    private fun pollAndCount(consumer: KafkaConsumer<String, String>, expected: Int) {
        val custCountryMap = HashMap<String, Int>()
        val deadline = System.currentTimeMillis() + 10_000
        var seen = 0
        while (seen < expected && System.currentTimeMillis() < deadline) {
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
                    logger.info { "counts=$custCountryMap" }
                }
                seen++
            }
        }
        if (seen < expected) {
            logger.error { "poll timed out seen=$seen expected=$expected" }
        }
    }
}
