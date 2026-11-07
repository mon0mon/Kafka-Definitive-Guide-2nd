package xyz.mon0mon.ch04.s02_create_consumer

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.2 Creating a Kafka Consumer
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 1.x  문자열 키, `poll(long)`.
 * - 2.x  `Properties` + FQCN deserializer, 수동 `close` ([BookCreateConsumer]).
 * - 4.4  `Map` + deserializer 인스턴스를 생성자에 넘긴다. `use`로 close를 보장한다.
 *         프로듀서는 [KafkaSettings.stringProducerProperties]. `poll(Duration)`만 쓴다.
 *
 * Replaces: [BookCreateConsumer]
 * Removed: `KafkaConsumer.poll(long)`
 * Still valid: `subscribe`, `poll(Duration)`, `group.id`
 */
object CurrentCreateConsumer {
    private const val TOPIC = "customerCountries"
    private const val GROUP = "CountryCounter"

    @JvmStatic
    fun main(args: Array<String>) {
        val producerProps = KafkaSettings.stringProducerProperties(clientId = "ch04-s02-current")
        val consumerConfigs = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG to GROUP,
            ConsumerConfig.CLIENT_ID_CONFIG to "ch04-s02-current",
        )

        KafkaProducer(producerProps, StringSerializer(), StringSerializer())
            .use { producer ->
                KafkaConsumer(consumerConfigs, StringDeserializer(), StringDeserializer()).use { consumer ->
                    // region 4.2 카프카 컨슈머 생성하기
                    logger.info {
                        "created consumer group=$GROUP clientId=ch04-s02-current bootstrap=${KafkaSettings.bootstrapServers()}"
                    }
                    // endregion

                    // region 4.3 토픽 구독하기
                    consumer.subscribe(listOf(TOPIC))
                    // endregion

                    // region 4.4 폴링 루프
                    awaitAssignment(consumer)

                    val samples = listOf(
                        "Precision Products" to "France",
                        "Biomedical Materials" to "USA",
                        "Precision Products" to "France",
                    )
                    for ((customer, country) in samples) {
                        try {
                            val metadata = producer.send(ProducerRecord(TOPIC, customer, country)).get()
                            logger.info {
                                "sent topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()} customer=$customer country=$country"
                            }
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
                                custCountryMap[country] = (custCountryMap[country] ?: 0) + 1
                                logger.info { "counts=$custCountryMap" }
                            }
                            seen++
                        }
                    }
                    if (seen < samples.size) {
                        logger.error { "poll timed out seen=$seen expected=${samples.size}" }
                    }
                    // endregion
                }
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
