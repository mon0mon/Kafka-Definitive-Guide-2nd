package xyz.mon0mon.ch04.s03_subscribe

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.regex.Pattern

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.3 Subscribing to Topics
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `Properties` + FQCN deserializer, 수동 `close` ([BookSubscribe]).
 * - 4.4  `Map` + deserializer 인스턴스, `use`. 구독 API는 `subscribe(Collection)` / `subscribe(Pattern)` 그대로다.
 *
 * Replaces: [BookSubscribe]
 * Still valid: `subscribe(Collection)`, `subscribe(Pattern)`, `subscription()`
 */
object CurrentSubscribe {
    private const val TOPIC = "customerCountries"

    @JvmStatic
    fun main(args: Array<String>) {
        // region 4.3 토픽 구독하기
        KafkaProducer(
            KafkaSettings.stringProducerProperties(clientId = "ch04-s03-current"),
            StringSerializer(),
            StringSerializer(),
        ).use { producer ->
            KafkaConsumer(consumerConfigs("CountryCounter", "ch04-s03-current"), StringDeserializer(), StringDeserializer())
                .use { consumer ->
                    consumer.subscribe(listOf(TOPIC))
                    awaitAssignment(consumer, "CountryCounter")
                    logger.info { "subscription=${consumer.subscription()} assignment=${consumer.assignment()}" }

                    val samples = listOf(
                        "Precision Products" to "France",
                        "Biomedical Materials" to "USA",
                        "Precision Products" to "France",
                    )
                    sendAll(producer, samples)
                    pollAndCount(consumer, samples.size)
                }

            KafkaConsumer(
                consumerConfigs("CountryCounterPattern", "ch04-s03-current-pattern"),
                StringDeserializer(),
                StringDeserializer(),
            ).use { patternConsumer ->
                patternConsumer.subscribe(Pattern.compile("customer.*"))
                awaitAssignment(patternConsumer, "CountryCounterPattern")
                logger.info {
                    "subscription=${patternConsumer.subscription()} assignment=${patternConsumer.assignment()}"
                }
                sendAll(producer, listOf("Acme Freight" to "Canada"))
                pollAndCount(patternConsumer, 1)
            }
        }
        // endregion
    }

    private fun consumerConfigs(group: String, clientId: String): Map<String, Any> = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
        ConsumerConfig.GROUP_ID_CONFIG to group,
        ConsumerConfig.CLIENT_ID_CONFIG to clientId,
    )

    private fun sendAll(producer: KafkaProducer<String, String>, samples: List<Pair<String, String>>) {
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
                    custCountryMap[country] = (custCountryMap[country] ?: 0) + 1
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
