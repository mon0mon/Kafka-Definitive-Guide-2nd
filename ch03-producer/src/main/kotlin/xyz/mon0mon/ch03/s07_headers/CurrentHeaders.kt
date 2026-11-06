package xyz.mon0mon.ch03.s07_headers

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings
import java.nio.charset.StandardCharsets
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * Book: §3.7 Headers
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  생성 후 `headers().add(String, byte[])` ([BookHeaders]).
 * - 4.4  `RecordHeader`와 `UTF_8`. 프로듀서와 컨슈머는 `use`.
 *         보낸 오프셋을 `assign` + `seek`으로 읽어 헤더 문자열을 로그에 남긴다.
 *
 * Replaces: [BookHeaders]
 * Still valid: `Headers.add`, `RecordHeader`, `Header.key`, `Header.value`
 */
object CurrentHeaders {
    private const val TOPIC = "CustomerCountry"

    @JvmStatic
    fun main(args: Array<String>) {
        val props = KafkaSettings.stringProducerProperties(clientId = "ch03-s07-current")

        KafkaProducer(props, StringSerializer(), StringSerializer()).use { producer ->
            // region 3.7 헤더
            val record = ProducerRecord(TOPIC, "Precision Products", "France")
            record.headers().add(
                RecordHeader("privacy-level", "YOLO".toByteArray(StandardCharsets.UTF_8)),
            )

            val metadata = producer.send(record).get()
            logger.info {
                "sent topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()}"
            }
            readHeaders(metadata.topic(), metadata.partition(), metadata.offset())
            // endregion
        }
    }

    private fun readHeaders(topic: String, partition: Int, offset: Long) {
        val configs = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG to "ch03-s07-current",
            ConsumerConfig.CLIENT_ID_CONFIG to "ch03-s07-current",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        )
        KafkaConsumer(configs, StringDeserializer(), StringDeserializer()).use { consumer ->
            val tp = TopicPartition(topic, partition)
            consumer.assign(listOf(tp))
            consumer.seek(tp, offset)
            val records = consumer.poll(Duration.ofSeconds(10))
            if (records.isEmpty) {
                logger.error { "no record at $topic-$partition@$offset" }
                return
            }
            records.forEach { record ->
                val headers = record.headers().joinToString(prefix = "[", postfix = "]") { header ->
                    "${header.key()}=${String(header.value(), StandardCharsets.UTF_8)}"
                }
                logger.info {
                    "read key=${record.key()} value=${record.value()} " +
                        "partition=${record.partition()} offset=${record.offset()} headers=$headers"
                }
            }
        }
    }
}
