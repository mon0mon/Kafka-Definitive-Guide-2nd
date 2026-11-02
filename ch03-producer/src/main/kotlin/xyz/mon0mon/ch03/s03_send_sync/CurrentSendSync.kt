package xyz.mon0mon.ch03.s03_send_sync

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import xyz.mon0mon.common.KafkaSettings

private val logger = KotlinLogging.logger {}

/**
 * Book: §3.3 Sending a Message to Kafka
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `Properties` + FQCN serializer, `Callback` 클래스, 수동 `close` ([BookSendSync])
 * - 4.4  `KafkaSettings.stringProducerProperties` + `use`.
 *         비동기 완료는 `Callback` 대신 SAM 람다. `send` / `send().get()`은 그대로 쓴다.
 *
 * Replaces: [BookSendSync]
 * Still valid: `send(record)`, `send(record).get()`, `Callback`
 */
object CurrentSendSync {
    @JvmStatic
    fun main(args: Array<String>) {
        val props = KafkaSettings.stringProducerProperties(clientId = "ch03-s03-current")

        KafkaProducer<String, String>(props).use { producer ->
            // region 3.3 카프카로 메시지 전달하기
            var record = ProducerRecord("CustomerCountry", "Precision Products", "France")
            try {
                producer.send(record)
            } catch (e: Exception) {
                logger.error(e) { "send failed" }
            }
            // endregion

            // region 3.3.1 동기적으로 메시지 전송하기
            record = ProducerRecord("CustomerCountry", "Precision Products", "France2")
            try {
                val metadata = producer.send(record).get()
                logger.info {
                    "sent topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()}"
                }
            } catch (e: Exception) {
                logger.error(e) { "send failed" }
            }
            // endregion

            // region 3.3.2 비동기적으로 메시지 전송하기
            record = ProducerRecord("CustomerCountry", "Biomedical Materials", "USA")
            producer.send(record) { metadata, exception ->
                if (exception != null) {
                    logger.error(exception) { "send failed" }
                } else if (metadata != null) {
                    logger.info {
                        "sent topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()}"
                    }
                }
            }
            // endregion
        }
    }
}
