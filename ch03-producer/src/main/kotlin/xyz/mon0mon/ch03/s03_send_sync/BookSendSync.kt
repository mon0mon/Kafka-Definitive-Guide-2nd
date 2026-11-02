package xyz.mon0mon.ch03.s03_send_sync

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import xyz.mon0mon.common.KafkaSettings
import java.util.Properties

private val logger = KotlinLogging.logger { }

/**
 * Book: §3.3 Sending a Message to Kafka
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  fire-and-forget `send`, 동기 `send().get()`, 비동기 `Callback` (책, 이 파일).
 * - 4.4  전송 API는 같다. 생성/종료 관용만 [CurrentSendSync]처럼 바꾼다.
 *
 * Still valid: `send(record)`, `send(record).get()`, `Callback`
 */
object BookSendSync {
    @JvmStatic
    fun main(args: Array<String>) {
        val kafkaProps = Properties()
        kafkaProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        kafkaProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"

        val producer = KafkaProducer<String, String>(kafkaProps)
        try {
            // region 3.3 카프카로 메시지 전달하기
            var record = ProducerRecord("CustomerCountry", "Precision Products", "France")
            try {
                producer.send(record)
            } catch (e: Exception) {
                // 4.4: 책이 printStackTrace. 지금은 KotlinLogging.
                logger.error(e) { "send failed" }
            }
            // endregion

            // region 3.3.1 동기적으로 메시지 전송하기
            record = ProducerRecord("CustomerCountry", "Precision Products", "France2")
            try {
                producer.send(record).get()
            } catch (e: Exception) {
                // 4.4: 책이 printStackTrace. 지금은 KotlinLogging.
                logger.error(e) { "send failed" }
            }
            // endregion

            // region 3.3.2 비동기적으로 메시지 전송하기
            class DemoProducerCallback : Callback {
                override fun onCompletion(
                    metadata: RecordMetadata?, exception: java.lang.Exception?
                ) {
                    if (exception != null) {
                        // 4.4: 책이 printStackTrace. 지금은 KotlinLogging.
                        logger.error(exception) { "send failed" }
                    }
                }
            }

            record = ProducerRecord("CustomerCountry", "Biomedical Materials", "USA")
            producer.send(record, DemoProducerCallback())
            // Kotlin SAM 단축화
            producer.send(record) { _, exception ->
                exception?.let { logger.error(it) { "send failed" } }
            }
            // endregion
        } finally {
            // 4.4: 책 스니펫은 close를 생략한다. close()가 대기 중인 send를 flush한 뒤 프로듀서 스레드를 끝낸다.
            producer.close()
        }
    }
}
