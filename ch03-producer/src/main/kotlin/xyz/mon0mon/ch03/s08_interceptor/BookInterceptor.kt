package xyz.mon0mon.ch03.s08_interceptor

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import xyz.mon0mon.common.KafkaSettings
import java.lang.Exception
import java.util.Properties
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger { }

/**
 * Book: §3.8 Interceptors
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `onAcknowledgement(metadata, exception)` (책, 이 파일).
 * - 4.4  프로듀서는 `onAcknowledgement(metadata, exception, headers)`를 호출한다.
 *         3인자 기본 구현이 2인자를 부르므로 이 오버라이드는 그대로 실행된다.
 *
 * Still valid: `ProducerInterceptor`, `interceptor.classes`, `onSend`
 */
class BookInterceptor : ProducerInterceptor<String, String> {

    val executorService = Executors.newSingleThreadScheduledExecutor()

    companion object {
        val numSent = AtomicLong(0)
        val numAcked = AtomicLong(0)

        fun run() {
            logger.info { "numSent.getAndSet(0) : ${numSent.getAndSet(0)}" }
            logger.info { "numAcked.getAndSet(0) : ${numAcked.getAndSet(0)}" }
        }

        @JvmStatic
        fun main(args: Array<String>) {
            val kafkaProps = Properties()
            kafkaProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
            kafkaProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
                "org.apache.kafka.common.serialization.StringSerializer"
            kafkaProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
                "org.apache.kafka.common.serialization.StringSerializer"
            kafkaProps[ProducerConfig.INTERCEPTOR_CLASSES_CONFIG] =
                "xyz.mon0mon.ch03.s08_interceptor.BookInterceptor"
            // 4.4: 책은 창 크기만 읽고 send는 없다. 1초 창에서 집계 로그를 한 번 본다.
            kafkaProps["counting.interceptor.window.size.ms"] = "1000"

            val producer = KafkaProducer<String, String>(kafkaProps)
            try {
                // region 3.8 인터셉터
                repeat(2) { index ->
                    producer.send(ProducerRecord("CustomerCountry", "Precision Products", "France-$index")).get()
                }
                Thread.sleep(1_500)
                // endregion
            } finally {
                // 4.4: close()가 인터셉터 close로 이어져 집계 스레드를 끈다.
                producer.close()
            }
        }
    }

    // region 3.8 인터셉터
    override fun configure(configs: Map<String?, *>?) {
        val windowSize = configs?.get("counting.interceptor.window.size.ms").toString().toLong()
        executorService.scheduleAtFixedRate(BookInterceptor::run, windowSize, windowSize, TimeUnit.MILLISECONDS)
    }

    override fun onSend(
        record: ProducerRecord<String?, String?>?
    ): ProducerRecord<String?, String?>? {
        numSent.incrementAndGet()
        return record
    }

    override fun onAcknowledgement(
        metadata: RecordMetadata?, exception: Exception?
    ) {
        numAcked.incrementAndGet()
    }

    override fun close() {
        executorService.shutdownNow()
    }
    // endregion
}
