package xyz.mon0mon.ch03.s08_interceptor

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerInterceptor
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}

/**
 * Book: §3.8 Interceptors
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  2인자 `onAcknowledgement(metadata, exception)` ([BookInterceptor]).
 * - 4.4  `ProducerInterceptors`는 3인자 `onAcknowledgement(metadata, exception, headers)`를 호출한다.
 *         인터셉터는 `::class.java.name`으로 넘긴다. 시리얼라이저는 인스턴스다.
 *         집계 스레드는 daemon이고 `close`에서 `shutdownNow`한다.
 *
 * Replaces: [BookInterceptor]
 * Still valid: `ProducerInterceptor`, `ProducerConfig.INTERCEPTOR_CLASSES_CONFIG`, `onSend`
 */
class CurrentInterceptor : ProducerInterceptor<String, String> {
    private val executor = Executors.newSingleThreadScheduledExecutor { runnable ->
        Thread(runnable, "ch03-s08-current-interceptor").apply { isDaemon = true }
    }

    // region 3.8 인터셉터
    override fun configure(configs: Map<String, *>) {
        val windowSize = windowMs(configs)
        executor.scheduleAtFixedRate(CurrentInterceptor::report, windowSize, windowSize, TimeUnit.MILLISECONDS)
    }

    override fun onSend(record: ProducerRecord<String, String>): ProducerRecord<String, String> {
        numSent.incrementAndGet()
        return record
    }

    override fun onAcknowledgement(metadata: RecordMetadata?, exception: Exception?, headers: Headers?) {
        numAcked.incrementAndGet()
    }

    override fun close() {
        executor.shutdownNow()
    }
    // endregion

    companion object {
        private const val WINDOW_MS = "counting.interceptor.window.size.ms"

        private val numSent = AtomicLong(0)
        private val numAcked = AtomicLong(0)
        private val reported = CountDownLatch(1)

        private fun report() {
            logger.info { "numSent=${numSent.getAndSet(0)} numAcked=${numAcked.getAndSet(0)}" }
            reported.countDown()
        }

        private fun windowMs(configs: Map<String, *>): Long {
            val raw = configs[WINDOW_MS] ?: error("$WINDOW_MS is required")
            return when (raw) {
                is Number -> raw.toLong()
                is String -> raw.toLong()
                else -> error("$WINDOW_MS must be a number, was ${raw.javaClass.name}")
            }
        }

        @JvmStatic
        fun main(args: Array<String>) {
            val configs = mapOf<String, Any>(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
                ProducerConfig.CLIENT_ID_CONFIG to "ch03-s08-current",
                ProducerConfig.ACKS_CONFIG to "all",
                ProducerConfig.INTERCEPTOR_CLASSES_CONFIG to CurrentInterceptor::class.java.name,
                WINDOW_MS to 1_000L,
            )

            KafkaProducer(configs, StringSerializer(), StringSerializer()).use { producer ->
                // region 3.8 인터셉터
                repeat(2) { index ->
                    val metadata = producer.send(
                        ProducerRecord("CustomerCountry", "Precision Products", "France-$index"),
                    ).get()
                    logger.info {
                        "sent topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()}"
                    }
                }
                check(reported.await(5, TimeUnit.SECONDS)) { "interceptor report did not run" }
                // endregion
            }
        }
    }
}
