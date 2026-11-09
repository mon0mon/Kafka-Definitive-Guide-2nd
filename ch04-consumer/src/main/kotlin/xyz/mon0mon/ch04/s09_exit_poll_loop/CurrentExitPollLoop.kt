package xyz.mon0mon.ch04.s09_exit_poll_loop

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.concurrent.CountDownLatch

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.9 How to Exit
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  셧다운 훅, `Properties` + FQCN deserializer, `commitSync()`, `finally`에서 수동 `close()` ([BookExitPollLoop]).
 * - 2.0  `close(Duration)`. 인자 없는 `close()`는 기본 30초로 이 메서드를 호출한다.
 * - 3.0  `poll(long)` 삭제.
 * - 4.4  `Map` + deserializer 인스턴스, `use`. 루프를 깨는 방법은 여전히 다른 스레드의 `wakeup()`이다.
 *        스레드 인터럽트는 쓰지 않는다. `commitSync(Duration)`으로 커밋 대기를 드러낸다.
 *        `use`가 `close()`를 호출해 그룹을 떠난다.
 *
 * Replaces: [BookExitPollLoop]
 * Still valid: `wakeup()`, `WakeupException`, `poll(Duration)`, `commitSync(Duration)`, `close(Duration)`
 */
object CurrentExitPollLoop {
    private const val TOPIC = "ch04Exit"
    private const val GROUP = "CountryCounterExitCurrent"

    @JvmStatic
    fun main(args: Array<String>) {
        ensureTopic(TOPIC)

        val producerProps = KafkaSettings.stringProducerProperties(clientId = "ch04-s09-current")
        val consumerConfigs = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG to GROUP,
            ConsumerConfig.CLIENT_ID_CONFIG to "ch04-s09-current",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
        )

        KafkaProducer(producerProps, StringSerializer(), StringSerializer()).use { producer ->
            KafkaConsumer(consumerConfigs, StringDeserializer(), StringDeserializer()).use { consumer ->
                consumer.subscribe(listOf(TOPIC))
                awaitAssignment(consumer)
                if (consumer.assignment().isEmpty()) {
                    return
                }

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

                // region 4.9 폴링 루프를 벗어나는 방법
                val mainThread = Thread.currentThread()
                val hook = Thread {
                    logger.info { "Starting exit..." }
                    consumer.wakeup()
                    try {
                        mainThread.join()
                    } catch (e: InterruptedException) {
                        logger.error(e) { "shutdown hook interrupted" }
                    }
                }
                hook.name = "ch04-s09-current-shutdown"
                Runtime.getRuntime().addShutdownHook(hook)

                val lastProcessed = HashMap<TopicPartition, Long>()
                try {
                    val deadline = System.currentTimeMillis() + 15_000
                    var seen = 0
                    while (true) {
                        if (seen >= samples.size) {
                            wakeupFromOtherThread(consumer)
                            consumer.poll(Duration.ofMillis(10_000))
                        }
                        if (seen < samples.size && System.currentTimeMillis() > deadline) {
                            logger.error { "poll timed out seen=$seen expected=${samples.size}" }
                            consumer.wakeup()
                        }
                        val records = consumer.poll(Duration.ofMillis(1000))
                        logger.info { "${System.currentTimeMillis()} -- waiting for data..." }
                        for (record in records) {
                            logger.info {
                                "offset=${record.offset()} key=${record.key()} value=${record.value()}"
                            }
                            lastProcessed[TopicPartition(record.topic(), record.partition())] = record.offset()
                            seen++
                        }
                        for (tp in consumer.assignment()) {
                            logger.info { "Committing offset at position:${consumer.position(tp)}" }
                        }
                        consumer.commitSync(Duration.ofSeconds(10))
                    }
                } catch (e: WakeupException) {
                    logger.info { "wakeup, closing" }
                } finally {
                    try {
                        logGroupOffsets(lastProcessed)
                        logger.info { "Closed consumer and we are done" }
                    } finally {
                        removeHook(hook)
                    }
                }
                // endregion
            }
        }
    }

    private fun wakeupFromOtherThread(consumer: KafkaConsumer<String, String>) {
        val entered = CountDownLatch(1)
        Thread {
            entered.await()
            logger.info { "Starting exit..." }
            consumer.wakeup()
        }.apply {
            name = "ch04-s09-current-exit"
            isDaemon = true
            start()
        }
        entered.countDown()
    }

    private fun removeHook(hook: Thread) {
        try {
            Runtime.getRuntime().removeShutdownHook(hook)
        } catch (e: IllegalStateException) {
            logger.info { "shutdown already in progress" }
        }
    }

    private fun ensureTopic(topic: String) {
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
        )
        AdminClient.create(configs).use { admin ->
            val names = admin.listTopics().names().get()
            if (names.contains(topic)) {
                val partitions = admin.describeTopics(listOf(topic)).allTopicNames().get()
                    .getValue(topic).partitions().size
                logger.info { "topic exists name=$topic partitions=$partitions" }
                return
            }
            admin.createTopics(listOf(NewTopic(topic, 1, 1.toShort()))).all().get()
            logger.info { "topic created name=$topic partitions=1" }
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

    private fun logGroupOffsets(lastProcessed: Map<TopicPartition, Long>) {
        if (lastProcessed.isEmpty()) {
            logger.error { "no records processed group=$GROUP topic=$TOPIC" }
            return
        }
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
        )
        AdminClient.create(configs).use { admin ->
            val offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get()
            for ((tp, processed) in lastProcessed) {
                val stored = offsets[tp]
                val expected = processed + 1
                logger.info {
                    "group=$GROUP topic=${tp.topic()} partition=${tp.partition()} committed=${stored?.offset()} expected=$expected"
                }
                if (stored?.offset() != expected) {
                    logger.error {
                        "commit mismatch partition=${tp.partition()} expected=$expected committed=${stored?.offset()}"
                    }
                }
            }
        }
    }
}
