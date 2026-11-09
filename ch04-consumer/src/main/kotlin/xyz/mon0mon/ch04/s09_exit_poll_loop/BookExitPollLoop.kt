package xyz.mon0mon.ch04.s09_exit_poll_loop

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Properties
import java.util.concurrent.CountDownLatch

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.9 How to Exit
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  셧다운 훅에서 `wakeup()`. `poll()`이 `WakeupException`을 던지면 무시하고 `finally`에서 `close()` (책 2판, 이 파일).
 *        `wakeup()`만 다른 스레드에서 호출할 수 있다. `close()`는 남은 오프셋을 커밋하고 그룹을 떠난다.
 * - 3.0  `poll(long)` 삭제. `poll(Duration)`만 남았다.
 * - 4.4  `wakeup()`, `WakeupException`, `close()`, `commitSync()`는 그대로다.
 *        스레드 인터럽트로 루프를 깨는 방법은 쓰지 않는다. 4.4 관용은 [CurrentExitPollLoop].
 *
 * Still valid: `wakeup()`, `WakeupException`, `poll(Duration)`, `commitSync()`, `close()`
 */
object BookExitPollLoop {
    private const val TOPIC = "ch04Exit"
    private const val GROUP = "CountryCounterExit"

    @JvmStatic
    fun main(args: Array<String>) {
        // 4.4: 책은 customerCountries가 이미 있다고 가정한다.
        // 지금은 이 절만의 토픽을 만들어 다른 예제의 그룹 오프셋과 섞이지 않게 한다.
        ensureTopic(TOPIC)

        val kafkaProps = Properties()
        kafkaProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ConsumerConfig.GROUP_ID_CONFIG] = GROUP
        kafkaProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        kafkaProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        // 4.4: 책은 루프에서 commitSync()를 호출한다.
        // 자동 커밋을 끄면 저장 오프셋이 그 호출에서만 올라간다.
        kafkaProps[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        val consumer = KafkaConsumer<String, String>(kafkaProps)

        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        val producer = KafkaProducer<String, String>(producerProps)
        var consumerClosed = false

        try {
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
            hook.name = "ch04-s09-book-shutdown"
            Runtime.getRuntime().addShutdownHook(hook)

            val lastProcessed = HashMap<TopicPartition, Long>()
            try {
                // 4.4: 책은 while (true)이고, Ctrl-C가 셧다운 훅을 실행한다.
                // 지금은 샘플을 다 읽은 뒤 훅과 같은 순서로 다른 스레드가 wakeup()을 호출한다.
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
                    consumer.commitSync()
                }
            } catch (e: WakeupException) {
                // 4.4: 책은 종료용 WakeupException을 무시한다.
                logger.info { "wakeup, closing" }
            } finally {
                try {
                    consumer.close()
                    consumerClosed = true
                    logger.info { "Closed consumer and we are done" }
                    logGroupOffsets(lastProcessed)
                } finally {
                    removeHook(hook)
                }
            }
            // endregion
        } finally {
            producer.close()
            if (!consumerClosed) {
                consumer.close()
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
            name = "ch04-s09-book-exit"
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
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        AdminClient.create(props).use { admin ->
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
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        AdminClient.create(props).use { admin ->
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
