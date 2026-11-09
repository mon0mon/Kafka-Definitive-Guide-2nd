package xyz.mon0mon.ch04.s06_offset_commit.p04_sync_async_commit

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
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Properties

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.6.4 Combining Synchronous and Asynchronous Commits
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  폴링 배치마다 `commitAsync()`. 종료 `finally`에서 `commitSync()` 후 `close()` (책 2판, 이 파일).
 * - 4.4  두 메서드는 그대로다. `poll(long)`만 없어 `poll(Duration)`.
 *         종료 커밋이 저장됐는지는 `committed(Set, Duration)`으로 읽는다.
 *         4.4 관용은 [CurrentSyncAsyncCommit].
 *
 * Still valid: `commitAsync()`, `commitSync()`, `poll(Duration)`
 */
object BookSyncAsyncCommit {
    private const val TOPIC = "ch04OffsetCommit"
    private const val GROUP = "CountryCounterCommit"

    @JvmStatic
    fun main(args: Array<String>) {
        // 4.4: 책은 customerCountries가 이미 있다고 가정한다.
        // 지금은 이 절만의 토픽을 만들어 커밋 오프셋이 다른 예제와 섞이지 않게 한다.
        ensureTopic(TOPIC)

        val kafkaProps = Properties()
        kafkaProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ConsumerConfig.GROUP_ID_CONFIG] = GROUP
        kafkaProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        kafkaProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        // 4.4: 책은 자동 커밋을 끄고, 루프는 commitAsync, 종료는 commitSync만 쓴다.
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
            // 4.4: 책 스니펫은 구독이 끝난 컨슈머를 가정한다.
            // 지금은 파티션을 받은 뒤에 보내야 latest 그룹도 새 레코드를 읽는다.
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

            // region 4.6.4 동기와 비동기 커밋 함께 사용하기
            val timeout = Duration.ofMillis(100)
            val lastProcessed = HashMap<TopicPartition, Long>()
            try {
                // 4.4: 책은 while (true)다. 지금은 샘플을 다 읽으면 빠져나와 finally의 commitSync()를 실행한다.
                val deadline = System.currentTimeMillis() + 10_000
                var seen = 0
                while (seen < samples.size && System.currentTimeMillis() < deadline) {
                    val records = consumer.poll(timeout)
                    for (record in records) {
                        logger.info {
                            "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} customer=${record.key()} country=${record.value()}"
                        }
                        lastProcessed[TopicPartition(record.topic(), record.partition())] = record.offset()
                        seen++
                    }
                    consumer.commitAsync()
                }
                if (seen < samples.size) {
                    logger.error { "poll timed out seen=$seen expected=${samples.size}" }
                }
            } catch (e: Exception) {
                logger.error(e) { "Unexpected error" }
            } finally {
                try {
                    consumer.commitSync()
                    // 4.4: 책은 commitSync() 다음 close()로 끝난다.
                    // 지금은 그룹에 저장된 다음 오프셋을 확인한 뒤 닫는다.
                    logCommitted(consumer, lastProcessed)
                    logGroupOffsets(GROUP)
                } finally {
                    consumer.close()
                    consumerClosed = true
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

    private fun logCommitted(
        consumer: KafkaConsumer<String, String>,
        lastProcessed: Map<TopicPartition, Long>,
    ) {
        val stored = consumer.committed(consumer.assignment(), Duration.ofSeconds(10))
        for (tp in consumer.assignment()) {
            val committedOffset = stored[tp]?.offset()
            val processed = lastProcessed[tp]
            logger.info {
                "committed topic=${tp.topic()} partition=${tp.partition()} offset=$committedOffset lastProcessed=$processed"
            }
            if (processed != null && committedOffset != processed + 1) {
                logger.error {
                    "commit mismatch partition=${tp.partition()} expected=${processed + 1} committed=$committedOffset"
                }
            }
        }
    }

    private fun logGroupOffsets(group: String) {
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        AdminClient.create(props).use { admin ->
            val offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get()
            val topicOffsets = offsets.filterKeys { it.topic() == TOPIC }
            if (topicOffsets.isEmpty()) {
                logger.error { "group offset missing group=$group topic=$TOPIC" }
                return
            }
            for ((tp, meta) in topicOffsets) {
                logger.info {
                    "group=$group topic=${tp.topic()} partition=${tp.partition()} committed=${meta.offset()}"
                }
            }
        }
    }
}
