package xyz.mon0mon.ch04.s06_offset_commit.p04_sync_async_commit

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.6.4 Combining Synchronous and Asynchronous Commits
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `Properties` + FQCN deserializer, 루프 `commitAsync()`, 종료 `finally`에서 `commitSync()` 후 수동 `close` ([BookSyncAsyncCommit]).
 * - 4.4  `Map` + deserializer 인스턴스, `use`. `commitAsync()`와 종료 `commitSync()`는 그대로다.
 *         `use`가 `close()`를 호출하므로, `commitSync()`는 그 블록이 끝나기 전에 둔다.
 *         저장 오프셋은 `committed(Set, Duration)`으로 확인한다.
 *
 * Replaces: [BookSyncAsyncCommit]
 * Still valid: `commitAsync()`, `commitSync()`, `poll(Duration)`, `committed(Set, Duration)`
 */
object CurrentSyncAsyncCommit {
    private const val TOPIC = "ch04OffsetCommit"
    private const val GROUP = "CountryCounterCommit"

    @JvmStatic
    fun main(args: Array<String>) {
        ensureTopic(TOPIC)

        val producerProps = KafkaSettings.stringProducerProperties(clientId = "ch04-s064-current")
        val consumerConfigs = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG to GROUP,
            ConsumerConfig.CLIENT_ID_CONFIG to "ch04-s064-current",
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

                // region 4.6.4 동기와 비동기 커밋 함께 사용하기
                val lastProcessed = HashMap<TopicPartition, Long>()
                try {
                    val deadline = System.currentTimeMillis() + 10_000
                    var seen = 0
                    while (seen < samples.size && System.currentTimeMillis() < deadline) {
                        val records = consumer.poll(Duration.ofMillis(100))
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
                    consumer.commitSync()
                    logCommitted(consumer, lastProcessed)
                    logGroupOffsets(GROUP)
                }
                // endregion
            }
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
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
        )
        AdminClient.create(configs).use { admin ->
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
