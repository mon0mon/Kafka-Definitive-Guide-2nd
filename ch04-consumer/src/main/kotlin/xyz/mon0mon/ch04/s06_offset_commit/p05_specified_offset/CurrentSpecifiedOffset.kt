package xyz.mon0mon.ch04.s06_offset_commit.p05_specified_offset

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Optional

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.6.5 Committing a Specified Offset
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `OffsetAndMetadata(offset + 1, "no metadata")`, 콜백 `null`, 1000건마다 `commitAsync` ([BookSpecifiedOffset]).
 * - 2.1  `OffsetAndMetadata`에 리더 에포크가 생겼다. 레코드의 `leaderEpoch()`를 같이 넣는다.
 * - 4.4  `Map` + deserializer 인스턴스, `use`. 콜백은 SAM 람다.
 *        저장 오프셋은 `committed(Set, Duration)`으로 확인한다.
 *
 * Replaces: [BookSpecifiedOffset]
 * Still valid: `commitAsync(Map, OffsetCommitCallback)`, `poll(Duration)`, `committed(Set, Duration)`
 */
object CurrentSpecifiedOffset {
    private const val TOPIC = "ch04OffsetCommit"
    private const val GROUP = "CountryCounterCommit"

    @JvmStatic
    fun main(args: Array<String>) {
        ensureTopic(TOPIC)

        val producerProps = KafkaSettings.stringProducerProperties(clientId = "ch04-s065-current")
        val consumerConfigs = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG to GROUP,
            ConsumerConfig.CLIENT_ID_CONFIG to "ch04-s065-current",
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

                // region 4.6.5 특정 오프셋 커밋
                val currentOffsets = HashMap<TopicPartition, OffsetAndMetadata>()
                val leaderEpochs = HashMap<TopicPartition, Optional<Int>>()
                var count = 0
                val lastProcessed = HashMap<TopicPartition, Long>()
                val deadline = System.currentTimeMillis() + 10_000
                var seen = 0
                while (seen < samples.size && System.currentTimeMillis() < deadline) {
                    val records = consumer.poll(Duration.ofMillis(100))
                    for (record in records) {
                        logger.info {
                            "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} customer=${record.key()} country=${record.value()}"
                        }
                        val tp = TopicPartition(record.topic(), record.partition())
                        lastProcessed[tp] = record.offset()
                        leaderEpochs[tp] = record.leaderEpoch()
                        currentOffsets[tp] = OffsetAndMetadata(
                            record.offset() + 1,
                            record.leaderEpoch(),
                            "no metadata",
                        )
                        count++
                        seen++
                    }
                    if (seen >= samples.size && count > 0) {
                        val snapshot = HashMap(currentOffsets)
                        consumer.commitAsync(snapshot) { committed, exception ->
                            if (exception == null) {
                                logger.info { "commit callback offsets=$committed" }
                            } else {
                                logger.error(exception) { "commit failed offsets=$committed" }
                            }
                        }
                    }
                }
                if (seen < samples.size) {
                    logger.error { "poll timed out seen=$seen expected=${samples.size}" }
                }
                awaitCommitted(consumer, lastProcessed, leaderEpochs)
                logCommitted(consumer, lastProcessed, leaderEpochs)
                logGroupOffsets(GROUP)
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

    private fun awaitCommitted(
        consumer: KafkaConsumer<String, String>,
        lastProcessed: Map<TopicPartition, Long>,
        leaderEpochs: Map<TopicPartition, Optional<Int>>,
    ) {
        if (lastProcessed.isEmpty()) {
            return
        }
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200))
            val stored = consumer.committed(consumer.assignment(), Duration.ofSeconds(10))
            val matched = lastProcessed.all { (tp, processed) ->
                val meta = stored[tp]
                meta?.offset() == processed + 1 &&
                    meta.metadata() == "no metadata" &&
                    meta.leaderEpoch() == leaderEpochs[tp]
            }
            if (matched) {
                return
            }
        }
        logger.error { "async commit timed out" }
    }

    private fun logCommitted(
        consumer: KafkaConsumer<String, String>,
        lastProcessed: Map<TopicPartition, Long>,
        leaderEpochs: Map<TopicPartition, Optional<Int>>,
    ) {
        val stored = consumer.committed(consumer.assignment(), Duration.ofSeconds(10))
        for (tp in consumer.assignment()) {
            val meta = stored[tp]
            val processed = lastProcessed[tp]
            logger.info {
                "committed topic=${tp.topic()} partition=${tp.partition()} offset=${meta?.offset()} metadata=${meta?.metadata()} leaderEpoch=${meta?.leaderEpoch()} lastProcessed=$processed"
            }
            if (processed != null && meta?.offset() != processed + 1) {
                logger.error {
                    "commit mismatch partition=${tp.partition()} expected=${processed + 1} committed=${meta?.offset()}"
                }
            }
            if (processed != null && meta?.metadata() != "no metadata") {
                logger.error {
                    "metadata mismatch partition=${tp.partition()} expected=no metadata committed=${meta?.metadata()}"
                }
            }
            val expectedEpoch = leaderEpochs[tp]
            if (processed != null && expectedEpoch != null && meta?.leaderEpoch() != expectedEpoch) {
                logger.error {
                    "leader epoch mismatch partition=${tp.partition()} expected=$expectedEpoch committed=${meta?.leaderEpoch()}"
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
                    "group=$group topic=${tp.topic()} partition=${tp.partition()} committed=${meta.offset()} metadata=${meta.metadata()} leaderEpoch=${meta.leaderEpoch()}"
                }
            }
        }
    }
}
