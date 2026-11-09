package xyz.mon0mon.ch04.s07_rebalance_listener

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.GroupProtocol
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.7 Rebalance Listeners
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  eager 리밸런스. `onPartitionsRevoked`에서 `currentOffsets` 전체를 `commitSync`.
 *        `onPartitionsLost` 없음. 메타데이터 `null` ([BookRebalanceListener]).
 * - 2.4  `onPartitionsLost` 기본 구현이 `onPartitionsRevoked`를 호출한다. 잃었을 때는 커밋하면 안 된다.
 * - 4.0  `group.protocol=consumer`(KIP-848). 브로커가 할당하고, revoke는 실제로 넘어가는 파티션만 받는다.
 *        `partition.assignment.strategy`는 이 프로토콜에서 둘 수 없다.
 * - 4.4  기본 `group.protocol`은 아직 `classic`이다. 이 파일은 `consumer`로 둔다.
 *        `Map` + deserializer 인스턴스, `use`. `OffsetAndMetadata`에 리더 에포크를 넣는다.
 *        revoke에서는 콜백으로 받은 파티션만 `commitSync(Map, Duration)`.
 *        `onPartitionsLost`는 커밋하지 않고 로컬 맵만 지운다.
 *
 * Replaces: [BookRebalanceListener]
 * Still valid: `subscribe(Collection, ConsumerRebalanceListener)`, `commitSync(Map, Duration)`, `commitAsync(Map, OffsetCommitCallback)`, `poll(Duration)`
 */
object CurrentRebalanceListener {
    private const val TOPIC = "ch04Rebalance"
    private const val GROUP = "CountryCounterRebalanceCurrent"
    private const val PARTITIONS = 2

    @JvmStatic
    fun main(args: Array<String>) {
        if (!ensureTopic(TOPIC)) {
            return
        }

        val producerProps = KafkaSettings.stringProducerProperties(clientId = "ch04-s07-current")
        val consumerConfigs = consumerConfigs(clientId = "ch04-s07-current")

        KafkaProducer(producerProps, StringSerializer(), StringSerializer()).use { producer ->
            KafkaConsumer(consumerConfigs, StringDeserializer(), StringDeserializer()).use { consumer ->
                val currentOffsets = HashMap<TopicPartition, OffsetAndMetadata>()
                val listener = HandleRebalance(consumer, currentOffsets)
                var joiner: Joiner? = null
                try {
                    // region 4.7 리밸런스 리스너
                    consumer.subscribe(listOf(TOPIC), listener)
                    awaitAssignment(consumer, PARTITIONS)
                    if (consumer.assignment().size < PARTITIONS) {
                        return
                    }

                    val samples = listOf(
                        0 to ("Precision Products" to "France"),
                        1 to ("Biomedical Materials" to "USA"),
                    )
                    for ((partition, record) in samples) {
                        val (customer, country) = record
                        try {
                            val metadata = producer.send(ProducerRecord(TOPIC, partition, customer, country)).get()
                            logger.info {
                                "sent topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()} customer=$customer country=$country"
                            }
                        } catch (e: Exception) {
                            logger.error(e) { "send failed" }
                        }
                    }

                    val deadline = System.currentTimeMillis() + 10_000
                    var seen = 0
                    while (seen < samples.size && System.currentTimeMillis() < deadline) {
                        val records = consumer.poll(Duration.ofMillis(100))
                        for (record in records) {
                            logger.info {
                                "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} customer=${record.key()} country=${record.value()}"
                            }
                            currentOffsets[TopicPartition(record.topic(), record.partition())] =
                                OffsetAndMetadata(record.offset() + 1, record.leaderEpoch(), "no metadata")
                            seen++
                        }
                        val snapshot = HashMap(currentOffsets)
                        consumer.commitAsync(snapshot) { committed, exception ->
                            if (exception == null) {
                                logger.info { "commit callback offsets=$committed" }
                            } else {
                                logger.error(exception) { "commit failed offsets=$committed" }
                            }
                        }
                    }
                    if (seen < samples.size) {
                        logger.error { "poll timed out seen=$seen expected=${samples.size}" }
                        return
                    }

                    joiner = startJoiner()
                    val rebalanceDeadline = System.currentTimeMillis() + 20_000
                    var committedOnRebalance: Map<TopicPartition, OffsetAndMetadata> = emptyMap()
                    while (committedOnRebalance.isEmpty() && System.currentTimeMillis() < rebalanceDeadline) {
                        consumer.poll(Duration.ofMillis(200))
                        committedOnRebalance = listener.lastRevokeCommit
                    }
                    if (committedOnRebalance.isEmpty()) {
                        logger.error { "rebalance timed out assignment=${consumer.assignment()}" }
                    } else {
                        logger.info { "assignment after rebalance=${consumer.assignment()}" }
                        logGroupOffsets(committedOnRebalance)
                    }
                    // endregion
                } catch (e: WakeupException) {
                    logger.info { "wakeup, closing" }
                } catch (e: Exception) {
                    logger.error(e) { "Unexpected error" }
                } finally {
                    joiner?.stop()
                    try {
                        val stillOwned = HashMap<TopicPartition, OffsetAndMetadata>()
                        for (tp in consumer.assignment()) {
                            val meta = currentOffsets[tp]
                            if (meta != null) {
                                stillOwned[tp] = meta
                            }
                        }
                        if (stillOwned.isNotEmpty()) {
                            consumer.commitSync(stillOwned, Duration.ofSeconds(10))
                        }
                    } catch (e: CommitFailedException) {
                        logger.error(e) { "final commit failed" }
                    }
                }
            }
        }
    }

    private fun consumerConfigs(clientId: String): Map<String, Any> = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
        ConsumerConfig.GROUP_ID_CONFIG to GROUP,
        ConsumerConfig.CLIENT_ID_CONFIG to clientId,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
        ConsumerConfig.GROUP_PROTOCOL_CONFIG to GroupProtocol.CONSUMER.name.lowercase(),
    )

    private class HandleRebalance(
        private val consumer: KafkaConsumer<String, String>,
        private val currentOffsets: MutableMap<TopicPartition, OffsetAndMetadata>,
    ) : ConsumerRebalanceListener {
        @Volatile
        var lastRevokeCommit: Map<TopicPartition, OffsetAndMetadata> = emptyMap()

        override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
            logger.info { "assigned partitions=$partitions" }
        }

        override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
            val toCommit = HashMap<TopicPartition, OffsetAndMetadata>()
            for (tp in partitions) {
                val meta = currentOffsets[tp]
                if (meta != null) {
                    toCommit[tp] = meta
                }
            }
            logger.info { "revoked partitions=$partitions committing=$toCommit" }
            if (toCommit.isEmpty()) {
                return
            }
            consumer.commitSync(toCommit, Duration.ofSeconds(10))
            lastRevokeCommit = HashMap(toCommit)
        }

        override fun onPartitionsLost(partitions: Collection<TopicPartition>) {
            logger.info { "lost partitions=$partitions; skip commit" }
            for (tp in partitions) {
                currentOffsets.remove(tp)
            }
        }
    }

    private class Joiner(
        private val consumer: KafkaConsumer<String, String>,
        private val thread: Thread,
        private val running: AtomicBoolean,
    ) {
        fun stop() {
            running.set(false)
            consumer.wakeup()
            thread.join(10_000)
        }
    }

    private fun startJoiner(): Joiner {
        val joiner = KafkaConsumer<String, String>(
            consumerConfigs(clientId = "ch04-s07-current-joiner"),
            StringDeserializer(),
            StringDeserializer(),
        )
        val running = AtomicBoolean(true)
        val thread = Thread {
            try {
                joiner.subscribe(listOf(TOPIC))
                while (running.get()) {
                    joiner.poll(Duration.ofMillis(200))
                }
            } catch (e: WakeupException) {
                if (running.get()) {
                    logger.error(e) { "joiner wakeup" }
                }
            } finally {
                joiner.close()
            }
        }
        thread.name = "ch04-s07-current-joiner"
        thread.isDaemon = true
        thread.start()
        return Joiner(joiner, thread, running)
    }

    private fun ensureTopic(topic: String): Boolean {
        val props = java.util.Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        AdminClient.create(props).use { admin ->
            val names = admin.listTopics().names().get()
            if (names.contains(topic)) {
                val partitions = admin.describeTopics(listOf(topic)).allTopicNames().get()
                    .getValue(topic).partitions().size
                logger.info { "topic exists name=$topic partitions=$partitions" }
                if (partitions < PARTITIONS) {
                    logger.error { "topic partitions=$partitions expected=$PARTITIONS name=$topic" }
                    return false
                }
                return true
            }
            admin.createTopics(listOf(NewTopic(topic, PARTITIONS, 1.toShort()))).all().get()
            logger.info { "topic created name=$topic partitions=$PARTITIONS" }
            return true
        }
    }

    private fun awaitAssignment(consumer: KafkaConsumer<String, String>, expected: Int) {
        val deadline = System.currentTimeMillis() + 15_000
        while (consumer.assignment().size < expected && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200))
        }
        if (consumer.assignment().size < expected) {
            logger.error { "assignment timed out group=$GROUP topic=$TOPIC assignment=${consumer.assignment()}" }
        }
    }

    private fun logGroupOffsets(expected: Map<TopicPartition, OffsetAndMetadata>) {
        val props = java.util.Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        AdminClient.create(props).use { admin ->
            val offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get()
            for ((tp, meta) in expected) {
                val stored = offsets[tp]
                val epoch: Optional<Int>? = stored?.leaderEpoch()
                logger.info {
                    "group=$GROUP topic=${tp.topic()} partition=${tp.partition()} committed=${stored?.offset()} expected=${meta.offset()} epoch=${epoch?.orElse(null)}"
                }
                if (stored?.offset() != meta.offset()) {
                    logger.error {
                        "commit mismatch partition=${tp.partition()} expected=${meta.offset()} committed=${stored?.offset()}"
                    }
                }
            }
        }
    }
}
