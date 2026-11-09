package xyz.mon0mon.ch04.s07_rebalance_listener

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.7 Rebalance Listeners
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `ConsumerRebalanceListener`. `onPartitionsRevoked`에서 `commitSync(currentOffsets)`.
 *        루프는 `commitAsync(currentOffsets, null)`. 종료 `finally`에서 다시 `commitSync` (책 2판, 이 파일).
 *        `onPartitionsAssigned` 본문은 비어 있다. `onPartitionsLost`는 없다.
 * - 2.4  `onPartitionsLost` 기본 구현이 `onPartitionsRevoked`를 호출한다.
 * - 4.0  `group.protocol=consumer`(KIP-848)가 생겼지만 기본값은 아직 `classic`이다.
 * - 4.4  `subscribe(Collection, ConsumerRebalanceListener)`와 `commitSync(Map)`은 그대로다.
 *        기본 할당은 RangeAssignor(eager)라 revoke가 전체 할당을 넘긴다.
 *        소유하지 않은 파티션까지 커밋하면 `CommitFailedException`이 난다.
 *        4.4 관용은 [CurrentRebalanceListener].
 *
 * Still valid: `subscribe(Collection, ConsumerRebalanceListener)`, `commitSync(Map)`, `commitAsync(Map, OffsetCommitCallback)`, `poll(Duration)`
 */
object BookRebalanceListener {
    private const val TOPIC = "ch04Rebalance"
    private const val GROUP = "CountryCounterRebalance"
    private const val PARTITIONS = 2

    @JvmStatic
    fun main(args: Array<String>) {
        // 4.4: 책은 customerCountries가 이미 있다고 가정한다.
        // 지금은 파티션 2개인 이 절만의 토픽을 만들어 다른 그룹의 오프셋과 섞이지 않게 한다.
        if (!ensureTopic(TOPIC)) {
            return
        }

        val kafkaProps = Properties()
        kafkaProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ConsumerConfig.GROUP_ID_CONFIG] = GROUP
        kafkaProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        kafkaProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        // 4.4: 책은 자동 커밋을 끄고, 리밸런스 콜백과 루프에서만 커밋한다.
        kafkaProps[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"

        val consumer = KafkaConsumer<String, String>(kafkaProps)
        val currentOffsets = HashMap<TopicPartition, OffsetAndMetadata>()
        val listener = HandleRebalance(consumer, currentOffsets)

        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        val producer = KafkaProducer<String, String>(producerProps)

        var joiner: Joiner? = null
        try {
            // region 4.7 리밸런스 리스너
            consumer.subscribe(listOf(TOPIC), listener)
            // 4.4: 책 스니펫은 구독이 끝난 컨슈머를 가정한다.
            // 지금은 파티션을 받은 뒤에 보내야 새 레코드를 읽는다.
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
                    // 4.4: 책은 키만 넣고 파티션은 지정하지 않는다.
                    // 지금은 두 파티션에 한 건씩 넣어 revoke 커밋을 파티션마다 확인한다.
                    val metadata = producer.send(ProducerRecord(TOPIC, partition, customer, country)).get()
                    logger.info {
                        "sent topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()} customer=$customer country=$country"
                    }
                } catch (e: Exception) {
                    logger.error(e) { "send failed" }
                }
            }

            // 4.4: 책은 while (true). 지금은 보낸 건수만큼 읽고, 두 번째 멤버로 리밸런스를 일으킨 뒤 끝낸다.
            val deadline = System.currentTimeMillis() + 10_000
            var seen = 0
            while (seen < samples.size && System.currentTimeMillis() < deadline) {
                val records = consumer.poll(Duration.ofMillis(100))
                for (record in records) {
                    logger.info {
                        "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} customer=${record.key()} country=${record.value()}"
                    }
                    currentOffsets[TopicPartition(record.topic(), record.partition())] =
                        OffsetAndMetadata(record.offset() + 1, null)
                    seen++
                }
                consumer.commitAsync(currentOffsets, null)
            }
            if (seen < samples.size) {
                logger.error { "poll timed out seen=$seen expected=${samples.size}" }
                return
            }

            // 4.4: 책은 리스너만 보여 준다.
            // 지금은 같은 그룹에 두 번째 컨슈머를 넣어 revoke에서 commitSync한 오프셋을 확인한다.
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
                // 4.4: 책은 finally에서 currentOffsets 전체를 commitSync한다.
                // 리밸런스로 소유를 잃은 파티션이 맵에 있으면 CommitFailedException이 난다.
                // 지금은 아직 소유한 파티션만 고른다. close()가 나머지 revoke 커밋을 한 번 더 호출한다.
                val stillOwned = HashMap<TopicPartition, OffsetAndMetadata>()
                for (tp in consumer.assignment()) {
                    val meta = currentOffsets[tp]
                    if (meta != null) {
                        stillOwned[tp] = meta
                    }
                }
                if (stillOwned.isNotEmpty()) {
                    consumer.commitSync(stillOwned)
                }
            } catch (e: CommitFailedException) {
                logger.error(e) { "final commit failed" }
            } finally {
                producer.close()
                consumer.close()
            }
        }
    }

    private class HandleRebalance(
        private val consumer: KafkaConsumer<String, String>,
        private val currentOffsets: Map<TopicPartition, OffsetAndMetadata>,
    ) : ConsumerRebalanceListener {
        @Volatile
        var lastRevokeCommit: Map<TopicPartition, OffsetAndMetadata> = emptyMap()

        override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
            // 4.4: 책의 onPartitionsAssigned 본문은 비어 있다. 할당 확인용 로그만 남긴다.
            logger.info { "assigned partitions=$partitions" }
        }

        override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
            logger.info { "Lost partitions in rebalance. Committing current offsets:$currentOffsets" }
            // 4.4: 책은 currentOffsets 전체를 commitSync한다.
            // 콜백에 없는 파티션은 이미 소유가 아니므로, 지금은 넘어온 파티션만 고른다.
            val toCommit = HashMap<TopicPartition, OffsetAndMetadata>()
            for (tp in partitions) {
                val meta = currentOffsets[tp]
                if (meta != null) {
                    toCommit[tp] = meta
                }
            }
            if (toCommit.isEmpty()) {
                return
            }
            consumer.commitSync(toCommit)
            lastRevokeCommit = HashMap(toCommit)
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
        val props = Properties()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        props[ConsumerConfig.GROUP_ID_CONFIG] = GROUP
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        val joiner = KafkaConsumer<String, String>(props)
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
        thread.name = "ch04-s07-book-joiner"
        thread.isDaemon = true
        thread.start()
        return Joiner(joiner, thread, running)
    }

    private fun ensureTopic(topic: String): Boolean {
        val props = Properties()
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
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        AdminClient.create(props).use { admin ->
            val offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get()
            for ((tp, meta) in expected) {
                val stored = offsets[tp]
                logger.info {
                    "group=$GROUP topic=${tp.topic()} partition=${tp.partition()} committed=${stored?.offset()} expected=${meta.offset()}"
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
