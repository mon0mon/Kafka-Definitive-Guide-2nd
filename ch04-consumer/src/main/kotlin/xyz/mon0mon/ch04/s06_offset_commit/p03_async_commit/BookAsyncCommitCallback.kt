package xyz.mon0mon.ch04.s06_offset_commit.p03_async_commit

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetCommitCallback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.6.3 Asynchronous Commit — commitAsync(OffsetCommitCallback)
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `OffsetCommitCallback.onComplete(offsets, exception)`. 실패만 로그한다 (책 2판, 이 파일).
 * - 4.4  인터페이스는 `@FunctionalInterface`라 SAM으로 넘긴다. 람다만 쓰는 형태는 [CurrentAsyncCommitCallback].
 *         콜백 없는 `commitAsync()`는 [BookAsyncCommit].
 *
 * Still valid: `commitAsync(OffsetCommitCallback)`, `poll(Duration)`
 */
object BookAsyncCommitCallback {
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
        // 4.4: 붙여 넣은 코드는 프로듀서 Properties를 컨슈머에 넘겼다.
        // 책은 자동 커밋을 끄고 콜백이 있는 commitAsync만 쓴다.
        kafkaProps[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"

        val consumer = KafkaConsumer<String, String>(kafkaProps)

        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        val producer = KafkaProducer<String, String>(producerProps)

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

            // region 4.6.3 비동기적 커밋
            val timeout = Duration.ofMillis(100)
            val lastProcessed = HashMap<TopicPartition, Long>()
            val deadline = System.currentTimeMillis() + 10_000
            val callbackSeen = AtomicInteger()
            var commits = 0
            var seen = 0
            while (seen < samples.size && System.currentTimeMillis() < deadline) {
                val records = consumer.poll(timeout)
                for (record in records) {
                    // 4.4: 붙여 넣은 format은 offset 자리에 key를 넣었다. 책은 offset, key, value 순이다.
                    logger.info {
                        "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} customer=${record.key()} country=${record.value()}"
                    }
                    lastProcessed[TopicPartition(record.topic(), record.partition())] = record.offset()
                    seen++
                }
                consumer.commitAsync(OffsetCommitCallback { offsets, exception ->
                    callbackSeen.incrementAndGet()
                    if (exception != null) {
                        logger.error(exception) { "Commit failed for offsets $offsets" }
                    } else {
                        // 4.4: 책은 실패만 로그한다. 지금은 콜백이 호출됐는지 확인한다.
                        logger.info { "commit callback offsets=$offsets" }
                    }
                })
                commits++
            }
            if (seen < samples.size) {
                logger.error { "poll timed out seen=$seen expected=${samples.size}" }
            }
            // 4.4: 콜백은 다음 poll에서 돈다. 지금은 호출 횟수와 저장 오프셋을 확인한다.
            awaitCommitted(consumer, lastProcessed, callbackSeen, commits)
            logCommitted(consumer, lastProcessed)
            logGroupOffsets(GROUP)
            if (callbackSeen.get() < commits) {
                logger.error { "commit callback missing seen=${callbackSeen.get()} commits=$commits" }
            }
            // endregion
        } finally {
            producer.close()
            consumer.close()
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

    private fun awaitCommitted(
        consumer: KafkaConsumer<String, String>,
        lastProcessed: Map<TopicPartition, Long>,
        callbackSeen: AtomicInteger,
        commits: Int,
    ) {
        val deadline = System.currentTimeMillis() + 10_000
        while (System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200))
            val stored = consumer.committed(consumer.assignment(), Duration.ofSeconds(10))
            val offsetsMatch = lastProcessed.all { (tp, processed) ->
                stored[tp]?.offset() == processed + 1
            }
            if (offsetsMatch && callbackSeen.get() >= commits) {
                return
            }
        }
        logger.error { "async commit timed out callbacks=${callbackSeen.get()} commits=$commits" }
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
