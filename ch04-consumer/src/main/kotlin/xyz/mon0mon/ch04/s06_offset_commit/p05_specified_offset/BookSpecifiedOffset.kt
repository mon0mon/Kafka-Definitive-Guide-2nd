package xyz.mon0mon.ch04.s06_offset_commit.p05_specified_offset

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Properties

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.6.5 Committing a Specified Offset
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  파티션마다 `OffsetAndMetadata(offset + 1, "no metadata")`를 맵에 넣고,
 *        `count % 1000 == 0`일 때 `commitAsync(currentOffsets, null)` (책 2판, 이 파일).
 *        커밋하는 값은 처리한 오프셋이 아니라 다음에 읽을 오프셋이다.
 * - 4.4  같은 오버로드가 남아 있다. 맵에는 리더 에포크가 없다.
 *        에포크를 넣는 형태는 [CurrentSpecifiedOffset].
 *
 * Still valid: `commitAsync(Map, OffsetCommitCallback)`, `OffsetAndMetadata(long, String)`, `poll(Duration)`
 */
object BookSpecifiedOffset {
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
        // 4.4: 책은 자동 커밋을 끄고, 지정한 오프셋만 commitAsync로 보낸다.
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

            // region 4.6.5 특정 오프셋 커밋
            // 4.4: 책은 while (true). 지금은 보낸 건수만큼 읽고 끝낸다.
            val currentOffsets = HashMap<TopicPartition, OffsetAndMetadata>()
            var count = 0
            val timeout = Duration.ofMillis(100)
            val lastProcessed = HashMap<TopicPartition, Long>()
            val deadline = System.currentTimeMillis() + 10_000
            var seen = 0
            while (seen < samples.size && System.currentTimeMillis() < deadline) {
                val records = consumer.poll(timeout)
                for (record in records) {
                    logger.info {
                        "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} customer=${record.key()} country=${record.value()}"
                    }
                    val tp = TopicPartition(record.topic(), record.partition())
                    lastProcessed[tp] = record.offset()
                    currentOffsets[tp] = OffsetAndMetadata(record.offset() + 1, "no metadata")
                    count++
                    seen++
                }
                // 4.4: 책은 레코드 루프 안에서 count % 1000 == 0 일 때 commitAsync(currentOffsets, null).
                // count는 0부터라 첫 건도 커밋된다. 지금은 배치를 맵에 반영한 뒤 샘플을 다 봤을 때 한 번만 보낸다.
                // 더 작은 오프셋이 나중에 적용되면 그룹 오프셋이 되돌아간다.
                if (seen >= samples.size && count > 0) {
                    consumer.commitAsync(currentOffsets, null)
                }
            }
            if (seen < samples.size) {
                logger.error { "poll timed out seen=$seen expected=${samples.size}" }
            }
            // 4.4: commitAsync는 브로커 응답 전에 반환한다.
            // 지금은 다음 poll로 요청을 보낸 뒤 저장된 다음 오프셋과 메타데이터를 확인한다.
            awaitCommitted(consumer, lastProcessed)
            logCommitted(consumer, lastProcessed)
            logGroupOffsets(GROUP)
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
                meta?.offset() == processed + 1 && meta.metadata() == "no metadata"
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
    ) {
        val stored = consumer.committed(consumer.assignment(), Duration.ofSeconds(10))
        for (tp in consumer.assignment()) {
            val meta = stored[tp]
            val processed = lastProcessed[tp]
            logger.info {
                "committed topic=${tp.topic()} partition=${tp.partition()} offset=${meta?.offset()} metadata=${meta?.metadata()} lastProcessed=$processed"
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
                    "group=$group topic=${tp.topic()} partition=${tp.partition()} committed=${meta.offset()} metadata=${meta.metadata()}"
                }
            }
        }
    }
}
