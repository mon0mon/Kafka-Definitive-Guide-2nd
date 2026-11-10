package xyz.mon0mon.ch04.s11_standalone_consumer

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.11 Standalone Consumer
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `Properties` + FQCN deserializer, `partitionsFor(String)`, 인자 없는 `commitSync()` ([BookStandaloneConsumer]).
 * - 3.0  `poll(long)` 삭제.
 * - 4.4  `Map` + deserializer 인스턴스, `use`, `partitionsFor(String, Duration)`, `commitSync(Duration)`.
 *        `group.protocol`은 설정하지 않는다. `assign`은 classic이든 consumer(KIP-848)이든 그룹 프로토콜을 타지 않는다.
 *        `group.id`는 여전히 오프셋 커밋의 키다. 없으면 `commitSync`가 실패한다.
 *
 * Replaces: [BookStandaloneConsumer]
 * Still valid: `partitionsFor(String, Duration)`, `assign(Collection)`, `poll(Duration)`, `commitSync(Duration)`
 *
 * 흐름:
 * 1. 파티션 3개인 토픽을 만든다. 한 컨슈머가 그룹 없이 모든 파티션을 읽으려면 파티션이 둘 이상이어야 한다.
 * 2. `group.id`를 넣는다. `assign`은 이 값으로 그룹에 가입하지 않는다. `commitSync`가 오프셋을 저장할 키로만 쓴다.
 * 3. `partitionsFor`로 파티션 목록을 받아 `TopicPartition`으로 바꾸고 `assign`한다. `subscribe`는 호출하지 않는다.
 * 4. 커밋이 없으면 첫 `poll`이 `auto.offset.reset`(기본 latest)으로 현재 로그 끝을 읽기 위치로 잡는다.
 *    샘플은 그 `poll` 다음에 넣는다. 파티션마다 한 건이다.
 * 5. 이어서 `poll`하고, 배치마다 `commitSync`한다. 저장되는 오프셋은 처리한 오프셋 + 1이다.
 * 6. 그룹 멤버는 0이다. 오프셋만 그 그룹 키 아래에 남고, 리밸런스는 없다.
 */
object CurrentStandaloneConsumer {
    private const val TOPIC = "ch04Standalone"
    private const val GROUP = "CountryCounterStandaloneCurrent"
    private const val PARTITION_COUNT = 3

    @JvmStatic
    fun main(args: Array<String>) {
        if (!ensureTopic(TOPIC)) {
            return
        }

        val producerProps = KafkaSettings.stringProducerProperties(clientId = "ch04-s11-current")
        val consumerConfigs = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG to GROUP,
            ConsumerConfig.CLIENT_ID_CONFIG to "ch04-s11-current",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
        )

        KafkaProducer(producerProps, StringSerializer(), StringSerializer()).use { producer ->
            KafkaConsumer(consumerConfigs, StringDeserializer(), StringDeserializer()).use { consumer ->
                // region 4.11 독립형 컨슈머
                val partitionInfos = consumer.partitionsFor(TOPIC, Duration.ofSeconds(10))
                if (partitionInfos.isNullOrEmpty()) {
                    logger.error { "no partitions topic=$TOPIC" }
                    return
                }

                val partitions = partitionInfos.map { TopicPartition(it.topic(), it.partition()) }
                // subscribe와 assign은 한 컨슈머에서 같이 쓰지 못한다. 여기서는 assign만 호출한다.
                consumer.assign(partitions)
                // assignment()는 내부 집합의 뷰다. 나중에 바뀌었는지 보려면 지금 내용을 복사해 둔다.
                val assigned = consumer.assignment().toSet()
                logger.info { "assigned partitions=$assigned group=$GROUP" }
                if (assigned.size != PARTITION_COUNT) {
                    logger.error { "assignment size=${assigned.size} expected=$PARTITION_COUNT" }
                    return
                }

                // 커밋이 없으면 첫 poll이 latest로 로그 끝을 읽기 위치로 잡는다.
                // 샘플을 그 전에 넣으면 끝보다 앞에 있어 읽히지 않는다.
                val skipped = consumer.poll(Duration.ofMillis(200))
                logger.info { "pinned position before samples skipped=${skipped.count()}" }
                consumer.commitSync(Duration.ofSeconds(10))

                val produced = produceSamples(producer)
                if (produced.size < PARTITION_COUNT) {
                    return
                }

                val deadline = System.currentTimeMillis() + 15_000
                var seen = 0
                val lastProcessed = HashMap<TopicPartition, Long>()
                while (seen < produced.size && System.currentTimeMillis() < deadline) {
                    val records = consumer.poll(Duration.ofMillis(1000))
                    for (record in records) {
                        logger.info {
                            "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} customer=${record.key()} country=${record.value()}"
                        }
                        lastProcessed[TopicPartition(record.topic(), record.partition())] = record.offset()
                        seen++
                    }
                    // 이 배치의 다음 읽기 위치를 그룹 키 아래에 저장한다. 멤버 가입은 하지 않는다.
                    consumer.commitSync(Duration.ofSeconds(10))
                }
                if (seen < produced.size) {
                    logger.error { "poll timed out seen=$seen expected=${produced.size}" }
                }
                if (consumer.assignment() != assigned) {
                    logger.error { "assignment changed from=$assigned to=${consumer.assignment()}" }
                }
                logMembership()
                logGroupOffsets(lastProcessed)
                // endregion
            }
        }
    }

    private fun produceSamples(producer: KafkaProducer<String, String>): List<RecordMetadata> {
        // 파티션 번호로 한 건씩 넣어, assign이 세 파티션을 모두 읽는지 확인한다.
        val samples = listOf(
            Triple(0, "Precision Products", "France"),
            Triple(1, "Biomedical Materials", "USA"),
            Triple(2, "Acme", "UK"),
        )
        val produced = ArrayList<RecordMetadata>()
        for ((partition, customer, country) in samples) {
            try {
                val metadata = producer.send(ProducerRecord(TOPIC, partition, customer, country)).get()
                produced.add(metadata)
                logger.info {
                    "sent topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()} customer=$customer country=$country"
                }
            } catch (e: Exception) {
                logger.error(e) { "send failed partition=$partition" }
            }
        }
        return produced
    }

    private fun ensureTopic(topic: String): Boolean {
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
        )
        AdminClient.create(configs).use { admin ->
            val names = admin.listTopics().names().get()
            if (names.contains(topic)) {
                val partitions = admin.describeTopics(listOf(topic)).allTopicNames().get()
                    .getValue(topic).partitions().size
                if (partitions != PARTITION_COUNT) {
                    logger.error { "topic partition count=$partitions expected=$PARTITION_COUNT name=$topic" }
                    return false
                }
                logger.info { "topic exists name=$topic partitions=$partitions" }
                return true
            }
            admin.createTopics(listOf(NewTopic(topic, PARTITION_COUNT, 1.toShort()))).all().get()
            logger.info { "topic created name=$topic partitions=$PARTITION_COUNT" }
            return true
        }
    }

    private fun logMembership() {
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
        )
        AdminClient.create(configs).use { admin ->
            try {
                val description = admin.describeConsumerGroups(listOf(GROUP))
                    .describedGroups()
                    .getValue(GROUP)
                    .get()
                logger.info {
                    "group=$GROUP state=${description.groupState()} members=${description.members().size} simple=${description.isSimpleConsumerGroup()}"
                }
                if (description.members().isNotEmpty()) {
                    logger.error { "standalone consumer joined the group members=${description.members()}" }
                }
            } catch (e: Exception) {
                logger.error(e) { "describe group failed group=$GROUP" }
            }
        }
    }

    private fun logGroupOffsets(lastProcessed: Map<TopicPartition, Long>) {
        if (lastProcessed.size != PARTITION_COUNT) {
            logger.error { "processed partitions=${lastProcessed.keys} expected=$PARTITION_COUNT" }
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
