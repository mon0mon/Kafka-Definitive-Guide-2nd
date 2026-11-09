package xyz.mon0mon.ch04.s08_specific_offset

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Optional

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.8 Consuming Records with Specific Offsets
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `seek(TopicPartition, long)`. 저장 값은 처리한 레코드의 `offset` ([BookSpecificOffset]).
 * - 2.4  `onPartitionsLost`. 파티션을 이미 잃었으면 외부 트랜잭션을 커밋하지 않는다.
 * - 2.5  `seek(TopicPartition, OffsetAndMetadata)`. 리더 에포크를 함께 넘긴다.
 * - 4.4  `Map` + deserializer 인스턴스, `use`.
 *        외부 저장소에는 다음에 읽을 오프셋과 그 레코드의 리더 에포크를 넣는다.
 *
 * Replaces: [BookSpecificOffset]
 * Still valid: `seek(TopicPartition, long)`, `seek(TopicPartition, OffsetAndMetadata)`, `seekToBeginning`, `seekToEnd`, `poll(Duration)`
 */
object CurrentSpecificOffset {
    private const val TOPIC = "ch04SpecificOffset"
    private const val GROUP = "ExternalOffsetReader"

    private val dbOffsets = HashMap<TopicPartition, OffsetAndMetadata>()

    @JvmStatic
    fun main(args: Array<String>) {
        ensureTopic(TOPIC)

        val producerProps = KafkaSettings.stringProducerProperties(clientId = "ch04-s08-current")
        val consumerConfigs = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG to GROUP,
            ConsumerConfig.CLIENT_ID_CONFIG to "ch04-s08-current",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "latest",
        )

        KafkaProducer(producerProps, StringSerializer(), StringSerializer()).use { producer ->
            KafkaConsumer(consumerConfigs, StringDeserializer(), StringDeserializer()).use { consumer ->
                val produced = produceSamples(producer)
                if (produced.size < 3) {
                    return
                }
                val target = produced[1]
                val last = produced.last()
                if (target.partition() != last.partition()) {
                    logger.error { "samples landed on more than one partition" }
                    return
                }
                storeOffsetInDB(target.topic(), target.partition(), target.offset(), Optional.empty<Int>())

                // 파티션 할당은 poll() 안에서 끝난다. 리스너가 그 직후, 조회 전에 seek한다.
                consumer.subscribe(listOf(TOPIC), SaveOffsetsOnRebalance(consumer))

                // region 4.8 특정 오프셋의 레코드 읽어오기
                val expected = (last.offset() - target.offset() + 1).toInt()
                val deadline = System.currentTimeMillis() + 15_000
                var seen = 0
                var checkedFirst = false
                while (seen < expected && System.currentTimeMillis() < deadline) {
                    val records = consumer.poll(Duration.ofMillis(100))
                    for (record in records) {
                        if (record.partition() != target.partition() || record.offset() < target.offset()) {
                            logger.error {
                                "read before seek partition=${record.partition()} offset=${record.offset()} seek=${target.offset()}"
                            }
                            continue
                        }
                        if (!checkedFirst) {
                            checkedFirst = true
                            if (record.offset() != target.offset()) {
                                logger.error { "seek mismatch expected=${target.offset()} actual=${record.offset()}" }
                            } else {
                                logger.info { "seek matched offset=${record.offset()}" }
                            }
                        }
                        processRecord(record)
                        storeRecordInDB(record)
                        storeOffsetInDB(
                            record.topic(),
                            record.partition(),
                            record.offset() + 1,
                            record.leaderEpoch(),
                        )
                        seen++
                    }
                    commitDBTransaction()
                }
                if (seen < expected) {
                    logger.error { "poll timed out seen=$seen expected=$expected seek=${target.offset()}" }
                }
                logExternalOffsets(last)
                // endregion
            }
        }
    }

    // subscribe()는 파티션을 바로 주지 않는다. 할당은 poll() 안에서 끝나고,
    // onPartitionsAssigned는 그 직후, 레코드를 가져오기 전에 호출된다.
    // 여기서 seek하면 같은 poll()이 외부 오프셋부터 읽는다.
    // seek를 poll()이 반환한 뒤에 하면, 그 조회는 latest나 그룹 오프셋을 쓰고 다음 poll()부터 원하는 위치가 된다.
    // 리밸런스로 파티션을 다시 받아도 이 콜백이 호출된다. 시작 때 한 번 seek하면 그때는 다시 latest에서 시작한다.
    private class SaveOffsetsOnRebalance(
        private val consumer: KafkaConsumer<String, String>,
    ) : ConsumerRebalanceListener {
        override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
            logger.info { "revoked partitions=$partitions" }
            commitDBTransaction()
        }

        override fun onPartitionsLost(partitions: Collection<TopicPartition>) {
            logger.info { "partitions lost $partitions; external commit skipped" }
        }

        // region 4.8 특정 오프셋의 레코드 읽어오기
        override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
            // 이번 poll()의 조회 위치를 외부 저장소 오프셋으로 덮는다.
            for (partition in partitions) {
                val stored = getOffsetFromDB(partition)
                logger.info {
                    "seek topic=${partition.topic()} partition=${partition.partition()} offset=${stored.offset()} leaderEpoch=${stored.leaderEpoch()}"
                }
                consumer.seek(partition, stored)
            }
        }
        // endregion
    }

    private fun produceSamples(producer: KafkaProducer<String, String>): List<RecordMetadata> {
        val samples = listOf(
            "Precision Products" to "France",
            "Biomedical Materials" to "USA",
            "Precision Products" to "France",
        )
        val produced = ArrayList<RecordMetadata>()
        for ((customer, country) in samples) {
            try {
                val metadata = producer.send(ProducerRecord(TOPIC, customer, country)).get()
                produced.add(metadata)
                logger.info {
                    "sent topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()} customer=$customer country=$country"
                }
            } catch (e: Exception) {
                logger.error(e) { "send failed" }
            }
        }
        return produced
    }

    private fun processRecord(record: ConsumerRecord<String, String>) {
        logger.info {
            "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} leaderEpoch=${record.leaderEpoch()} customer=${record.key()} country=${record.value()}"
        }
    }

    private fun storeRecordInDB(record: ConsumerRecord<String, String>) {
        logger.info {
            "stored record topic=${record.topic()} partition=${record.partition()} offset=${record.offset()}"
        }
    }

    private fun storeOffsetInDB(
        topic: String,
        partition: Int,
        offset: Long,
        leaderEpoch: Optional<Int>,
    ) {
        dbOffsets[TopicPartition(topic, partition)] = OffsetAndMetadata(offset, leaderEpoch, "external")
    }

    private fun getOffsetFromDB(partition: TopicPartition): OffsetAndMetadata =
        dbOffsets[partition] ?: error("no offset in external store for $partition")

    private fun commitDBTransaction() {
        logger.info { "db transaction committed offsets=$dbOffsets" }
    }

    private fun logExternalOffsets(last: RecordMetadata) {
        val tp = TopicPartition(last.topic(), last.partition())
        val stored = dbOffsets[tp]
        logger.info {
            "external topic=${tp.topic()} partition=${tp.partition()} next=${stored?.offset()} leaderEpoch=${stored?.leaderEpoch()} metadata=${stored?.metadata()}"
        }
        if (stored?.offset() != last.offset() + 1) {
            logger.error {
                "external offset mismatch expected=${last.offset() + 1} stored=${stored?.offset()}"
            }
        }
        if (stored?.metadata() != "external") {
            logger.error { "metadata mismatch expected=external stored=${stored?.metadata()}" }
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
}
