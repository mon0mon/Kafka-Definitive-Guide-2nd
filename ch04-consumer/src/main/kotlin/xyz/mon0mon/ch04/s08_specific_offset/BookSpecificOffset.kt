package xyz.mon0mon.ch04.s08_specific_offset

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Properties

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.8 Consuming Records with Specific Offsets
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `seekToBeginning` / `seekToEnd`로 파티션 끝으로 이동한다.
 *        외부 저장소의 오프셋은 `onPartitionsAssigned`에서 `seek(TopicPartition, long)` (책 2판, 이 파일).
 *        처리한 레코드의 `offset`을 그대로 저장한다.
 * - 4.4  `seek(long)`, `seekToBeginning`, `seekToEnd`는 그대로다.
 *        리더 에포크를 넣는 `seek(OffsetAndMetadata)`와 `onPartitionsLost`는 [CurrentSpecificOffset].
 *
 * Still valid: `seek(TopicPartition, long)`, `seekToBeginning`, `seekToEnd`, `poll(Duration)`, `ConsumerRebalanceListener`
 */
object BookSpecificOffset {
    private const val TOPIC = "ch04SpecificOffset"
    private const val GROUP = "ExternalOffsetReader"

    private val dbOffsets = HashMap<TopicPartition, Long>()

    @JvmStatic
    fun main(args: Array<String>) {
        // 4.4: 책은 customerCountries가 이미 있다고 가정한다.
        // 지금은 이 절만의 토픽을 만들어 다른 예제의 오프셋과 섞이지 않게 한다.
        ensureTopic(TOPIC)

        val kafkaProps = Properties()
        kafkaProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ConsumerConfig.GROUP_ID_CONFIG] = GROUP
        kafkaProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        kafkaProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        // 4.4: 책은 오프셋을 카프카가 아니라 외부에 둔다. 자동 커밋을 끄지 않으면 그룹 오프셋이 seek와 따로 움직인다.
        kafkaProps[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
        // 4.4: 커밋이 없으면 기본값이 latest라 이미 있는 레코드는 건너뛴다.
        // seek가 그 위치를 덮는지를 보려면 이 기본값이 필요하다.
        kafkaProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"

        val consumer = KafkaConsumer<String, String>(kafkaProps)

        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        val producer = KafkaProducer<String, String>(producerProps)

        try {
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
            // 책은 데이터베이스에 이미 있는 오프셋으로 seek한다고 가정한다.
            // 지금은 두 번째 레코드의 오프셋을 그 값으로 넣는다. 첫 레코드는 읽지 않는다.
            storeOffsetInDB(target.topic(), target.partition(), target.offset())

            // 파티션 할당은 poll() 안에서 끝난다. 리스너가 그 직후, 조회 전에 seek한다.
            consumer.subscribe(listOf(TOPIC), SaveOffsetsOnRebalance(consumer))

            // region 4.8 특정 오프셋의 레코드 읽어오기
            // 4.4: 책은 while (true). 지금은 seek 위치부터 이번 배치의 마지막 오프셋까지 읽고 끝낸다.
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
                    storeOffsetInDB(record.topic(), record.partition(), record.offset())
                    seen++
                }
                commitDBTransaction()
            }
            if (seen < expected) {
                logger.error { "poll timed out seen=$seen expected=$expected seek=${target.offset()}" }
            }
            logger.info { "external offsets=$dbOffsets" }
            // endregion
        } finally {
            producer.close()
            consumer.close()
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

        // region 4.8 특정 오프셋의 레코드 읽어오기
        override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
            // 이번 poll()의 조회 위치를 외부 저장소 오프셋으로 덮는다.
            for (partition in partitions) {
                val offset = getOffsetFromDB(partition)
                logger.info {
                    "seek topic=${partition.topic()} partition=${partition.partition()} offset=$offset"
                }
                consumer.seek(partition, offset)
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
            "topic=${record.topic()} partition=${record.partition()} offset=${record.offset()} customer=${record.key()} country=${record.value()}"
        }
    }

    private fun storeRecordInDB(record: ConsumerRecord<String, String>) {
        logger.info {
            "stored record topic=${record.topic()} partition=${record.partition()} offset=${record.offset()}"
        }
    }

    private fun storeOffsetInDB(topic: String, partition: Int, offset: Long) {
        dbOffsets[TopicPartition(topic, partition)] = offset
    }

    private fun getOffsetFromDB(partition: TopicPartition): Long =
        dbOffsets[partition] ?: error("no offset in external store for $partition")

    private fun commitDBTransaction() {
        logger.info { "db transaction committed offsets=$dbOffsets" }
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
}
