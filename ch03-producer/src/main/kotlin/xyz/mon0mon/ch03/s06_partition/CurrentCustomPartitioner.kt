package xyz.mon0mon.ch03.s06_partition

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.Utils
import xyz.mon0mon.common.KafkaSettings

private val logger = KotlinLogging.logger {}

/**
 * Book: §3.6 Partitions
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  책: `Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1)` ([BookBananaPartitioner]).
 * - 4.0  기본 파티셔너 클래스 삭제. 커스텀 구현은 `partitioner.class`로 연결한다.
 * - 4.4  `Utils.toPositive(murmur2)`로 음수 파티션을 피한다. 파티션이 1개면 나머지 연산을 하지 않는다.
 *         프로듀서는 `Map`, serializer 인스턴스, `use`.
 *
 * Replaces: [BookCustomPartitioner]
 * Removed: `DefaultPartitioner`, `UniformStickyPartitioner`, `partitioner.ignore.keys`
 * Still valid: `Partitioner`, `ProducerConfig.PARTITIONER_CLASS_CONFIG`
 */
class CurrentBananaPartitioner : Partitioner {
    // region 3.6 파티션
    override fun partition(
        topic: String,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any?,
        valueBytes: ByteArray?,
        cluster: Cluster,
    ): Int {
        val numPartitions = cluster.partitionsForTopic(topic).size
        if (numPartitions == 0) {
            throw InvalidRecordException("Topic $topic has no partitions")
        }
        if (keyBytes == null || key !is String) {
            throw InvalidRecordException("We expect all messages to have customer name as key")
        }
        if (numPartitions == 1 || key == "Banana") {
            return numPartitions - 1
        }
        return Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - 1)
    }
    // endregion

    override fun close() {}

    override fun configure(configs: Map<String, *>) {}
}

object CurrentCustomPartitioner {
    private const val TOPIC = "ch03-s06-partition"

    @JvmStatic
    fun main(args: Array<String>) {
        // region 3.6 파티션
        ensurePartitions(TOPIC, 3)

        val configs = mapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ProducerConfig.CLIENT_ID_CONFIG to "ch03-s06-current",
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.PARTITIONER_CLASS_CONFIG to CurrentBananaPartitioner::class.java,
        )

        KafkaProducer(configs, StringSerializer(), StringSerializer()).use { producer ->
            listOf("Banana", "Apple").forEach { key ->
                val metadata = producer.send(ProducerRecord(TOPIC, key, key)).get()
                logger.info { "key=$key partition=${metadata.partition()}" }
            }
        }
        // endregion
    }

    private fun ensurePartitions(topic: String, partitions: Int) {
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
        )
        AdminClient.create(configs).use { admin ->
            val names = admin.listTopics().names().get()
            if (!names.contains(topic)) {
                admin.createTopics(listOf(NewTopic(topic, partitions, 1.toShort()))).all().get()
                return
            }
            val current = admin.describeTopics(listOf(topic)).allTopicNames().get().getValue(topic).partitions().size
            if (current < partitions) {
                admin.createPartitions(mapOf(topic to NewPartitions.increaseTo(partitions))).all().get()
            }
        }
    }
}
