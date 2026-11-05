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
import org.apache.kafka.common.utils.Utils
import xyz.mon0mon.common.KafkaSettings
import java.util.Properties

private val logger = KotlinLogging.logger {}

/**
 * Book: §3.6 Partitions
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  키 `Banana`는 마지막 파티션, 나머지는 `abs(murmur2) % (numPartitions - 1)` (책, [BookBananaPartitioner]).
 * - 4.0  `DefaultPartitioner`, `UniformStickyPartitioner` 삭제 (KIP-794). 커스텀 `Partitioner`와 `partitioner.class`는 남는다.
 *         키 없는 기본 분배는 프로듀서 내부 `BuiltInPartitioner`.
 * - 4.4  인터페이스 메서드는 `partition`과 `close`. `configure`는 `Configurable`. `onNewBatch`는 없다.
 *
 * Removed: `org.apache.kafka.clients.producer.internals.DefaultPartitioner`, `UniformStickyPartitioner`, `partitioner.ignore.keys`
 * Still valid: `Partitioner`, `ProducerConfig.PARTITIONER_CLASS_CONFIG`, `Utils.murmur2`
 */
class BookBananaPartitioner : Partitioner {
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

        if (keyBytes == null || key !is String) {
            throw InvalidRecordException("We expect all messages to have customer name as key")
        }

        if (key == "Banana") {
            // Banana will always go to last partition
            return numPartitions - 1
        }

        // Other record will get hashed to the rest of the partitions
        // 4.4: kotlin.math.abs는 Int.MIN_VALUE에서 ArithmeticException이다. 책의 java.lang.Math.abs는 그 값을 음수로 남긴다.
        return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1)
    }
    // endregion

    override fun close() {
        // 4.4: 책은 close()를 빈 구현으로 둔다. TODO()는 프로듀서 close에서 예외가 된다.
    }

    override fun configure(configs: Map<String, *>) {
        // 4.4: Configurable.configure(Map<String, ?>). 책은 설정을 읽지 않는다.
    }
}

object BookCustomPartitioner {
    private const val TOPIC = "ch03-s06-partition"

    @JvmStatic
    fun main(args: Array<String>) {
        // region 3.6 파티션
        // 4.4: 책 식은 파티션이 2개 이상일 때 성립한다. 브로커 기본값 1이면 `(numPartitions - 1)`이 0이다.
        ensurePartitions(TOPIC, 3)

        val kafkaProps = Properties()
        // 1.x
        // kafkaProps["bootstrap.servers"] = "localhost:9092"
        // kafkaProps["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        // kafkaProps["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        // kafkaProps["partitioner.class"] = "xyz.mon0mon.ch03.s06_partition.BookBananaPartitioner"

        kafkaProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        kafkaProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        kafkaProps[ProducerConfig.PARTITIONER_CLASS_CONFIG] = BookBananaPartitioner::class.java.name

        val producer = KafkaProducer<String, String>(kafkaProps)
        try {
            listOf("Banana", "Apple").forEach { key ->
                val metadata = producer.send(ProducerRecord(TOPIC, key, key)).get()
                logger.info { "key=$key partition=${metadata.partition()}" }
            }
        } finally {
            // 4.4: 책 스니펫은 close를 생략한다. close()가 프로듀서 스레드를 끝낸다.
            producer.close()
        }
        // endregion
    }

    private fun ensurePartitions(topic: String, partitions: Int) {
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        AdminClient.create(props).use { admin ->
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
