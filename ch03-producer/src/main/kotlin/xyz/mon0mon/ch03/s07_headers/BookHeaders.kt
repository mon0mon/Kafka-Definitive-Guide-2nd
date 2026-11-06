package xyz.mon0mon.ch03.s07_headers

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Properties

private val logger = KotlinLogging.logger {}

/**
 * Book: §3.7 Headers
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `headers().add(key, bytes)` (책, 이 파일). 스니펫은 send하지 않는다.
 * - 4.4  `Headers.add`는 그대로다. `RecordMetadata`에는 헤더가 없다.
 *
 * Still valid: `ProducerRecord.headers`, `Headers.add`
 */
object BookHeaders {
    private const val TOPIC = "CustomerCountry"

    @JvmStatic
    fun main(args: Array<String>) {
        val kafkaProps = Properties()
        kafkaProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        kafkaProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"

        val producer = KafkaProducer<String, String>(kafkaProps)
        try {
            // region 3.7 헤더
            val record = ProducerRecord(TOPIC, "Precision Products", "France")
            record.headers().add("privacy-level", "YOLO".encodeToByteArray())

            // 4.4: 책은 헤더만 붙인다. 메타데이터에는 헤더가 없어서, 적재된 오프셋을 다시 읽는다.
            val metadata = producer.send(record).get()
            logger.info {
                "sent topic=${metadata.topic()} partition=${metadata.partition()} offset=${metadata.offset()}"
            }
            readHeaders(metadata.topic(), metadata.partition(), metadata.offset())
            // endregion
        } finally {
            // 4.4: 책 스니펫은 close를 생략한다. close()가 대기 중인 send를 flush한 뒤 프로듀서 스레드를 끝낸다.
            producer.close()
        }
    }

    private fun readHeaders(topic: String, partition: Int, offset: Long) {
        val props = Properties()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        props[ConsumerConfig.GROUP_ID_CONFIG] = "ch03-s07-book"
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"

        val consumer = KafkaConsumer<String, String>(props)
        try {
            val tp = TopicPartition(topic, partition)
            consumer.assign(listOf(tp))
            consumer.seek(tp, offset)
            val records = consumer.poll(Duration.ofSeconds(10))
            if (records.isEmpty) {
                logger.error { "no record at $topic-$partition@$offset" }
                return
            }
            records.forEach { record ->
                val headers = record.headers().joinToString(prefix = "[", postfix = "]") { header ->
                    "${header.key()}=${header.value().decodeToString()}"
                }
                logger.info {
                    "read key=${record.key()} value=${record.value()} " +
                        "partition=${record.partition()} offset=${record.offset()} headers=$headers"
                }
            }
        } finally {
            consumer.close()
        }
    }
}
