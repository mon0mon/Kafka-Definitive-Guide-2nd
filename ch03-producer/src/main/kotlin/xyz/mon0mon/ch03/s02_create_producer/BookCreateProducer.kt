package xyz.mon0mon.ch03.s02_create_producer

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import xyz.mon0mon.common.KafkaSettings
import java.util.Properties

private val logger = KotlinLogging.logger { }

/**
 * Book: §3.2 Constructing a Kafka Producer
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 1.x  문자열 키 `"bootstrap.servers"`, `"key.serializer"`, `"value.serializer"`.
 *         오타는 런타임에야 드러난다.
 * - 2.x  `ProducerConfig.*_CONFIG` 상수 (책 2판 이후 권장, 이 파일).
 * - 4.4  상수는 그대로 유효하다. serializer는 FQCN 문자열 대신 클래스 객체를 쓰는 편이 안전하다
 *         → [CurrentCreateProducer].
 *
 * Removed: 이 절에는 4.0에서 삭제된 생성 API가 없다.
 * Still valid: `Properties` + `ProducerConfig` 상수 + `KafkaProducer(props)`
 */
object BookCreateProducer {
    @JvmStatic
    fun main(args: Array<String>) {
        // region 3.2 카프카 프로듀서 생성하기
        val kafkaProps = Properties()

        // 1.x
        // kafkaProps["bootstrap.servers"] = "localhost:9092"
        // kafkaProps["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        // kafkaProps["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"

        kafkaProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        kafkaProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"

        val producer = KafkaProducer<String, String>(kafkaProps)
        try {
            logger.info { "created producer bootstrap=${KafkaSettings.bootstrapServers()}" }
        } finally {
            // 4.4: 책 스니펫은 close를 생략한다. close()가 프로듀서 스레드를 끝낸다.
            producer.close()
        }
        // endregion
    }
}
