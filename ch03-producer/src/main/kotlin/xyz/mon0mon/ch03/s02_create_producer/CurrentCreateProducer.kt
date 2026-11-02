package xyz.mon0mon.ch03.s02_create_producer

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings

private val logger = KotlinLogging.logger {}

/**
 * Book: §3.2 Constructing a Kafka Producer
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 1.x  문자열 키
 * - 2.x  `ProducerConfig` 상수 + FQCN 문자열 serializer ([BookCreateProducer])
 * - 4.4  `Map` + serializer 인스턴스를 생성자에 넘긴다. `use`로 close를 보장한다.
 *
 * Replaces: [BookCreateProducer]
 * Still valid: `ProducerConfig` 상수
 */
object CurrentCreateProducer {
    @JvmStatic
    fun main(args: Array<String>) {
        // region 3.2 카프카 프로듀서 생성하기
        val configs = mapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ProducerConfig.CLIENT_ID_CONFIG to "ch03-s02-current",
            ProducerConfig.ACKS_CONFIG to "all",
        )

        KafkaProducer(configs, StringSerializer(), StringSerializer()).use { producer ->
            logger.info { "created producer clientId=ch03-s02-current bootstrap=${KafkaSettings.bootstrapServers()}" }
            producer.flush()
        }
        // endregion
    }
}
