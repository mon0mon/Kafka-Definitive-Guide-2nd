package xyz.mon0mon.ch04.s10_custom_deserializer

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.10.2 Avro Deserialization
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `Properties` + FQCN `KafkaAvroDeserializer`, 수동 `close`. 기본 결과는 `GenericRecord` ([BookAvroDeserializer]).
 *        자바 생성 클래스로 받으려면 `specific.avro.reader=true`.
 * - 3.0  `poll(long)` 삭제.
 * - 4.4  `Map` + 디시리얼라이저 인스턴스, `use`. 생성자에 넘긴 인스턴스는 Kafka가 `configure`하지 않는다.
 *        `specific.avro.reader`의 기본값은 여전히 false다. 이 모듈은 `GenericRecord`로 읽는다.
 *        `runS102Current`가 `-Dorg.apache.avro.SERIALIZABLE_PACKAGES=customerManagement.avro`를 넣는다.
 *
 * Replaces: [BookAvroDeserializer]
 * Removed: `KafkaConsumer.poll(long)`
 * Still valid: `KafkaAvroDeserializer`, `schema.registry.url`, `specific.avro.reader`, `GenericRecord`
 */
object CurrentAvroDeserializer {
    private const val TOPIC = "customerContacts"
    private const val GROUP = "CountryCounter"

    private val schemaString = """
        {"namespace": "customerManagement.avro", "type": "record", "name": "Customer",
         "fields": [
           {"name": "id", "type": "int"},
           {"name": "name", "type": "string"},
           {"name": "email", "type": ["null", "string"], "default": null}
         ]}
        """.trimIndent()

    @JvmStatic
    fun main(args: Array<String>) {
        val registryUrl = KafkaSettings.schemaRegistryUrl()
        val serdeConfigs = mapOf<String, Any>(
            KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to registryUrl,
        )
        val valueSerializer = KafkaAvroSerializer()
        val valueDeserializer = KafkaAvroDeserializer()
        valueSerializer.configure(serdeConfigs, false)
        valueDeserializer.configure(serdeConfigs, false)

        val producerConfigs = mapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ProducerConfig.CLIENT_ID_CONFIG to "ch04-s102-current",
            ProducerConfig.ACKS_CONFIG to "all",
            KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to registryUrl,
        )
        val consumerConfigs = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG to GROUP,
            ConsumerConfig.CLIENT_ID_CONFIG to "ch04-s102-current",
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG to registryUrl,
            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG to false,
        )

        @Suppress("UNCHECKED_CAST")
        val producer = KafkaProducer(
            producerConfigs,
            StringSerializer(),
            valueSerializer as Serializer<GenericRecord>,
        )
        @Suppress("UNCHECKED_CAST")
        val consumer = KafkaConsumer(
            consumerConfigs,
            StringDeserializer(),
            valueDeserializer as Deserializer<GenericRecord>,
        )
        producer.use { producer ->
            consumer.use { consumer ->
                consumer.subscribe(listOf(TOPIC))

                // region 4.10.2 에이브로 디시리얼라이저
                awaitAssignment(consumer)

                // Avro 1.12 허용 목록은 runS102Current의 JVM 인자다. main 안에서 setProperty하면 이미 늦다.
                val schema = Schema.Parser().parse(schemaString)
                val samples = listOf(
                    customer(schema, 0, "exampleCustomer0", "example0@example.com"),
                    customer(schema, 1, "exampleCustomer1", "example1@example.com"),
                )
                for (sample in samples) {
                    val name = sample.get("name").toString()
                    try {
                        val metadata = producer.send(ProducerRecord(TOPIC, name, sample)).get()
                        logger.info {
                            "sent topic=${metadata.topic()} partition=${metadata.partition()} " +
                                "offset=${metadata.offset()} name=$name"
                        }
                    } catch (e: Exception) {
                        logger.error(e) { "send failed" }
                    }
                }

                val deadline = System.currentTimeMillis() + 10_000
                var seen = 0
                while (seen < samples.size && System.currentTimeMillis() < deadline) {
                    val records = consumer.poll(Duration.ofMillis(100))
                    for (record in records) {
                        val value = record.value()
                        logger.info {
                            "value=$value partition=${record.partition()} offset=${record.offset()} " +
                                "id=${value?.get("id")} name=${value?.get("name")}"
                        }
                        seen++
                    }
                }
                if (seen < samples.size) {
                    logger.error { "poll timed out seen=$seen expected=${samples.size}" }
                }
                // endregion
            }
        }
    }

    private fun customer(schema: Schema, id: Int, name: String, email: String): GenericRecord =
        GenericData.Record(schema).apply {
            put("id", id)
            put("name", name)
            put("email", email)
        }

    private fun awaitAssignment(consumer: KafkaConsumer<String, GenericRecord>) {
        val deadline = System.currentTimeMillis() + 15_000
        while (consumer.assignment().isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(200))
        }
        if (consumer.assignment().isEmpty()) {
            logger.error { "assignment timed out group=$GROUP topic=$TOPIC" }
        }
    }
}
