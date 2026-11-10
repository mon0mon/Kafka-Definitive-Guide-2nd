package xyz.mon0mon.ch04.s10_custom_deserializer

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
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
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.Properties

private val logger = KotlinLogging.logger {}

/**
 * Book: §4.10.2 Avro Deserialization
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  `KafkaAvroDeserializer`와 `schema.registry.url`. 기본은 `GenericRecord`.
 *        생성된 자바 `Customer`로 받으려면 `specific.avro.reader=true` (책 2판, 이 파일의 두 번째 스니펫).
 * - 3.0  `poll(long)` 삭제. `poll(Duration)`만 남았다.
 * - 4.4  디시리얼라이저 클래스와 `schema.registry.url`은 그대로다.
 *        이 리포의 `customerManagement.avro.Customer`는 avro4k 클래스라 `specific.avro.reader`는 켜지 않는다.
 *        Avro 1.12는 그 클래스를 로드하기 전에 패키지 허용 목록을 본다.
 *        `runS102Book`이 `-Dorg.apache.avro.SERIALIZABLE_PACKAGES=customerManagement.avro`를 넣는다 → [CurrentAvroDeserializer].
 *
 * Still valid: `KafkaAvroDeserializer`, `schema.registry.url`, `specific.avro.reader`, `GenericRecord`
 */
object BookAvroDeserializer {
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
        // 1.x / 책
        // props["bootstrap.servers"] = "broker1:9092,broker2:9092"
        // props["group.id"] = "CountryCounter"
        // props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        // props["value.deserializer"] = "io.confluent.kafka.serializers.KafkaAvroDeserializer"
        // props["schema.registry.url"] = schemaUrl

        val kafkaProps = Properties()
        kafkaProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        kafkaProps[ConsumerConfig.GROUP_ID_CONFIG] = GROUP
        kafkaProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringDeserializer"
        kafkaProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
            "io.confluent.kafka.serializers.KafkaAvroDeserializer"
        kafkaProps[KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = KafkaSettings.schemaRegistryUrl()
        // 4.4: 책은 생성 Customer를 쓰려면 "specific.avro.reader" = "true".
        // 이 리포의 Customer는 avro4k 데이터 클래스라 SpecificRecord가 아니다. GenericRecord로 읽는다.
        kafkaProps[KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG] = false
        val consumer = KafkaConsumer<String, GenericRecord>(kafkaProps)

        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        // 4.4: 3.5의 키는 KafkaAvroSerializer다. 이 절의 키 디시리얼라이저는 StringDeserializer라서 키는 문자열로 보낸다.
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "io.confluent.kafka.serializers.KafkaAvroSerializer"
        producerProps[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = KafkaSettings.schemaRegistryUrl()
        val producer = KafkaProducer<String, GenericRecord>(producerProps)

        try {
            consumer.subscribe(listOf(TOPIC))

            // region 4.10.2 에이브로 디시리얼라이저
            // 4.4: 책은 while (true)로 이미 있는 레코드를 읽는다.
            // 지금은 그룹 가입으로 파티션을 받은 뒤 두 건을 보내고, 그 레코드를 로그한 다음 끝낸다.
            awaitAssignment(consumer)

            // 4.4: Avro 1.12는 이 풀네임 클래스가 클래스패스에 있으면 허용 목록을 본다.
            // runS102Book의 JVM 인자다. main 안에서 setProperty하면 이미 늦다.
            val schema = Schema.Parser().parse(schemaString)
            val samples = listOf(
                customer(schema, 0, "exampleCustomer0", "example0@example.com"),
                customer(schema, 1, "exampleCustomer1", "example1@example.com"),
            )
            for (sample in samples) {
                val name = sample.get("name").toString()
                try {
                    producer.send(ProducerRecord(TOPIC, name, sample)).get()
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
                    // 4.4: 책이 printf로 value, partition, offset을 출력. 지금은 KotlinLogging.
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
        } finally {
            producer.close()
            consumer.close()
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
