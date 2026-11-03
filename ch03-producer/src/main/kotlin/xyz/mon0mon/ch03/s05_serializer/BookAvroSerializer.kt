package xyz.mon0mon.ch03.s05_serializer

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import xyz.mon0mon.common.KafkaSettings
import java.util.Properties

/**
 * Book: §3.5.3 Using Avro Records with Kafka
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  책: `KafkaAvroSerializer`가 레코드 스키마를 Schema Registry에 등록하고, 메시지에는 스키마 ID만 남긴다.
 * - 4.4  키는 `KafkaAvroSerializer`. 값은 `ReflectAvro4kKafkaSerializer`라서 `GenericRecord`와 생성된 `Customer`를 한 프로듀서가 보낸다.
 *
 * Still valid: `schema.registry.url`, `GenericRecord`
 */
object BookAvroSerializer {
    @JvmStatic
    fun main(args: Array<String>) {
        // region 3.5.3 카프카에서 에이브로 레코드 사용하기
        // 1.x / 책
        // props["bootstrap.servers"] = "localhost:9092"
        // props["key.serializer"] = "io.confluent.kafka.serializers.KafkaAvroSerializer"
        // props["value.serializer"] = "io.confluent.kafka.serializers.KafkaAvroSerializer"
        // props["schema.registry.url"] = url
        // 책은 url을 선언하지 않고, 바로 아래에서 http://localhost:8081로 다시 넣는다.

        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "io.confluent.kafka.serializers.KafkaAvroSerializer"
        // 4.4: 책이 값도 KafkaAvroSerializer. 생성된 Customer는 GenericRecord가 아니라서 avro4k serializer를 쓴다.
        // 단일 타입의 스키마만 쓸 경우 SpecificAvro4kKafkaSerializer를 사용
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "com.github.avrokotlin.avro4k.kafka.confluent.ReflectAvro4kKafkaSerializer"
        props[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = KafkaSettings.schemaRegistryUrl()

        val schemaString = """
            {"namespace": "customerManagement.avro", "type": "record", "name": "Customer",
             "fields": [
               {"name": "id", "type": "int"},
               {"name": "name", "type": "string"},
               {"name": "email", "type": ["null", "string"], "default": null}
             ]}
            """.trimIndent()
        // 4.4: 책이 email default를 문자열 "null"로 적는다. union의 첫 타입이 null이라 JSON null이어야 파싱된다.

        val producer = KafkaProducer<String, Any>(props)
        producer.use { producer ->
            val parser = Schema.Parser()
            val schema = parser.parse(schemaString)

            // 4.4: 책이 customers를 선언하지 않는다. 루프 상한만 로컬로 둔다.
            val customers = 1
            for (nCustomers in 0 until customers) {
                val name = "exampleCustomer$nCustomers"
                val email = "example$nCustomers@example.com"
                val customer = GenericData.Record(schema)
                customer.put("id", nCustomers)
                customer.put("name", name)
                customer.put("email", email)

                // GenericRecord만 사용할 경우 ProducerRecord<String, GenericRecord>로 사용
                val generic = ProducerRecord<String, Any>("customerContacts", name, customer)
                producer.send(generic)

                val generated = CustomerGenerator.getNext()
                // 단일 타입만 사용할 경우 ProducerRecord<String, Customer>로 사용
                val specific = ProducerRecord<String, Any>("customerContacts", generated.name, generated)
                producer.send(specific)
            }
        }
        // endregion
    }
}

