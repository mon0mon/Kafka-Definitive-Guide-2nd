@file:OptIn(ExperimentalAvro4kApi::class)

package xyz.mon0mon.ch03.s05_serializer

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.kafka.confluent.ReflectAvro4kKafkaDeserializer
import com.github.avrokotlin.avro4k.kafka.confluent.ReflectAvro4kKafkaSerializer
import com.github.avrokotlin.avro4k.kafka.confluent.SpecificAvro4kKafkaDeserializer
import com.github.avrokotlin.avro4k.kafka.confluent.SpecificAvro4kKafkaSerializer
import com.github.avrokotlin.avro4k.schema
import customerManagement.avro.Customer
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.junit.jupiter.api.Test
import study.specific.Customer as SpecificCustomer
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * GenericRecord, 자바 SpecificRecord, avro4k 데이터 클래스가 같은 Confluent 와이어에서
 * 어떤 시리얼라이저와 디시리얼라이저를 타는지 비교한다.
 *
 * 브로커는 쓰지 않는다. [MockSchemaRegistryClient]가 스키마 ID를 발급한다.
 * 설명은 [avro-record-types.md](../../../../../../main/kotlin/xyz/mon0mon/ch03/s05_serializer/avro-record-types.md).
 */
class AvroRecordTypeSerdeTest {
    @Test
    fun `GenericRecord는 KafkaAvroSerializer와 KafkaAvroDeserializer로 왕복한다`() {
        val (client, props) = registry()
        val serializer = KafkaAvroSerializer(client, props)
        val deserializer = confluentDeserializer(client, specificReader = false)
        val record = GenericData.Record(studyGenericSchema())
        record.put("id", 1)
        record.put("name", "ada")
        record.put("email", "ada@example.com")

        val bytes = requireNotNull(serializer.serialize(GENERIC_TOPIC, record))

        assertConfluentHeader(bytes)
        val restored = deserializer.deserialize(GENERIC_TOPIC, bytes)
        val generic = assertIs<GenericRecord>(restored)
        assertFalse(restored is SpecificRecord)
        assertEquals(1, generic.get("id"))
        assertEquals("ada", generic.get("name").toString())
        assertEquals("ada@example.com", generic.get("email").toString())
    }

    @Test
    fun `SpecificRecord는 specific avro reader가 켜져 있을 때만 생성 클래스로 돌아온다`() {
        val (client, props) = registry()
        val serializer = KafkaAvroSerializer(client, props)
        val original = SpecificCustomer(1, "ada", "ada@example.com")
        val bytes = requireNotNull(serializer.serialize(SPECIFIC_TOPIC, original))
        assertConfluentHeader(bytes)

        val specific = confluentDeserializer(client, specificReader = true)
            .deserialize(SPECIFIC_TOPIC, bytes)
        val customer = assertIs<SpecificCustomer>(specific)
        assertEquals(1, customer.getId())
        assertEquals("ada", customer.getName())
        assertEquals("ada@example.com", customer.getEmail())

        val generic = confluentDeserializer(client, specificReader = false)
            .deserialize(SPECIFIC_TOPIC, bytes)
        assertIs<GenericRecord>(generic)
        assertFalse(generic is SpecificCustomer)
        assertEquals("ada", generic.get("name").toString())
    }

    @Test
    fun `avro4k Customer는 SpecificAvro4kDeserializer로 돌아오고 KafkaAvroDeserializer는 GenericRecord로 읽는다`() {
        val (client, props) = registry()
        val serializer = SpecificAvro4kKafkaSerializer<Customer>(
            isKey = false,
            props = props,
            schemaRegistry = client,
        )
        val original = Customer(1, "ada", "ada@example.com")
        val bytes = requireNotNull(serializer.serialize(AVRO4K_TOPIC, original))
        assertConfluentHeader(bytes)

        val restored = assertIs<Customer>(
            SpecificAvro4kKafkaDeserializer<Customer>(
                isKey = false,
                props = props,
                schemaRegistry = client,
            ).deserialize(AVRO4K_TOPIC, bytes),
        )
        assertEquals(original, restored)

        val generic = assertIs<GenericRecord>(
            confluentDeserializer(client, specificReader = false).deserialize(AVRO4K_TOPIC, bytes),
        )
        assertEquals(GenericData.Record::class, generic::class)
        assertEquals(1, generic.get("id"))
        assertEquals("ada", generic.get("name").toString())
        assertEquals("ada@example.com", generic.get("email").toString())
    }

    @Test
    fun `KafkaAvroSerializer는 avro4k Customer를 받지 않는다`() {
        val (client, props) = registry()
        val serializer = KafkaAvroSerializer(client, props)

        assertFails {
            serializer.serialize(AVRO4K_TOPIC, Customer(1, "ada", "ada@example.com"))
        }
    }

    @Test
    fun `ReflectAvro4kKafkaDeserializer는 스키마 풀네임으로 Customer를 찾는다`() {
        val (client, props) = registry()
        val serializer = ReflectAvro4kKafkaSerializer(
            isKey = false,
            props = props,
            schemaRegistry = client,
        )
        val schema = Avro.schema<Customer>()
        val generic = GenericData.Record(schema)
        generic.put("id", 1)
        generic.put("name", "ada")
        generic.put("email", "ada@example.com")
        val dataClass = Customer(2, "grace", "grace@example.com")

        val genericBytes = requireNotNull(serializer.serialize(REFLECT_TOPIC, generic))
        val dataClassBytes = requireNotNull(serializer.serialize(REFLECT_TOPIC, dataClass))
        assertConfluentHeader(genericBytes)
        assertConfluentHeader(dataClassBytes)

        val deserializer = ReflectAvro4kKafkaDeserializer(
            isKey = false,
            props = props,
            schemaRegistry = client,
        )
        assertEquals(
            Customer(1, "ada", "ada@example.com"),
            assertIs<Customer>(deserializer.deserialize(REFLECT_TOPIC, genericBytes)),
        )
        assertEquals(dataClass, assertIs<Customer>(deserializer.deserialize(REFLECT_TOPIC, dataClassBytes)))

        val asGeneric = assertIs<GenericRecord>(
            confluentDeserializer(client, specificReader = false).deserialize(REFLECT_TOPIC, dataClassBytes),
        )
        assertEquals(GenericData.Record::class, asGeneric::class)
        assertEquals("grace", asGeneric.get("name").toString())
    }

    private fun registry(): Pair<SchemaRegistryClient, Map<String, Any>> {
        val props = mapOf<String, Any>(
            KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to MOCK_URL,
            KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS to true,
        )
        return MockSchemaRegistryClient() to props
    }

    private fun confluentDeserializer(
        client: SchemaRegistryClient,
        specificReader: Boolean,
    ): KafkaAvroDeserializer {
        val deserializer = KafkaAvroDeserializer(client)
        deserializer.configure(
            mapOf(
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG to MOCK_URL,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG to specificReader,
            ),
            false,
        )
        return deserializer
    }

    private fun studyGenericSchema() = org.apache.avro.Schema.Parser().parse(
        """
        {"namespace":"study.generic","type":"record","name":"Customer",
         "fields":[
           {"name":"id","type":"int"},
           {"name":"name","type":"string"},
           {"name":"email","type":["null","string"],"default":null}
         ]}
        """.trimIndent(),
    )

    private fun assertConfluentHeader(bytes: ByteArray) {
        assertTrue(bytes.size > 5, "wire bytes=$bytes")
        assertEquals(0, bytes[0].toInt())
    }

    private companion object {
        private const val MOCK_URL = "mock://avro-record-types"
        private const val GENERIC_TOPIC = "study-generic"
        private const val SPECIFIC_TOPIC = "study-specific"
        private const val AVRO4K_TOPIC = "study-avro4k"
        private const val REFLECT_TOPIC = "study-reflect"
    }
}
