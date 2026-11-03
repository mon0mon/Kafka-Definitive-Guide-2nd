package xyz.mon0mon.ch03.s05_serializer

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.Properties
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * 스키마가 바뀔 때 Schema Registry의 호환성 검사가 거절·허용하는 경우와,
 * 허용된 변경으로 예전 바이트를 새 스키마로 읽으면 기본값이 채워지는 경우를 본다.
 *
 * 네임스페이스는 `evolution.avro`다. `customerManagement.avro.Customer`가 클래스패스에 있으면
 * Avro 1.12가 그 클래스를 로드하려 한다.
 *
 * 이미지는 Gradle이 `kafka.image`, `schema.registry.image`로 넘긴다. catalog의 kafka, confluent 버전과 같다.
 */
class SchemaCompatibilityTest {
    @Test
    fun `BACKWARD는 기본값이 있는 필드 추가를 허용한다`() {
        val client = registryClient()
        val subject = "backward-add-default"
        client.register(subject, AvroSchema(customer(ID, NAME)))

        val errors = client.testCompatibilityVerbose(subject, AvroSchema(customer(ID, NAME, PHONE_WITH_DEFAULT)))

        assertTrue(errors.isEmpty(), errors.joinToString())
    }

    @Test
    fun `BACKWARD는 기본값이 없는 필드 추가를 거절한다`() {
        val client = registryClient()
        val subject = "backward-add-required"
        client.register(subject, AvroSchema(customer(ID, NAME)))

        val candidate = AvroSchema(customer(ID, NAME, EMAIL_REQUIRED))
        assertFalse(client.testCompatibility(subject, candidate))
        assertTrue(client.testCompatibilityVerbose(subject, candidate).isNotEmpty())
    }

    @Test
    fun `FORWARD는 기본값이 없는 필드 삭제를 거절한다`() {
        val client = registryClient()
        val subject = "forward-delete-required"
        client.register(subject, AvroSchema(customer(ID, NAME)))
        client.updateCompatibility(subject, "FORWARD")

        val candidate = AvroSchema(customer(ID))
        assertFalse(client.testCompatibility(subject, candidate))
        assertTrue(client.testCompatibilityVerbose(subject, candidate).isNotEmpty())
    }

    @Test
    fun `기본값이 있는 필드를 추가하면 예전 레코드를 새 스키마로 읽는다`() {
        val topic = "evolution-read"
        val subject = "$topic-value"
        val writer = customer(ID, NAME)
        send(topic, writer, id = 7, name = "ada")

        val client = registryClient()
        client.register(subject, AvroSchema(customer(ID, NAME, PHONE_WITH_DEFAULT)))

        val consumerProps = Properties()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java.name
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = "evolution-read"
        consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        consumerProps["schema.registry.url"] = registryUrl()
        // 메시지에 들어 있는 ID의 스키마가 writer다. 최신 스키마를 reader로 쓰면 phone 기본값이 채워진다.
        consumerProps["use.latest.version"] = true

        KafkaConsumer<String, GenericRecord>(consumerProps).use { consumer ->
            consumer.subscribe(listOf(topic))
            val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
            while (System.nanoTime() < deadline) {
                val records = consumer.poll(Duration.ofMillis(500))
                if (records.isEmpty) {
                    continue
                }
                val value = records.first().value()
                assertEquals(7, value.get("id"))
                assertEquals("ada", value.get("name").toString())
                assertNull(value.get("phone"))
                assertTrue(value.schema.getField("phone") != null)
                return
            }
        }
        fail("evolution-read에서 레코드를 받지 못했다")
    }

    private fun send(topic: String, schema: Schema, id: Int, name: String) {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafka.bootstrapServers
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = KafkaAvroSerializer::class.java.name
        props[ProducerConfig.ACKS_CONFIG] = "all"
        props["schema.registry.url"] = registryUrl()

        val record = GenericData.Record(schema)
        record.put("id", id)
        record.put("name", name)
        KafkaProducer<String, GenericRecord>(props).use { producer ->
            producer.send(ProducerRecord(topic, name, record)).get()
        }
    }

    companion object {
        private const val ID = """{"name":"id","type":"int"}"""
        private const val NAME = """{"name":"name","type":"string"}"""
        private const val PHONE_WITH_DEFAULT =
            """{"name":"phone","type":["null","string"],"default":null}"""
        private const val EMAIL_REQUIRED = """{"name":"email","type":"string"}"""

        private val network: Network = Network.newNetwork()
        private val kafka: KafkaContainer = KafkaContainer(
            DockerImageName.parse(System.getProperty("kafka.image")),
        ).withNetwork(network).withListener("kafka:19092")
        private val schemaRegistry: GenericContainer<*> = GenericContainer(
            DockerImageName.parse(System.getProperty("schema.registry.image")),
        ).withNetwork(network)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:19092")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
            .dependsOn(kafka)
            .waitingFor(Wait.forHttp("/subjects").forPort(8081).forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(4))

        @BeforeAll
        @JvmStatic
        fun startCluster() {
            schemaRegistry.start()
        }

        @AfterAll
        @JvmStatic
        fun stopCluster() {
            schemaRegistry.stop()
            kafka.stop()
            network.close()
        }

        private fun registryUrl(): String =
            "http://${schemaRegistry.host}:${schemaRegistry.getMappedPort(8081)}"

        private fun registryClient(): CachedSchemaRegistryClient =
            CachedSchemaRegistryClient(registryUrl(), 16)

        private fun customer(vararg fields: String): Schema =
            Schema.Parser().parse(
                """
                {"type":"record","name":"Customer","namespace":"evolution.avro","fields":[${fields.joinToString(",")}]}
                """.trimIndent(),
            )
    }
}
