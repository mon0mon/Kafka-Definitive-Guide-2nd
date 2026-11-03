@file:OptIn(ExperimentalAvro4kApi::class)

package xyz.mon0mon.ch03.s05_serializer

import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.ExperimentalAvro4kApi
import com.github.avrokotlin.avro4k.kafka.confluent.ReflectAvro4kKafkaSerializer
import com.github.avrokotlin.avro4k.schema
import customerManagement.avro.Customer
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serializer
import xyz.mon0mon.common.KafkaSettings
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.inputStream

private val logger = KotlinLogging.logger {}

/**
 * Book: §3.5.3 Using Avro Records with Kafka
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.x  스키마 리터럴을 파싱해 `GenericRecord`를 만들고, `KafkaAvroSerializer`가 레지스트리에 등록한다 ([BookAvroSerializer])
 * - 4.4  `Customer.avsc`를 읽고 `CachedSchemaRegistryClient`로 등록한 뒤, 같은 subject의 최신 스키마로 레코드를 만든다.
 *        프로듀서는 `auto.register.schemas=false`라서 조회만 한다.
 *        값은 `ReflectAvro4kKafkaSerializer`. `GenericRecord`와 생성된 `Customer`를 한 프로듀서가 보낸다.
 *
 * Replaces: [BookAvroSerializer]
 * Still valid: `schema.registry.url`, `GenericRecord`
 */
object CurrentAvroSerializer {
    private const val TOPIC = "customerContacts"
    private const val KEY_SUBJECT = "$TOPIC-key"
    private const val VALUE_SUBJECT = "$TOPIC-value"

    @JvmStatic
    fun main(args: Array<String>) {
        // region 3.5.3 카프카에서 에이브로 레코드 사용하기
        val registryUrl = KafkaSettings.schemaRegistryUrl()
        // identityMapCapacity는 스키마 ID 캐시 크기다.
        CachedSchemaRegistryClient(registryUrl, 100).use { registry ->
            // region 스키마 파일에서 불러오기
            val schemaFile = customerAvsc()
            val schemaFromFile = schemaFile.inputStream().use { Schema.Parser().parse(it) }
            logger.info {
                "file=${schemaFile.toAbsolutePath()} fullName=${schemaFromFile.fullName} " +
                    "fields=${schemaFromFile.fields.joinToString { it.name() }}"
            }
            // endregion

            // region Schema Registry에 스키마 등록하기
            // 키 시리얼라이저도 customerContacts-key의 string 스키마를 조회한다.
            val keyId = registry.register(KEY_SUBJECT, AvroSchema(Schema.create(Schema.Type.STRING)))
            val valueId = registry.register(VALUE_SUBJECT, AvroSchema(schemaFromFile))
            // 생성된 Customer의 스키마가 파일과 다르면 값 subject에 한 버전 더 올라간다.
            val generatedSchema = Avro.schema<Customer>()
            val generatedId = registry.register(VALUE_SUBJECT, AvroSchema(generatedSchema))
            logger.info {
                "registered keySubject=$KEY_SUBJECT id=$keyId " +
                    "valueSubject=$VALUE_SUBJECT fileId=$valueId generatedId=$generatedId " +
                    "fileMatchesGenerated=${schemaFromFile == generatedSchema}"
            }
            // endregion

            // region Schema Registry에서 스키마 불러오기
            val latest = registry.getLatestSchemaMetadata(VALUE_SUBJECT)
            val schemaFromRegistry = Schema.Parser().parse(latest.schema)
            logger.info {
                "loaded subject=$VALUE_SUBJECT id=${latest.id} version=${latest.version} schema=${latest.schema}"
            }
            // endregion

            val configs = mapOf<String, Any>(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
                ProducerConfig.CLIENT_ID_CONFIG to "ch03-s05-current",
                ProducerConfig.ACKS_CONFIG to "all",
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG to registryUrl,
                // 위에서 등록했으므로 시리얼라이저는 subject에 있는 스키마 ID만 찾는다.
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS to false,
            )
            // 생성자에 시리얼라이저 인스턴스를 넘기면 Kafka가 configure를 호출하지 않는다.
            // KafkaAvroSerializer의 타입 인자가 Any?라서 키 타입 String에는 캐스트가 필요하다.
            val keySerializer = KafkaAvroSerializer()
            val valueSerializer = ReflectAvro4kKafkaSerializer()
            keySerializer.configure(configs, true)
            valueSerializer.configure(configs, false)
            @Suppress("UNCHECKED_CAST")
            val producer = KafkaProducer(
                configs,
                keySerializer as Serializer<String>,
                valueSerializer as Serializer<Any>,
            )
            producer.use { producer ->
                val name = "exampleCustomer0"
                val customer = GenericData.Record(schemaFromRegistry)
                customer.put("id", 0)
                customer.put("name", name)
                customer.put("email", "example0@example.com")

                val genericMetadata = producer.send(ProducerRecord(TOPIC, name, customer)).get()
                logger.info {
                    "generic topic=${genericMetadata.topic()} partition=${genericMetadata.partition()} " +
                        "offset=${genericMetadata.offset()}"
                }

                val generated = CustomerGenerator.getNext()
                val specificMetadata = producer.send(ProducerRecord(TOPIC, generated.name, generated)).get()
                logger.info {
                    "generated topic=${specificMetadata.topic()} partition=${specificMetadata.partition()} " +
                        "offset=${specificMetadata.offset()}"
                }
            }
        }
        // endregion
    }

    /**
     * `src/main/avro`는 컴파일 입력이라 런타임 클래스패스에 없다.
     * Gradle `run`의 작업 디렉터리는 모듈이고, IntelliJ는 저장소 루트인 경우가 있다.
     */
    private fun customerAvsc(): Path {
        val candidates = listOf(
            Path.of("src/main/avro/Customer.avsc"),
            Path.of("ch03-producer/src/main/avro/Customer.avsc"),
        )
        return candidates.firstOrNull { it.exists() }
            ?: error("Customer.avsc를 찾지 못했다. cwd=${Path.of("").toAbsolutePath()}")
    }
}
