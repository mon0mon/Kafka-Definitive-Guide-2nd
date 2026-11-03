# Schema Registry

§3.5.3 예제에서 스키마를 어디에 두고, 메시지에는 무엇을 남기는지 정리한 노트다. 실행 코드는 같은 디렉터리의 `CurrentAvroSerializer`다. Avro 레코드를 JVM에서 어떤 타입으로 들고 있는지는 [avro-serializer-kotlin.md](avro-serializer-kotlin.md)를 본다.

Schema Registry는 스키마 본문을 저장하고 ID를 발급하는 서버다. 이 저장소의 싱글 스택은 `confluentinc/cp-schema-registry:8.4.0`이고 주소는 `http://localhost:8081`이다. 클러스터 스택은 `http://localhost:18081`이다. 브로커가 아니다. 브로커는 레코드 바이트만 보관한다.

## 메시지에 들어가는 것

Confluent Avro 메시지 값은 세 조각이다.

1. 매직 바이트 `0x00`
2. 스키마 ID 4바이트
3. Avro 바이너리 본문

스키마 JSON은 메시지마다 반복되지 않는다. 컨슈머는 ID로 레지스트리에 본문을 물어 본문을 디코드한다. 같은 ID는 클라이언트 안에 캐시된다.

프로듀서가 ID를 얻는 방법은 두 가지다.

- 시리얼라이저가 레코드의 writer schema를 subject에 등록하고, 돌아온 ID를 붙인다. `auto.register.schemas` 기본값은 `true`다. `BookAvroSerializer`가 이 경로다.
- 애플리케이션이 먼저 등록하고, 시리얼라이저는 조회만 한다. `CurrentAvroSerializer`는 `CachedSchemaRegistryClient.register` 뒤에 `auto.register.schemas=false`를 둔다.

`use.latest.version=true`는 또 다른 설정이다. 시리얼라이저가 레코드에 붙어 있는 스키마 대신 subject의 최신 버전을 writer schema로 쓴다. `CurrentAvroSerializer`의 `getLatestSchemaMetadata`는 그 설정이 아니다. 애플리케이션이 최신 본문을 받아 `GenericData.Record`를 만들려고 호출한다.

## subject, version, id

스키마는 subject 아래에 버전으로 쌓인다. ID는 레지스트리 전체에서 스키마 본문마다 하나다. 같은 본문을 다른 subject에 등록하면 버전 번호는 subject마다 다르고 ID는 같다.

기본 이름 전략은 토픽 기준이다. `TopicNameStrategy`에서 값 subject는 `{topic}-value`, 키 subject는 `{topic}-key`다. 이 예제의 토픽 `customerContacts`는 다음 둘을 쓴다.

| subject | 스키마 |
|---------|--------|
| `customerContacts-key` | Avro `string`. 키 시리얼라이저가 고객 이름을 이 스키마로 보낸다. |
| `customerContacts-value` | `customerManagement.avro.Customer` |

`schema.registry.url`은 `KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG`다. 예제는 `KafkaSettings.schemaRegistryUrl()`로 읽고, Gradle `runS05Book` / `runS05Current`가 `-Dkafka.schema.registry.url`을 넣는다.

## 파일에서 읽기

`ch03-producer/src/main/avro/Customer.avsc`가 스키마의 기준 파일이다. `io.github.avro-kotlin`이 이 파일로 `customerManagement.avro.Customer`를 만든다. `src/main/avro`는 컴파일 입력이라 런타임 클래스패스에 실리지 않는다. `CurrentAvroSerializer`는 파일을 직접 연다.

```kotlin
val schemaFromFile = schemaFile.inputStream().use { Schema.Parser().parse(it) }
```

Gradle `run`의 작업 디렉터리는 `ch03-producer`다. IntelliJ에서 저장소 루트를 작업 디렉터리로 두면 `ch03-producer/src/main/avro/Customer.avsc`를 찾는다.

이 스키마의 풀네임은 `customerManagement.avro.Customer`다. Avro 1.12는 그 이름의 클래스를 로드하기 전에 `org.apache.avro.SERIALIZABLE_PACKAGES`를 본다. `runS05Book`과 `runS05Current`가 다음을 JVM 인자로 넣는다.

```text
-Dorg.apache.avro.SERIALIZABLE_PACKAGES=customerManagement.avro
```

IntelliJ에서 `main`을 직접 실행하면 이 인자가 없다. VM options에 같은 값을 넣어야 한다.

## 레지스트리에 등록하기

등록 클라이언트는 `io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient`다. `kafka-avro-serializer`가 이 클라이언트를 함께 가져온다. 두 번째 인자는 스키마 ID 캐시 크기다.

```kotlin
CachedSchemaRegistryClient(registryUrl, 100).use { registry ->
    val keyId = registry.register(KEY_SUBJECT, AvroSchema(Schema.create(Schema.Type.STRING)))
    val valueId = registry.register(VALUE_SUBJECT, AvroSchema(schemaFromFile))
    val generatedId = registry.register(VALUE_SUBJECT, AvroSchema(Avro.schema<Customer>()))
}
```

`register`는 그 subject에 같은 본문이 있으면 새 버전을 만들지 않고 기존 ID를 돌려준다. 다른 본문이면 호환성 검사를 통과한 다음 새 버전과 ID를 만든다. 이 레지스트리의 기본 호환성은 `BACKWARD`다. 새 스키마로 예전 데이터를 읽을 수 있어야 한다. 검사 예는 `SchemaCompatibilityTest`에 있다.

생성된 `Customer`의 스키마도 값 subject에 등록한다. `ReflectAvro4kKafkaSerializer`는 `GenericRecord`면 레코드에 붙은 스키마를, `Customer`면 `Avro.schema<Customer>()`를 writer schema로 쓰기 때문이다. 둘은 `Customer.avsc`와 같게 생성해 두었다. 실행 로그의 `fileMatchesGenerated`가 그 비교다.

키 스키마를 같이 등록하는 이유는 값만 등록하면 부족하기 때문이다. 키 시리얼라이저 `KafkaAvroSerializer`도 `auto.register.schemas=false`이면 `customerContacts-key`에서 `string`을 찾는다.

## 레지스트리에서 불러오기

`getLatestSchemaMetadata(subject)`가 최신 버전의 ID, 버전 번호, 스키마 JSON을 돌려준다. subject가 없으면 404다. 첫 실행에서 조회만 하면 실패하므로, 이 예제는 파일로 읽은 스키마를 먼저 등록한다.

```kotlin
val latest = registry.getLatestSchemaMetadata(VALUE_SUBJECT)
val schemaFromRegistry = Schema.Parser().parse(latest.schema)
val customer = GenericData.Record(schemaFromRegistry)
```

그다음 `GenericData.Record`와 `CustomerGenerator.getNext()`를 토픽 `customerContacts`로 보낸다. `send(...).get()`으로 브로커 응답을 기다린다. 시리얼라이저는 레코드의 스키마 본문으로 subject를 조회해 ID를 붙인다. 본문이 없으면 전송이 실패한다.

`KafkaProducer` 생성자에 시리얼라이저 인스턴스를 넘기면 Kafka 4.4는 `configure`를 호출하지 않는다. `CurrentAvroSerializer`는 `schema.registry.url`이 들어 있는 설정으로 `configure(configs, isKey)`를 직접 호출한다. 클래스 이름을 설정에 넣는 `BookAvroSerializer`는 Kafka가 시리얼라이저를 만들면서 `configure`까지 한다.

## REST로 확인하기

호스트에서 레지스트리를 바로 볼 수 있다. 싱글 스택 기준이다.

```bat
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/customerContacts-value/versions/latest
```

최신 응답의 `id`가 예제 로그의 `loaded ... id=`와 같다. Kafka UI는 http://localhost:9080 이고 Schema Registry는 `schema-registry:8081`로 붙어 있다.

## 실행

싱글 브로커와 레지스트리를 올리고 Current 예제를 실행한다.

```bat
gradlew.bat :ch03-producer:runS05Current
```

책의 리터럴 스키마 경로는 다음이다.

```bat
gradlew.bat :ch03-producer:runS05Book
```

이미 올라간 `customerContacts-value`와 호환되지 않는 스키마가 있으면 `register`가 거절한다. 학습 중 스키마를 바꿔 막혔다면 subject를 지운 뒤 다시 등록한다. 한 번은 소프트 삭제이고, `permanent=true`가 본문까지 지운다.

```bat
curl -X DELETE http://localhost:8081/subjects/customerContacts-value
curl -X DELETE "http://localhost:8081/subjects/customerContacts-value?permanent=true"
```
