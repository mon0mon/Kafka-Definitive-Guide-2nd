# Avro 레코드 세 가지와 Kafka 시리얼라이저

Kafka는 값 바이트만 저장한다. Avro와 Schema Registry는 `kafka-clients`에 없고, 그 바이트를 만들고 읽는 라이브러리가 따로 있다. 이 문서는 그 바이트를 JVM에서 어떤 객체로 들고 있느냐에 따라 시리얼라이저가 어떻게 갈리는지 정리한다.

비교 테스트는 [AvroRecordTypeSerdeTest.kt](../../../../../../test/kotlin/xyz/mon0mon/ch03/s05_serializer/AvroRecordTypeSerdeTest.kt)다. 브로커 없이 `MockSchemaRegistryClient`로 직렬화한 뒤 바로 역직렬화한다. 자바 `SpecificRecord` 샘플은 [Customer.kt](../../../../../../test/kotlin/study/specific/Customer.kt)다. 플러그인, 스키마 진화, 자바 서비스와 같이 쓰는 법은 [avro-serializer-kotlin.md](avro-serializer-kotlin.md)를 본다.

## 와이어는 하나다

Schema Registry를 쓰는 Confluent Avro 메시지는 다음 세 조각이다.

| 바이트 | 내용 |
|--------|------|
| 0 | 매직 바이트 `0x00` |
| 1–4 | 스키마 ID |
| 5… | Avro 바이너리 본문 |

스키마 전문은 메시지에 들어가지 않는다. 시리얼라이저가 레지스트리에 스키마를 등록하거나 이미 있는 ID를 찾고, ID만 붙인다. 컨슈머는 ID로 작성자 스키마를 가져와 본문을 읽는다. 그래서 스키마 진화와 작은 페이로드가 같이 성립한다. 브로커는 이 값이 Avro인지 모른다.

기본 subject는 토픽 기준이다. 값은 `{topic}-value`, 키는 `{topic}-key`다.

## 세 가지 JVM 타입

스키마의 풀네임은 `{namespace}.{name}`이다. 이 저장소의 `Customer.avsc`는 `customerManagement.avro.Customer`다. 아래 셋은 그 스키마를 담는 방식이 다르다.

### GenericRecord

실행 중에 `Schema`를 파싱해서 `GenericData.Record`를 만든다. 필드는 `put("name", "ada")`, `get("name")`처럼 문자열로 다룬다. 오타는 컴파일되지 않고 실행할 때 난다. 스키마를 미리 클래스로 만들지 않는 수집기, 라우터, 책의 3.5.3·4.10.2가 이 경로다.

`GenericRecord`는 Avro의 `IndexedRecord`다. Confluent `KafkaAvroSerializer`가 받는다. `KafkaAvroDeserializer`의 기본 결과도 이 타입이다.

### 자바 SpecificRecord

`.avsc`를 자바 Avro 컴파일러에 넣으면 `SpecificRecord`를 구현한 클래스가 생긴다. 스키마는 클래스의 `SCHEMA$`에 있고, 필드는 `getName()` 같은 메서드로 읽는다. 이 클래스도 `IndexedRecord`라서 `KafkaAvroSerializer`가 받는다. `GenericRecord`와 자바 `SpecificRecord`는 시리얼라이저 하나로 같이 보낼 수 있다.

이 저장소는 그 컴파일러를 쓰지 않는다. 비교 테스트만 `study.specific.Customer`를 손으로 구현해 두었다. 풀네임을 `customerManagement.avro.Customer`로 두면 avro4k 생성 클래스와 충돌한다.

### avro4k 데이터 클래스

`CustomerGenerator.getNext()`가 돌려주는 `customerManagement.avro.Customer`가 여기 해당한다. `.avsc`로 만들지만 `@Serializable` 데이터 클래스이고 `SpecificRecord`가 아니다. `customer.name`처럼 프로퍼티로 접근한다. `KafkaAvroSerializer`는 이 객체를 바이트로 바꾸는 방법을 모른다.

## 시리얼라이저를 고르는 기준

기준은 언어가 아니라, 애플리케이션이 레코드를 어떤 객체로 들고 싶은가이다.

| 들고 있는 객체 | 보내기 | 같은 타입으로 읽기 |
|----------------|--------|--------------------|
| `GenericRecord`만 | `KafkaAvroSerializer` | `KafkaAvroDeserializer`. `specific.avro.reader`는 기본값 `false` |
| 자바 `SpecificRecord` | `KafkaAvroSerializer` | `KafkaAvroDeserializer`와 `specific.avro.reader=true` |
| avro4k 클래스 하나 | `SpecificAvro4kKafkaSerializer<Customer>` | `SpecificAvro4kKafkaDeserializer<Customer>` |
| `GenericRecord`와 avro4k 클래스를 한 시리얼라이저에 섞음 | `ReflectAvro4kKafkaSerializer` | `ReflectAvro4kKafkaDeserializer`. 스키마 풀네임과 같은 클래스가 있으면 그 클래스, 없으면 `GenericRecord` |

`KafkaAvroSerializer`는 컴파일 시점에 고정된 타입만 받지 않는다. 실행 시점에 넘어온 객체가 `GenericRecord`, 자바 `SpecificRecord`, Avro 기본 타입이면 보낸다. ch04 `CurrentAvroDeserializer`가 `GenericData.Record`를 이 클래스로 보내는 이유다.

`ReflectAvro4kKafkaSerializer`도 실행 시점에 객체를 본다. 위 두 타입에 더해서 avro4k 데이터 클래스를 받는다. 나가는 바이트는 Confluent 와이어와 같다. ch03 `CurrentAvroSerializer`는 한 프로듀서에서 `GenericData.Record(schemaFromRegistry)`와 `CustomerGenerator.getNext()`를 같이 보내므로 이 시리얼라이저를 쓴다. `CustomerGenerator`의 결과가 `SpecificRecord`여서가 아니다.

`SpecificAvro4kKafkaSerializer<Customer>`는 값 타입이 컴파일 시점에 하나일 때 쓴다. 클래스 이름 문자열만으로 Kafka가 만들 수 없어서, 코드에서 객체를 만든다.

Kotlin이라고 avro4k를 기본으로 두지 않는다. 자바 서비스와 `SpecificRecord`를 공유하면 Confluent 쌍이 갈래가 적다. 레코드가 avro4k 데이터 클래스일 때만 avro4k 시리얼라이저가 필요하다. `GenericRecord`로 읽으면 컨슈머는 `KafkaAvroDeserializer`로 충분하다.

## `specific.avro.reader`

이 설정은 `KafkaAvroDeserializer`만 본다. 시리얼라이저는 레코드를 반환하지 않고, 이 값도 읽지 않는다. 키는 `KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG`이고 문자열 이름은 `specific.avro.reader`다. 기본값은 `false`다.

디시리얼라이저는 스키마 ID로 스키마를 찾은 다음 객체를 만든다.

- `false`이면 결과는 `GenericRecord`다. 클래스패스에 생성 클래스가 있어도 `record.get("name")`으로 읽는다. ch04가 이 값이다.
- `true`이면 스키마 풀네임과 같은 자바 `SpecificRecord`를 찾아 그 인스턴스를 만든다. `customer.getName()`으로 읽는다. 바이트 형식은 바뀌지 않고, 돌려주는 JVM 타입만 바뀐다.

avro4k `Customer`는 `SpecificRecord`가 아니라서 이 플래그의 대상이 아니다. Kotlin 클래스로 다시 받는 짝으로 avro4k는 `ReflectAvro4kKafkaDeserializer`와 `SpecificAvro4kKafkaDeserializer`를 제공한다. ch03 프로듀서는 소비를 하지 않으므로 이 옵션을 넣지 않는다.

`SpecificAvro4kKafkaDeserializer<Customer>`는 컴파일 시점에 정한 그 클래스만 만든다. `ReflectAvro4kKafkaDeserializer`는 스키마 풀네임으로 클래스를 찾는다. `customerManagement.avro.Customer`가 클래스패스에 있으면 `Customer`가 되고, 없으면 `GenericRecord`다. 둘 다 Confluent 와이어를 읽는다. avro4k 2.13은 Confluent 8.4가 호출하는 `getDatumReader(SchemaId, Schema, Schema)`를 덮는다. 2.10은 `getDatumReader(Schema, Schema)`만 덮어서 이 짝에서는 `GenericRecord`가 나왔다. `Avro.decodeFromGenericData`는 그 이전 우회이고, avro4k가 제거 예정으로 표시해 두었다.

## 이 저장소의 예제

- ch03 `CurrentAvroSerializer`는 값에 `ReflectAvro4kKafkaSerializer`를 쓴다. `GenericRecord`와 avro4k `Customer`를 한 프로듀서가 보낸다.
- ch04 `CurrentAvroDeserializer`는 값으로 `GenericRecord`만 다룬다. 보내기는 `KafkaAvroSerializer`, 읽기는 `KafkaAvroDeserializer`, `specific.avro.reader=false`다.
- Avro 1.12는 스키마 풀네임과 같은 클래스를 로드하기 전에 `org.apache.avro.SERIALIZABLE_PACKAGES`를 본다. 허용 목록은 JVM이 뜰 때 있어야 한다.

## 테스트가 확인하는 것

[AvroRecordTypeSerdeTest.kt](../../../../../../test/kotlin/xyz/mon0mon/ch03/s05_serializer/AvroRecordTypeSerdeTest.kt)

- `GenericRecord`는 `KafkaAvroSerializer`와 `KafkaAvroDeserializer`로 왕복하고, 결과는 `GenericRecord`다.
- 자바 `SpecificRecord`는 `KafkaAvroSerializer`로 보낸다. `specific.avro.reader=true`이면 `study.specific.Customer`로 돌아오고, `false`이면 같은 바이트가 `GenericRecord`다.
- avro4k `Customer`는 `SpecificAvro4kKafkaSerializer`로 보낸다. `SpecificAvro4kKafkaDeserializer<Customer>`는 `Customer`로 돌려준다. `KafkaAvroDeserializer`는 같은 바이트를 `GenericRecord`로 읽는다.
- `KafkaAvroSerializer`에 avro4k `Customer`를 넘기면 실패한다.
- `ReflectAvro4kKafkaSerializer` 하나가 `GenericRecord`와 avro4k `Customer`를 보낸다. `ReflectAvro4kKafkaDeserializer`는 둘 다 `Customer`로 읽는다. 스키마 풀네임이 `customerManagement.avro.Customer`이기 때문이다. `KafkaAvroDeserializer`는 `GenericRecord`다.

```bat
gradlew.bat :ch03-producer:test --tests xyz.mon0mon.ch03.s05_serializer.AvroRecordTypeSerdeTest
```

## 출처

라이브러리 버전은 `gradle/libs.versions.toml` 기준이다. Avro 1.12.2, Confluent 8.4.0, avro4k 2.13.0.

- [Apache Avro 1.12.0 Specification — Binary Encoding](https://avro.apache.org/docs/1.12.0/specification/#binary-encoding). 레코드는 필드 순서대로 인코딩되고, 본문에 필드 이름이 없다. 1.12.2 명세 페이지는 배포본에 없어 1.12.0 문서를 둔다.
- [GenericRecord](https://avro.apache.org/docs/1.12.0/api/java/org/apache/avro/generic/GenericRecord.html), [SpecificRecord](https://avro.apache.org/docs/1.12.0/api/java/org/apache/avro/specific/SpecificRecord.html). Avro 1.12.0 Java API.
- [Avro Schema Serializer and Deserializer](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-avro.html). `KafkaAvroSerializer`, `KafkaAvroDeserializer`, Generic과 Specific, `specific.avro.reader`.
- [Formats, Serializers, and Deserializers — Wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format). 매직 바이트와 스키마 ID.
- [`KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG`](https://github.com/confluentinc/schema-registry/blob/master/avro-serializer/src/main/java/io/confluent/kafka/serializers/KafkaAvroDeserializerConfig.java). 기본값 `false`. 켜면 `SpecificRecord` 클래스를 찾는다.
- [avro4k README](https://github.com/avro-kotlin/avro4k/blob/main/README.md). Kotlin 데이터 클래스와 Confluent Schema Registry 모듈.
- [avro4k-confluent-kafka-serializer](https://github.com/avro-kotlin/avro4k/tree/main/avro4k-confluent-kafka-serializer). `Specific`, `Reflect`, `Generic` 시리얼라이저가 있는 모듈.
