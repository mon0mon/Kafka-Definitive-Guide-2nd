# Avro 시리얼라이저와 Kotlin

3.5.3에서 `GenericRecord`와 생성된 클래스를 같이 보내 보면서 정리한 복습 노트다. 책 경로는 `BookAvroSerializer`, 파일과 Schema Registry에서 스키마를 읽는 경로는 `CurrentAvroSerializer`다. 레지스트리의 subject와 등록은 [schema-registry.md](schema-registry.md)를 본다.

한 줄로 말하면, Kafka에 나가는 바이트는 방식이 같아도 JVM 안의 타입은 세 갈래다.

- `GenericRecord`: 스키마를 실행 중에 들고, 필드를 문자열로 넣는다.
- 자바 `SpecificRecord`: `.avsc`로 만든 클래스가 스키마와 필드를 가진다. Confluent `KafkaAvroSerializer`가 받는 타입이다.
- Kotlin avro4k 데이터 클래스: `.avsc`로 만들지만 `SpecificRecord`가 아니다. `avro4k-confluent-kafka-serializer`로 같은 와이어 포맷을 만든다.

avro4k 클래스를 설정만으로 `SpecificRecord`로 바꿀 수는 없다. 둘은 다른 생성기의 결과다.

## 와이어 포맷은 같다

Schema Registry를 쓰는 Confluent Avro 메시지는 다음 세 조각이다.

1. 매직 바이트 `0x00`
2. 스키마 ID 4바이트
3. Avro 바이너리 본문

스키마 전문은 메시지마다 들어가지 않는다. 시리얼라이저가 레지스트리에 스키마를 등록하거나 이미 있는 ID를 찾고, 그 ID만 메시지에 붙인다. 같은 스키마는 프로듀서 안에서 캐시된다.

기본 subject 이름은 토픽 기준이다. 값 스키마는 `{topic}-value`, 키 스키마는 `{topic}-key`다. 이 예제의 토픽 `customerContacts`는 `customerContacts-value`와 `customerContacts-key`를 만든다.

그래서 `GenericRecord`로 보낸 레코드와 생성 클래스로 보낸 레코드는 브로커 입장에서 같은 종류의 Avro 레코드다. 차이는 애플리케이션이 그 바이트를 어떤 JVM 타입으로 만들고 읽느냐다.

등록된 스키마의 풀네임은 `{namespace}.{name}`이다. 이 저장소의 `Customer.avsc`는 `customerManagement.avro.Customer`다. 자바 클래스든 Kotlin 클래스든, 그 풀네임이 스키마와 맞아야 컨슈머가 구체 타입으로 돌릴 수 있다.

## GenericRecord

스키마를 컴파일 시점에 고정하지 않을 때 쓴다. 라우터, 수집기, 임의의 토픽을 읽는 도구, 그리고 책의 3.5.3 예제가 여기 해당한다.

필요한 것은 Apache Avro, `io.confluent:kafka-avro-serializer`, `schema.registry.url`이다. 값 시리얼라이저는 `io.confluent.kafka.serializers.KafkaAvroSerializer`다.

```kotlin
val schema = Schema.Parser().parse(schemaString)
val customer = GenericData.Record(schema)
customer.put("id", 0)
customer.put("name", "exampleCustomer0")
customer.put("email", "example0@example.com")

props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
    "io.confluent.kafka.serializers.KafkaAvroSerializer"
props[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] =
    KafkaSettings.schemaRegistryUrl()

producer.send(ProducerRecord("customerContacts", "exampleCustomer0", customer))
```

스키마 문자열을 전송 루프마다 새로 만들 필요는 없다. 클래스패스의 `.avsc`를 한 번 읽어 `Schema`로 두면 된다. 불편한 지점은 네트워크가 아니라 필드 접근이다. `put("email", ...)`의 오타는 컴파일되지 않고 실행할 때 난다.

컨슈머의 `KafkaAvroDeserializer`는 기본으로 `GenericRecord`를 돌려준다.

## 자바 SpecificRecord

스키마가 빌드 전에 정해진 프로덕션 애플리케이션의 표준 경로다. `.avsc`를 Avro 컴파일러에 넣으면 `SpecificRecordBase`를 상속한 자바 클래스가 생긴다. 스키마는 그 클래스의 `SCHEMA$`에 들어 있으므로 프로듀서 코드에 스키마 문자열이 없다.

`SpecificRecord`는 `IndexedRecord`다. `GenericRecord`도 `IndexedRecord`다. `KafkaAvroSerializer`는 둘 다 받는다. 한 프로듀서의 값 시리얼라이저가 하나여도 두 타입을 같이 보낼 수 있다.

```kotlin
val customer = Customer.newBuilder()
    .setId(0)
    .setName("exampleCustomer0")
    .setEmail("example0@example.com")
    .build()

props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
    "io.confluent.kafka.serializers.KafkaAvroSerializer"
producer.send(ProducerRecord("customerContacts", customer.getName(), customer))
```

컨슈머가 같은 클래스를 받으려면 다음이 필요하다.

- 생성 클래스가 컨슈머 클래스패스에 있다.
- 클래스의 풀네임이 레지스트리에 있는 스키마 풀네임과 같다.
- `specific.avro.reader=true`를 넣는다. 키가 `KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG`다. 이 값이 없으면 결과는 `GenericRecord`다.

```kotlin
props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] =
    "io.confluent.kafka.serializers.KafkaAvroDeserializer"
props[KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG] = true
```

자바 생성기의 기본 문자열 타입은 `org.apache.avro.util.Utf8`이다. Kotlin에서 `String`으로 쓰려면 생성 설정에 `stringType = "String"`을 둔다. 자바 시리얼라이저가 스키마에 `avro.java.string` 같은 자바 전용 속성을 붙여 등록하지 않게 하려면 `avro.remove.java.properties=true`를 둔다. 등록 스키마가 원본 `.avsc`에 가깝게 남는다.

생성기 현황은 빌드 도구마다 다르다.

- Maven은 `avro-maven-plugin`이 이 경로의 일반 선택이다.
- Gradle의 `com.github.davidmc24.gradle.plugin.avro` 1.9.1은 아직 예제가 많지만 보관 상태이고, 컴파일러 기준은 Avro 1.11이다.
- 이 저장소는 Gradle 9.6과 Avro 1.12.2라서 그 플러그인을 쓰지 않는다.

Kotlin 코드는 생성된 자바 클래스를 그냥 호출한다. 빌더는 뮤터블하고 `copy`가 없다. 도메인 로직으로 넘기기 전에 Kotlin 데이터 클래스로 한 번 복사하는 팀이 많다. Avro 타입은 프로듀서와 컨슈머 가장자리에 둔다.

## Kotlin avro4k

avro4k는 kotlinx.serialization으로 Avro를 다루는 Kotlin 라이브러리다. 이 저장소는 2.10.0을 `ch03-producer`에만 둔다.

`io.github.avro-kotlin`이 `src/main/avro/*.avsc`를 `build/generated/sources/avro/main`의 데이터 클래스로 만든다. 생성된 `Customer`는 `@Serializable`이고, 스키마 JSON은 `@AvroGenerated`에 있다. `SpecificRecord`를 구현하지 않는다.

`KafkaAvroSerializer`는 이 클래스를 받지 않는다. 같은 레지스트리 와이어 포맷을 만드는 쪽은 `com.github.avro-kotlin.avro4k:avro4k-confluent-kafka-serializer`다. 이 모듈 README는 API가 아직 초기라 이후 버전에서 바뀔 수 있다고 적는다.

시리얼라이저는 세 종류다. 패키지는 `com.github.avrokotlin.avro4k.kafka.confluent`이다.

| 클래스 | 쓰는 때 |
|--------|---------|
| `GenericAvro4kKafkaSerializer` | 보낼 때는 여러 타입, 읽을 때는 `GenericRecord`와 기본 타입 |
| `ReflectAvro4kKafkaSerializer` | 보낼 때는 `GenericRecord`, 자바 `SpecificRecord`, avro4k 클래스를 한 시리얼라이저가 처리. 읽을 때는 스키마 풀네임으로 클래스를 찾고, 없으면 `GenericRecord` |
| `SpecificAvro4kKafkaSerializer<T>` | 토픽 값 타입이 컴파일 시점에 하나일 때. 코드에서 `serializer()`를 넘겨 만든다 |

`Properties`에 클래스 이름 문자열을 넣는 책의 방식에는 `Reflect`와 `Generic`이 맞다. Kafka가 시리얼라이저를 무인자 생성자로 만들기 때문이다. `SpecificAvro4kKafkaSerializer<Customer>`는 타입을 생성자에서 넘기므로, 직접 객체를 만들거나 Serde로 쓸 때 맞다.

이 저장소의 `BookAvroSerializer`는 한 프로듀서에서 `GenericData.Record`와 `CustomerGenerator.getNext()`를 같이 보내므로 값을 `ReflectAvro4kKafkaSerializer`로 둔다. 키는 책과 같이 `KafkaAvroSerializer`다. 프로듀서 타입은 `KafkaProducer<String, Any>`다.

```kotlin
props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
    "io.confluent.kafka.serializers.KafkaAvroSerializer"
props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
    "com.github.avrokotlin.avro4k.kafka.confluent.ReflectAvro4kKafkaSerializer"
props[KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG] =
    KafkaSettings.schemaRegistryUrl()
```

`@Serializable`만으로는 부족하다. Kotlin serialization 컴파일러 플러그인이 `Customer`의 `Serializer`를 만들어야 한다. 플러그인이 없으면 `Serializer for class 'Customer' is not found`가 난다. 이 저장소는 `org.jetbrains.kotlin.plugin.serialization`을 `ch03-producer`에만 적용한다.

스키마의 기준을 코드에 두는 방법도 있다. `.avsc` 없이 `@Serializable` 데이터 클래스를 작성하면 `Avro.schema<Customer>()`가 스키마를 만든다. 여러 언어가 계약을 공유하면 `.avsc`를 기준 파일로 두고 클래스를 생성하는 쪽이 맞다. 이 저장소는 후자다. `Customer.avsc`만 생성 입력이고, 책의 `customer_old.json`과 `customer_new.json`은 둘 다 레코드 이름이 `Customer`라서 생성기에 넣지 않는다.

## 자바 생태계와 같이 갈 때

호환의 단위는 클래스 파일이 아니라 `.avsc`와 레지스트리에 등록된 스키마다. 자바 프로듀서가 보낸 바이트를 Kotlin 컨슈머가 읽고, 그 반대도 된다. 조건은 스키마 풀네임과 필드 타입이 맞고, 양쪽이 Confluent 와이어 포맷을 쓰는 것이다.

기존 자바 서비스가 `KafkaAvroSerializer`와 `SpecificRecord`를 이미 쓰면, Kotlin도 그 생성 jar를 의존성으로 두고 같은 시리얼라이저를 쓰는 편이 갈래가 적다.

- 스키마 저장소의 `.avsc`로 자바 클래스를 만들고 jar로 배포한다.
- Kotlin 모듈은 그 jar를 의존한다.
- 프로듀서와 컨슈머 설정은 자바와 같다. 컨슈머만 `specific.avro.reader=true`를 추가한다.
- 자바 시리얼라이저에 `avro.remove.java.properties=true`를 둬서 등록 스키마에 자바 전용 속성이 붙지 않게 한다.
- 운영에서 등록 권한이 나뉘어 있으면 `auto.register.schemas=false`로 두고, 스키마 등록은 CI나 별도 단계에서 한다. 호환성 기본으로 많이 쓰는 값은 `BACKWARD`다.

이 선택에서는 avro4k 플러그인, Kotlin serialization 플러그인, avro4k 시리얼라이저가 필요 없다. Kotlin 코드의 모습은 자바 빌더 호출이다. `String?`과 `copy`가 필요하면 가장자리에서 데이터 클래스로 옮긴다.

같은 `.avsc`로 자바 클래스와 avro4k 클래스를 각자 생성하는 구성도 와이어는 통한다. 다만 생성기 두 개가 스키마를 서로 다르게 정규화하면 레지스트리에 기대와 다른 버전이 올라간다. 한 조직에서 스키마의 기준 파일은 하나로 두고, 등록은 한쪽에서만 한다.

## Kotlin만 있는 신규 서비스

계약의 소비자가 Kotlin이라면 avro4k가 그 진영의 경로다. 자바 `SpecificRecord`를 만들 이유가 없다.

스키마 우선이면 이렇게 구성한다.

1. `.avsc`를 `src/main/avro`에 둔다. union의 기본값은 JSON `null`이다. 문자열 `"null"`은 파서가 거절한다.
2. `io.github.avro-kotlin`과 Kotlin serialization 플러그인을 그 모듈에만 적용한다.
3. 값 타입이 하나이면 코드에서 `SpecificAvro4kKafkaSerializer<Customer>`를 만든다.
4. 값 타입이 여러 개이거나 `Properties`의 클래스 이름만 쓸 수 있으면 `ReflectAvro4kKafkaSerializer`를 쓴다.
5. 레지스트리 URL 키는 그대로 `schema.registry.url`이다.

코드 우선이면 데이터 클래스를 직접 작성하고, avro4k가 클래스에서 스키마를 만들어 등록한다. 스키마가 저장소에서 리뷰되는 계약이면 스키마 우선이 맞다.

avro4k 컨슈머는 자바의 `specific.avro.reader`를 쓰지 않는다. `ReflectAvro4kKafkaDeserializer`가 스키마 풀네임으로 클래스를 찾는다. 클래스 이름이 스키마와 다르면 `@AvroAlias`를 달고, 그 클래스를 `Avro { contextual(...) }`에 등록해야 별칭으로 찾을 수 있다.

이 라이브러리로 보낸 메시지를 자바 컨슈머가 읽을 수 있다. `specific.avro.reader` 없이 읽으면 `GenericRecord`다. 자바 생성 클래스로 읽으려면 그 클래스가 클래스패스에 있고 풀네임이 같아야 한다.

## Avro 1.12의 클래스 허용 목록

Avro 1.12.2는 스키마가 지목하는 클래스를 임의로 만들지 않는다. `SpecificDatumReader`와 스키마의 `java-class`가 클래스를 로드하기 전에 `org.apache.avro.SERIALIZABLE_PACKAGES` 또는 `org.apache.avro.SERIALIZABLE_CLASSES`를 본다. 허용 목록 밖이면 `SecurityException`이다.

이 저장소에서는 생성된 `customerManagement.avro.Customer`가 클래스패스에 생긴 뒤, `GenericRecord`용 스키마를 파싱하다가 같은 풀네임의 클래스를 로드하려다 막혔다. 허용 목록은 Avro 클래스가 로드될 때 읽으므로 `main` 안의 `System.setProperty`는 늦다. `runS05Book`과 `runS05Current`의 JVM 인자로 둔다.

```text
-Dorg.apache.avro.SERIALIZABLE_PACKAGES=customerManagement.avro
```

IntelliJ에서 메인을 직접 실행하면 Gradle 태스크의 JVM 인자가 넘어가지 않는다. 같은 값을 VM options에 넣어야 한다. 신뢰하는 스키마만 있는 로컬이라도 `*`로 열지는 않는다. 패키지를 명시한다.

자바 `SpecificRecord` 컨슈머도 1.12.2에서 커스텀 패키지를 허용 목록에 넣어야 할 수 있다. 이 인자는 avro4k 전용 설정이 아니다.

## 이 저장소에 해당하는 것

| 항목 | 위치 |
|------|------|
| 스키마 | `../../../../../avro/Customer.avsc` |
| 생성 클래스 사용 | `CustomerGenerator.getNext()` |
| 두 경로를 한 번에 전송 | `BookAvroSerializer`, `CurrentAvroSerializer` |
| 파일 로드, 등록, 조회 | `CurrentAvroSerializer`, [schema-registry.md](schema-registry.md) |
| 플러그인과 JVM 인자 | `../../../../../../../build.gradle.kts` |
| 실행 | `gradlew.bat :ch03-producer:runS05Book`, `gradlew.bat :ch03-producer:runS05Current` |

`BookAvroSerializer`의 값 시리얼라이저가 `ReflectAvro4kKafkaSerializer`인 이유는 책의 `GenericRecord`와 avro4k `Customer`를 한 프로듀서에 남기기 위해서다. 키와 레지스트리 URL은 책의 흐름이다. URL은 `KafkaSettings.schemaRegistryUrl()`이고, Gradle `runS05Book`이 `-Dkafka.schema.registry.url`을 넣는다.

다시 볼 때의 기준은 다음이다.

- 스키마를 실행 중에만 안다. `GenericRecord`와 `KafkaAvroSerializer`.
- 자바 서비스와 생성 클래스를 공유한다. 자바 `SpecificRecord` jar와 `KafkaAvroSerializer`. 컨슈머는 `specific.avro.reader=true`.
- Kotlin 데이터 클래스가 애플리케이션 타입이다. avro4k 생성과 `avro4k-confluent-kafka-serializer`. 타입이 하나면 `SpecificAvro4kKafkaSerializer`, 여러 타입을 클래스 이름 설정으로 보내면 `ReflectAvro4kKafkaSerializer`.

## 스키마가 바뀔 때

호환성 검사는 레지스트리가 한다. 기본값은 `BACKWARD`다. 새 스키마를 읽는 쪽이 예전 데이터도 읽을 수 있어야 등록된다. `FORWARD`는 예전 스키마를 읽는 쪽이 새 데이터를 읽을 수 있어야 한다. 와이어 포맷의 매직 바이트와 스키마 ID는 그대로다. 바뀐 것은 레지스트리에 올라간 스키마 버전이다.

필드를 추가할 때 기본값이 있으면 `BACKWARD`다. 예전 바이트에는 그 필드가 없고, 새 reader 스키마의 기본값이 채워진다. 기본값이 없는 필드 추가는 `BACKWARD`에서 거절된다. 예전 데이터로 그 필드를 만들 수 없기 때문이다. 기본값이 없는 필드를 지우면 `FORWARD`에서 거절된다. 예전 reader는 그 필드를 아직 필수로 보는데, 새 데이터에는 값이 없다.

이 규칙은 `GenericRecord`, 자바 `SpecificRecord`, avro4k 데이터 클래스에 같이 적용된다. 검사 대상은 스키마이지 JVM 클래스가 아니다. 클래스는 스키마가 등록된 뒤에 다시 생성한다.

확인 코드는 `src/test/kotlin/xyz/mon0mon/ch03/s05_serializer/SchemaCompatibilityTest.kt`다. Testcontainers가 catalog와 같은 `apache/kafka:4.4.0`, `cp-schema-registry:8.4.0`을 띄운다. 테스트 스키마의 네임스페이스는 `evolution.avro`다. 실행 중인 예제의 `customerManagement.avro.Customer`와 풀네임이 겹치지 않게 하려는 것이다.

```bat
gradlew.bat :ch03-producer:test --tests xyz.mon0mon.ch03.s05_serializer.SchemaCompatibilityTest
```
