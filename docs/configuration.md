# 프로젝트 설정

Kafka: The Definitive Guide 2nd Edition을 코드로 따라가기 위한 학습 리포 설정입니다. 챕터마다 Gradle 서브프로젝트를 두고, `run`이 Docker Compose로 Kafka를 기동합니다.

## 스택

| 항목 | 값 | 위치 |
|------|-----|------|
| Gradle | 9.6.0 | `gradle/wrapper/gradle-wrapper.properties` |
| Kotlin | 2.4.21 | `gradle/libs.versions.toml` |
| JVM toolchain | 25 | `kotlin-library.conventions.gradle.kts` |
| kafka-clients | 4.4.0 | `gradle/libs.versions.toml` |
| Apache Avro | 1.12.2 | `gradle/libs.versions.toml` |
| kafka-avro-serializer | 8.4.0 (Confluent Platform, Kafka 4.4) | `gradle/libs.versions.toml`, `kotlin-library.conventions` |
| avro4k | 2.10.0 (`io.github.avro-kotlin`, `avro4k-confluent-kafka-serializer`, Kotlin serialization) | `ch03-producer`만. `src/main/avro/*.avsc` → Kotlin 클래스 |
| Schema Registry | `confluentinc/cp-schema-registry:8.4.0` | `docker/compose-*.yml` |
| 로깅 | kotlin-logging 8.0.4 + Logback 1.5.21 | `gradle/libs.versions.toml`, `common/.../logback.xml` |
| 브로커 이미지 | `apache/kafka:4.4.0` (KRaft) | `docker/compose-*.yml` |
| Kafka UI | `ghcr.io/kafbat/kafka-ui:v1.5.0` | `docker/compose-*.yml` |
| Compose 플러그인 | `com.avast.gradle.docker-compose` 0.17.21 | 루트 `build.gradle.kts` |
| Testcontainers | 2.0.5 (`testcontainers`, `testcontainers-kafka`) | `ch03-producer` test. 이미지는 catalog의 kafka 4.4.0, confluent 8.4.0 |
| 그룹 | `xyz.mon0mon` | convention 플러그인 |

버전은 `gradle/libs.versions.toml`에서만 올립니다. 서브프로젝트 `build.gradle.kts`에 버전을 적지 않습니다.

## 디렉터리

```text
.
├── buildSrc/                      공통 Gradle 컨벤션
├── common/                        bootstrap, producer Properties
├── ch03-producer/                 3장 Producer. 절마다 Book / Current
├── docker/
│   ├── compose-single.yml         KRaft 1노드 + Schema Registry + UI
│   └── compose-cluster.yml        3 컨트롤러 + 3 브로커 + Schema Registry + UI
├── gradle/libs.versions.toml
├── docs/configuration.md          이 문서
├── build.gradle.kts               루트 Compose만 담당 (앱 아님)
└── settings.gradle.kts
```

루트에는 애플리케이션 소스가 없습니다. 예제는 전부 서브프로젝트에 둡니다.

## Gradle 모듈

`settings.gradle.kts`가 모듈을 등록합니다.

```kotlin
include("common")
include("ch03-producer")
```

| 모듈 | 플러그인 | 역할 |
|------|----------|------|
| 루트 | `com.avast.gradle.docker-compose` | `composeUp` / `clusterComposeUp` |
| `common` | `kotlin-library.conventions` | 라이브러리. Kafka 주소와 기본 Properties |
| `ch03-producer` | `chapter.conventions` | `application` + Compose 연동 |

저장소는 `settings.gradle.kts`의 `dependencyResolutionManagement`만 사용합니다. (`FAIL_ON_PROJECT_REPOS`)

Maven Central 외에 `io.confluent` 그룹만 `https://packages.confluent.io/maven/`에서 받습니다. `KafkaAvroSerializer`는 Central에 없습니다. Confluent 8.4가 `kafka-clients:8.4.0-ccs`를 요구하므로, `kotlin-library.conventions`가 Apache `kafka-clients` 4.4.0을 `strictly`로 고정합니다.

## buildSrc 컨벤션

`buildSrc`는 Gradle이 자동으로 컴파일해서 빌드 스크립트 클래스패스에 올립니다.

| 파일 | 플러그인 ID | 하는 일 |
|------|-------------|---------|
| `kotlin-library.conventions.gradle.kts` | `kotlin-library.conventions` | Kotlin JVM 25, kafka-clients, Apache Avro, kafka-avro-serializer, kotlin-logging, Logback, JUnit |
| `chapter.conventions.gradle.kts` | `chapter.conventions` | 위 설정 + `application` + Compose `run` 연동 |
| `xyz/mon0mon/gradle/ChapterExtension.kt` | — | `chapter { kafkaCluster }` 타입 |
| `xyz/mon0mon/gradle/VersionCatalog.kt` | — | 컨벤션에서 version catalog 조회 |

라이브러리 모듈은 `kotlin-library.conventions`만, 실행 예제 모듈은 `chapter.conventions`를 씁니다.

Avro 시리얼라이저의 `GenericRecord`, 자바 `SpecificRecord`, Kotlin avro4k 차이는 [avro-serializer-kotlin.md](../ch03-producer/src/main/kotlin/xyz/mon0mon/ch03/s05_serializer/avro-serializer-kotlin.md)에 정리해 두었습니다. Schema Registry의 subject, 등록, 조회는 [schema-registry.md](../ch03-producer/src/main/kotlin/xyz/mon0mon/ch03/s05_serializer/schema-registry.md)에 정리해 두었습니다.

Avro 스키마로 Kotlin 클래스를 만드는 챕터만 `io.github.avro-kotlin`을 추가합니다. 입력은 그 모듈의 `src/main/avro/*.avsc`이고, 생성 결과는 `build/generated/sources/avro/main`입니다. `ch03-producer`는 `Customer.avsc` 하나만 생성합니다. `customer_old.json`과 `customer_new.json`은 책의 스키마 진화 원문이라 생성 입력에 넣지 않습니다. 둘 다 레코드 이름이 `Customer`입니다. `runS05Book`과 `runS05Current`는 Avro 1.12 클래스 허용 목록으로 `-Dorg.apache.avro.SERIALIZABLE_PACKAGES=customerManagement.avro`를 넣습니다.

## 챕터별 클러스터 선택

예제 모듈 `build.gradle.kts`:

```kotlin
import xyz.mon0mon.gradle.KafkaCluster

plugins {
    id("chapter.conventions")
}

chapter {
    kafkaCluster.set(KafkaCluster.SINGLE)
    examples.put("S02Book", "xyz.mon0mon.ch03.s02_create_producer.BookCreateProducer")
    examples.put("S02Current", "xyz.mon0mon.ch03.s02_create_producer.CurrentCreateProducer")
    examples.put("S03Book", "xyz.mon0mon.ch03.s03_send_sync.BookSendSync")
    examples.put("S03Current", "xyz.mon0mon.ch03.s03_send_sync.CurrentSendSync")
}

dependencies {
    implementation(project(":common"))
}

application {
    mainClass.set("xyz.mon0mon.ch03.s03_send_sync.CurrentSendSync")
}
```

`kafkaCluster` 기본값은 `SINGLE`입니다. 복제·ISR이 필요하면 `CLUSTER`로 바꿉니다.

`KafkaCluster` 정의 (`ChapterExtension.kt`):

| 값 | Compose 태스크 | bootstrap | Schema Registry | UI |
|----|----------------|-----------|-----------------|-----|
| `SINGLE` | `:composeUp` | `localhost:9092` | http://localhost:8081 | http://localhost:9080 |
| `CLUSTER` | `:clusterComposeUp` | `localhost:19092,19093,19094` | http://localhost:18081 | http://localhost:19080 |

`run` 시 convention이 하는 일:

1. 선택한 Compose 태스크에 `dependsOn`
2. JVM에 `-Dkafka.bootstrap.servers=...`와 `-Dkafka.schema.registry.url=...` 주입. 값은 `KafkaCluster`의 `bootstrapServers`, `schemaRegistryUrl`
3. `KafkaSettings.bootstrapServers()`와 `KafkaSettings.schemaRegistryUrl()`가 그 시스템 프로퍼티를 읽음. 없으면 싱글 주소로 떨어짐

예제 코드는 토폴로지를 하드코딩하지 않습니다.

```kotlin
val props = KafkaSettings.stringProducerProperties(clientId = "ch03-s03-current")
```

## 예제 트랙 (Book / Current)

챕터 모듈은 책 절마다 폴더를 두고, 실행 파일을 둘로 나눕니다. 비교 설명은 파일 최상단 KDoc만 씁니다.

```text
ch03-producer/src/main/kotlin/xyz/mon0mon/ch03/
  s02_create_producer/
    BookCreateProducer.kt
    CurrentCreateProducer.kt
  s03_send_sync/
    BookSendSync.kt
    CurrentSendSync.kt
  s05_serializer/
    BookAvroSerializer.kt
    CurrentAvroSerializer.kt
```

| 트랙 | 파일 | 내용 |
|------|------|------|
| book | `Book*.kt` | 책에 가까운 코드. 4.4에서 컴파일되게만 고치고, 고친 줄에 이유를 적는다. |
| current | `Current*.kt` | 같은 절의 4.4 권장 코드. |
| removed | 실행 파일 없음 | 4.0에서 삭제된 API는 KDoc `Removed:` / `Evolution:`에만 적는다. |

진입점은 `object` + `@JvmStatic fun main`이다. `chapter.examples`에 등록하면 `run{Name}` JavaExec가 생긴다. `run`은 `application.mainClass`(보통 그 챕터의 대표 Current)다.

KDoc 헤더 필드: `Book`, `Track`, `Client`, `Evolution`, 필요 시 `Replaces` / `Removed` / `Still valid`.

파일 안 학습 위치는 IntelliJ `region`으로 표시한다. 이름은 책 절 제목을 그대로 쓴다.

```kotlin
// region 3.2 카프카 프로듀서 생성하기
val producer = KafkaProducer<String, String>(kafkaProps)
// endregion

// region 3.3 카프카로 메시지 전달하기
producer.send(record)
// endregion
```

한 실행 파일에 이어지는 스니펫이 있으면 폴더를 억지로 쪼개지 않고 region으로 나눈다. 폴더명(`s02_create_producer`)은 그 파일이 시작하는 대표 절이다. Current 파일은 Book과 **같은 region 이름**을 쓴다.

예제 출력은 `println` / `printStackTrace` 대신 `KotlinLogging`을 쓴다. Book 트랙도 같다. 책의 `System.out.println`은 출력 수단만 로거로 바꾼다.

```kotlin
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

logger.info { "sent topic=${metadata.topic()} offset=${metadata.offset()}" }
logger.error(e) { "send failed" }
```

3장 실행:

```bat
gradlew.bat :ch03-producer:run
gradlew.bat :ch03-producer:runS02Book
gradlew.bat :ch03-producer:runS02Current
gradlew.bat :ch03-producer:runS03Book
gradlew.bat :ch03-producer:runS03Current
gradlew.bat :ch03-producer:runS05Book
gradlew.bat :ch03-producer:runS05Current
```

시스템 프로퍼티가 없으면 `localhost:9092`로 떨어집니다. IntelliJ에서 `main`을 직접 실행한 경우가 여기 해당합니다.

## Docker Compose

루트 `build.gradle.kts`가 두 스택을 등록합니다.

| | 싱글 | 클러스터 |
|---|------|----------|
| 파일 | `docker/compose-single.yml` | `docker/compose-cluster.yml` |
| Compose 프로젝트 | `kdg` | `kdg-cluster` |
| 태스크 | `composeUp` / `composeDownForced` | `clusterComposeUp` / `clusterComposeDownForced` |
| 브로커 역할 | broker+controller 1노드 | controller 3 + broker 3 |
| 호스트 포트 | 9092 | 19092, 19093, 19094 |
| Schema Registry | http://localhost:8081 | http://localhost:18081 |
| UI 포트 | 9080 | 19080 |
| 컨테이너 | `kdg-single-kafka`, `kdg-single-schema-registry`, `kdg-single-ui` | `kdg-cluster-controller*`, `kdg-cluster-broker*`, `kdg-cluster-schema-registry`, `kdg-cluster-ui` |
| 볼륨 | named volume `kdg-single-data` | named volume `kdg-cluster-*` |

공통 동작:

- `stopContainers = false` — `run`이 끝나도 컨테이너를 유지합니다. 연속 실행과 UI 확인이 쉽습니다.
- `removeVolumes = false` — `down` 이후에도 토픽 데이터가 남습니다.
- `waitForTcpPorts = true` — 브로커 포트가 열린 뒤에 예제를 실행합니다.
- 브로커 `healthcheck`는 `kafka-broker-api-versions.sh`입니다.
- UI 컨테이너 포트 `8080`은 대기에서 빼 둡니다. 예제는 브로커만 있으면 됩니다.

싱글과 클러스터는 호스트 포트가 달라서 동시에 떠 있어도 됩니다.

`composeDown`은 `stopContainers = false`라서 컨테이너를 안 내립니다. 정리는 `composeDownForced` / `clusterComposeDownForced`를 씁니다.

## 실행 흐름

```text
./gradlew :ch03-producer:run
        │
        ├─ :composeUp          (kafkaCluster = SINGLE인 경우)
        │     docker compose -p kdg up
        │     kafka healthy + TCP 9092
        │
        └─ CurrentSendSync
              -Dkafka.bootstrap.servers=localhost:9092
              CustomerCountry로 fire-and-forget, 동기, 비동기 전송
```

Kafka만 올리거나 내리기:

```bat
gradlew.bat composeUp
gradlew.bat composeDownForced
gradlew.bat clusterComposeUp
gradlew.bat clusterComposeDownForced
```

첫 전송에서 `UNKNOWN_TOPIC_OR_PARTITION`이 한 번 나올 수 있습니다. 싱글 compose는 `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`라서 곧 토픽이 만들어집니다.

스키마 호환성 테스트는 Compose 스택과 별도로 Testcontainers가 `apache/kafka`와 `cp-schema-registry`를 띄웁니다. Docker가 떠 있어야 합니다.

```bat
gradlew.bat :ch03-producer:test --tests xyz.mon0mon.ch03.s05_serializer.SchemaCompatibilityTest
```

## 새 챕터 모듈

1. `settings.gradle.kts`에 `include("ch04-consumer")`
2. `ch04-consumer/build.gradle.kts`:

```kotlin
import xyz.mon0mon.gradle.KafkaCluster

plugins {
    id("chapter.conventions")
}

chapter {
    kafkaCluster.set(KafkaCluster.SINGLE)
    examples.put("S02Book", "xyz.mon0mon.ch04.s02_create_consumer.BookCreateConsumer")
    examples.put("S02Current", "xyz.mon0mon.ch04.s02_create_consumer.CurrentCreateConsumer")
}

dependencies {
    implementation(project(":common"))
}

application {
    mainClass.set("xyz.mon0mon.ch04.s02_create_consumer.CurrentCreateConsumer")
}
```

3. `ch04-consumer/src/main/kotlin/xyz/mon0mon/ch04/...`에 `main` 작성
4. `./gradlew :ch04-consumer:run`

Streams, Connect, 보안 등 추가 의존성과 compose overlay는 그 챕터에서만 붙입니다.

## IntelliJ

권장: Gradle 패널 → `:ch03-producer` → `Tasks` → `application` → `run`. Compose와 bootstrap 주입이 적용됩니다.

`main`을 직접 Run하면 Compose도 안 뜨고 `-Dkafka.bootstrap.servers`도 없습니다.

직접 Run이 필요하면:

1. Before Run에 Gradle 태스크 `composeUp` 또는 `clusterComposeUp`
2. VM options에 `-Dkafka.bootstrap.servers=localhost:9092`와 `-Dkafka.schema.registry.url=http://localhost:8081`. 클러스터는 `19092`–`19094`와 `http://localhost:18081`

Gradle 모듈이 안 보이면 Gradle 새로고침을 한 번 합니다.

## 설계 메모

- 책 2판은 ZooKeeper 설치를 다루지만, 이 리포는 Kafka 4.4 KRaft를 씁니다.
- Compose 플러그인을 챕터가 아니라 루트에만 둡니다. 모든 챕터가 같은 `kdg` / `kdg-cluster` 스택을 재사용합니다.
- 볼륨은 호스트 절대 경로가 아니라 named volume입니다.
- convention 위치는 `buildSrc`입니다. 플러그인을 다른 리포에 퍼블리시하지 않습니다.
