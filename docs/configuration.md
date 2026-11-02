# 프로젝트 설정

Kafka: The Definitive Guide 2nd Edition을 코드로 따라가기 위한 학습 리포 설정입니다. 챕터마다 Gradle 서브프로젝트를 두고, `run`이 Docker Compose로 Kafka를 기동합니다.

## 스택

| 항목 | 값 | 위치 |
|------|-----|------|
| Gradle | 9.6.0 | `gradle/wrapper/gradle-wrapper.properties` |
| Kotlin | 2.4.21 | `gradle/libs.versions.toml` |
| JVM toolchain | 25 | `kotlin-library.conventions.gradle.kts` |
| kafka-clients | 4.4.0 | `gradle/libs.versions.toml` |
| 브로커 이미지 | `apache/kafka:4.4.0` (KRaft) | `docker/compose-*.yml` |
| Kafka UI | `ghcr.io/kafbat/kafka-ui:v1.5.0` | `docker/compose-*.yml` |
| Compose 플러그인 | `com.avast.gradle.docker-compose` 0.17.21 | 루트 `build.gradle.kts` |
| 그룹 | `xyz.mon0mon` | convention 플러그인 |

버전은 `gradle/libs.versions.toml`에서만 올립니다. 서브프로젝트 `build.gradle.kts`에 버전을 적지 않습니다.

## 디렉터리

```text
.
├── buildSrc/                      공통 Gradle 컨벤션
├── common/                        bootstrap, producer Properties
├── ch03-producer/                 3장 Producer 예제
├── docker/
│   ├── compose-single.yml         KRaft 1노드 + UI
│   └── compose-cluster.yml        3 컨트롤러 + 3 브로커 + UI
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

## buildSrc 컨벤션

`buildSrc`는 Gradle이 자동으로 컴파일해서 빌드 스크립트 클래스패스에 올립니다.

| 파일 | 플러그인 ID | 하는 일 |
|------|-------------|---------|
| `kotlin-library.conventions.gradle.kts` | `kotlin-library.conventions` | Kotlin JVM 25, kafka-clients, slf4j-simple, JUnit |
| `chapter.conventions.gradle.kts` | `chapter.conventions` | 위 설정 + `application` + Compose `run` 연동 |
| `xyz/mon0mon/gradle/ChapterExtension.kt` | — | `chapter { kafkaCluster }` 타입 |
| `xyz/mon0mon/gradle/VersionCatalog.kt` | — | 컨벤션에서 version catalog 조회 |

라이브러리 모듈은 `kotlin-library.conventions`만, 실행 예제 모듈은 `chapter.conventions`를 씁니다.

## 챕터별 클러스터 선택

예제 모듈 `build.gradle.kts`:

```kotlin
import xyz.mon0mon.gradle.KafkaCluster

plugins {
    id("chapter.conventions")
}

chapter {
    kafkaCluster.set(KafkaCluster.SINGLE)
}

dependencies {
    implementation(project(":common"))
}

application {
    mainClass.set("xyz.mon0mon.ch03.ProducerExampleKt")
}
```

`kafkaCluster` 기본값은 `SINGLE`입니다. 복제·ISR이 필요하면 `CLUSTER`로 바꿉니다.

`KafkaCluster` 정의 (`ChapterExtension.kt`):

| 값 | Compose 태스크 | bootstrap | UI |
|----|----------------|-----------|-----|
| `SINGLE` | `:composeUp` | `localhost:9092` | http://localhost:9080 |
| `CLUSTER` | `:clusterComposeUp` | `localhost:19092,19093,19094` | http://localhost:19080 |

`run` 시 convention이 하는 일:

1. 선택한 Compose 태스크에 `dependsOn`
2. JVM에 `-Dkafka.bootstrap.servers=...` 주입
3. `KafkaSettings.bootstrapServers()`가 그 시스템 프로퍼티를 읽음

예제 코드는 토폴로지를 하드코딩하지 않습니다.

```kotlin
val props = KafkaSettings.stringProducerProperties(clientId = "ch03-producer")
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
| UI 포트 | 9080 | 19080 |
| 컨테이너 | `kdg-single-kafka`, `kdg-single-ui` | `kdg-cluster-controller*`, `kdg-cluster-broker*`, `kdg-cluster-ui` |
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
        └─ ProducerExample
              -Dkafka.bootstrap.servers=localhost:9092
              ch03-example 토픽으로 메시지 5개 동기 전송
```

Kafka만 올리거나 내리기:

```bat
gradlew.bat composeUp
gradlew.bat composeDownForced
gradlew.bat clusterComposeUp
gradlew.bat clusterComposeDownForced
```

첫 전송에서 `UNKNOWN_TOPIC_OR_PARTITION`이 한 번 나올 수 있습니다. 싱글 compose는 `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`라서 곧 토픽이 만들어집니다.

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
}

dependencies {
    implementation(project(":common"))
}

application {
    mainClass.set("xyz.mon0mon.ch04.ConsumerExampleKt")
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
2. VM options에 `-Dkafka.bootstrap.servers=localhost:9092` 또는 클러스터 주소

Gradle 모듈이 안 보이면 Gradle 새로고침을 한 번 합니다.

## 설계 메모

- 책 2판은 ZooKeeper 설치를 다루지만, 이 리포는 Kafka 4.4 KRaft를 씁니다.
- Compose 플러그인을 챕터가 아니라 루트에만 둡니다. 모든 챕터가 같은 `kdg` / `kdg-cluster` 스택을 재사용합니다.
- 볼륨은 호스트 절대 경로가 아니라 named volume입니다.
- convention 위치는 `buildSrc`입니다. 플러그인을 다른 리포에 퍼블리시하지 않습니다.
