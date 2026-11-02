# Kafka The Definitive Guide 2nd — 학습용 예제

책 챕터별 Gradle 서브프로젝트입니다. `run`이 Docker Compose로 Kafka를 먼저 올립니다.

설정 상세는 [docs/configuration.md](docs/configuration.md)를 봅니다.

## 필요 환경

- JDK 25 (없으면 Gradle toolchain이 받습니다)
- Docker Desktop

## 바로 실행

```bat
gradlew.bat :ch03-producer:run
```

macOS / Linux:

```bash
./gradlew :ch03-producer:run
```

1. 싱글 브로커(`localhost:9092`)가 뜹니다.
2. 기본 `run`은 §3.3 Current입니다. `CustomerCountry`로 fire-and-forget, 동기, 비동기 전송을 한 번씩 합니다.
3. 컨테이너는 그대로 둡니다. UI: http://localhost:9080

책 코드와 4.4 권장 코드를 나눠 실행하려면:

```bat
gradlew.bat :ch03-producer:runS02Book
gradlew.bat :ch03-producer:runS02Current
gradlew.bat :ch03-producer:runS03Book
gradlew.bat :ch03-producer:runS03Current
```

내리기:

```bat
gradlew.bat composeDownForced
```

## 모듈

| 경로 | 역할 |
|------|------|
| `common` | bootstrap 주소, producer 기본 Properties |
| `ch03-producer` | 3장 Producer. 절마다 `Book*` / `Current*` |
| `docker/compose-single.yml` | KRaft 1노드 + UI (9092 / 9080) |
| `docker/compose-cluster.yml` | 3브로커 + UI (19092–19094 / 19080) |
| `buildSrc` | 챕터/라이브러리 convention |

## 챕터별 싱글 / 클러스터

모듈 `build.gradle.kts`:

```kotlin
import xyz.mon0mon.gradle.KafkaCluster

chapter {
    kafkaCluster.set(KafkaCluster.SINGLE)
    // kafkaCluster.set(KafkaCluster.CLUSTER)
}
```

기본값은 `SINGLE`입니다. 복제·ISR이 필요한 챕터만 `CLUSTER`로 바꿉니다.

| | SINGLE | CLUSTER |
|---|--------|---------|
| 기동 | `composeUp` | `clusterComposeUp` |
| bootstrap | `localhost:9092` | `localhost:19092,19093,19094` |
| UI | http://localhost:9080 | http://localhost:19080 |

`run`이 선택한 Compose를 올리고 `-Dkafka.bootstrap.servers`를 넣습니다. 예제는 `KafkaSettings.bootstrapServers()`로 그 값을 읽습니다.

## 새 챕터

1. `settings.gradle.kts`에 `include("ch04-consumer")`
2. 모듈에 `chapter.conventions` 적용 (`docs/configuration.md`의 템플릿)
3. `./gradlew :ch04-consumer:run`

## IntelliJ

Gradle 패널에서 `:ch03-producer` → `application` → `run`을 씁니다. `main`을 직접 Run하면 Kafka가 자동으로 안 뜹니다.
