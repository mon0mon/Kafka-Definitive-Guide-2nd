# Agent rules

학습 리포. 책(Kafka The Definitive Guide 2nd) 스니펫을 기준으로 예제를 쌓고, 같은 절의 **4.4 권장 코드**를 옆에 둔다.

레이아웃·KDoc 필드·Gradle 등록의 단일 출처: [docs/configuration.md](docs/configuration.md) **예제 트랙 (Book / Current)**.

## 트리거

사용자가 예제 파일(`@...kt`)을 가리키고 아래처럼 말하면 **Current 트랙 루틴**을 실행한다.

- Current 트랙으로 만들어줘
- 최신 버전에서 어떻게 쓰는지 작성해줘
- 이 파일을 4.4 기준으로 보정하고 Current를 만들어줘
- Book/Current로 나눠줘

파일을 지정하지 않으면 어떤 절인지 한 번만 확인한다.

## Current 트랙 루틴

1. [docs/configuration.md](docs/configuration.md) 예제 트랙과 같은 챕터의 기존 `Book*`/`Current*` 한 쌍을 읽는다. 새 파일은 그 형태를 따른다.
2. 사용자 파일을 **Book 원본**으로 본다. Current로 바꿔 덮어쓰지 않는다.
3. 절 번호·주제를 경로, 파일명, 주석, 책 목차에서 고른다. 폴더가 규칙과 다르면 `s{절}_{topic}/Book{Name}.kt`로 옮긴다.
   - 패키지: `xyz.mon0mon.ch{NN}.{folder}`
   - 진입점: `object Book{Name} { @JvmStatic fun main(...) }`
4. Book 파일은 4.4에서 **컴파일·실행되게만** 고친다.
   - 책 흐름(변수명, 전송 방식, 예제 데이터)은 유지한다.
   - 고친 줄에 `// 4.4: 책이 X. 지금은 Y.` 를 붙인다.
   - bootstrap은 `KafkaSettings.bootstrapServers()` (또는 `stringProducerProperties`).
   - producer에 `ConsumerConfig` deserializer를 넣지 않는다. serializer는 `ProducerConfig.*_SERIALIZER_*`.
   - 프로세스가 끝나는 예제는 `close()` 또는 `finally`를 넣는다.
   - 1.x 문자열 키(`"bootstrap.servers"`)는 살아 있는 코드로 두지 않고 KDoc/주석으로만 남긴다.
   - 사용자가 넣은 `// region …` / `// endregion`은 유지한다. 없으면 책 절 제목으로 감싼다 (`// region 3.2 카프카 프로듀서 생성하기`).
5. 같은 폴더에 `Current{Name}.kt`를 새로 만든다. 같은 절의 목표를 kafka-clients **4.4.0** 관용으로 다시 쓴다.
   - `ProducerConfig` 상수, serializer는 `StringSerializer::class.java` 또는 생성자 인자
   - `KafkaProducer(...).use { }`
   - 토픽/레코드 의미는 Book과 같게 둔다
   - Book과 **같은 region 이름**으로 대응 블록을 나눈다. 한 파일에 여러 절 스니펫이 있으면 Current도 같은 경계를 따른다.
6. 두 파일 최상단에 KDoc만 둔다. 절별 `notes.md`는 만들지 않는다.
   - 필수: `Book`, `Track` (`book` | `current`), `Client: kafka-clients 4.4.0`, `Evolution`
   - Current: `Replaces: [Book...]`
   - 있으면: `Removed`, `Still valid`
   - `Evolution`은 이 절에서 갈라진 지점만 (1.x → 2.x/책 → 3.x → 4.0/4.4). 해당 없는 메이저는 생략.
7. 4.0에서 삭제된 API는 실행 코드로 두지 않는다. KDoc `Removed:`에 대체만 적는다.
8. 모듈 `build.gradle.kts`의 `chapter.examples`에 등록한다.
   - `S{절}Book` → Book FQCN
   - `S{절}Current` → Current FQCN
   - `run`의 `application.mainClass`는 그 챕터의 대표 Current. 사용자 요청 없이 바꾸지 않는다.
9. `./gradlew :{module}:compileKotlin`으로 컴파일한다. 실행은 사용자가 요청할 때만.
10. 응답은 학습용으로 짧게:
    - 만든/옮긴 경로
    - Book에서 고친 줄 (컴파일·잘못된 API)
    - Current가 Book과 다른 점 3~6개 (API, 기본값, close, serializer)
    - 실행: `gradlew.bat :{module}:runS{절}Book` / `runS{절}Current`

## 하지 말 것

- Book과 Current를 한 `main`에 섞지 않는다.
- 사용자 `region` 주석을 지우거나 이름을 임의로 바꾸지 않는다.
- 다른 절·다른 챕터까지 임의로 확장하지 않는다.
- `docs/configuration.md`에 있는 디렉터리·region 규칙을 AGENTS.md에 다시 풀어 쓰지 않는다. 링크하고 따른다.
