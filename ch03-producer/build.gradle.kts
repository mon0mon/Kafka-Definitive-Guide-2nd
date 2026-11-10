import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.testing.Test
import xyz.mon0mon.gradle.KafkaCluster

plugins {
    id("chapter.conventions")
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.avro.kotlin)
}

chapter {
    kafkaCluster.set(KafkaCluster.SINGLE)
    examples.put("S02Book", "xyz.mon0mon.ch03.s02_create_producer.BookCreateProducer")
    examples.put("S02Current", "xyz.mon0mon.ch03.s02_create_producer.CurrentCreateProducer")
    examples.put("S03Book", "xyz.mon0mon.ch03.s03_send_sync.BookSendSync")
    examples.put("S03Current", "xyz.mon0mon.ch03.s03_send_sync.CurrentSendSync")
    examples.put("S05Book", "xyz.mon0mon.ch03.s05_serializer.BookAvroSerializer")
    examples.put("S05Current", "xyz.mon0mon.ch03.s05_serializer.CurrentAvroSerializer")
    examples.put("S06Book", "xyz.mon0mon.ch03.s06_partition.BookCustomPartitioner")
    examples.put("S06Current", "xyz.mon0mon.ch03.s06_partition.CurrentCustomPartitioner")
    examples.put("S07Book", "xyz.mon0mon.ch03.s07_headers.BookHeaders")
    examples.put("S07Current", "xyz.mon0mon.ch03.s07_headers.CurrentHeaders")
    examples.put("S08Book", "xyz.mon0mon.ch03.s08_interceptor.BookInterceptor")
    examples.put("S08Current", "xyz.mon0mon.ch03.s08_interceptor.CurrentInterceptor")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.avro4k.confluent.kafka.serializer)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.kafka)
}

tasks.withType<Test>().configureEach {
    // 이미지 태그는 catalog의 kafka, confluent 버전과 같게 둔다.
    systemProperty("kafka.image", "apache/kafka:${libs.versions.kafka.get()}")
    systemProperty("schema.registry.image", "confluentinc/cp-schema-registry:${libs.versions.confluent.get()}")
    // Avro 1.12는 스키마 풀네임과 같은 클래스를 로드하기 전에 이 값을 본다.
    // 테스트 JVM이 뜬 뒤에 넣으면 늦다.
    systemProperty(
        "org.apache.avro.SERIALIZABLE_PACKAGES",
        "customerManagement.avro,study.specific",
    )
}

application {
    mainClass.set("xyz.mon0mon.ch03.s03_send_sync.CurrentSendSync")
}

afterEvaluate {
    listOf("runS05Book", "runS05Current").forEach { taskName ->
        tasks.named<JavaExec>(taskName) {
            // Avro 1.12는 스키마 풀네임과 같은 클래스를 로드하기 전에 패키지 허용 목록을 본다.
            // 이 값은 Avro 클래스가 로드되기 전에 있어야 해서 main 안에서는 늦다.
            jvmArgs("-Dorg.apache.avro.SERIALIZABLE_PACKAGES=customerManagement.avro")
        }
    }
}
