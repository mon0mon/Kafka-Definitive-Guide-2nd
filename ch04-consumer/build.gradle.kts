import org.gradle.api.tasks.JavaExec
import xyz.mon0mon.gradle.KafkaCluster

plugins {
    id("chapter.conventions")
}

chapter {
    kafkaCluster.set(KafkaCluster.SINGLE)
    examples.put("S02Book", "xyz.mon0mon.ch04.s02_create_consumer.BookCreateConsumer")
    examples.put("S02Current", "xyz.mon0mon.ch04.s02_create_consumer.CurrentCreateConsumer")
    examples.put("S03Book", "xyz.mon0mon.ch04.s03_subscribe.BookSubscribe")
    examples.put("S03Current", "xyz.mon0mon.ch04.s03_subscribe.CurrentSubscribe")
    examples.put("S04Book", "xyz.mon0mon.ch04.s04_poll_loop.BookPollLoop")
    examples.put("S04Current", "xyz.mon0mon.ch04.s04_poll_loop.CurrentPollLoop")
    examples.put("S06Book", "xyz.mon0mon.ch04.s06_offset_commit.BookCurrentOffsetCommit")
    examples.put("S06Current", "xyz.mon0mon.ch04.s06_offset_commit.CurrentCurrentOffsetCommit")
    examples.put("S063Book", "xyz.mon0mon.ch04.s06_offset_commit.p03_async_commit.BookAsyncCommit")
    examples.put("S063Current", "xyz.mon0mon.ch04.s06_offset_commit.p03_async_commit.CurrentAsyncCommit")
    examples.put(
        "S063CallbackBook",
        "xyz.mon0mon.ch04.s06_offset_commit.p03_async_commit.BookAsyncCommitCallback",
    )
    examples.put(
        "S063CallbackCurrent",
        "xyz.mon0mon.ch04.s06_offset_commit.p03_async_commit.CurrentAsyncCommitCallback",
    )
    examples.put(
        "S063RetryCurrent",
        "xyz.mon0mon.ch04.s06_offset_commit.p03_async_commit.CurrentAsyncCommitRetry",
    )
    examples.put(
        "S064Book",
        "xyz.mon0mon.ch04.s06_offset_commit.p04_sync_async_commit.BookSyncAsyncCommit",
    )
    examples.put(
        "S064Current",
        "xyz.mon0mon.ch04.s06_offset_commit.p04_sync_async_commit.CurrentSyncAsyncCommit",
    )
    examples.put(
        "S065Book",
        "xyz.mon0mon.ch04.s06_offset_commit.p05_specified_offset.BookSpecifiedOffset",
    )
    examples.put(
        "S065Current",
        "xyz.mon0mon.ch04.s06_offset_commit.p05_specified_offset.CurrentSpecifiedOffset",
    )
    examples.put("S07Book", "xyz.mon0mon.ch04.s07_rebalance_listener.BookRebalanceListener")
    examples.put("S07Current", "xyz.mon0mon.ch04.s07_rebalance_listener.CurrentRebalanceListener")
    examples.put("S08Book", "xyz.mon0mon.ch04.s08_specific_offset.BookSpecificOffset")
    examples.put("S08Current", "xyz.mon0mon.ch04.s08_specific_offset.CurrentSpecificOffset")
    examples.put("S09Book", "xyz.mon0mon.ch04.s09_exit_poll_loop.BookExitPollLoop")
    examples.put("S09Current", "xyz.mon0mon.ch04.s09_exit_poll_loop.CurrentExitPollLoop")
    examples.put("S101Book", "xyz.mon0mon.ch04.s10_custom_deserializer.BookCustomerRoundTrip")
    examples.put("S101Current", "xyz.mon0mon.ch04.s10_custom_deserializer.CurrentCustomerRoundTrip")
    examples.put("S102Book", "xyz.mon0mon.ch04.s102_avro_deserializer.BookAvroDeserializer")
    examples.put("S102Current", "xyz.mon0mon.ch04.s102_avro_deserializer.CurrentAvroDeserializer")
}

dependencies {
    implementation(project(":common"))
    implementation(project(":ch03-producer"))
}

application {
    mainClass.set("xyz.mon0mon.ch04.s02_create_consumer.CurrentCreateConsumer")
}

afterEvaluate {
    listOf("runS102Book", "runS102Current").forEach { taskName ->
        tasks.named<JavaExec>(taskName) {
            // Avro 1.12는 스키마 풀네임과 같은 클래스를 로드하기 전에 패키지 허용 목록을 본다.
            // ch03이 만든 customerManagement.avro.Customer가 이 모듈 클래스패스에 있다.
            jvmArgs("-Dorg.apache.avro.SERIALIZABLE_PACKAGES=customerManagement.avro")
        }
    }
}
