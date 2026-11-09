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
}

dependencies {
    implementation(project(":common"))
}

application {
    mainClass.set("xyz.mon0mon.ch04.s02_create_consumer.CurrentCreateConsumer")
}
