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
}

dependencies {
    implementation(project(":common"))
}

application {
    mainClass.set("xyz.mon0mon.ch04.s02_create_consumer.CurrentCreateConsumer")
}
