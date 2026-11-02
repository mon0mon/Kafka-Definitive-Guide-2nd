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
