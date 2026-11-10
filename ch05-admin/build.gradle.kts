import xyz.mon0mon.gradle.KafkaCluster

plugins {
    id("chapter.conventions")
}

chapter {
    kafkaCluster.set(KafkaCluster.SINGLE)
    examples.put("S03Book", "xyz.mon0mon.ch05.s03_topic_management.BookTopicManagement")
    examples.put("S03Current", "xyz.mon0mon.ch05.s03_topic_management.CurrentTopicManagement")
}

dependencies {
    implementation(project(":common"))
}

application {
    mainClass.set("xyz.mon0mon.ch05.s03_topic_management.CurrentTopicManagement")
}
