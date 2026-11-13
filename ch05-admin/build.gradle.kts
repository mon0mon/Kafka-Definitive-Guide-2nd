import xyz.mon0mon.gradle.KafkaCluster

plugins {
    id("chapter.conventions")
}

chapter {
    kafkaCluster.set(KafkaCluster.SINGLE)
    examples.put("S03Book", "xyz.mon0mon.ch05.s03_topic_management.BookTopicManagement")
    examples.put("S03Current", "xyz.mon0mon.ch05.s03_topic_management.CurrentTopicManagement")
    examples.put("S04Book", "xyz.mon0mon.ch05.s04_config_management.BookConfigManagement")
    examples.put("S04Current", "xyz.mon0mon.ch05.s04_config_management.CurrentConfigManagement")
    examples.put("S05Book", "xyz.mon0mon.ch05.s05_consumer_group.BookConsumerGroupManagement")
    examples.put("S05Current", "xyz.mon0mon.ch05.s05_consumer_group.CurrentConsumerGroupManagement")
    examples.put("S06Book", "xyz.mon0mon.ch05.s06_cluster_metadata.BookClusterMetadata")
    examples.put("S06Current", "xyz.mon0mon.ch05.s06_cluster_metadata.CurrentClusterMetadata")
    examples.put("S07Book", "xyz.mon0mon.ch05.s07_advanced_admin.BookAdvancedAdmin")
    examples.put("S07Current", "xyz.mon0mon.ch05.s07_advanced_admin.CurrentAdvancedAdmin")
}

dependencies {
    implementation(project(":common"))
}

application {
    mainClass.set("xyz.mon0mon.ch05.s03_topic_management.CurrentTopicManagement")
}
