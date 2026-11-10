package xyz.mon0mon.ch05.s03_topic_management

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import xyz.mon0mon.common.KafkaSettings
import java.util.Properties
import java.util.concurrent.ExecutionException

private val logger = KotlinLogging.logger {}

/**
 * Book: §5.3 Essential Topic Management
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 0.11 `AdminClient`, `listTopics`, `describeTopics`, `createTopics`, `deleteTopics`.
 * - 2.x  `DescribeTopicsResult.values()`로 토픽별 `KafkaFuture`를 꺼낸다 (책 2판, 이 파일).
 *        파티션 수와 복제 계수를 비우면 브로커 기본값을 쓴다.
 * - 3.1  `TopicCollection`, `topicNameValues()`, `allTopicNames()`. `values()`와 `all()`은 deprecated.
 * - 4.0  `DescribeTopicsResult.values()`와 `all()` 삭제. `topicNameValues()`만 남았다.
 * - 4.4  `Admin.create`와 `use`가 권장 → [CurrentTopicManagement].
 *
 * Removed: `DescribeTopicsResult.values()`, `DescribeTopicsResult.all()`
 * Still valid: `AdminClient.create(Properties)`, `listTopics().names()`, `NewTopic(name, partitions, rf)`,
 *              `CreateTopicsResult.numPartitions()`, `deleteTopics(Collection).all()`
 *
 * 흐름:
 * 1. 5.2에서 만든 AdminClient로 클러스터의 토픽 이름을 나열한다. `names().get()`이 컨트롤러 응답을 기다린다.
 * 2. `demo-topic`을 describe한다. 없으면 파티션 6, 복제 1로 만들고 `numPartitions()`로 요청이 받아들여졌는지 확인한다.
 * 3. 토픽을 지운 뒤 새 describe로 다시 본다. 삭제는 비동기라서 직후에는 토픽이 아직 보일 수 있다.
 */
object BookTopicManagement {
    private const val TOPIC_NAME = "demo-topic"
    private val TOPIC_LIST = listOf(TOPIC_NAME)
    private const val NUM_PARTITIONS = 6
    private const val REP_FACTOR: Short = 1

    @JvmStatic
    fun main(args: Array<String>) {
        // 5.2에서 만든 AdminClient를 이 절이 사용한다. bootstrap만 넣는다.
        // 1.x
        // props["bootstrap.servers"] = "localhost:9092"
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        val admin = AdminClient.create(props)
        try {
            // region 5.3 필수적인 토픽 관리 기능
            // listTopics()는 바로 이름이 아니라 Future를 준다. get()이 끝나야 목록이 있다.
            val topics = admin.listTopics()
            topics.names().get().forEach { name ->
                logger.info { name }
            }

            // 없는 토픽도 예외가 호출 즉시 나지 않는다. 토픽별 Future의 get()에서 난다.
            val demoTopic = admin.describeTopics(TOPIC_LIST)
            try {
                // 4.4: 책이 values(). 4.0에서 삭제. topicNameValues().
                val topicDescription = demoTopic.topicNameValues().getValue(TOPIC_NAME).get()
                logger.info { "Description of demo topic:$topicDescription" }

                if (topicDescription.partitions().size != NUM_PARTITIONS) {
                    logger.info { "Topic has wrong number of partitions." }
                }
            } catch (e: ExecutionException) {
                // 토픽이 없을 때만 만든다. 권한 오류나 타임아웃은 그대로 올린다.
                if (e.cause !is UnknownTopicOrPartitionException) {
                    logger.error(e) { "describe failed" }
                    throw e
                }

                logger.info { "Topic $TOPIC_NAME does not exist. Going to create it now" }
                // 파티션 수나 복제 계수를 비우면 브로커의 num.partitions, default.replication.factor를 쓴다.
                // admin.createTopics(listOf(NewTopic(TOPIC_NAME, Optional.empty(), Optional.empty())))
                val newTopic = admin.createTopics(
                    listOf(NewTopic(TOPIC_NAME, NUM_PARTITIONS, REP_FACTOR)),
                )
                // numPartitions()도 Future다. 컨트롤러가 생성을 받아들인 뒤의 파티션 수다.
                if (newTopic.numPartitions(TOPIC_NAME).get() != NUM_PARTITIONS) {
                    logger.info { "Topic was created with wrong number of partitions. Exiting." }
                }
            }

            // 삭제를 컨트롤러가 받아들여도, 브로커 메타데이터에서는 잠시 토픽이 남을 수 있다.
            admin.deleteTopics(TOPIC_LIST).all().get()
            try {
                admin.describeTopics(TOPIC_LIST).topicNameValues().getValue(TOPIC_NAME).get()
                logger.info { "Topic $TOPIC_NAME is still around" }
            } catch (e: ExecutionException) {
                if (e.cause is UnknownTopicOrPartitionException) {
                    logger.info { "Topic $TOPIC_NAME is gone" }
                } else {
                    logger.error(e) { "describe after delete failed" }
                    throw e
                }
            }
            // endregion
        } finally {
            admin.close()
        }
    }
}
