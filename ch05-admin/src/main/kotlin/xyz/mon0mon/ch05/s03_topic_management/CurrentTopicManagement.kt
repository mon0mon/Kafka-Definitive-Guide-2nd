package xyz.mon0mon.ch05.s03_topic_management

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.TopicCollection
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import xyz.mon0mon.common.KafkaSettings
import java.util.concurrent.ExecutionException

private val logger = KotlinLogging.logger {}

/**
 * Book: §5.3 Essential Topic Management
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 0.11 `AdminClient`와 `Properties`.
 * - 2.x  `DescribeTopicsResult.values()` ([BookTopicManagement]).
 * - 3.1  `TopicCollection`, `topicNameValues()`.
 * - 4.0  `values()` / `all()` 삭제.
 * - 4.4  `Admin.create(Map)`과 `use`. 토픽은 이름이든 ID든 `TopicCollection`으로 넘긴다.
 *        내부 토픽은 `ListTopicsOptions.listInternal(false)`로 뺀다.
 *
 * Replaces: [BookTopicManagement]
 * Removed: `DescribeTopicsResult.values()`, `DescribeTopicsResult.all()`
 * Still valid: `listTopics`, `createTopics`, `NewTopic`, `deleteTopics`, `numPartitions()`
 *
 * 흐름:
 * 1. `Admin`을 열고 사용자 토픽 이름만 나열한다.
 * 2. `TopicCollection.ofTopicNames`로 `demo-topic`을 describe한다. 없으면 파티션 6, 복제 1로 만들고 파티션 수와 복제 계수를 확인한다.
 * 3. 이름 기준으로 지운 뒤 새 describe로 확인한다. 삭제가 전파되기 전이면 토픽이 아직 보인다.
 */
object CurrentTopicManagement {
    private const val TOPIC_NAME = "demo-topic"
    private val TOPIC_LIST = listOf(TOPIC_NAME)
    private const val NUM_PARTITIONS = 6
    private const val REP_FACTOR: Short = 1

    @JvmStatic
    fun main(args: Array<String>) {
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            AdminClientConfig.CLIENT_ID_CONFIG to "ch05-s03-current",
        )

        Admin.create(configs).use { admin ->
            // region 5.3 필수적인 토픽 관리 기능
            // __consumer_offsets 같은 내부 토픽은 이 절의 대상이 아니다.
            val names = admin.listTopics(ListTopicsOptions().listInternal(false)).names().get()
            logger.info { "topics=${names.sorted()}" }

            val topicDescription = try {
                admin.describeTopics(TopicCollection.ofTopicNames(TOPIC_LIST))
                    .topicNameValues()
                    .getValue(TOPIC_NAME)
                    .get()
            } catch (e: ExecutionException) {
                if (e.cause !is UnknownTopicOrPartitionException) {
                    logger.error(e) { "describe failed" }
                    throw e
                }
                null
            }

            if (topicDescription != null) {
                val partitions = topicDescription.partitions()
                logger.info {
                    "exists name=${topicDescription.name()} partitions=${partitions.size} leaders=${partitions.map { it.leader().id() }}"
                }
                if (partitions.size != NUM_PARTITIONS) {
                    logger.error { "Topic has wrong number of partitions. actual=${partitions.size} expected=$NUM_PARTITIONS" }
                }
            } else {
                logger.info { "Topic $TOPIC_NAME does not exist. Going to create it now" }
                val created = admin.createTopics(
                    listOf(NewTopic(TOPIC_NAME, NUM_PARTITIONS, REP_FACTOR)),
                )
                val partitions = created.numPartitions(TOPIC_NAME).get()
                val replication = created.replicationFactor(TOPIC_NAME).get()
                logger.info { "created name=$TOPIC_NAME partitions=$partitions replication=$replication" }
                if (partitions != NUM_PARTITIONS) {
                    logger.error { "Topic was created with wrong number of partitions. actual=$partitions expected=$NUM_PARTITIONS" }
                }
            }

            // 토픽별 Future를 기다린다. all()은 배치 전체라서 어느 토픽이 실패했는지 바로 안 보인다.
            admin.deleteTopics(TopicCollection.ofTopicNames(TOPIC_LIST))
                .topicNameValues()
                .getValue(TOPIC_NAME)
                .get()
            logger.info { "delete accepted name=$TOPIC_NAME" }

            try {
                val again = admin.describeTopics(TopicCollection.ofTopicNames(TOPIC_LIST))
                    .topicNameValues()
                    .getValue(TOPIC_NAME)
                    .get()
                logger.info { "Topic $TOPIC_NAME is still around partitions=${again.partitions().size}" }
            } catch (e: ExecutionException) {
                if (e.cause is UnknownTopicOrPartitionException) {
                    logger.info { "Topic $TOPIC_NAME is gone" }
                } else {
                    logger.error(e) { "describe after delete failed" }
                    throw e
                }
            }
            // endregion
        }
    }
}
