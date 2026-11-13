package xyz.mon0mon.ch05.s07_advanced_admin

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewPartitionReassignment
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.admin.RecordsToDelete
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ElectionNotNeededException
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException
import org.apache.kafka.common.errors.NoReassignmentInProgressException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import xyz.mon0mon.common.KafkaSettings
import java.util.Optional
import java.util.Properties
import java.util.concurrent.ExecutionException

private val logger = KotlinLogging.logger {}

/**
 * Book: §5.7 고급 어드민 작업
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 0.11 `deleteRecords` (KIP-107). 지정한 오프셋보다 작은 레코드를 지운다.
 * - 1.0  `createPartitions` (KIP-195). 파티션은 늘릴 수만 있다.
 * - 2.4  `electLeaders` (KIP-460). `electPreferredLeaders`는 이때부터 deprecated.
 * - 2.4  `alterPartitionReassignments`, `listPartitionReassignments` (KIP-455).
 * - 4.0  `electPreferredLeaders` 삭제. `electLeaders(ElectionType, Set)`만 남았다.
 * - 4.4  `Admin.create`와 `use`가 권장 → [CurrentAdvancedAdmin].
 *
 * Removed: `Admin.electPreferredLeaders`
 * Still valid: `createPartitions`, `NewPartitions.increaseTo`, `deleteRecords`, `RecordsToDelete.beforeOffset`,
 *              `electLeaders`, `ElectionType.PREFERRED`, `ElectionType.UNCLEAN`,
 *              `alterPartitionReassignments`, `NewPartitionReassignment`, `listPartitionReassignments`
 *
 * 흐름:
 * 1. 5.2에서 만든 AdminClient로 `ch05-advanced`를 지우고 파티션 1, 복제 1로 다시 만든다.
 *    파티션 수는 줄일 수 없어서, 매 실행이 `increaseTo`를 타게 토픽을 비운다.
 * 2. `NewPartitions.increaseTo(2)`로 파티션을 2개까지 늘린다. 기존 레코드는 그대로 두고, 키의 파티션만 이후 레코드부터 바뀐다.
 * 3. 파티션 0에 레코드 3건을 넣고, 그 로그 끝보다 앞선 오프셋을 `deleteRecords`로 지운다.
 * 4. 파티션 0에 선호 리더 선출과 언클린 선출을 요청한다. 리더가 이미 있으면 `ElectionNotNeededException`이다.
 * 5. 현재 레플리카 목록으로 재할당을 요청한 뒤 진행 중 목록을 보고, `Optional.empty()`로 취소를 요청한다.
 *    이 실습 브로커는 node.id 1 하나라서 다른 브로커로 옮기지 않는다.
 */
object BookAdvancedAdmin {
    private const val TOPIC_NAME = "ch05-advanced"
    private const val INITIAL_PARTITIONS = 1
    private const val TARGET_PARTITIONS = 2
    private const val REP_FACTOR: Short = 1
    private const val RECORD_COUNT = 3
    private val PARTITION = TopicPartition(TOPIC_NAME, 0)

    @JvmStatic
    fun main(args: Array<String>) {
        // 1.x
        // props["bootstrap.servers"] = "localhost:9092"
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        val admin = AdminClient.create(props)
        try {
            // 책은 이미 있는 customers 토픽을 고친다. 이 예제는 혼자 실행되고, 파티션은 줄일 수 없다.
            recreateTopic(admin)

            // region 5.7.1 토픽에 파티션 추가하기
            // increaseTo의 인자는 추가분이 아니라 늘어난 뒤의 총 파티션 수다.
            // 키가 있으면 hash(key) % 파티션 수가 바뀌어 이후 같은 키도 다른 파티션으로 간다. 이미 쌓인 레코드는 옮기지 않는다.
            admin.createPartitions(
                mapOf(TOPIC_NAME to NewPartitions.increaseTo(TARGET_PARTITIONS)),
            ).all().get()
            val partitions = awaitPartitionCount(admin, TARGET_PARTITIONS)
            logger.info { "partitions increased to $partitions" }
            // endregion

            // region 5.7.2 토픽에서 레코드 삭제하기
            // 지울 오프셋이 있게 파티션 0에만 레코드를 넣는다. 방금 생긴 파티션 1은 비어 있다.
            produceRecords()
            val endOffset = logOffset(admin, OffsetSpec.latest())
            // beforeOffset은 그 오프셋 자체는 남기고, 그보다 작은 오프셋만 지운다. 로그 끝을 넘기면 안 된다.
            val deleted = admin.deleteRecords(
                mapOf(PARTITION to RecordsToDelete.beforeOffset(endOffset)),
            )
            deleted.all().get()
            val lowWatermark = deleted.lowWatermarks().getValue(PARTITION).get().lowWatermark()
            val earliest = logOffset(admin, OffsetSpec.earliest())
            logger.info { "deleted before offset=$endOffset lowWatermark=$lowWatermark earliest=$earliest" }
            // endregion

            // region 5.7.3 리더 선출
            // 4.4: 책은 electPreferredLeaders일 수 있다. 2.4에서 deprecated, 4.0에서 삭제. electLeaders(PREFERRED).
            // 선호 레플리카는 레플리카 목록의 첫 브로커다. 이미 리더면 선출은 필요 없다.
            elect(admin, ElectionType.PREFERRED)
            // 언클린은 ISR 밖의 레플리카도 리더로 올려 그 레플리카에 없는 레코드를 버린다.
            // 리더가 살아 있으면 컨트롤러가 거절한다. unclean.leader.election.enable은 바꾸지 않는다.
            elect(admin, ElectionType.UNCLEAN)
            // endregion

            // region 5.7.4 레플리카 재할당
            // 목표 목록의 첫 브로커가 새 선호 레플리카다. 없는 브로커 id는 INVALID_REPLICA_ASSIGNMENT다.
            val replicaIds = replicaIds(admin)
            logger.info { "current replicas=$replicaIds" }
            alterReplicas(admin, Optional.of(NewPartitionReassignment(replicaIds)))
            val ongoing = admin.listPartitionReassignments().reassignments().get()
            if (ongoing.isEmpty()) {
                logger.info { "no partition reassignment in progress" }
            } else {
                ongoing.forEach { (partition, reassignment) ->
                    logger.info {
                        "reassigning $partition replicas=${reassignment.replicas()} " +
                            "adding=${reassignment.addingReplicas()} removing=${reassignment.removingReplicas()}"
                    }
                }
            }
            // Optional.empty()는 진행 중인 재할당을 취소한다. 진행 중이 아니면 NO_REASSIGNMENT_IN_PROGRESS다.
            alterReplicas(admin, Optional.empty())
            // endregion
        } finally {
            admin.close()
        }
    }

    private fun recreateTopic(admin: AdminClient) {
        try {
            admin.deleteTopics(listOf(TOPIC_NAME)).all().get()
            logger.info { "deleting $TOPIC_NAME" }
        } catch (e: ExecutionException) {
            if (e.cause !is UnknownTopicOrPartitionException) {
                logger.error(e) { "delete topic failed" }
                throw e
            }
        }

        val deadline = System.currentTimeMillis() + 20_000
        while (System.currentTimeMillis() < deadline) {
            val gone = try {
                admin.describeTopics(listOf(TOPIC_NAME)).topicNameValues().getValue(TOPIC_NAME).get()
                false
            } catch (e: ExecutionException) {
                if (e.cause is UnknownTopicOrPartitionException) {
                    true
                } else {
                    logger.error(e) { "describe while deleting failed" }
                    throw e
                }
            }
            if (gone) {
                admin.createTopics(
                    listOf(NewTopic(TOPIC_NAME, INITIAL_PARTITIONS, REP_FACTOR)),
                ).all().get()
                logger.info {
                    "created $TOPIC_NAME partitions=$INITIAL_PARTITIONS replication=$REP_FACTOR"
                }
                return
            }
            Thread.sleep(200)
        }
        error("$TOPIC_NAME is still present after delete")
    }

    private fun awaitPartitionCount(admin: AdminClient, expected: Int): Int {
        val deadline = System.currentTimeMillis() + 10_000
        var seen = 0
        while (System.currentTimeMillis() < deadline) {
            seen = admin.describeTopics(listOf(TOPIC_NAME))
                .topicNameValues()
                .getValue(TOPIC_NAME)
                .get()
                .partitions()
                .size
            if (seen >= expected) {
                return seen
            }
            // 컨트롤러가 파티션을 만들어도 브로커 메타데이터에는 잠시 뒤 반영된다.
            Thread.sleep(200)
        }
        return seen
    }

    private fun produceRecords() {
        KafkaProducer<String, String>(
            KafkaSettings.stringProducerProperties(clientId = "ch05-s07-book"),
        ).use { producer ->
            repeat(RECORD_COUNT) { index ->
                val metadata = producer.send(
                    ProducerRecord(TOPIC_NAME, 0, "k$index", "v$index"),
                ).get()
                logger.info { "produced partition=${metadata.partition()} offset=${metadata.offset()}" }
            }
        }
    }

    private fun logOffset(admin: AdminClient, spec: OffsetSpec): Long =
        admin.listOffsets(mapOf(PARTITION to spec)).all().get().getValue(PARTITION).offset()

    private fun elect(admin: AdminClient, electionType: ElectionType) {
        try {
            admin.electLeaders(electionType, setOf(PARTITION)).all().get()
            logger.info { "$electionType election accepted for $PARTITION" }
        } catch (e: ExecutionException) {
            val cause = generateSequence<Throwable>(e) { it.cause }
                .firstOrNull { it is ElectionNotNeededException }
            if (cause == null) {
                logger.error(e) { "$electionType election failed" }
                throw e
            }
            logger.info { "$electionType election not needed: ${cause.message}" }
        }
    }

    private fun replicaIds(admin: AdminClient): List<Int> {
        val info = admin.describeTopics(listOf(TOPIC_NAME))
            .topicNameValues()
            .getValue(TOPIC_NAME)
            .get()
            .partitions()
            .first { it.partition() == PARTITION.partition() }
        return info.replicas().map { it.id() }
    }

    private fun alterReplicas(
        admin: AdminClient,
        reassignment: Optional<NewPartitionReassignment>,
    ) {
        try {
            admin.alterPartitionReassignments(mapOf(PARTITION to reassignment)).all().get()
            val action = if (reassignment.isPresent) "started" else "cancelled"
            logger.info { "reassignment $action for $PARTITION" }
        } catch (e: ExecutionException) {
            val cause = generateSequence<Throwable>(e) { it.cause }.firstOrNull { current ->
                current is InvalidReplicaAssignmentException || current is NoReassignmentInProgressException
            }
            if (cause == null) {
                logger.error(e) { "alter partition reassignment failed" }
                throw e
            }
            // 같은 레플리카 목록은 이동이 아니다. 취소할 재할당이 없으면 85번 오류다.
            logger.info { "reassignment not applied: ${cause.message}" }
        }
    }
}
