package xyz.mon0mon.ch05.s07_advanced_admin

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions
import org.apache.kafka.clients.admin.CreatePartitionsOptions
import org.apache.kafka.clients.admin.DeleteRecordsOptions
import org.apache.kafka.clients.admin.ElectLeadersOptions
import org.apache.kafka.clients.admin.ListOffsetsOptions
import org.apache.kafka.clients.admin.ListPartitionReassignmentsOptions
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
import java.util.concurrent.ExecutionException

private val logger = KotlinLogging.logger {}

/**
 * Book: §5.7 고급 어드민 작업
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 0.11 `deleteRecords` ([BookAdvancedAdmin]).
 * - 1.0  `createPartitions`. `CreatePartitionsOptions.validateOnly`는 적용하지 않고 검사만 한다.
 * - 2.4  `electLeaders`. `ElectionType.PREFERRED`와 `UNCLEAN`.
 * - 2.4  `alterPartitionReassignments`. 빈 `Optional`은 진행 중 재할당을 취소한다.
 * - 4.0  `electPreferredLeaders` 삭제.
 * - 4.4  `Admin.create(Map)`, `client.id`, 호출별 timeout, `use`.
 *        배치의 `all()` 대신 토픽·파티션 Future로 어느 항목이 실패했는지 본다.
 *
 * Replaces: [BookAdvancedAdmin]
 * Removed: `Admin.electPreferredLeaders`
 * Still valid: `createPartitions`, `NewPartitions.increaseTo`, `deleteRecords`, `RecordsToDelete.beforeOffset`,
 *              `electLeaders`, `alterPartitionReassignments`, `listPartitionReassignments`
 *
 * 흐름:
 * 1. `Admin`을 열고 `ch05-advanced`를 지운 뒤 파티션 1, 복제 1로 다시 만든다.
 * 2. `CreatePartitionsOptions`로 총 2개까지 늘리고, describe에 새 파티션이 보일 때까지 본다.
 * 3. 파티션 0에 레코드 3건을 넣고 `DeleteRecordsOptions`로 로그 끝 앞을 지운 뒤 low watermark를 읽는다.
 * 4. `ElectLeadersOptions`로 선호 선출과 언클린 선출을 하고 `partitions()`의 파티션별 오류를 로그로 남긴다.
 * 5. 현재 레플리카로 재할당을 요청하고, 그 파티션만 `listPartitionReassignments`로 본 다음 취소를 요청한다.
 */
object CurrentAdvancedAdmin {
    private const val TOPIC_NAME = "ch05-advanced"
    private const val INITIAL_PARTITIONS = 1
    private const val TARGET_PARTITIONS = 2
    private const val REP_FACTOR: Short = 1
    private const val RECORD_COUNT = 3
    private const val CLIENT_ID = "ch05-s07-current"
    private const val TIMEOUT_MS = 10_000
    private val PARTITION = TopicPartition(TOPIC_NAME, 0)

    @JvmStatic
    fun main(args: Array<String>) {
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            AdminClientConfig.CLIENT_ID_CONFIG to CLIENT_ID,
        )

        Admin.create(configs).use { admin ->
            recreateTopic(admin)

            // region 5.7.1 토픽에 파티션 추가하기
            // 총 파티션 수다. 키 있는 토픽은 이후 레코드의 파티션이 바뀐다. 쌓인 데이터는 재배치되지 않는다.
            admin.createPartitions(
                mapOf(TOPIC_NAME to NewPartitions.increaseTo(TARGET_PARTITIONS)),
                CreatePartitionsOptions().timeoutMs(TIMEOUT_MS),
            ).values().getValue(TOPIC_NAME).get()
            val partitions = awaitPartitionCount(admin, TARGET_PARTITIONS)
            logger.info { "partitions increased to $partitions clientId=$CLIENT_ID" }
            // endregion

            // region 5.7.2 토픽에서 레코드 삭제하기
            produceRecords()
            val endOffset = logOffset(admin, OffsetSpec.latest())
            // 지정 오프셋은 삭제 경계다. 그 오프셋의 레코드는 남고, 더 작은 오프셋이 로그 시작에서 빠진다.
            val deleted = admin.deleteRecords(
                mapOf(PARTITION to RecordsToDelete.beforeOffset(endOffset)),
                DeleteRecordsOptions().timeoutMs(TIMEOUT_MS),
            )
            val lowWatermark = deleted.lowWatermarks().getValue(PARTITION).get().lowWatermark()
            val earliest = logOffset(admin, OffsetSpec.earliest())
            logger.info {
                "deleted partition=${PARTITION.partition()} before=$endOffset " +
                    "lowWatermark=$lowWatermark earliest=$earliest"
            }
            // endregion

            // region 5.7.3 리더 선출
            // partitions()의 Optional이 비어 있으면 그 파티션은 선출에 성공한 것이다.
            // ElectionNotNeededException은 선호 레플리카가 이미 리더이거나, 언클린이 필요 없다는 뜻이다.
            elect(admin, ElectionType.PREFERRED)
            // 언클린은 데이터를 버릴 수 있다. 이 클러스터는 리더가 살아 있어 선출이 적용되지 않는다.
            elect(admin, ElectionType.UNCLEAN)
            // endregion

            // region 5.7.4 레플리카 재할당
            // 이 Compose의 node.id는 1이다. 목표를 현재 레플리카와 같게 두면 브로커 간 이동은 없다.
            val replicaIds = replicaIds(admin)
            logger.info { "current replicas=$replicaIds" }
            alterReplicas(admin, Optional.of(NewPartitionReassignment(replicaIds)))
            val ongoing = admin.listPartitionReassignments(
                setOf(PARTITION),
                ListPartitionReassignmentsOptions().timeoutMs(TIMEOUT_MS),
            ).reassignments().get()
            val reassignment = ongoing[PARTITION]
            if (reassignment == null) {
                logger.info { "no reassignment for $PARTITION" }
            } else {
                logger.info {
                    "reassigning $PARTITION replicas=${reassignment.replicas()} " +
                        "adding=${reassignment.addingReplicas()} removing=${reassignment.removingReplicas()}"
                }
            }
            alterReplicas(admin, Optional.empty())
            // endregion
        }
    }

    private fun recreateTopic(admin: Admin) {
        try {
            admin.deleteTopics(listOf(TOPIC_NAME)).topicNameValues().getValue(TOPIC_NAME).get()
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
                admin.createTopics(listOf(NewTopic(TOPIC_NAME, INITIAL_PARTITIONS, REP_FACTOR)))
                    .values()
                    .getValue(TOPIC_NAME)
                    .get()
                logger.info {
                    "created $TOPIC_NAME partitions=$INITIAL_PARTITIONS replication=$REP_FACTOR"
                }
                return
            }
            Thread.sleep(200)
        }
        error("$TOPIC_NAME is still present after delete")
    }

    private fun awaitPartitionCount(admin: Admin, expected: Int): Int {
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
            Thread.sleep(200)
        }
        return seen
    }

    private fun produceRecords() {
        KafkaProducer<String, String>(
            KafkaSettings.stringProducerProperties(clientId = CLIENT_ID),
        ).use { producer ->
            repeat(RECORD_COUNT) { index ->
                val metadata = producer.send(
                    ProducerRecord(TOPIC_NAME, 0, "k$index", "v$index"),
                ).get()
                logger.info { "produced partition=${metadata.partition()} offset=${metadata.offset()}" }
            }
        }
    }

    private fun logOffset(admin: Admin, spec: OffsetSpec): Long =
        admin.listOffsets(
            mapOf(PARTITION to spec),
            ListOffsetsOptions().timeoutMs(TIMEOUT_MS),
        ).partitionResult(PARTITION).get().offset()

    private fun elect(admin: Admin, electionType: ElectionType) {
        val results = admin.electLeaders(
            electionType,
            setOf(PARTITION),
            ElectLeadersOptions().timeoutMs(TIMEOUT_MS),
        ).partitions().get()
        val error = results[PARTITION]
        when {
            error == null -> logger.info { "$electionType returned no result for $PARTITION" }
            error.isEmpty -> logger.info { "$electionType election accepted for $PARTITION" }
            error.get() is ElectionNotNeededException ->
                logger.info { "$electionType election not needed: ${error.get().message}" }
            else -> {
                val failure = error.get()
                logger.error(failure) { "$electionType election failed" }
                throw failure
            }
        }
    }

    private fun replicaIds(admin: Admin): List<Int> {
        val info = admin.describeTopics(listOf(TOPIC_NAME))
            .topicNameValues()
            .getValue(TOPIC_NAME)
            .get()
            .partitions()
            .first { it.partition() == PARTITION.partition() }
        return info.replicas().map { it.id() }
    }

    private fun alterReplicas(
        admin: Admin,
        reassignment: Optional<NewPartitionReassignment>,
    ) {
        try {
            admin.alterPartitionReassignments(
                mapOf(PARTITION to reassignment),
                AlterPartitionReassignmentsOptions().timeoutMs(TIMEOUT_MS),
            ).values().getValue(PARTITION).get()
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
            logger.info { "reassignment not applied: ${cause.message}" }
        }
    }
}
