package xyz.mon0mon.ch05.s05_consumer_group

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec
import org.apache.kafka.clients.admin.ListGroupsOptions
import org.apache.kafka.clients.admin.ListOffsetsOptions
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.admin.MemberToRemove
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.GroupState
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.GroupIdNotFoundException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.serialization.StringDeserializer
import xyz.mon0mon.common.KafkaSettings
import java.time.Duration
import java.util.concurrent.ExecutionException

private val logger = KotlinLogging.logger {}

/**
 * Book: §5.5 컨슈머 그룹 관리
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.4  `alterConsumerGroupOffsets` ([BookConsumerGroupManagement]). 그룹은 비어 있어야 한다.
 * - 2.8  `deleteConsumerGroups`.
 * - 3.0  `listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec>)`. 여러 그룹을 한 호출에 담는다.
 * - 4.0  `groupState()`. `state()`는 deprecated.
 * - 4.1  `listConsumerGroups` deprecated. `listGroups(ListGroupsOptions.forConsumerGroups())`.
 * - 4.4  `Admin.create(Map)`과 `use`. 오프셋 변경과 삭제는 파티션 Future로 기다리고,
 *        그룹 삭제는 `deletedGroups()`로 어느 그룹이 실패했는지 본다.
 *
 * Replaces: [BookConsumerGroupManagement]
 * Still valid: `describeConsumerGroups`, `listConsumerGroupOffsets`, `listOffsets`, `OffsetSpec.latest()`,
 *              `alterConsumerGroupOffsets`, `deleteConsumerGroupOffsets`, `deleteConsumerGroups`,
 *              `removeMembersFromConsumerGroup`
 *
 * 흐름:
 * 1. `Admin`을 열고 `ch05-consumer-group`이 없으면 파티션 1, 복제 1로 만든다.
 * 2. 레코드 3건을 커밋한 뒤 컨슈머를 닫고, 그룹 `ch05-s05`가 `EMPTY`가 될 때까지 본다.
 * 3. `listGroups`로 컨슈머 그룹만 고른다. `valid()`와 `errors()`를 같이 본다.
 * 4. `groupState()`와 멤버를 describe한다. 정적 멤버만 `group.instance.id`로 제외할 수 있다.
 * 5. `ListConsumerGroupOffsetsSpec`으로 이 그룹의 커밋을 읽고, `OffsetSpec.latest()`로 로그 끝을 읽어 lag을 계산한다.
 *    파티션별로 0에 다시 쓴 뒤 lag을 다시 본다. 로그 끝 조회는 `partitionResult`로 기다린다.
 * 6. 파티션별로 커밋을 지운 뒤, `deletedGroups()`로 그룹 삭제를 확인하고 describe로 부재를 본다.
 */
object CurrentConsumerGroupManagement {
    private const val TOPIC_NAME = "ch05-consumer-group"
    private const val GROUP_ID = "ch05-s05"
    private val GROUP_LIST = listOf(GROUP_ID)
    private const val CLIENT_ID = "ch05-s05-current"
    private const val NUM_PARTITIONS = 1
    private const val REP_FACTOR: Short = 1
    private const val RECORD_COUNT = 3
    private const val TIMEOUT_MS = 10_000

    @JvmStatic
    fun main(args: Array<String>) {
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            AdminClientConfig.CLIENT_ID_CONFIG to CLIENT_ID,
        )

        Admin.create(configs).use { admin ->
            // region 5.5 컨슈머 그룹 관리
            ensureTopic(admin)
            commitSampleOffsets()
            awaitEmpty(admin)

            // classic 컨슈머와 KIP-848 컨슈머를 한 목록으로 받는다. share, streams 그룹은 빠진다.
            val listed = admin.listGroups(
                ListGroupsOptions.forConsumerGroups().timeoutMs(TIMEOUT_MS),
            )
            val listErrors = listed.errors().get()
            if (listErrors.isNotEmpty()) {
                listErrors.forEach { error -> logger.error(error) { "listGroups partial failure" } }
            }
            listed.valid().get().forEach { group ->
                logger.info {
                    "group=${group.groupId()} type=${group.type().orElse(null)} protocol=${group.protocol()} state=${group.groupState().orElse(null)}"
                }
            }

            val description = admin.describeConsumerGroups(
                GROUP_LIST,
                DescribeConsumerGroupsOptions().timeoutMs(TIMEOUT_MS),
            ).describedGroups().getValue(GROUP_ID).get()
            logger.info {
                "describe group=$GROUP_ID type=${description.type()} state=${description.groupState()} assignor=${description.partitionAssignor()} members=${description.members().size} coordinator=${description.coordinator()?.id()}"
            }

            // 동적 멤버의 consumerId()는 MemberToRemove에 쓸 수 없다. 정적 멤버의 instance id만 받는다.
            val instanceIds = description.members().mapNotNull { member -> member.groupInstanceId().orElse(null) }
            if (instanceIds.isNotEmpty()) {
                admin.removeMembersFromConsumerGroup(
                    GROUP_ID,
                    RemoveMembersFromConsumerGroupOptions(instanceIds.map { MemberToRemove(it) }),
                ).all().get()
                logger.info { "removed static members=$instanceIds" }
            } else {
                logger.info { "no static members. empty group can change offsets" }
            }

            val offsets = listCommitted(admin)
            // 커밋은 다음에 읽을 위치다. lag은 그 값과 파티션 리더의 로그 끝 차이다.
            logLag(admin, "committed", offsets, TIMEOUT_MS)

            val prepareOffsets = HashMap<TopicPartition, OffsetAndMetadata>()
            offsets.forEach { (partition, metadata) ->
                if (metadata != null) {
                    // 다음 읽기 위치다. 0이면 그 파티션의 맨 앞에서 다시 읽는다.
                    prepareOffsets[partition] = OffsetAndMetadata(0)
                }
            }
            if (prepareOffsets.isEmpty()) {
                logger.info { "no committed offsets to alter" }
            } else {
                val altered = admin.alterConsumerGroupOffsets(
                    GROUP_ID,
                    prepareOffsets,
                    AlterConsumerGroupOffsetsOptions().timeoutMs(TIMEOUT_MS),
                )
                // all()은 실패한 파티션을 한 예외로 묶는다. 파티션 Future로 어느 파티션인지 남긴다.
                prepareOffsets.keys.forEach { partition ->
                    altered.partitionResult(partition).get()
                }
                // 0으로 되돌리면 lag은 로그 끝과 같아진다. 그룹이 비어 있으므로 읽는 컨슈머는 아직 없다.
                logLag(admin, "altered", listCommitted(admin), TIMEOUT_MS)
            }

            val partitions = offsets.keys
            if (partitions.isNotEmpty()) {
                val deleted = admin.deleteConsumerGroupOffsets(
                    GROUP_ID,
                    partitions,
                    DeleteConsumerGroupOffsetsOptions().timeoutMs(TIMEOUT_MS),
                )
                partitions.forEach { partition ->
                    deleted.partitionResult(partition).get()
                }
                // 커밋이 없으면 뺄 위치가 없다. 다음 기동은 auto.offset.reset에 맡긴다.
                logLag(admin, "offsets deleted", listCommitted(admin), TIMEOUT_MS)
            }

            try {
                admin.deleteConsumerGroups(
                    GROUP_LIST,
                    DeleteConsumerGroupsOptions().timeoutMs(TIMEOUT_MS),
                ).deletedGroups().getValue(GROUP_ID).get()
                logger.info { "delete accepted group=$GROUP_ID" }
            } catch (e: ExecutionException) {
                if (e.cause is GroupIdNotFoundException) {
                    logger.info { "group $GROUP_ID was already gone" }
                } else {
                    logger.error(e) { "delete group failed" }
                    throw e
                }
            }

            try {
                val again = admin.describeConsumerGroups(
                    GROUP_LIST,
                    DescribeConsumerGroupsOptions().timeoutMs(TIMEOUT_MS),
                ).describedGroups().getValue(GROUP_ID).get()
                logger.info { "group $GROUP_ID is still around state=${again.groupState()}" }
            } catch (e: ExecutionException) {
                if (e.cause is GroupIdNotFoundException) {
                    logger.info { "group $GROUP_ID is gone" }
                } else {
                    logger.error(e) { "describe after delete failed" }
                    throw e
                }
            }
            // endregion
        }
    }

    private fun listCommitted(admin: Admin): Map<TopicPartition, OffsetAndMetadata?> {
        // 스펙에 파티션을 비우면 이 그룹이 커밋한 파티션을 모두 가져온다.
        return admin.listConsumerGroupOffsets(
            mapOf(GROUP_ID to ListConsumerGroupOffsetsSpec()),
        ).partitionsToOffsetAndMetadata(GROUP_ID).get()
    }

    private fun ensureTopic(admin: Admin) {
        val exists = try {
            admin.describeTopics(listOf(TOPIC_NAME)).topicNameValues().getValue(TOPIC_NAME).get()
            true
        } catch (e: ExecutionException) {
            if (e.cause !is UnknownTopicOrPartitionException) {
                logger.error(e) { "describe topic failed" }
                throw e
            }
            false
        }
        if (!exists) {
            admin.createTopics(listOf(NewTopic(TOPIC_NAME, NUM_PARTITIONS, REP_FACTOR))).all().get()
            logger.info { "created $TOPIC_NAME partitions=$NUM_PARTITIONS replication=$REP_FACTOR" }
        }
    }

    private fun commitSampleOffsets() {
        KafkaProducer<String, String>(
            KafkaSettings.stringProducerProperties(clientId = CLIENT_ID),
        ).use { producer ->
            repeat(RECORD_COUNT) { index ->
                producer.send(ProducerRecord(TOPIC_NAME, "k$index", "v$index")).get()
            }
        }

        val consumerProps = mapOf<String, Any>(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG to GROUP_ID,
            ConsumerConfig.CLIENT_ID_CONFIG to "$CLIENT_ID-consumer",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        )
        KafkaConsumer<String, String>(consumerProps).use { consumer ->
            consumer.subscribe(listOf(TOPIC_NAME))
            var seen = 0
            val deadline = System.currentTimeMillis() + 20_000
            while (seen < RECORD_COUNT && System.currentTimeMillis() < deadline) {
                seen += consumer.poll(Duration.ofMillis(500)).count()
            }
            if (seen == 0) {
                throw IllegalStateException("consumer read nothing from $TOPIC_NAME")
            }
            consumer.commitSync()
            logger.info { "committed group=$GROUP_ID records=$seen" }
        }
    }

    private fun awaitEmpty(admin: Admin) {
        val deadline = System.currentTimeMillis() + 15_000
        var lastState = "missing"
        while (System.currentTimeMillis() < deadline) {
            try {
                val description = admin.describeConsumerGroups(
                    GROUP_LIST,
                    DescribeConsumerGroupsOptions().timeoutMs(TIMEOUT_MS),
                ).describedGroups().getValue(GROUP_ID).get()
                lastState = description.groupState().toString()
                if (description.groupState() == GroupState.EMPTY && description.members().isEmpty()) {
                    return
                }
            } catch (e: ExecutionException) {
                if (e.cause !is GroupIdNotFoundException) {
                    throw e
                }
                lastState = "not-found"
            }
            Thread.sleep(200)
        }
        throw IllegalStateException("group $GROUP_ID did not become EMPTY. last=$lastState")
    }
}

private fun logLag(
    admin: Admin,
    label: String,
    committed: Map<TopicPartition, OffsetAndMetadata?>,
    timeoutMs: Int,
) {
    val request = HashMap<TopicPartition, OffsetSpec>()
    committed.forEach { (partition, metadata) ->
        if (metadata != null) {
            // latest()는 다음에 기록될 오프셋이다. 마지막 레코드 오프셋보다 1 크다.
            request[partition] = OffsetSpec.latest()
        }
    }
    if (request.isEmpty()) {
        logger.info { "$label offsets=none" }
        return
    }
    val listed = admin.listOffsets(request, ListOffsetsOptions().timeoutMs(timeoutMs))
    val latest = request.keys.associateWith { partition ->
        listed.partitionResult(partition).get()
    }
    committed.forEach { (partition, metadata) ->
        if (metadata == null) {
            return@forEach
        }
        val end = latest.getValue(partition).offset()
        val lag = end - metadata.offset()
        logger.info {
            "$label ${partition.topic()}-${partition.partition()} committed=${metadata.offset()} latest=$end lag=$lag"
        }
    }
}
