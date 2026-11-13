package xyz.mon0mon.ch05.s05_consumer_group

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.ListGroupsOptions
import org.apache.kafka.clients.admin.ListOffsetsResult
import org.apache.kafka.clients.admin.MemberToRemove
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.OffsetSpec
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
import java.util.Properties
import java.util.concurrent.ExecutionException

private val logger = KotlinLogging.logger {}

/**
 * Book: §5.5 컨슈머 그룹 관리
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 0.11 `listConsumerGroups`, `describeConsumerGroups`.
 * - 2.0  `listConsumerGroupOffsets`.
 * - 2.3  `removeMembersFromConsumerGroup`. `MemberToRemove`는 `group.instance.id`다.
 * - 2.4  `deleteConsumerGroupOffsets`, `alterConsumerGroupOffsets`. 그룹에 멤버가 있으면 실패한다.
 *        책 2판은 이 API로 커밋 오프셋을 조회하고 바꾼다.
 * - 2.8  `deleteConsumerGroups`.
 * - 4.0  `ConsumerGroupDescription.state()` deprecated. `groupState()`.
 * - 4.1  `listConsumerGroups`와 `ConsumerGroupListing`이 deprecated, `forRemoval`.
 *        이 파일은 `listGroups(ListGroupsOptions.forConsumerGroups())`를 호출한다.
 * - 4.4  `Admin.create`가 권장 → [CurrentConsumerGroupManagement].
 *
 * Removed: 없음. `listConsumerGroups`는 삭제 예정이라 실행 코드에 두지 않는다.
 * Still valid: `listGroups(ListGroupsOptions.forConsumerGroups()).valid()`, `describeConsumerGroups().describedGroups()`,
 *              `listConsumerGroupOffsets().partitionsToOffsetAndMetadata()`,
 *              `listOffsets` + `OffsetSpec.latest()`, `ListOffsetsResult.all()`,
 *              `alterConsumerGroupOffsets().all()`, `deleteConsumerGroupOffsets().all()`,
 *              `deleteConsumerGroups().all()`
 *
 * 흐름:
 * 1. `ch05-consumer-group`이 없으면 파티션 1, 복제 1로 만든다. 책은 그룹이 이미 있다고 본다.
 * 2. 레코드 3건을 쓰고, 그룹 `ch05-s05`가 읽은 뒤 `commitSync`한다. 컨슈머를 닫아 그룹을 비운다.
 * 3. `listGroups`로 컨슈머 그룹만 본다. `valid()`는 브로커 일부 실패를 삼키므로 `errors()`도 본다.
 * 4. `describeConsumerGroups`로 상태와 멤버를 읽는다. 상태는 `groupState()`다.
 * 5. 정적 멤버(`group.instance.id`)가 있을 때만 `MemberToRemove`로 제외한다. 이 준비 그룹은 비어 있다.
 * 6. `listConsumerGroupOffsets`로 커밋을 읽고, `listOffsets(OffsetSpec.latest())`로 로그 끝을 읽어 lag을 계산한다.
 *    그다음 파티션마다 다음 읽기 위치를 0으로 `alterConsumerGroupOffsets`하고 lag을 다시 본다.
 * 7. 같은 파티션의 커밋을 `deleteConsumerGroupOffsets`로 지운다. 다음 컨슈머의 시작점은 `auto.offset.reset`에 맡긴다.
 * 8. `deleteConsumerGroups`로 그룹을 지우고, describe로 없어졌는지 확인한다.
 */
object BookConsumerGroupManagement {
    private const val TOPIC_NAME = "ch05-consumer-group"
    private const val GROUP_ID = "ch05-s05"
    private val GROUP_LIST = listOf(GROUP_ID)
    private const val NUM_PARTITIONS = 1
    private const val REP_FACTOR: Short = 1
    private const val RECORD_COUNT = 3

    @JvmStatic
    fun main(args: Array<String>) {
        // 1.x
        // props["bootstrap.servers"] = "localhost:9092"
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        val admin = AdminClient.create(props)
        try {
            // region 5.5 컨슈머 그룹 관리
            ensureTopic(admin)
            // 오프셋을 바꿀 그룹이 있어야 한다. 쓰고, 읽고, 커밋한 뒤 멤버를 나간다.
            commitSampleOffsets()
            awaitEmpty(admin)

            // 4.4: 책이 listConsumerGroups().valid(). 4.1부터 deprecated, forRemoval.
            // ConsumerGroupListing도 같이 빠진다. forConsumerGroups()가 그 목록이다.
            // valid()는 실패한 브로커를 빼 버린다. 빈 목록이 전체 성공은 아니다.
            val listed = admin.listGroups(ListGroupsOptions.forConsumerGroups())
            val listErrors = listed.errors().get()
            if (listErrors.isNotEmpty()) {
                listErrors.forEach { error -> logger.error(error) { "listGroups partial failure" } }
            }
            listed.valid().get().forEach { group ->
                logger.info { "group=${group.groupId()} state=${group.groupState().orElse(null)} simple=${group.isSimpleConsumerGroup()}" }
            }

            // describedGroups()는 그룹 id에서 KafkaFuture로 가는 맵이다. get()이 코디네이터 응답을 기다린다.
            val description = admin.describeConsumerGroups(GROUP_LIST)
                .describedGroups()
                .getValue(GROUP_ID)
                .get()
            // 4.4: 책이 state(). 4.0부터 deprecated. groupState().
            logger.info {
                "describe group=$GROUP_ID state=${description.groupState()} assignor=${description.partitionAssignor()} members=${description.members().size}"
            }

            // MemberToRemove에 넣는 값은 consumerId()가 아니다. 정적 멤버의 group.instance.id다.
            // 빈 컬렉션은 IllegalArgumentException이다. 인자 없는 옵션은 멤버 전체를 내보낸다.
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

            val offsets = admin.listConsumerGroupOffsets(GROUP_ID)
                .partitionsToOffsetAndMetadata()
                .get()
            // 커밋은 다음에 읽을 위치다. 최신 메시지와의 거리는 파티션 리더의 로그 끝과 비교해야 나온다.
            logLag(admin, "committed", offsets)

            // 그룹에 멤버가 있으면 코디네이터가 거절한다. 바뀐 값은 다음 할당 때 읽힌다.
            val prepareOffsets = HashMap<TopicPartition, OffsetAndMetadata>()
            offsets.forEach { (partition, metadata) ->
                if (metadata != null) {
                    prepareOffsets[partition] = OffsetAndMetadata(0)
                }
            }
            if (prepareOffsets.isEmpty()) {
                logger.info { "no committed offsets to alter" }
            } else {
                admin.alterConsumerGroupOffsets(GROUP_ID, prepareOffsets).all().get()
                val altered = admin.listConsumerGroupOffsets(GROUP_ID)
                    .partitionsToOffsetAndMetadata()
                    .get()
                // 0으로 되돌리면 lag은 로그 끝과 같아진다. 그룹이 비어 있으므로 읽는 컨슈머는 아직 없다.
                logLag(admin, "altered", altered)
            }

            // 오프셋을 지우면 그룹은 남아도 커밋은 없다. 다음 기동은 auto.offset.reset을 따른다.
            val partitions = offsets.keys
            if (partitions.isNotEmpty()) {
                admin.deleteConsumerGroupOffsets(GROUP_ID, partitions).all().get()
                val afterDelete = admin.listConsumerGroupOffsets(GROUP_ID)
                    .partitionsToOffsetAndMetadata()
                    .get()
                // 커밋이 없으면 뺄 위치가 없다. 다음 기동은 auto.offset.reset에 맡긴다.
                logLag(admin, "offsets deleted", afterDelete)
            }

            try {
                admin.deleteConsumerGroups(GROUP_LIST).all().get()
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
                val again = admin.describeConsumerGroups(GROUP_LIST)
                    .describedGroups()
                    .getValue(GROUP_ID)
                    .get()
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
        } finally {
            admin.close()
        }
    }

    private fun ensureTopic(admin: AdminClient) {
        val exists = try {
            // 4.4: 책이 values(). 4.0에서 삭제. topicNameValues().
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
            KafkaSettings.stringProducerProperties(clientId = "ch05-s05-book"),
        ).use { producer ->
            repeat(RECORD_COUNT) { index ->
                producer.send(ProducerRecord(TOPIC_NAME, "k$index", "v$index")).get()
            }
        }

        val consumerProps = Properties()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = GROUP_ID
        consumerProps[ConsumerConfig.CLIENT_ID_CONFIG] = "ch05-s05-book-consumer"
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumerProps[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
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
            // 처리한 다음 위치가 그룹 오프셋으로 남는다. 멤버가 나간 뒤에 Admin이 그 값을 바꾼다.
            consumer.commitSync()
            logger.info { "committed group=$GROUP_ID records=$seen" }
        }
    }

    private fun awaitEmpty(admin: AdminClient) {
        val deadline = System.currentTimeMillis() + 15_000
        var lastState = "missing"
        while (System.currentTimeMillis() < deadline) {
            try {
                val description = admin.describeConsumerGroups(GROUP_LIST)
                    .describedGroups()
                    .getValue(GROUP_ID)
                    .get()
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
    admin: AdminClient,
    label: String,
    committed: Map<TopicPartition, OffsetAndMetadata?>,
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
    // 4.4: ListOffsetsResult.all()은 남아 있다. DescribeTopicsResult.all()과는 다르다.
    val latest: Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> =
        admin.listOffsets(request).all().get()
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
