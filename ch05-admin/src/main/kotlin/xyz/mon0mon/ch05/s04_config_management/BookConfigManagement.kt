package xyz.mon0mon.ch05.s04_config_management

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import xyz.mon0mon.common.KafkaSettings
import java.util.Properties
import java.util.concurrent.ExecutionException

private val logger = KotlinLogging.logger {}

/**
 * Book: §5.4 설정 관리
 * Track: book
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 0.11 `describeConfigs`, `alterConfigs`. `alterConfigs`는 넘긴 맵으로 그 리소스의 설정을 통째로 바꾼다.
 * - 1.1  `ConfigEntry.source()`. 기본값과 덮어쓴 값을 구분한다.
 * - 2.3  `incrementalAlterConfigs` (KIP-339). `AlterConfigOp`로 SET, DELETE, APPEND, SUBTRACT만 적용한다.
 *        책 2판은 이 API를 쓴다. `alterConfigs`는 이때부터 deprecated.
 * - 4.0  `Admin.alterConfigs` 삭제. `incrementalAlterConfigs`만 남았다.
 * - 4.4  `describeConfigs`와 `incrementalAlterConfigs`는 그대로다. 진입점은 `Admin.create` → [CurrentConfigManagement].
 *
 * Removed: `Admin.alterConfigs(Map<ConfigResource, Config>)`
 * Still valid: `describeConfigs`, `DescribeConfigsResult.all()`, `incrementalAlterConfigs`,
 *              `ConfigResource`, `ConfigEntry`, `AlterConfigOp`
 *
 * 흐름:
 * 1. 5.2에서 만든 AdminClient로 `ch05-config` 토픽을 확인한다. 책은 이미 있는 토픽의 설정을 본다. 없으면 파티션 1, 복제 1로 만든다.
 * 2. `ConfigResource(TOPIC, 이름)`으로 `describeConfigs`를 호출하고 `all().get()`에서 `Config`를 꺼낸다.
 *    엔트리에는 브로커 기본값이 포함된다. `isDefault()`와 `source()`로 덮어쓴 값만 가른다.
 * 3. `cleanup.policy`에 `compact`가 있는지 본다. `delete,compact`도 압착이 켜진 것이다. 기본값 `delete`는 아니다.
 * 4. `retention.ms`를 하루(86400000)로 SET한다. 지정하지 않은 키는 그대로다.
 * 5. 같은 키를 describe해서 `DYNAMIC_TOPIC_CONFIG`로 바뀌었는지 본다.
 * 6. DELETE로 그 덮어쓰기를 지운다. 값은 브로커 기본값으로 돌아간다.
 */
object BookConfigManagement {
    private const val TOPIC_NAME = "ch05-config"
    private const val NUM_PARTITIONS = 1
    private const val REP_FACTOR: Short = 1

    // 1.x "retention.ms"
    private const val RETENTION_MS = "86400000"

    @JvmStatic
    fun main(args: Array<String>) {
        // 1.x
        // props["bootstrap.servers"] = "localhost:9092"
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = KafkaSettings.bootstrapServers()
        val admin = AdminClient.create(props)
        try {
            // region 5.4 설정 관리
            // describeConfigs는 없는 토픽에서 UnknownTopicOrPartitionException을 준다.
            // 책은 앞 절에서 만든 토픽을 그대로 쓴다. 이 예제는 혼자 실행되므로 없으면 만든다.
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

            // 타입과 이름 한 쌍이 설정을 가진 리소스다. 브로커면 타입은 BROKER, 이름은 브로커 id다.
            val resource = ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME)
            // all()은 요청한 리소스가 모두 성공해야 끝난다. 하나라도 실패하면 이 Future가 실패한다.
            val config = admin.describeConfigs(listOf(resource)).all().get().getValue(resource)
            logConfigs(config, "before")
            // 1.x "cleanup.policy", "compact"
            // 값은 리스트다. compact만인 경우와 delete,compact인 경우 모두 압착이 켜져 있다.
            val compaction = config.get(TopicConfig.CLEANUP_POLICY_CONFIG)
            val compacted = compaction.value()?.contains(TopicConfig.CLEANUP_POLICY_COMPACT) == true
            if (compacted) {
                logger.info { "$TOPIC_NAME is compacted. cleanup.policy=${compaction.value()}" }
            } else {
                logger.info { "$TOPIC_NAME is not compacted. cleanup.policy=${compaction.value()}" }
            }

            // alterConfigs(리소스 → Config)는 4.0에서 삭제됐다.
            // 그 API는 맵에 없는 키를 기본값으로 되돌린다. 민감한 값은 describe에 안 나오므로 읽어서 다시 쓰는 방식으로도 지키지 못한다.
            // incrementalAlterConfigs는 이 연산만 적용한다. SET은 값을 덮어쓴다.
            val setOp = AlterConfigOp(
                ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, RETENTION_MS),
                AlterConfigOp.OpType.SET,
            )
            admin.incrementalAlterConfigs(mapOf(resource to listOf(setOp))).all().get()

            val updated = admin.describeConfigs(listOf(resource)).all().get().getValue(resource)
            val retention = updated.get(TopicConfig.RETENTION_MS_CONFIG)
            logger.info {
                "after set retention.ms=${retention.value()} default=${retention.isDefault()} source=${retention.source()}"
            }
            if (retention.value() != RETENTION_MS) {
                logger.error { "retention.ms was not updated. actual=${retention.value()} expected=$RETENTION_MS" }
            }

            // DELETE는 토픽에 덮어쓴 값을 지운다. 인자의 value는 쓰이지 않는다.
            // APPEND와 SUBTRACT는 cleanup.policy 같은 리스트 값에만 쓴다.
            val deleteOp = AlterConfigOp(
                ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, null),
                AlterConfigOp.OpType.DELETE,
            )
            admin.incrementalAlterConfigs(mapOf(resource to listOf(deleteOp))).all().get()

            val restored = admin.describeConfigs(listOf(resource)).all().get().getValue(resource)
            val restoredRetention = restored.get(TopicConfig.RETENTION_MS_CONFIG)
            logger.info {
                "after delete retention.ms=${restoredRetention.value()} default=${restoredRetention.isDefault()} source=${restoredRetention.source()}"
            }
            // endregion
        } finally {
            admin.close()
        }
    }

    // 책은 엔트리마다 이름과 값을 출력한다. 대부분은 브로커 기본값이다.
    private fun logConfigs(config: Config, phase: String) {
        logger.info { "$phase entries=${config.entries().size}" }
        config.entries().forEach { entry ->
            logger.info {
                "$phase ${entry.name()}=${entry.value()} default=${entry.isDefault()} readonly=${entry.isReadOnly()} sensitive=${entry.isSensitive()} source=${entry.source()}"
            }
        }
    }
}
