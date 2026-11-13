package xyz.mon0mon.ch05.s04_config_management

import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.AlterConfigsOptions
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.DescribeConfigsOptions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import xyz.mon0mon.common.KafkaSettings
import java.util.concurrent.ExecutionException

private val logger = KotlinLogging.logger {}

/**
 * Book: §5.4 설정 관리
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 0.11 `describeConfigs`와 `alterConfigs`.
 * - 2.3  `incrementalAlterConfigs` ([BookConfigManagement]).
 * - 4.0  `alterConfigs` 삭제.
 * - 4.4  `Admin.create(Map)`과 `use`. describe와 alter는 리소스별 `values()`로 기다린다.
 *        `DescribeConfigsOptions.includeSynonyms(true)`면 `retention.ms` 옆에 `log.retention.ms` 같은 동의어가 붙는다.
 *
 * Replaces: [BookConfigManagement]
 * Removed: `Admin.alterConfigs(Map<ConfigResource, Config>)`
 * Still valid: `describeConfigs`, `incrementalAlterConfigs`, `ConfigResource`, `AlterConfigOp`, `TopicConfig`
 *
 * 흐름:
 * 1. `Admin`을 열고 `ch05-config`가 없으면 파티션 1, 복제 1로 만든다.
 * 2. `DescribeConfigsOptions`로 동의어를 포함해 토픽 설정을 읽는다. 기본값과 `retention.ms`의 출처를 남긴다.
 * 3. `cleanup.policy`를 토큰으로 나눠 `compact`가 있는지 본다. `delete,compact`도 압착이 켜진 것이다.
 * 4. `retention.ms`만 SET한다. 타임아웃은 `AlterConfigsOptions`에 둔다.
 * 5. 같은 키를 다시 읽어 `DYNAMIC_TOPIC_CONFIG`와 동의어를 확인한다.
 * 6. DELETE로 덮어쓰기를 지우고, 기본값으로 돌아왔는지 확인한다.
 */
object CurrentConfigManagement {
    private const val TOPIC_NAME = "ch05-config"
    private const val CLIENT_ID = "ch05-s04-current"
    private const val NUM_PARTITIONS = 1
    private const val REP_FACTOR: Short = 1
    private const val RETENTION_MS = "86400000"
    private const val TIMEOUT_MS = 10_000

    @JvmStatic
    fun main(args: Array<String>) {
        val configs = mapOf<String, Any>(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to KafkaSettings.bootstrapServers(),
            AdminClientConfig.CLIENT_ID_CONFIG to CLIENT_ID,
        )

        Admin.create(configs).use { admin ->
            // region 5.4 설정 관리
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
                admin.createTopics(listOf(NewTopic(TOPIC_NAME, NUM_PARTITIONS, REP_FACTOR)))
                    .values()
                    .getValue(TOPIC_NAME)
                    .get()
                logger.info { "created $TOPIC_NAME partitions=$NUM_PARTITIONS replication=$REP_FACTOR" }
            }

            val resource = ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME)
            val before = describe(admin, resource)
            logOverrides(before, "before")
            logRetention(before, "before")
            logCompaction(before)

            // SET은 이 키만 바꾼다. 다른 토픽 설정은 요청에 없어도 유지된다.
            val setOp = AlterConfigOp(
                ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, RETENTION_MS),
                AlterConfigOp.OpType.SET,
            )
            alter(admin, resource, setOp)
            val updated = describe(admin, resource)
            logRetention(updated, "after set")
            val retention = updated.get(TopicConfig.RETENTION_MS_CONFIG)
            if (retention.value() != RETENTION_MS) {
                logger.error { "retention.ms was not updated. actual=${retention.value()} expected=$RETENTION_MS" }
            }

            // DELETE의 value는 브로커가 무시한다. null이면 된다.
            val deleteOp = AlterConfigOp(
                ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, null),
                AlterConfigOp.OpType.DELETE,
            )
            alter(admin, resource, deleteOp)
            logRetention(describe(admin, resource), "after delete")
            // endregion
        }
    }

    // all()은 배치 전체가 성공해야 한다. 토픽이 하나여도 어느 리소스의 Future인지 values()로 고른다.
    private fun describe(admin: Admin, resource: ConfigResource): Config {
        val options = DescribeConfigsOptions()
            .includeSynonyms(true)
            .timeoutMs(TIMEOUT_MS)
        return admin.describeConfigs(listOf(resource), options)
            .values()
            .getValue(resource)
            .get()
    }

    private fun alter(admin: Admin, resource: ConfigResource, op: AlterConfigOp) {
        admin.incrementalAlterConfigs(
            mapOf(resource to listOf(op)),
            AlterConfigsOptions().timeoutMs(TIMEOUT_MS),
        ).values().getValue(resource).get()
    }

    private fun logOverrides(config: Config, phase: String) {
        val overrides = config.entries()
            .filter { !it.isDefault() }
            .map { "${it.name()}=${it.value()} source=${it.source()}" }
        logger.info { "$phase entries=${config.entries().size} overrides=$overrides" }
    }

    // 문자열 "compact"를 통째로 찾으면 다른 토큰에 섞인 경우와 구분되지 않는다.
    // 리스트 값은 쉼표로 나뉘므로 토큰이 compact인지 본다.
    private fun logCompaction(config: Config) {
        val entry = config.get(TopicConfig.CLEANUP_POLICY_CONFIG)
        val policies = entry.value().orEmpty().split(",").map { it.trim() }.filter { it.isNotEmpty() }
        val compacted = TopicConfig.CLEANUP_POLICY_COMPACT in policies
        if (compacted) {
            logger.info { "$TOPIC_NAME is compacted. cleanup.policy=${entry.value()} source=${entry.source()}" }
        } else {
            logger.info { "$TOPIC_NAME is not compacted. cleanup.policy=${entry.value()} source=${entry.source()}" }
        }
    }

    // synonyms()는 includeSynonyms(true)일 때만 채워진다. 맨 앞이 지금 적용된 값이다.
    private fun logRetention(config: Config, phase: String) {
        val entry = config.get(TopicConfig.RETENTION_MS_CONFIG)
        logger.info {
            "$phase retention.ms=${entry.value()} default=${entry.isDefault()} source=${entry.source()} type=${entry.type()}"
        }
        entry.synonyms().forEach { synonym ->
            logger.info { "$phase synonym ${synonym.name()}=${synonym.value()} source=${synonym.source()}" }
        }
    }
}
