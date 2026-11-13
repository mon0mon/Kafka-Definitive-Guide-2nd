package xyz.mon0mon.ch05.s08_testing

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.AlterConfigsOptions
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.MockAdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Book: §5.8 테스트하기
 * Track: current
 * Client: kafka-clients 4.4.0
 *
 * Evolution:
 * - 2.5  [BookTopicCreator]는 `incrementalAlterConfigs`가 비어 있어 Mockito로 호출만 센다.
 * - 4.4  `MockAdminClient.create()` 빌더가 브로커 수, 컨트롤러, 기본 파티션을 정한다.
 *        `incrementalAlterConfigs`는 SET과 DELETE를 메모리에 넣는다. spy가 필요 없다.
 *        클래스는 `kafka-clients`의 `test-fixtures`에 있다. `close(Duration)`으로 닫는다.
 *
 * Replaces: [BookTopicCreator]
 * Still valid: `createTopics`, `listTopics`, `describeConfigs`, `incrementalAlterConfigs`
 *
 * 흐름:
 * 1. `MockAdminClient.create().numBrokers(1).build()`로 브로커 하나를 만든다. bootstrap은 없다.
 * 2. 이름이 `test`로 시작할 때만 토픽을 만든다. `createTopics`가 끝난 뒤에 설정을 바꾼다.
 * 3. `cleanup.policy`만 SET한다. 대기는 리소스별 `values()`다.
 * 4. `listTopics`와 `describeConfigs`로 토픽과 압착 정책을 확인한다.
 */
class CurrentTopicCreator(
    private val admin: Admin,
) {
    fun maybeCreateTopic(topicName: String) {
        if (!topicName.lowercase().startsWith("test")) {
            return
        }
        // region 5.8 테스트하기
        val topic = NewTopic(topicName, 1, 1.toShort())
        // 목도 future를 바로 끝내지만, 실제 Admin에서는 생성이 끝나기 전에 alter하면 토픽이 없다.
        admin.createTopics(listOf(topic)).all().get()

        val resource = ConfigResource(ConfigResource.Type.TOPIC, topicName)
        val compaction = ConfigEntry(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_COMPACT,
        )
        val ops = listOf(AlterConfigOp(compaction, AlterConfigOp.OpType.SET))
        admin.incrementalAlterConfigs(mapOf(resource to ops), AlterConfigsOptions().timeoutMs(TIMEOUT_MS))
            .values()
            .getValue(resource)
            .get()
        // endregion
    }

    private companion object {
        const val TIMEOUT_MS = 10_000
    }
}

class CurrentAdminClientTest {
    @Test
    fun `test로 시작하면 토픽을 만들고 cleanup policy를 compact로 둔다`() {
        // 빌더 기본 컨트롤러는 첫 브로커다. 포트는 1000부터다. 실브로커 주소가 아니다.
        MockAdminClient.create().numBrokers(1).build().use { admin ->
            CurrentTopicCreator(admin).maybeCreateTopic("test.is.a.test.topic")

            assertTrue(admin.listTopics().names().get().contains("test.is.a.test.topic"))
            assertEquals(
                TopicConfig.CLEANUP_POLICY_COMPACT,
                cleanupPolicy(admin, "test.is.a.test.topic"),
            )
        }
    }

    @Test
    fun `test로 시작하지 않으면 토픽을 만들지 않는다`() {
        MockAdminClient.create().numBrokers(1).build().use { admin ->
            CurrentTopicCreator(admin).maybeCreateTopic("not.a.test")

            assertFalse(admin.listTopics().names().get().contains("not.a.test"))
        }
    }

    private fun cleanupPolicy(admin: Admin, topic: String): String {
        val resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
        val config = admin.describeConfigs(listOf(resource))
            .values()
            .getValue(resource)
            .get()
        return config.get(TopicConfig.CLEANUP_POLICY_CONFIG).value()
    }
}
